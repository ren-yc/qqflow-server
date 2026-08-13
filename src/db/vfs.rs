//! Custom SQLite VFS that presents QQ's header-prefixed `nt_msg.db` to
//! SQLCipher as a standard SQLCipher 4 database — the no-copy live-read
//! mechanism (WeFlow-style: open the live source file directly, no mirror).
//!
//! QQ NT's `nt_msg.db` starts with a 1024-byte PLAINTEXT custom header
//! (magic `SQLite header 3\0`, page size 4096, `QQ_NT DB`, db UUID); the
//! real SQLCipher structure (KDF salt at offset 1024, encrypted with
//! SQLCipher 4's static header key) begins after it. The old design stripped
//! those bytes by copying the file into a mirror directory; this VFS strips
//! them virtually:
//!   - main db file  : every xRead/xWrite/xFetch offset += 1024, xFileSize -= 1024
//!   - -wal / -shm / -journal / anything else: pass-through, unshifted
//!     (the WAL has no custom header)
//!
//! The offset applies to ANY `*.db` main file opened through this VFS, not
//! just `nt_msg.db` — sibling NT databases in the same `nt_db` directory
//! (e.g. `group_info.db`, `nt_uid_mapping.db`) are created by the same
//! client stack and may carry the same 1024-byte header. A headerless file
//! opened shifted simply fails key verification (read-only open — no
//! corruption, no files created); callers retry without the offset.
//!
//! `PRAGMA cipher_plaintext_header_size` cannot do this: `lockBtree`
//! memcmps the DECODED page-1 buffer against "SQLite format 3\0" while the
//! codec copies the plaintext header bytes in verbatim (QQ's magic is
//! "SQLite header 3\0" → SQLITE_NOTADB), and the KDF salt is always read at
//! file offset 0 (`sqlite3_codec_ctx_init_kdf_salt`), ignoring the pragma.
//! Presenting `source[1024..]` through a VFS is byte-identical to the
//! header-stripped mirror file, so the proven decrypt pipeline transfers
//! unchanged.

use std::alloc::Layout;
use std::ffi::{c_int, c_void, CStr};
use std::ptr;
use std::sync::{Once, OnceLock};

use anyhow::Result;
use libsqlite3_sys as ffi;

use crate::db::scan::CUSTOM_HEADER_LEN;

/// Name of the registered VFS (passed to `Connection::open_with_flags_and_vfs`).
pub const VFS_NAME: &str = "qqflow-offset";

/// Wrapper stored in SQLite's szOsFile buffer for shifted main-db files.
/// `sqlite3_file` must be the first member so `*mut sqlite3_file` casts work.
#[repr(C)]
struct OffsetFile {
    base: ffi::sqlite3_file,
    /// Separately allocated parent osFile (a full `szOsFile` allocation).
    parent: *mut ffi::sqlite3_file,
    /// Total allocation size of `parent`, for dealloc in xClose.
    size: usize,
}

static INSTALLED: Once = Once::new();

/// The default VFS we cloned (our own struct's xOpen is replaced with
/// `x_open`, so callbacks recover the parent's pointers from here). Written
/// once inside `ensure_installed` BEFORE the VFS is registered; every x_open
/// on any thread reads a fully-initialized pointer, so sharing it across
/// threads is sound. (Raw pointers are not `Sync` — hence the newtype.)
static PARENT_VFS: OnceLock<SyncVfsPtr> = OnceLock::new();

/// Marker that the parent-VFS pointer is immutable after installation.
struct SyncVfsPtr(*mut ffi::sqlite3_vfs);
unsafe impl Sync for SyncVfsPtr {}
unsafe impl Send for SyncVfsPtr {}

static REGISTER_RC: std::sync::atomic::AtomicI32 =
    std::sync::atomic::AtomicI32::new(ffi::SQLITE_OK);

/// Register the `qqflow-offset` VFS once per process (idempotent).
pub fn ensure_installed() -> Result<()> {
    unsafe {
        if !ffi::sqlite3_vfs_find(c"qqflow-offset".as_ptr()).is_null() {
            return Ok(());
        }
        INSTALLED.call_once(|| {
            // The closure body runs inside this unsafe context.
            let parent = ffi::sqlite3_vfs_find(ptr::null()); // default ("win32")
            if parent.is_null() {
                REGISTER_RC.store(ffi::SQLITE_ERROR, std::sync::atomic::Ordering::SeqCst);
                return;
            }
            let _ = PARENT_VFS.set(SyncVfsPtr(parent));
            let mut vfs = *parent; // Copy: clone the default VFS struct
            vfs.zName = c"qqflow-offset".as_ptr();
            vfs.pNext = ptr::null_mut();
            vfs.xOpen = Some(x_open); // every other method delegates via the clone
            let raw = Box::into_raw(Box::new(vfs));
            REGISTER_RC.store(
                ffi::sqlite3_vfs_register(raw, 0), // makeDefault = 0
                std::sync::atomic::Ordering::SeqCst,
            );
            // Deliberately leak the registration struct: SQLite's global VFS
            // list references it for the process lifetime and open files
            // point into it (the old `static mut VFS_STORAGE` box kept it
            // alive the same way).
            std::mem::forget(Box::from_raw(raw));
        });
        if REGISTER_RC.load(std::sync::atomic::Ordering::SeqCst) != ffi::SQLITE_OK {
            anyhow::bail!("sqlite3_vfs_register({VFS_NAME}) failed");
        }
        Ok(())
    }
}

/// Only the main db file gets the offset. The pager always sets
/// SQLITE_OPEN_MAIN_DB for it (the WAL/shm get their own flags); the
/// filename check is a second guard (case-insensitive `*.db` suffix — the
/// source `nt_msg.db` and any sibling NT database that may carry the same
/// 1024-byte header — excluding `-wal`/`-shm`/`-journal`).
fn is_offset_main(z_name: ffi::sqlite3_filename, flags: c_int) -> bool {
    if flags & ffi::SQLITE_OPEN_MAIN_DB == 0 {
        return false;
    }
    let name = unsafe { CStr::from_ptr(z_name) }.to_bytes();
    let mut lower = Vec::with_capacity(name.len());
    lower.extend(name.iter().map(u8::to_ascii_lowercase));
    if lower.ends_with(b"-wal") || lower.ends_with(b"-shm") || lower.ends_with(b"-journal") {
        return false;
    }
    lower.ends_with(b".db")
}

unsafe extern "C" fn x_open(
    vfs: *mut ffi::sqlite3_vfs,
    z_name: ffi::sqlite3_filename,
    p_file: *mut ffi::sqlite3_file,
    flags: c_int,
    p_out_flags: *mut c_int,
) -> c_int {
    unsafe {
        // The parent's xOpen — never `(*vfs).xOpen`, which is OUR own
        // (replaced at clone time); that would recurse forever.
        let Some(parent_vfs) = PARENT_VFS.get() else {
            return ffi::SQLITE_ERROR; // never installed
        };
        let parent_vfs = parent_vfs.0;
        let parent_open = match (*parent_vfs).xOpen {
            Some(f) => f,
            None => return ffi::SQLITE_ERROR,
        };
        // In-memory databases and non-main files pass through unshifted.
        if z_name.is_null() || !is_offset_main(z_name, flags) {
            return parent_open(parent_vfs, z_name, p_file, flags, p_out_flags);
        }
        // The parent osFile needs its own full szOsFile allocation (SQLite's
        // pFile buffer is only szOsFile bytes and must hold our header too).
        let total = ((*vfs).szOsFile as usize + 7) & !7usize; // 8-aligned
        let layout = match Layout::from_size_align(total, 8) {
            Ok(l) => l,
            Err(_) => return ffi::SQLITE_NOMEM,
        };
        let raw = std::alloc::alloc(layout);
        if raw.is_null() {
            return ffi::SQLITE_NOMEM;
        }
        let parent_file = raw as *mut ffi::sqlite3_file;
        let rc = parent_open(vfs, z_name, parent_file, flags, p_out_flags);
        if rc != ffi::SQLITE_OK {
            std::alloc::dealloc(raw, layout);
            return rc;
        }
        let header = p_file as *mut OffsetFile;
        (*header).base.pMethods = &OFFSET_IO_METHODS;
        (*header).parent = parent_file;
        (*header).size = total;
        ffi::SQLITE_OK
    }
}

/// (wrapped header, parent file) from the file pointer SQLite hands us.
unsafe fn split(f: *mut ffi::sqlite3_file) -> (*mut OffsetFile, *mut ffi::sqlite3_file) {
    unsafe {
        let header = f as *mut OffsetFile;
        (header, (*header).parent)
    }
}

/// Delegate an io method to the parent file's method table. `$fn` is the
/// Rust wrapper name, `$field` the parent table's field (camelCase).
macro_rules! delegate {
    ($fn:ident, $field:ident; $($arg:ident: $ty:ty),*) => {
        unsafe extern "C" fn $fn(f: *mut ffi::sqlite3_file, $($arg: $ty),*) -> c_int {
            unsafe {
                let (_h, parent) = split(f);
                match (*parent).pMethods.as_ref().and_then(|m| m.$field) {
                    Some(op) => op(parent, $($arg),*),
                    None => ffi::SQLITE_ERROR,
                }
            }
        }
    };
}

delegate!(x_truncate, xTruncate; size: i64);
delegate!(x_sync, xSync; flags: c_int);
delegate!(x_lock, xLock; locktype: c_int);
delegate!(x_unlock, xUnlock; locktype: c_int);
delegate!(x_check_reserved_lock, xCheckReservedLock; p_res_out: *mut c_int);
delegate!(x_file_control, xFileControl; op: c_int, p_arg: *mut c_void);
delegate!(x_sector_size, xSectorSize;);
delegate!(x_device_characteristics, xDeviceCharacteristics;);
delegate!(x_shm_map, xShmMap; ipg: c_int, pgsz: c_int, want_read: c_int, pp: *mut *mut c_void);
delegate!(x_shm_lock, xShmLock; offset: c_int, n: c_int, flags: c_int);
delegate!(x_shm_unmap, xShmUnmap; delete_flag: c_int);
delegate!(x_unfetch, xUnfetch; ofst: i64, p: *mut c_void);

unsafe extern "C" fn x_close(f: *mut ffi::sqlite3_file) -> c_int {
    unsafe {
        let (header, parent) = split(f);
        let rc = match (*parent).pMethods.as_ref().and_then(|m| m.xClose) {
            Some(op) => op(parent),
            None => ffi::SQLITE_ERROR,
        };
        // Same layout as in x_open (validated there; 8 is a power of two and
        // `size` is 8-aligned, so this construction cannot fail).
        let layout = Layout::from_size_align_unchecked((*header).size, 8);
        std::alloc::dealloc(parent as *mut u8, layout);
        rc
    }
}

unsafe extern "C" fn x_read(
    f: *mut ffi::sqlite3_file,
    buf: *mut c_void,
    amt: c_int,
    ofst: i64,
) -> c_int {
    unsafe {
        let (_h, parent) = split(f);
        match (*parent).pMethods.as_ref().and_then(|m| m.xRead) {
            Some(op) => op(parent, buf, amt, ofst + CUSTOM_HEADER_LEN as i64),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_write(
    f: *mut ffi::sqlite3_file,
    buf: *const c_void,
    amt: c_int,
    ofst: i64,
) -> c_int {
    unsafe {
        let (_h, parent) = split(f);
        match (*parent).pMethods.as_ref().and_then(|m| m.xWrite) {
            Some(op) => op(parent, buf, amt, ofst + CUSTOM_HEADER_LEN as i64),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_file_size(f: *mut ffi::sqlite3_file, p_size: *mut i64) -> c_int {
    unsafe {
        let (_h, parent) = split(f);
        let fsize = match (*parent).pMethods.as_ref().and_then(|m| m.xFileSize) {
            Some(op) => op,
            None => return ffi::SQLITE_ERROR,
        };
        let rc = fsize(parent, p_size);
        if rc == ffi::SQLITE_OK && !p_size.is_null() {
            // Present the logical (header-stripped) size.
            *p_size = (*p_size - CUSTOM_HEADER_LEN as i64).max(0);
        }
        rc
    }
}

unsafe extern "C" fn x_fetch(
    f: *mut ffi::sqlite3_file,
    ofst: i64,
    amt: c_int,
    pp: *mut *mut c_void,
) -> c_int {
    unsafe {
        let (_h, parent) = split(f);
        match (*parent).pMethods.as_ref().and_then(|m| m.xFetch) {
            Some(op) => op(parent, ofst + CUSTOM_HEADER_LEN as i64, amt, pp),
            None => ffi::SQLITE_ERROR,
        }
    }
}

unsafe extern "C" fn x_shm_barrier(f: *mut ffi::sqlite3_file) {
    unsafe {
        let (_h, parent) = split(f);
        if let Some(op) = (*parent).pMethods.as_ref().and_then(|m| m.xShmBarrier) {
            op(parent);
        }
    }
}

static OFFSET_IO_METHODS: ffi::sqlite3_io_methods = ffi::sqlite3_io_methods {
    iVersion: 3,
    xClose: Some(x_close),
    xRead: Some(x_read),
    xWrite: Some(x_write),
    xTruncate: Some(x_truncate),
    xSync: Some(x_sync),
    xFileSize: Some(x_file_size),
    xLock: Some(x_lock),
    xUnlock: Some(x_unlock),
    xCheckReservedLock: Some(x_check_reserved_lock),
    xFileControl: Some(x_file_control),
    xSectorSize: Some(x_sector_size),
    xDeviceCharacteristics: Some(x_device_characteristics),
    xShmMap: Some(x_shm_map),
    xShmLock: Some(x_shm_lock),
    xShmBarrier: Some(x_shm_barrier),
    xShmUnmap: Some(x_shm_unmap),
    xFetch: Some(x_fetch),
    xUnfetch: Some(x_unfetch),
};

#[cfg(test)]
mod tests {
    use super::*;
    use std::ffi::CString;

    #[test]
    fn main_db_predicate() {
        // Fictional paths — only the filename suffix matters.
        let main = CString::new(r"C:\SomeUser\nt_qq\nt_db\nt_msg.db").unwrap();
        let wal = CString::new(r"C:\SomeUser\nt_qq\nt_db\nt_msg.db-wal").unwrap();
        let shm = CString::new(r"C:\SomeUser\nt_qq\nt_db\nt_msg.db-shm").unwrap();
        let journal = CString::new(r"C:\SomeUser\nt_qq\nt_db\nt_msg.db-journal").unwrap();

        assert!(is_offset_main(main.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        assert!(!is_offset_main(wal.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        assert!(!is_offset_main(shm.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        assert!(!is_offset_main(journal.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        // Without the MAIN_DB flag nothing is shifted.
        assert!(!is_offset_main(main.as_ptr(), ffi::SQLITE_OPEN_READONLY));
        // Any *.db main file gets the offset — siblings like group_info.db
        // may carry the same header; non-db suffixes never do.
        let other = CString::new(r"C:\x\y.db").unwrap();
        assert!(is_offset_main(other.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        let group_info = CString::new(r"C:\SomeUser\nt_qq\nt_db\group_info.db").unwrap();
        assert!(is_offset_main(group_info.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        let notdb = CString::new(r"C:\x\y.db-shm2").unwrap();
        assert!(!is_offset_main(notdb.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        let txt = CString::new(r"C:\x\notes.txt").unwrap();
        assert!(!is_offset_main(txt.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
        // Case-insensitive.
        let upper = CString::new(r"C:\Q\NT_MSG.DB").unwrap();
        assert!(is_offset_main(upper.as_ptr(), ffi::SQLITE_OPEN_MAIN_DB));
    }
}
