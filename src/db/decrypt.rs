//! Open a QQ NT database with SQLCipher — either a header-stripped copy
//! (`open_decrypted`, mirror path) or the LIVE source file in place
//! (`open_live`, through the `qqflow-offset` VFS that virtually strips the
//! 1024-byte custom header — see `crate::db::vfs`).
//!
//! PRAGMA order and parameters follow the QQBackup ecosystem's verified
//! sequence for NTQQ databases (page size BEFORE key, kdf_iter=4000,
//! HMAC-SHA1 with PBKDF2-HMAC-SHA512 KDF, AES-256-CBC); on verification
//! failure we retry with HMAC-SHA512 (QQFlow's fallback).
//!
//! Note: `PRAGMA cipher_plaintext_header_size` is deliberately NOT used —
//! it cannot handle QQ's layout (see `vfs.rs` docs for the two failure
//! modes). The offset VFS is the mechanism for live reads.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

use super::scan::CUSTOM_HEADER_LEN;
use super::vfs;

pub const QQ_PAGE_SIZE: i64 = 4096;
pub const QQ_KDF_ITER: i64 = 4000;

fn escape_key(key: &str) -> String {
    key.replace('\'', "''")
}

fn pragma_suite(key: &str, hmac: &str) -> String {
    format!(
        "PRAGMA cipher_page_size = {QQ_PAGE_SIZE};\n\
         PRAGMA key = '{}';\n\
         PRAGMA kdf_iter = {QQ_KDF_ITER};\n\
         PRAGMA cipher_hmac_algorithm = {hmac};\n\
         PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;\n\
         PRAGMA cipher = 'aes-256-cbc';\n",
        escape_key(key)
    )
}

/// Shared open core: PRAGMA suite with HMAC-SHA1 → HMAC-SHA512 retry and
/// sqlite_master verification.
fn open_checked(path: &Path, key: &str, flags: OpenFlags, vfs_name: Option<&str>) -> Result<Connection> {
    for hmac in ["HMAC_SHA1", "HMAC_SHA512"] {
        let conn = match vfs_name {
            Some(name) => Connection::open_with_flags_and_vfs(path, flags, name),
            None => Connection::open_with_flags(path, flags),
        }
        .with_context(|| format!("open {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_millis(3000))?;
        if let Err(e) = conn.execute_batch(&pragma_suite(key, hmac)) {
            tracing::debug!("pragma suite ({hmac}) failed: {e}");
            continue;
        }
        match verify(&conn) {
            Ok(true) => return Ok(conn),
            Ok(false) => {
                tracing::debug!("key verification failed (hmac={hmac})");
            }
            Err(e) => {
                tracing::debug!("sqlite_master query failed (hmac={hmac}): {e}");
            }
        }
    }
    anyhow::bail!(
        "数据库解密失败：请确认密钥正确（16 字节 ASCII，可先用 qq-win-db-key 重新提取）"
    )
}

/// Open `path` (a header-stripped SQLCipher database) with the given key.
/// Verifies by reading sqlite_master; retries with HMAC-SHA512 on failure.
pub fn open_decrypted(path: &Path, key: &str) -> Result<Connection> {
    // `Connection::open` creates missing files by default — a missing or
    // header-less mirror main.db must fail loudly instead of "verifying" as
    // a freshly created empty database.
    let meta = std::fs::metadata(path)
        .with_context(|| format!("镜像数据库不存在: {}", path.display()))?;
    if meta.len() == 0 {
        anyhow::bail!(
            "镜像数据库为空: {}（源库小于 1024 字节自定义头或镜像损坏）",
            path.display()
        );
    }
    open_checked(
        path,
        key,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        None,
    )
}

/// Open the LIVE source `nt_msg.db` (with its 1024-byte custom header)
/// read-only and in place, via the `qqflow-offset` VFS. Never copies and
/// never creates files; usable while the QQ client holds the database.
pub fn open_live(path: &Path, key: &str) -> Result<Connection> {
    vfs::ensure_installed()?;
    let meta = std::fs::metadata(path)
        .with_context(|| format!("源数据库不存在: {}", path.display()))?;
    if meta.len() <= CUSTOM_HEADER_LEN {
        anyhow::bail!(
            "源数据库过小: {} ({} 字节 <= 自定义头 {CUSTOM_HEADER_LEN} 字节)，不是有效的 nt_msg.db",
            path.display(),
            meta.len()
        );
    }
    open_checked(path, key, OpenFlags::SQLITE_OPEN_READ_ONLY, Some(vfs::VFS_NAME))
}

/// Verify the key by reading sqlite_master (only possible with a valid key).
/// Requires at least one table: an empty database would trivially "verify".
pub fn verify(conn: &Connection) -> Result<bool> {
    let n: i64 = conn.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_key_sql() {
        assert_eq!(escape_key("a'b"), "a''b");
        assert_eq!(escape_key("plain16bytestr"), "plain16bytestr");
    }

    #[test]
    fn empty_and_missing_database_rejected() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_empty_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.db");
        std::fs::write(&empty, b"").unwrap();
        let err = open_decrypted(&empty, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("为空"), "got: {err:#}");

        let missing = dir.join("missing.db");
        let err = open_decrypted(&missing, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("不存在"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_live_rejects_missing_and_tiny_source() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_live_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("nt_msg.db");
        let err = open_live(&missing, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("不存在"), "got: {err:#}");

        let tiny = dir.join("nt_msg.db");
        std::fs::write(&tiny, vec![0u8; 512]).unwrap();
        let err = open_live(&tiny, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("过小"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn garbage_file_reports_key_error() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_garbage_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let junk = dir.join("junk.db");
        std::fs::write(&junk, vec![0xAAu8; 8192]).unwrap();
        let err = open_decrypted(&junk, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("密钥"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }
}
