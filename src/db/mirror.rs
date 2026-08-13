//! Mirror directory: a per-account temp workspace holding
//!   - `main.db`      : nt_msg.db with the 1024-byte custom header stripped
//!   - `main.db-wal`  : nt_msg.db-wal copied verbatim (WAL has no custom header)
//!
//! Both files remain SQLCipher-encrypted; SQLCipher decrypts in memory.
//! This mirrors QQFlow's proven copy-to-cache approach while keeping the
//! WAL in sync so that real-time polling sees messages written by a running
//! QQ client (which live in the WAL, not the main file).
//!
//! Checkpoint detection: when SQLite checkpoints, it writes merged pages back
//! into the main file (size/mtime change) and truncates/resets the WAL. We
//! stat the source main file each poll and rebuild the mirror on change.

use std::io::{Read, Seek, SeekFrom, Write};
use std::path::{Path, PathBuf};
use std::time::SystemTime;

use anyhow::{Context, Result};

use super::scan::{DbInfo, CUSTOM_HEADER_LEN};

pub struct Mirror {
    pub src_main: PathBuf,
    pub src_wal: PathBuf,
    pub mirror_dir: PathBuf,
    pub main_path: PathBuf,
    pub wal_path: PathBuf,
    src_len: u64,
    src_mtime: SystemTime,
    /// Source WAL (len, mtime) as of the last copy; drives the cheap
    /// `changed()` check of the fast poll loop.
    wal_stat: Option<(u64, SystemTime)>,
}

/// Stat a file as (len, mtime), None when absent.
fn stat_of(path: &Path) -> Option<(u64, SystemTime)> {
    let meta = std::fs::metadata(path).ok()?;
    Some((meta.len(), meta.modified().unwrap_or(SystemTime::UNIX_EPOCH)))
}

impl Mirror {
    /// Create (or refresh) the mirror for `info` under `mirror_root/<qq>/`.
    pub fn new(info: &DbInfo, mirror_root: &Path) -> Result<Self> {
        let dir = mirror_root.join(&info.qq);
        std::fs::create_dir_all(&dir).with_context(|| format!("create {}", dir.display()))?;
        let mut m = Self {
            src_main: info.path.clone(),
            src_wal: info.path.with_extension("db-wal"),
            mirror_dir: dir.clone(),
            main_path: dir.join("main.db"),
            wal_path: dir.join("main.db-wal"),
            src_len: 0,
            src_mtime: SystemTime::UNIX_EPOCH,
            wal_stat: None,
        };
        m.rebuild()?;
        Ok(m)
    }

    /// Copy source main (header stripped) + WAL (verbatim) into the mirror.
    pub fn rebuild(&mut self) -> Result<()> {
        let meta = std::fs::metadata(&self.src_main)
            .with_context(|| format!("stat {}", self.src_main.display()))?;
        if meta.len() <= CUSTOM_HEADER_LEN {
            anyhow::bail!(
                "源数据库过小: {} ({} 字节 <= 自定义头 {CUSTOM_HEADER_LEN} 字节)，不是有效的 nt_msg.db",
                self.src_main.display(),
                meta.len()
            );
        }
        self.src_len = meta.len();
        self.src_mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);

        // Strip the 1024-byte custom header by streaming from offset 1024.
        let mut src = std::fs::File::open(&self.src_main)
            .with_context(|| format!("open {}", self.src_main.display()))?;
        src.seek(SeekFrom::Start(CUSTOM_HEADER_LEN))
            .with_context(|| format!("seek {}", self.src_main.display()))?;
        let mut dst = std::fs::File::create(&self.main_path)
            .with_context(|| format!("create {}", self.main_path.display()))?;
        std::io::copy(&mut src, &mut dst)
            .with_context(|| format!("copy {} -> {}", self.src_main.display(), self.main_path.display()))?;
        dst.flush().ok();

        self.copy_wal_and_track()?;
        // Drop any stale shared-memory index from a previous incarnation:
        // the WAL was replaced, so a leftover -shm index is meaningless and
        // can confuse SQLCipher's salt checks on the next open.
        let _ = std::fs::remove_file(self.wal_path.with_extension("db-shm"));
        tracing::debug!("mirror rebuilt for {}", self.src_main.display());
        Ok(())
    }

    /// Copy the WAL file verbatim. No-op when the source WAL is absent.
    fn copy_wal_verbatim(&self) -> Result<()> {
        match std::fs::copy(&self.src_wal, &self.wal_path) {
            Ok(_) => {}
            Err(e) if e.kind() == std::io::ErrorKind::NotFound => {
                let _ = std::fs::remove_file(&self.wal_path);
            }
            Err(e) => {
                return Err(e)
                    .with_context(|| format!("copy WAL {} -> {}", self.src_wal.display(), self.wal_path.display()));
            }
        }
        Ok(())
    }

    /// Copy the source WAL and refresh its tracked stat. If the source
    /// changed WHILE the copy ran (a frame appended mid-copy), the tracked
    /// stat is kept at the pre-copy value so the next `changed()` check
    /// re-triggers and picks the frame up — never silently drop it.
    fn copy_wal_and_track(&mut self) -> Result<()> {
        let pre = stat_of(&self.src_wal);
        self.copy_wal_verbatim()?;
        let post = stat_of(&self.src_wal);
        self.wal_stat = if pre == post { post } else { pre };
        Ok(())
    }

    /// Cheap change detection for the fast poll loop: stat the source main
    /// file (checkpoint/rebuild) and the source WAL (new frames) against
    /// the values captured at the last sync. Two metadata calls, no IO.
    pub fn changed(&self) -> bool {
        let main_unchanged = match std::fs::metadata(&self.src_main) {
            Ok(m) => {
                m.len() == self.src_len
                    && m.modified().unwrap_or(SystemTime::UNIX_EPOCH) == self.src_mtime
            }
            Err(_) => false, // main file missing/unreadable: force a sync attempt
        };
        if !main_unchanged {
            return true;
        }
        stat_of(&self.src_wal) != self.wal_stat
    }

    /// Poll-time sync: re-copy the WAL (cheap, ≤ ~4 MB). If the source main
    /// file changed (checkpoint or rebuild), refresh the whole mirror.
    /// Returns true when a rebuild happened.
    ///
    /// Order matters: the WAL is copied BEFORE stat'ing the main file. If a
    /// checkpoint lands between the two operations, the stat detects the
    /// main-file change and triggers a full rebuild; copying WAL-first means
    /// we never combine a stale main with a reset WAL (whose salt would not
    /// match, silently dropping tail frames until the next poll).
    pub fn sync(&mut self) -> Result<bool> {
        self.copy_wal_and_track()?;
        let meta = std::fs::metadata(&self.src_main)
            .with_context(|| format!("stat {}", self.src_main.display()))?;
        let mtime = meta.modified().unwrap_or(SystemTime::UNIX_EPOCH);
        if meta.len() != self.src_len || mtime != self.src_mtime {
            tracing::debug!("source main changed (checkpoint detected), rebuilding mirror");
            self.rebuild()?;
            return Ok(true);
        }
        Ok(false)
    }

}

/// Helper used by tests and index builder: read `path` skipping `skip` bytes.
pub fn read_skipping(path: &Path, skip: u64) -> Result<Vec<u8>> {
    let mut f = std::fs::File::open(path)?;
    f.seek(SeekFrom::Start(skip))?;
    let mut buf = Vec::new();
    f.read_to_end(&mut buf)?;
    Ok(buf)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn header_strip_math() {
        // The mirror copy must skip exactly CUSTOM_HEADER_LEN bytes.
        let src = std::env::temp_dir().join(format!("qqflow_mirror_test_{}", std::process::id()));
        std::fs::create_dir_all(&src).unwrap();
        let raw = src.join("raw.bin");
        let mut bytes = vec![0u8; CUSTOM_HEADER_LEN as usize + 100];
        bytes[CUSTOM_HEADER_LEN as usize] = 0xAA;
        std::fs::write(&raw, &bytes).unwrap();
        let stripped = read_skipping(&raw, CUSTOM_HEADER_LEN).unwrap();
        assert_eq!(stripped.len(), 100);
        assert_eq!(stripped[0], 0xAA);
        let _ = std::fs::remove_dir_all(&src);
    }
}
