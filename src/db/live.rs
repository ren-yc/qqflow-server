//! Long-lived read-only live connection to the source `nt_msg.db`
//! (WeFlow-style). No copies, no mirror dir — the offset VFS
//! (`crate::db::vfs`) does the 1024-byte header translation.
//!
//! The connection stays open across polls (warm page cache, no per-poll
//! key derivation); SQLite handles WAL checkpoints transparently. On fatal
//! SQLite errors (CORRUPT/NOTADB/IOERR — e.g. the source db was recreated
//! under us) the handle is dropped and a reopen is armed after a cooldown,
//! mirroring WeFlow's `forceReopen` pattern.

use std::path::PathBuf;
use std::time::{Duration, Instant};

use anyhow::Result;
use rusqlite::Connection;

use super::decrypt::open_live;

/// Cooldown before reopening after a fatal error (WeFlow: 15 s).
const REOPEN_COOLDOWN: Duration = Duration::from_secs(15);

pub struct LiveReader {
    path: PathBuf,
    key: String,
    conn: Option<Connection>,
    reopen_after: Option<Instant>,
}

impl LiveReader {
    pub fn new(path: PathBuf, key: String) -> Self {
        Self {
            path,
            key,
            conn: None,
            reopen_after: None,
        }
    }

    /// Open the live database and verify the key now — a wrong key or a
    /// missing source fails loudly at registration time (unchanged UX).
    pub fn open(&mut self) -> Result<()> {
        let conn = open_live(&self.path, &self.key)?;
        self.conn = Some(conn);
        Ok(())
    }

    pub fn is_open(&self) -> bool {
        self.conn.is_some()
    }

    /// The live connection, reopening when closed. Cooldown-aware: after a
    /// fatal error we refuse to reopen for `REOPEN_COOLDOWN` so a broken
    /// database is not hammered every poll. *Open* failures (QQ closed,
    /// file missing) do NOT arm the cooldown — the 30 s fallback poll is
    /// the natural retry cadence for those.
    pub fn acquire(&mut self) -> Result<&Connection> {
        if self.conn.is_none() {
            if let Some(deadline) = self.reopen_after
                && Instant::now() < deadline
            {
                anyhow::bail!("QQ 数据库暂不可用（重试冷却中）");
            }
            self.open()?;
            self.reopen_after = None;
        }
        Ok(self.conn.as_ref().expect("just opened"))
    }

    /// Drop the connection and arm the reopen cooldown after a fatal
    /// SQLite error (SQLITE_CORRUPT / SQLITE_NOTADB / SQLITE_IOERR): the
    /// handle is worthless; the next acquire() waits out the cooldown then
    /// reopens fresh.
    pub fn mark_fatal(&mut self, err: &anyhow::Error) {
        tracing::warn!("fatal database error, reopening after cooldown: {err:#}");
        self.conn = None;
        self.reopen_after = Some(Instant::now() + REOPEN_COOLDOWN);
    }

    /// Drop the connection and reopen immediately on the next acquire —
    /// for when the source db was recreated under the watcher (QQ
    /// reinstall), skipping the cooldown a stale-handle read would incur.
    pub fn force_reopen(&mut self) {
        self.conn = None;
        self.reopen_after = None;
    }
}
