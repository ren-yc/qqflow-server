//! Long-lived read-only live connection to the source `nt_msg.db`
//! (WeFlow-style). No copies, no mirror dir — the offset VFS
//! (`crate::db::vfs`) does the 1024-byte header translation.
//!
//! The connection stays open across polls (warm page cache, no per-poll
//! key derivation); SQLite handles WAL checkpoints transparently. When
//! closed (QQ not running yet) the next `acquire()` reopens it; a failing
//! read simply retries on the next trigger — no drop/reopen machinery
//! (WeFlow's forceReopen pattern deliberately dropped: detecting a replaced
//! source db is out of scope, a restart recovers).

use std::path::PathBuf;

use anyhow::Result;
use rusqlite::Connection;

use super::decrypt::open_live;

pub struct LiveReader {
    path: PathBuf,
    key: String,
    conn: Option<Connection>,
}

impl LiveReader {
    pub fn new(path: PathBuf, key: String) -> Self {
        Self { path, key, conn: None }
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

    /// The live connection, reopening when closed (QQ closed at startup —
    /// the 30 s fallback poll drives the retry cadence). No cooldown: a
    /// failing handle is simply retried on the next trigger.
    pub fn acquire(&mut self) -> Result<&Connection> {
        if self.conn.is_none() {
            self.open()?;
        }
        Ok(self.conn.as_ref().expect("just opened"))
    }
}
