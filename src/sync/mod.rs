//! Sync engine: file-system-event-driven push + passive manual sync.
//!
//! New messages are detected by watching the source `nt_db` directory with
//! notify (see `watch`): ReadDirectoryChangesW / inotify / FSEvents events
//! are debounced and trigger a sync pass (incremental append + SSE
//! broadcast). The pass is a zero-file-IO indexed query against the
//! long-lived read-only connection to the LIVE source database (WeFlow
//! style — no mirror, no copies). A slow fallback poll guards against
//! silently dropped watch events and re-attaches a dead watcher.
//!
//! The same per-account `AccountSync` is shared with the manual sync
//! endpoint (`POST /api/v1/sync`): callers trigger `poll_once` on demand
//! and receive the newly appended records. QQ writes new messages into
//! `nt_msg.db-wal`; recall messages ("你猜猜撤回了什么") are detected by
//! the parser and emitted as `message.revoke`.

pub mod events;
pub mod watch;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};

use anyhow::Result;
use parking_lot::{Mutex, RwLock};
use tokio::sync::broadcast;

use crate::db::live::LiveReader;
use crate::parser::types::{ChatType, MessageRecord, MsgType};
use crate::store::index;
use crate::store::Store;

pub use events::Event;

/// One account's sync machinery: the live reader (mutex-guarded so the
/// manual sync endpoint and the poll loop can share it) plus everything
/// needed to run a sync pass.
pub struct AccountSync {
    pub reader: Arc<Mutex<LiveReader>>,
    pub store: Arc<RwLock<Store>>,
    pub tx: broadcast::Sender<Event>,
    /// Set when a sync failed; the poll loop then retries even though the
    /// reader state is unchanged.
    retry: AtomicBool,
}

impl AccountSync {
    pub fn new(
        reader: Arc<Mutex<LiveReader>>,
        store: Arc<RwLock<Store>>,
        tx: broadcast::Sender<Event>,
    ) -> Self {
        Self { reader, store, tx, retry: AtomicBool::new(false) }
    }

    /// Cheap change detection (no file IO): true right after a failed sync
    /// (retry flag) or while the live connection is closed (QQ not running
    /// yet — reopen on the next poll).
    pub fn changed(&self) -> bool {
        self.retry.swap(false, Ordering::SeqCst) || !self.reader.lock().is_open()
    }

    /// One sync pass: read rows above the watermark from the LIVE source,
    /// append them, broadcast SSE events, and return the appended records.
    /// Safe to call concurrently — the reader mutex and the store write
    /// lock serialize overlapping passes (the second one finds nothing new).
    pub fn poll_once(&self) -> Result<Vec<MessageRecord>> {
        let mut reader = self.reader.lock();
        let result = self.poll_locked(&mut reader);
        if let Err(e) = &result {
            classify(&mut reader, e);
            self.retry.store(true, Ordering::SeqCst);
        }
        result
    }

    fn poll_locked(&self, reader: &mut LiveReader) -> Result<Vec<MessageRecord>> {
        let conn = reader.acquire()?;

        // Read phase: parse rows above the watermark without touching the
        // store. If either table's read fails, nothing has been applied —
        // the retry re-reads the same rows instead of duplicating them.
        let (wm_g, wm_c) = {
            let guard = self.store.read();
            (guard.watermark_group, guard.watermark_c2c)
        };
        let (new_wm_g, new_g) = index::read_new(conn, ChatType::Group, wm_g)?;
        let (new_wm_c, new_c) = index::read_new(conn, ChatType::C2c, wm_c)?;

        // Apply phase: one write-lock critical section — append both tables,
        // advance both watermarks, and build the SSE events.
        let events: Vec<Event> = {
            let mut guard = self.store.write();
            index::apply_records(&mut guard, &new_g);
            index::apply_records(&mut guard, &new_c);
            guard.watermark_group = new_wm_g;
            guard.watermark_c2c = new_wm_c;
            new_g
                .iter()
                .chain(&new_c)
                .map(|r| {
                    let group_name = guard
                        .conversation(r.chat_type, &r.talker)
                        .map(|c| c.name.clone());
                    if r.parsed.msg_type == MsgType::Recall {
                        Event::message_revoke(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            Some(r.from_nick.clone()),
                            r.parsed.content.clone(),
                            r.ts,
                        )
                    } else {
                        Event::message_new(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            Some(r.from_nick.clone()),
                            r.parsed.content.clone(),
                            r.ts,
                        )
                    }
                })
                .collect()
        };
        for ev in events {
            let _ = self.tx.send(ev);
        }

        let mut all = new_g;
        all.extend(new_c);
        Ok(all)
    }
}

/// Classify a poll error: fatal SQLite codes (CORRUPT / NOTADB / IOERR —
/// e.g. the source db was recreated underneath us) drop the connection and
/// arm the reopen cooldown; transient errors (BUSY, CANTOPEN) are left to
/// the retry flag and the next event/fallback tick.
fn classify(reader: &mut LiveReader, err: &anyhow::Error) {
    let fatal = err.chain().any(|cause| {
        matches!(
            cause.downcast_ref::<rusqlite::Error>(),
            Some(rusqlite::Error::SqliteFailure(ffi_err, _))
                if matches!(
                    ffi_err.code,
                    rusqlite::ffi::ErrorCode::DatabaseCorrupt
                        | rusqlite::ffi::ErrorCode::NotADatabase
                        | rusqlite::ffi::ErrorCode::SystemIoFailure
                )
        )
    });
    if fatal {
        reader.mark_fatal(err);
    }
}

/// Registry of per-account sync engines, shared with the HTTP layer for
/// the manual-sync endpoint. Accounts are registered as their indexes
/// finish building.
pub struct SyncEngine {
    accounts: Mutex<Vec<Arc<AccountSync>>>,
}

impl SyncEngine {
    pub fn new() -> Self {
        Self { accounts: Mutex::new(Vec::new()) }
    }

    pub fn register(&self, account: Arc<AccountSync>) {
        self.accounts.lock().push(account);
    }

    pub fn snapshot(&self) -> Vec<Arc<AccountSync>> {
        self.accounts.lock().clone()
    }

    /// Manual sync: run a full pass on every registered account and return
    /// all newly appended records, newest first.
    pub fn sync_all(&self) -> Vec<MessageRecord> {
        let mut out = Vec::new();
        for account in self.snapshot() {
            match account.poll_once() {
                Ok(records) => out.extend(records),
                Err(e) => tracing::warn!("manual sync error: {e:#}"),
            }
        }
        out.sort_by_key(|r| std::cmp::Reverse((r.ts, r.rowid)));
        out
    }
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new()
    }
}

