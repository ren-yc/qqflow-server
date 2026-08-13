//! Sync engine: file-system-event-driven push + passive manual sync.
//!
//! New messages are detected by watching the source `nt_db` directory with
//! notify (see `watch`): ReadDirectoryChangesW / inotify / FSEvents events
//! are debounced and trigger a full sync (mirror refresh + decrypt +
//! incremental append + SSE broadcast). A slow fallback poll guards against
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

use crate::db::decrypt;
use crate::db::mirror::Mirror;
use crate::parser::types::{ChatType, MessageRecord, MsgType};
use crate::store::index;
use crate::store::Store;

pub use events::Event;

/// One account's sync machinery: the mirror (mutex-guarded so the manual
/// sync endpoint and the poll loop can share it) plus everything needed to
/// run a full sync pass.
pub struct AccountSync {
    pub mirror: Arc<Mutex<Mirror>>,
    pub key: String,
    pub store: Arc<RwLock<Store>>,
    pub tx: broadcast::Sender<Event>,
    /// Set when a sync failed; the poll loop then retries even though the
    /// file stats are unchanged.
    retry: AtomicBool,
}

impl AccountSync {
    pub fn new(
        mirror: Arc<Mutex<Mirror>>,
        key: String,
        store: Arc<RwLock<Store>>,
        tx: broadcast::Sender<Event>,
    ) -> Self {
        Self { mirror, key, store, tx, retry: AtomicBool::new(false) }
    }

    /// Cheap change detection (two stats). Always true right after a
    /// failed sync (retry flag).
    pub fn changed(&self) -> bool {
        self.retry.swap(false, Ordering::SeqCst) || self.mirror.lock().changed()
    }

    /// One full sync pass: refresh the mirror, append rows above the
    /// watermark, broadcast SSE events, and return the appended records.
    /// Safe to call concurrently — the mirror mutex and the store write
    /// lock serialize overlapping passes (the second one finds nothing new).
    pub fn poll_once(&self) -> Result<Vec<MessageRecord>> {
        let mut mirror = self.mirror.lock();
        match self.poll_locked(&mut mirror) {
            Ok(records) => Ok(records),
            Err(e) => {
                self.retry.store(true, Ordering::SeqCst);
                Err(e)
            }
        }
    }

    fn poll_locked(&self, mirror: &mut Mirror) -> Result<Vec<MessageRecord>> {
        mirror.sync()?;
        let conn = decrypt::open_decrypted(&mirror.main_path, &self.key)?;

        // Read phase: parse rows above the watermark without touching the
        // store. If either table's read fails, nothing has been applied —
        // the retry re-reads the same rows instead of duplicating them.
        let (wm_g, wm_c) = {
            let guard = self.store.read();
            (guard.watermark_group, guard.watermark_c2c)
        };
        let (new_wm_g, new_g) = index::read_new(&conn, ChatType::Group, wm_g)?;
        let (new_wm_c, new_c) = index::read_new(&conn, ChatType::C2c, wm_c)?;

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

