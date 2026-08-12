//! Change-driven poll loop + manual sync engine.
//!
//! A fast stat loop (default 200 ms) checks the source WAL/main files for
//! changes — two metadata calls, no IO — and only runs the full sync
//! (mirror refresh + decrypt + incremental append + SSE broadcast) when
//! something changed. Idle periods cost nothing beyond the stat.
//!
//! The same per-account `AccountSync` is shared with the manual sync
//! endpoint (`POST /api/v1/sync`): callers trigger `poll_once` on demand
//! and receive the newly appended records. QQ writes new messages into
//! `nt_msg.db-wal`; recall messages ("你猜猜撤回了什么") are detected by
//! the parser and emitted as `message.revoke`.

pub mod events;

use std::sync::Arc;
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::Duration;

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

        let mut guard = self.store.write();
        let wm_g = guard.watermark_group;
        let wm_c = guard.watermark_c2c;

        let (new_wm_g, new_g) = index::append_new(&conn, ChatType::Group, &mut guard, wm_g)?;
        let (new_wm_c, new_c) = index::append_new(&conn, ChatType::C2c, &mut guard, wm_c)?;

        for r in new_g.iter().chain(&new_c) {
            let group_name = guard
                .conversation(r.chat_type, &r.talker)
                .map(|c| c.name.clone());
            let ev = if r.parsed.msg_type == MsgType::Recall {
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
            };
            let _ = self.tx.send(ev);
        }

        guard.watermark_group = new_wm_g;
        guard.watermark_c2c = new_wm_c;

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

/// Spawn one change-driven poll task per account. Runs until `shutdown`
/// turns true: stat the source files at `interval`, run a full sync only
/// when `AccountSync::changed()` says so.
pub async fn spawn(
    account: Arc<AccountSync>,
    interval: Duration,
    shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || {
        loop {
            if *shutdown.borrow() {
                break;
            }
            if account.changed()
                && let Err(e) = account.poll_once()
            {
                tracing::warn!("poll error: {e:#}");
            }
            std::thread::sleep(interval);
        }
        Ok(())
    })
    .await
    .map_err(|e| anyhow::anyhow!("poll task panicked: {e}"))?
}
