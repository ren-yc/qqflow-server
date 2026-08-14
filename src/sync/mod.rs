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

use std::path::PathBuf;
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
    /// Source main db path — derives the `-wal` path the fallback stats.
    db_path: PathBuf,
    /// `nt_db` directory — sibling group-info databases live here (name maps).
    db_dir: PathBuf,
    /// SQLCipher key — sibling databases share it (name maps).
    key: String,
    /// Last observed (mtime-millis, size) of the source WAL; the fallback
    /// poll compares against it to detect changes the watcher missed.
    last_wal: Mutex<Option<(u64, u64)>>,
}

impl AccountSync {
    pub fn new(
        reader: Arc<Mutex<LiveReader>>,
        store: Arc<RwLock<Store>>,
        tx: broadcast::Sender<Event>,
        db_path: PathBuf,
        db_dir: PathBuf,
        key: String,
    ) -> Self {
        Self {
            reader,
            store,
            tx,
            retry: AtomicBool::new(false),
            db_path,
            db_dir,
            key,
            last_wal: Mutex::new(None),
        }
    }

    /// Re-read name maps (备注/群名) from nt_msg.db + sibling databases.
    /// Registration and manual sync only — never on watch ticks, keeping
    /// the poll pass zero-file-IO. Best-effort: a failure leaves the
    /// previous maps in place.
    pub fn refresh_names(&self) {
        let mut reader = self.reader.lock();
        let Ok(conn) = reader.acquire() else {
            tracing::debug!("[names] refresh skipped: live connection unavailable");
            return;
        };
        let known = {
            let guard = self.store.read();
            crate::store::names::KnownKeys::from_store(&guard)
        };
        let maps = crate::store::names::load_names(conn, &self.db_dir, &self.key, &known);
        let mut guard = self.store.write();
        guard.names = maps;
    }

    /// Cheap change detection (no data IO — at most one metadata stat):
    /// true right after a failed sync (retry flag), while the live
    /// connection is closed (QQ not running yet — reopen on the next
    /// poll), or when the source WAL changed since the last check
    /// (insurance against silently dropped watch events: QQ appends new
    /// rows into the WAL).
    pub fn changed(&self) -> bool {
        if self.retry.swap(false, Ordering::SeqCst) || !self.reader.lock().is_open() {
            return true;
        }
        let Some(snap) = self.wal_snapshot() else {
            return false; // no WAL yet — the watcher stays the only trigger
        };
        let mut last = self.last_wal.lock();
        if *last != Some(snap) {
            *last = Some(snap);
            return true;
        }
        false
    }

    /// (mtime-millis, size) of the source WAL, falling back to the main
    /// file when the WAL is absent (fully checkpointed, QQ closed).
    /// Metadata only — never reads page data.
    fn wal_snapshot(&self) -> Option<(u64, u64)> {
        let mut wal = self.db_path.as_os_str().to_owned();
        wal.push("-wal");
        [PathBuf::from(wal), self.db_path.clone()]
            .iter()
            .find_map(|p| {
                let m = std::fs::metadata(p).ok()?;
                let ms = m.modified().ok()?;
                let d = ms.duration_since(std::time::UNIX_EPOCH).ok()?;
                Some((d.as_millis() as u64, m.len()))
            })
    }

    /// One sync pass: read rows above the watermark from the LIVE source,
    /// append them, broadcast SSE events, and return the appended records.
    /// Safe to call concurrently — the reader mutex and the store write
    /// lock serialize overlapping passes (the second one finds nothing new).
    pub fn poll_once(&self) -> Result<Vec<MessageRecord>> {
        let mut reader = self.reader.lock();
        let result = self.poll_locked(&mut reader);
        if result.is_err() {
            // The read phase left the store untouched — retry the same rows
            // on the next trigger instead of duplicating them.
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
            // Media root may predate the account (sync can run before the
            // index build in edge paths); resolve it lazily once.
            if guard.media_root.is_none() {
                guard.media_root = self.db_dir.parent().map(|p| p.join("nt_data"));
            }
            index::apply_records(&mut guard, &new_g);
            index::apply_records(&mut guard, &new_c);
            guard.watermark_group = new_wm_g;
            guard.watermark_c2c = new_wm_c;
            new_g
                .iter()
                .chain(&new_c)
                .map(|r| {
                    // Display names resolve through the name maps — the
                    // remark (私聊) / group-info name (群聊) wins when known;
                    // in a group the sender's per-conversation card (40090)
                    // wins over the global name (never leaks across chats).
                    let group_name = Some(guard.display_name(r.chat_type, &r.talker));
                    let source_name = Some(guard.display_sender(r.chat_type, &r.talker, &r.from_uid));
                    let media = r.parsed.media.clone();
                    if r.parsed.msg_type == MsgType::Recall {
                        Event::message_revoke(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                            media,
                        )
                    } else {
                        Event::message_new(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                            media,
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
    /// all newly appended records, newest first. Name maps (备注/群名) ride
    /// the manual sync — the one place a client-visible refresh happens.
    pub fn sync_all(&self) -> Vec<MessageRecord> {
        let mut out = Vec::new();
        for account in self.snapshot() {
            match account.poll_once() {
                Ok(records) => {
                    out.extend(records);
                    account.refresh_names();
                }
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

