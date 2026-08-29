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
use crate::parser::types::{ChatType, MsgType};
use crate::store::index;
use crate::store::query::MessageOut;
use crate::store::Store;

pub use events::Event;

/// One account's sync machinery: the live reader (mutex-guarded so the
/// manual sync endpoint and the poll loop can share it) plus everything
/// needed to run a sync pass.
pub struct AccountSync {
    /// Owning account — the key `SyncEngine` unregisters by.
    pub qq: String,
    pub reader: Arc<Mutex<LiveReader>>,
    pub store: Arc<RwLock<Store>>,
    pub tx: broadcast::Sender<Event>,
    /// Set when a sync failed; the poll loop then retries even though the
    /// reader state is unchanged.
    retry: AtomicBool,
    /// Set by deregistration. A pass already inside `poll_once` cannot be
    /// cancelled — `spawn_blocking` work runs to completion even after its
    /// awaiting task is aborted — so the flag is what stops it from writing
    /// into a store that no longer belongs to this account.
    stopped: AtomicBool,
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
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        qq: String,
        reader: Arc<Mutex<LiveReader>>,
        store: Arc<RwLock<Store>>,
        tx: broadcast::Sender<Event>,
        db_path: PathBuf,
        db_dir: PathBuf,
        key: String,
    ) -> Self {
        Self {
            qq,
            reader,
            store,
            tx,
            retry: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            db_path,
            db_dir,
            key,
            last_wal: Mutex::new(None),
        }
    }

    /// Retire this account's sync: no further pass may write to the store.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
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

    /// Rebuild the media cache-index fallback snapshot from nt_data, then
    /// re-apply media registration for keys the previous (stale) snapshot
    /// missed — rows applied between the snapshot and now get their second
    /// chance here. Manual sync only (real file IO — the caller runs it on
    /// the blocking pool); watch ticks never call this and only consult
    /// the snapshot through pure map lookups.
    pub fn refresh_media_fallback(&self) {
        let root = {
            let guard = self.store.read();
            guard
                .media_root
                .clone()
                .or_else(|| crate::store::media::media_root_of(&self.db_dir))
        };
        let Some(root) = root else {
            return;
        };
        let index = crate::store::media::scan_cache_index(&root);
        let mut guard = self.store.write();
        guard.media_fallback = index;
        crate::store::index::reapply_media_registration(&mut guard);
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
    /// append them, broadcast SSE events, and return the appended messages
    /// (already shaped as `MessageOut`, with the mediaId fetchability
    /// filter applied). Safe to call concurrently — the reader mutex and
    /// the store write lock serialize overlapping passes (the second one
    /// finds nothing new).
    pub fn poll_once(&self) -> Result<Vec<MessageOut>> {
        let mut reader = self.reader.lock();
        let result = self.poll_locked(&mut reader);
        if result.is_err() {
            // The read phase left the store untouched — retry the same rows
            // on the next trigger instead of duplicating them.
            self.retry.store(true, Ordering::SeqCst);
        }
        result
    }

    fn poll_locked(&self, reader: &mut LiveReader) -> Result<Vec<MessageOut>> {
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

        // Apply phase: one write-lock critical section — apply both tables
        // FIRST so this batch's own media registrations are visible to the
        // SSE mediaId filter below (a row that rescues its media through the
        // cache-index fallback must advertise the same fetchable id in its
        // own event), then build the SSE events and response rows (the
        // records are borrowed — see apply_records), advance watermarks.
        let (events, outs): (Vec<Event>, Vec<MessageOut>) = {
            let mut guard = self.store.write();
            // Deregistration check, deliberately the first thing inside the
            // critical section rather than at `poll_once`'s entry: the read
            // phase above holds no store lock, so a pass that started before
            // the deregistration would otherwise reach this point and write
            // this account's rows (and watermarks) into the cleared store —
            // resurrecting data the operator just removed. Checking under the
            // same lock deregistration takes makes the two mutually exclusive.
            if self.is_stopped() {
                tracing::debug!(qq = %self.qq, "sync pass discarded: account deregistered");
                return Ok(Vec::new());
            }
            // Media root may predate the account (sync can run before the
            // index build in edge paths); resolve it lazily once.
            if guard.media_root.is_none() {
                guard.media_root = crate::store::media::media_root_of(&self.db_dir);
            }
            index::apply_records(&mut guard, &new_g);
            index::apply_records(&mut guard, &new_c);
            guard.watermark_group = new_wm_g;
            guard.watermark_c2c = new_wm_c;
            let events: Vec<Event> = new_g
                .iter()
                .chain(&new_c)
                .map(|r| {
                    // Display names resolve through the name maps — the
                    // remark (私聊) / group-info name (群聊) wins when known;
                    // in a group the sender's per-conversation card (40090)
                    // wins over the global name (never leaks across chats).
                    let group_name = Some(guard.display_name(r.chat_type, &r.talker));
                    let source_name = Some(guard.display_sender(r.chat_type, &r.talker, &r.from_uid));
                    if r.parsed.msg_type == MsgType::Recall {
                        Event::message_revoke(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                        )
                    } else {
                        // mediaId filtered by the same rule as the REST rows
                        // (registered live path only) — the media object is
                        // the path-free PushMedia view.
                        let media_id = r
                            .parsed
                            .media
                            .as_ref()
                            .and_then(|m| crate::store::query::fetchable_media_id(&guard, m));
                        Event::message_new(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                            r.parsed.media.clone(),
                            media_id,
                        )
                    }
                })
                .collect();
            // Response rows: shaped under the lock so the store-dependent
            // fields (mediaId fetchability, senderName) see this batch's own
            // registrations — same single shaping function as the messages
            // query, so the two paths cannot drift.
            let outs: Vec<MessageOut> = new_g
                .iter()
                .chain(&new_c)
                .map(|r| crate::store::query::shape_record(&guard, r))
                .collect();
            (events, outs)
        };
        for ev in events {
            let _ = self.tx.send(ev);
        }
        Ok(outs)
    }
}

/// Registry of per-account sync engines, shared with the HTTP layer for
/// the manual-sync endpoint. Accounts are registered as their indexes
/// finish building.
pub struct SyncEngine {
    accounts: Mutex<Vec<Arc<AccountSync>>>,
    /// Per-account watch task handles, so deregistration can abort the task
    /// that would otherwise keep the source database and its directory
    /// handle open for the rest of the process's life. Previously the handle
    /// was dropped at spawn time, which detached the task permanently.
    watchers: Mutex<std::collections::HashMap<String, tokio::task::JoinHandle<Result<()>>>>,
}

impl SyncEngine {
    pub fn new() -> Self {
        Self { accounts: Mutex::new(Vec::new()), watchers: Mutex::new(Default::default()) }
    }

    pub fn register(&self, account: Arc<AccountSync>) {
        self.accounts.lock().push(account);
    }

    /// Track one account's watch task (replacing any previous handle, which a
    /// re-registration after a failure produces).
    pub fn attach_watcher(&self, qq: &str, handle: tokio::task::JoinHandle<Result<()>>) {
        if let Some(old) = self.watchers.lock().insert(qq.to_string(), handle) {
            old.abort();
        }
    }

    /// Remove `qq` from the registry and return its sync handle plus its
    /// watch task, if it was registered. The caller stops them — this only
    /// detaches them from the engine so no later pass can pick them up.
    pub fn unregister(
        &self,
        qq: &str,
    ) -> (Option<Arc<AccountSync>>, Option<tokio::task::JoinHandle<Result<()>>>) {
        let account = {
            let mut accs = self.accounts.lock();
            accs.iter().position(|a| a.qq == qq).map(|i| accs.remove(i))
        };
        let watcher = self.watchers.lock().remove(qq);
        (account, watcher)
    }

    pub fn snapshot(&self) -> Vec<Arc<AccountSync>> {
        self.accounts.lock().clone()
    }

    /// Manual sync: run a full pass on every registered account and return
    /// `(newMessages, revokeMessages)` — how many rows the pass appended,
    /// split the same way WeFlow splits them (a recall is not counted as a
    /// new message). Name maps (备注/群名) and the media cache-index fallback
    /// ride the manual sync — the one place a client-visible refresh happens.
    /// The fallback refreshes BEFORE the poll so newly-read rows can already
    /// register through it.
    ///
    /// The rows themselves are deliberately not returned: the endpoint is a
    /// trigger, and clients read the appended rows back through
    /// `/api/v1/messages` (or receive them on the SSE stream) rather than
    /// from a second, differently-shaped copy.
    pub fn sync_all(&self) -> (usize, usize) {
        let (mut new_count, mut revoke_count) = (0usize, 0usize);
        for account in self.snapshot() {
            // A deregistration concurrent with this call may already have
            // stopped the account after `snapshot` cloned it; `poll_once`
            // would bail out anyway, but `refresh_media_fallback` writes to
            // the store without that check, so skip the whole account.
            if account.is_stopped() {
                continue;
            }
            account.refresh_media_fallback();
            match account.poll_once() {
                Ok(messages) => {
                    for m in &messages {
                        if MsgType::from_code(m.local_type) == MsgType::Recall {
                            revoke_count += 1;
                        } else {
                            new_count += 1;
                        }
                    }
                    account.refresh_names();
                }
                Err(e) => tracing::warn!("manual sync error: {e:#}"),
            }
        }
        (new_count, revoke_count)
    }
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new()
    }
}

