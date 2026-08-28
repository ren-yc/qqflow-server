//! HTTP layer: axum router with WeFlow-compatible endpoints, plus the
//! client-driven account initialization machinery.

pub mod auth;
pub mod error;
pub mod handlers;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::routing::{delete, get, post};
use axum::Router;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;
use tokio::sync::broadcast;

use crate::config;
use crate::db;
use crate::db::live::LiveReader;
use crate::db::scan::DbInfo;
use crate::sync;
use crate::sync::Event;
use crate::store::{self, index, AppState, Store};

/// Per-account readiness state (serialized as-is into the token-protected
/// `GET /api/v1/accounts`; `/health` reports the coarser [`AccountPhase`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountStatus {
    /// Scanned at startup, no key registered yet.
    AwaitingKey,
    /// Background build running (live open + decrypt + index).
    Indexing,
    /// Index built, incremental sync active.
    Ready,
    /// Initialization failed — a corrected registration recovers.
    Error,
}

impl AccountStatus {
    pub fn is_ready(&self) -> bool {
        matches!(self, Self::Ready)
    }
}

/// Per-account readiness (exposed via the authenticated account detail
/// endpoint and used for startup gating).
#[derive(Debug, Clone, Serialize)]
pub struct AccountState {
    pub qq: String,
    pub state: AccountStatus,
    pub message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// What `/health` may disclose about the bound account — deliberately NOT
/// [`AccountStatus`].
///
/// `/health` is unauthenticated, so it must not reveal which QQ accounts
/// exist on this machine, how many there are, or where their databases live.
/// The startup scan seeds one `AwaitingKey` entry per account directory it
/// finds, which makes the *count* of those entries a disclosure in itself.
/// This enum has no `AwaitingKey` variant at all, so leaking discovery
/// results through `/health` is a type error rather than a review item.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "snake_case")]
pub enum AccountPhase {
    /// No account is bound (nothing registered, or it was deregistered).
    Unregistered,
    /// The bound account is building its index.
    Indexing,
    /// The bound account is serving.
    Ready,
    /// The bound account failed to initialize; re-registering it recovers.
    Error,
}

impl From<AccountStatus> for AccountPhase {
    fn from(s: AccountStatus) -> Self {
        match s {
            // Unreachable via `bound_account` (which filters AwaitingKey out),
            // but mapping it to `Unregistered` keeps the invariant true by
            // construction for any future caller.
            AccountStatus::AwaitingKey => Self::Unregistered,
            AccountStatus::Indexing => Self::Indexing,
            AccountStatus::Ready => Self::Ready,
            AccountStatus::Error => Self::Error,
        }
    }
}

/// The one account this server instance is bound to, if any.
///
/// The store is a single global index with no account dimension: one set of
/// conversations, one pair of sync watermarks, one media root. Binding a
/// second account would overwrite the first one's index and cross-contaminate
/// watermarks (the two databases have independent rowid spaces), so at most
/// one account may be past `AwaitingKey` at a time — an invariant the
/// registration handler enforces by rejecting a second qq.
///
/// `AwaitingKey` entries are startup-scan discoveries, not bindings.
pub fn bound_account(accounts: &[AccountState]) -> Option<&AccountState> {
    accounts.iter().find(|a| a.state != AccountStatus::AwaitingKey)
}

/// Outcome of claiming the single account binding — see [`begin_indexing`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum BindOutcome {
    /// The binding is now held by this qq and `indexing` was set.
    Bound,
    /// This same qq is already registered; nothing changed.
    SameQq(AccountStatus),
    /// A different qq holds the binding; nothing changed.
    Occupied { qq: String, status: AccountStatus },
}

/// One buffered SSE event (WeFlow contract: replay cap 1000, TTL 10 min).
pub struct HistoryItem {
    pub id: u64,
    pub at: std::time::Instant,
    pub name: String,
    pub payload: serde_json::Value,
}

#[derive(Default)]
pub struct HistoryBuf {
    items: std::collections::VecDeque<HistoryItem>,
    last_id: u64,
}

impl HistoryBuf {
    pub const MAX: usize = 1000;
    pub const TTL: std::time::Duration = std::time::Duration::from_secs(600);

    /// Append an event and return its id (monotonic).
    pub fn append(&mut self, name: String, payload: serde_json::Value) -> u64 {
        self.last_id += 1;
        self.items.push_back(HistoryItem {
            id: self.last_id,
            at: std::time::Instant::now(),
            name,
            payload,
        });
        while self.items.len() > Self::MAX {
            self.items.pop_front();
        }
        self.last_id
    }

    /// Drop every buffered event while KEEPING the id counter.
    ///
    /// Used by deregistration: the buffered events describe an account that
    /// no longer exists, so replaying them would hand a reconnecting client
    /// messages the server can no longer serve. The counter must survive —
    /// ids are what `Last-Event-ID` resumes from, so restarting at 1 would
    /// leave a client holding `last-event-id: 500` silently receiving nothing
    /// until the next 500 events had accumulated.
    pub fn clear_items(&mut self) {
        self.items.clear();
    }

    /// Events with id > `since`, still within the TTL window.
    pub fn replay_since(&self, since: u64) -> Vec<(u64, String, serde_json::Value)> {
        let now = std::time::Instant::now();
        self.items
            .iter()
            .filter(|i| i.id > since && now.duration_since(i.at) < Self::TTL)
            .map(|i| (i.id, i.name.clone(), i.payload.clone()))
            .collect()
    }
}

/// Runtime per-account registration machinery (client-driven startup).
pub struct AccountRegistry {
    /// All known accounts: platform-scan results plus client registrations.
    pub accounts_db: Mutex<Vec<DbInfo>>,
    /// Accounts the STARTUP SCAN found, as opposed to ones a client
    /// introduced with an explicit `db_path`. Deregistration resets the
    /// former to `awaiting_key` (the platform will still find them next
    /// boot, so pretending otherwise until a restart would be a lie) and
    /// removes the latter outright (nothing on this machine knows about them
    /// once the client's registration is gone).
    pub scanned: std::collections::HashSet<String>,
    /// Bumped by every deregistration. An `init_account` already in flight
    /// compares the value it started with and abandons its work if it
    /// changed, so a build cannot install its index into a store that was
    /// cleared while it ran.
    pub epoch: std::sync::atomic::AtomicU64,
    /// Watch behavior handed to deferred watch tasks.
    pub watch_cfg: crate::sync::watch::WatchConfig,
    /// Shutdown signal receiver (cloned per deferred watch task).
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

impl AccountRegistry {
    /// `accounts` seeds the registry with the startup scan results; clients
    /// add or override entries via `upsert_db` at registration time.
    pub fn new(
        accounts: Vec<DbInfo>,
        watch_cfg: crate::sync::watch::WatchConfig,
        shutdown: tokio::sync::watch::Receiver<bool>,
    ) -> Self {
        // Derived here rather than taken as a parameter: the scan results ARE
        // this argument, so every existing construction site stays correct
        // without a signature change.
        let scanned = accounts.iter().map(|a| a.qq.clone()).collect();
        Self {
            accounts_db: Mutex::new(accounts),
            scanned,
            epoch: std::sync::atomic::AtomicU64::new(0),
            watch_cfg,
            shutdown,
        }
    }

    /// True when the startup scan discovered this account by itself.
    pub fn is_scanned(&self, qq: &str) -> bool {
        self.scanned.contains(qq)
    }

    /// Forget one client-registered account's database location.
    pub fn remove_db(&self, qq: &str) {
        self.accounts_db.lock().retain(|a| a.qq != qq);
    }

    /// Account location known for `qq` (startup scan or earlier registration).
    pub fn find_db(&self, qq: &str) -> Option<DbInfo> {
        self.accounts_db.lock().iter().find(|a| a.qq == qq).cloned()
    }

    /// Register or override one account's database location.
    pub fn upsert_db(&self, info: DbInfo) {
        let mut reg = self.accounts_db.lock();
        match reg.iter_mut().find(|a| a.qq == info.qq) {
            Some(a) => *a = info,
            None => reg.push(info),
        }
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    use handlers::*;
    Router::new()
        .route("/health", get(health::handler).post(health::handler))
        .route("/api/v1/health", get(health::handler).post(health::handler))
        .route("/api/v1/accounts", get(accounts::list_handler).post(accounts::handler))
        .route("/api/v1/accounts/{qq}", delete(accounts::delete_handler))
        // POST alias for clients (and proxies) that cannot issue DELETE.
        .route("/api/v1/accounts/{qq}/deregister", post(accounts::delete_handler))
        .route("/api/v1/messages", get(messages::handler).post(messages::handler))
        .route("/api/v1/media/{id}", get(media::handler).post(media::handler))
        .route(
            "/api/v1/media/{talker}/{media_type}/{file}",
            get(media::exported_handler).post(media::exported_handler),
        )
        .route("/api/v1/sessions", get(sessions::handler).post(sessions::handler))
        .route("/api/v1/sessions/{id}/messages", get(chatlab_pull::handler))
        .route("/api/v1/contacts", get(contacts::handler).post(contacts::handler))
        .route("/api/v1/group-members", get(group_members::handler).post(group_members::handler))
        .route("/api/v1/push/messages", get(push_events::handler).post(push_events::handler))
        .route("/api/v1/sync", get(sync::handler).post(sync::handler))
        .with_state(state)
}

/// Replace the store with a freshly built index and re-baseline SSE
/// subscribers: a client that connected while we were indexing received a
/// `sync(0,0)` event and would otherwise never learn the real watermarks.
fn install_index(store: &Arc<RwLock<Store>>, tx: &broadcast::Sender<Event>, st: Store) {
    let (wm_g, wm_c) = {
        let mut guard = store.write();
        *guard = st;
        (guard.watermark_group, guard.watermark_c2c)
    };
    let _ = tx.send(Event::sync(wm_g, wm_c, chrono::Utc::now().timestamp()));
}

/// Insert or update one account's state entry, unconditionally.
///
/// Test-only: every production write goes through
/// [`set_account_state_if_current`], which cannot resurrect a deregistered
/// account.
#[cfg(test)]
fn set_account_state(state: &AppState, qq: &str, status: AccountStatus, count: usize, error: Option<String>) {
    write_account_state(&mut state.accounts.write(), qq, status, count, error);
}

fn write_account_state(
    accs: &mut Vec<AccountState>,
    qq: &str,
    status: AccountStatus,
    count: usize,
    error: Option<String>,
) {
    match accs.iter_mut().find(|a| a.qq == qq) {
        Some(a) => {
            a.state = status;
            a.message_count = count;
            a.error = error;
        }
        None => accs.push(AccountState {
            qq: qq.into(),
            state: status,
            message_count: count,
            error,
        }),
    }
}

/// `set_account_state`, unless a deregistration happened since `epoch` was
/// read. Returns false when the write was skipped.
///
/// The epoch load and the write share one lock acquisition so a
/// deregistration cannot slip between them: otherwise a build finishing at
/// that exact moment would re-create the account entry that was just removed,
/// leaving a `ready` account with no index behind it.
fn set_account_state_if_current(
    state: &AppState,
    qq: &str,
    epoch: u64,
    status: AccountStatus,
    count: usize,
    error: Option<String>,
) -> bool {
    let mut accs = state.accounts.write();
    if state.init.epoch.load(Ordering::SeqCst) != epoch {
        return false;
    }
    write_account_state(&mut accs, qq, status, count, error);
    true
}

/// Claim the single account binding for `qq` and flip it to `indexing`.
///
/// Both the occupancy check and the flip happen inside one write lock, so
/// two concurrent registrations for different accounts serialize here and
/// exactly one wins — the loser sees `Occupied` rather than silently
/// overwriting the winner's index. A duplicate registration of the *same* qq
/// observes `SameQq` instead of spawning a second initialization.
///
/// An account in `Error` still holds the binding: freeing it on failure would
/// let one transient decrypt error hand the server to a different account
/// without anyone asking. Re-registering the same qq recovers; switching
/// accounts requires an explicit deregistration.
fn begin_indexing(state: &AppState, qq: &str) -> BindOutcome {
    let mut accs = state.accounts.write();
    if let Some(b) = bound_account(&accs) {
        if b.qq != qq {
            return BindOutcome::Occupied { qq: b.qq.clone(), status: b.state };
        }
        if matches!(b.state, AccountStatus::Ready | AccountStatus::Indexing) {
            return BindOutcome::SameQq(b.state);
        }
        // Same qq in `error` — fall through and retry the build.
    }
    match accs.iter_mut().find(|a| a.qq == qq) {
        Some(a) => {
            a.state = AccountStatus::Indexing;
            a.message_count = 0;
            a.error = None;
        }
        None => accs.push(AccountState {
            qq: qq.to_string(),
            state: AccountStatus::Indexing,
            message_count: 0,
            error: None,
        }),
    }
    BindOutcome::Bound
}

/// Base URL for exported media links (`mediaUrl`). An explicit `override`
/// (`--base-url`) wins; otherwise `http://<host>:<port>` — except bind-all
/// addresses (0.0.0.0 / ::), which are not reachable as URLs and fall back
/// to 127.0.0.1 (LAN clients must pass `--base-url` explicitly). IPv6 hosts
/// are bracketed: `http://[::1]:5032`.
fn derive_base_url(host: &str, port: u16, override_url: Option<&str>) -> String {
    match override_url {
        Some(url) => url.to_string(),
        None => {
            let host = match host {
                "0.0.0.0" | "::" => {
                    tracing::warn!(
                        "[init] 绑定地址 {host} 不可作为 URL，mediaUrl 回退 127.0.0.1；局域网客户端请用 --base-url 显式指定"
                    );
                    "127.0.0.1".to_string()
                }
                h => h.to_string(),
            };
            if host.contains(':') && !host.starts_with('[') {
                format!("http://[{host}]:{port}")
            } else {
                format!("http://{host}:{port}")
            }
        }
    }
}

/// Global readiness = at least one REGISTERED account, all of them `ready`.
///
/// `AwaitingKey` entries are excluded because the startup scan seeds one per
/// account directory it finds. Counting them meant that on a machine with two
/// QQ profiles, registering one would leave `/health` reporting `starting`
/// forever — the other account has no key and never will unless a client sends
/// one. Readiness answers "can I serve the data I was given", so only accounts
/// a client actually registered participate.
pub fn update_ready(state: &AppState) {
    let accs = state.accounts.read();
    let mut registered = accs
        .iter()
        .filter(|a| a.state != AccountStatus::AwaitingKey)
        .peekable();
    let all_ready = registered.peek().is_some() && registered.all(|a| a.state.is_ready());
    state.ready.store(all_ready, Ordering::SeqCst);
}

/// Result of a deregistration attempt.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum DeregisterOutcome {
    /// The account was bound and is now gone.
    Deregistered {
        /// Its state-machine value immediately before the removal.
        previous: AccountStatus,
        /// Whether an index actually existed (a `ready` account, or one whose
        /// build had already installed rows) — false when the account never
        /// got past `indexing`.
        index_cleared: bool,
        /// Directory count removed under the export root (0 when the purge
        /// was not requested).
        purged_dirs: usize,
    },
    /// Nothing is bound; there is nothing to deregister.
    NotRegistered,
    /// A DIFFERENT account is bound. Deliberately not treated as success:
    /// the qq in the path is a safety interlock, so a client that has drifted
    /// out of sync with the server learns that rather than believing it just
    /// removed something.
    QqMismatch { occupied_by: String, status: AccountStatus },
}

/// Exported-media subdirectories the server itself creates, per
/// `<exportRoot>/<talker>/<kind>/<file>` (see `store::media_export`).
const EXPORT_KINDS: [&str; 4] = ["images", "voices", "videos", "emojis"];

/// Remove the exported-media directories this account produced, and nothing
/// else. Returns how many were removed.
///
/// Scoped deliberately narrowly: only `<export_root>/<talker>/<kind>` for a
/// talker this account actually had, and only for the four kinds the exporter
/// writes. `export_root` comes from `--media-export-dir` and may well be a
/// directory the operator also keeps other things in, so a recursive delete
/// of the root is never an option; the talker directory itself is removed
/// only via `remove_dir`, which refuses to touch it unless it is empty.
fn purge_exported_media(root: &std::path::Path, talkers: &[String]) -> usize {
    let mut removed = 0usize;
    for talker in talkers {
        // Talkers come from the database, not the request, but they end up as
        // a path segment — same containment rule as the media route.
        let bad = talker.is_empty()
            || talker == "."
            || talker == ".."
            || talker.contains('/')
            || talker.contains('\\');
        if bad {
            tracing::warn!("[deregister] 跳过异常 talker 目录名: {talker:?}");
            continue;
        }
        let dir = root.join(talker);
        for kind in EXPORT_KINDS {
            let sub = dir.join(kind);
            match std::fs::remove_dir_all(&sub) {
                Ok(()) => removed += 1,
                Err(e) if e.kind() == std::io::ErrorKind::NotFound => {}
                Err(e) => tracing::warn!("[deregister] 清理导出媒体失败 {}: {e}", sub.display()),
            }
        }
        // Empty-only: anything the server did not put there survives.
        let _ = std::fs::remove_dir(&dir);
    }
    removed
}

/// Undo one account's registration: stop its sync, drop its index, and return
/// the server to the unregistered state it boots in.
///
/// Blocking (file IO when `purge_media` is set, plus the store write lock) —
/// callers run it on the blocking pool.
///
/// The step order is load-bearing:
/// 1. detach and `stop()` the sync BEFORE clearing, so a pass already past
///    its read phase discards its rows instead of writing them into the
///    cleared store;
/// 2. bump the epoch, so an `init_account` still running abandons its build
///    instead of installing an index for an account that no longer exists;
/// 3. clear the store, then the SSE history, then broadcast the reset
///    baseline — broadcasting before the history clear would wipe the very
///    event a reconnecting client needs to learn its watermarks went to zero.
pub fn deregister_account(state: &AppState, qq: &str, purge_media: bool) -> DeregisterOutcome {
    let previous = {
        let accs = state.accounts.read();
        match bound_account(&accs) {
            None => return DeregisterOutcome::NotRegistered,
            Some(b) if b.qq != qq => {
                return DeregisterOutcome::QqMismatch { occupied_by: b.qq.clone(), status: b.state }
            }
            Some(b) => b.state,
        }
    };

    // 1. Retire the sync side. `stop()` is what actually protects the store;
    // aborting the watch task only stops FUTURE passes, because a pass
    // already inside `spawn_blocking` runs to completion regardless.
    let (account, watcher) = state.sync.unregister(qq);
    if let Some(a) = &account {
        a.stop();
    }
    if let Some(w) = watcher {
        w.abort();
    }

    // 2. Invalidate any in-flight initialization.
    state.init.epoch.fetch_add(1, Ordering::SeqCst);

    // 3. Drop the index, collecting the talkers to purge while we still can.
    let (talkers, index_cleared) = {
        let mut guard = state.store.write();
        let talkers: Vec<String> = if purge_media {
            guard.convs.values().map(|c| c.talker.clone()).collect()
        } else {
            Vec::new()
        };
        let had_index = !guard.convs.is_empty();
        *guard = Store::default();
        (talkers, had_index)
    };
    state.history.lock().clear_items();
    let _ = state.events.send(Event::sync(0, 0, chrono::Utc::now().timestamp()));

    // 4. Reset the account entry. A scanned account reverts to `awaiting_key`
    // and keeps its db_path (the platform will find it again next boot, so
    // claiming otherwise would be false); a client-introduced one disappears
    // entirely, because nothing on this machine knows about it any more.
    {
        let mut accs = state.accounts.write();
        if state.init.is_scanned(qq) {
            if let Some(a) = accs.iter_mut().find(|a| a.qq == qq) {
                a.state = AccountStatus::AwaitingKey;
                a.message_count = 0;
                a.error = None;
            }
        } else {
            accs.retain(|a| a.qq != qq);
            state.init.remove_db(qq);
        }
    }
    update_ready(state);

    let purged_dirs =
        if purge_media { purge_exported_media(&state.export_root, &talkers) } else { 0 };
    tracing::info!(
        "[deregister] QQ {qq} 已注销 (原状态 {previous:?}, 索引已清理 {index_cleared}, 清理媒体目录 {purged_dirs})"
    );
    DeregisterOutcome::Deregistered { previous, index_cleared, purged_dirs }
}

/// Full per-account initialization: open the LIVE source read-only, verify
/// the key, build the index (blocking pool), SSE baseline broadcast,
/// `AccountSync` registration, watch task. No copies, no mirror dir.
/// On failure the account enters the `error` state with the reason —
/// recoverable by posting a corrected registration to /api/v1/accounts.
/// The caller (the registration handler) has already flipped the account
/// to `indexing` synchronously so /health shows it immediately.
pub async fn init_account(state: &Arc<AppState>, info: DbInfo, key: String) {
    let qq = info.qq.clone();
    // Deregistration bumps this. Checked again at both points where the build
    // would become visible, so a registration that is cancelled mid-flight
    // cannot resurrect itself: the decrypt + index of a large account takes
    // seconds to minutes, which is plenty of time for a client to change its
    // mind, and without this the build would install its index into a store
    // the operator had just emptied.
    let epoch = state.init.epoch.load(Ordering::SeqCst);
    let cancelled = {
        let state = state.clone();
        move || state.init.epoch.load(Ordering::SeqCst) != epoch
    };

    let store = state.store.clone();
    let tx = state.events.clone();
    let info_for_build = info.clone();
    let key_for_build = key.clone();
    let cancelled_in_build = cancelled.clone();
    // `Ok(None)` = cancelled mid-build (deregistered), as distinct from
    // `Err` = the build genuinely failed.
    let result = tokio::task::spawn_blocking(move || -> Result<Option<(Arc<Mutex<LiveReader>>, usize)>> {
        let mut reader = LiveReader::new(info_for_build.path.clone(), key_for_build.clone());
        reader.open()?; // verify the key now — bad key → error state (unchanged UX)
        let conn = reader.acquire()?;
        // Media root: <root>/<qq>/nt_qq/nt_db -> <root>/<qq>/nt_qq/nt_data;
        // relative "45812" local cache paths resolve against it. Supplied to
        // build_index up front so media registration can refresh stale paths.
        let nt_db_dir = info_for_build
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        let media_root = store::media::media_root_of(nt_db_dir);
        let mut st = index::build_index(conn, media_root.as_deref())?;
        // uid→备注/QQ、群号→群名 maps (best-effort — empty on schema churn).
        st.names = store::names::load_names(
            conn,
            nt_db_dir,
            &key_for_build,
            &store::names::KnownKeys::from_store(&st),
        );
        let count: usize = st.convs.values().map(|c| c.msgs.len()).sum();
        // Cancelled during the build (decrypt + index is the slow part) —
        // drop the freshly built index instead of installing it.
        if cancelled_in_build() {
            return Ok(None);
        }
        install_index(&store, &tx, st);
        Ok(Some((Arc::new(Mutex::new(reader)), count)))
    })
    .await;

    match result {
        Ok(Ok(Some((reader, count)))) => {
            let watch_dir = info
                .path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf();
            let account = Arc::new(sync::AccountSync::new(
                qq.clone(),
                reader,
                state.store.clone(),
                state.events.clone(),
                info.path.clone(),
                watch_dir.clone(),
                key.clone(),
            ));
            state.sync.register(account.clone());
            // The handle is kept (not dropped as before) so deregistration can
            // abort the task — otherwise it holds the source database and its
            // directory handle open for the rest of the process's life.
            let watcher = tokio::spawn(sync::watch::spawn(
                account,
                watch_dir,
                state.init.watch_cfg.clone(),
                state.init.shutdown.clone(),
            ));
            state.sync.attach_watcher(&qq, watcher);
            // Registering first and checking after means a deregistration that
            // lands in this window is guaranteed to be noticed by one side or
            // the other: either it finds the account in the engine and stops
            // it, or the epoch check below fails and we retire ourselves.
            if !set_account_state_if_current(state, &qq, epoch, AccountStatus::Ready, count, None) {
                let (a, w) = state.sync.unregister(&qq);
                if let Some(a) = a {
                    a.stop();
                }
                if let Some(w) = w {
                    w.abort();
                }
                tracing::info!("[init] QQ {qq} 初始化完成但已被注销，结果丢弃");
                return;
            }
            tracing::info!("[init] QQ {qq} 索引完成: {count} 条消息");
        }
        Ok(Ok(None)) => {
            tracing::info!("[init] QQ {qq} 初始化中被注销，已放弃本次构建");
            return;
        }
        Ok(Err(e)) => {
            if !set_account_state_if_current(
                state,
                &qq,
                epoch,
                AccountStatus::Error,
                0,
                Some(format!("{e:#}")),
            ) {
                tracing::info!("[init] QQ {qq} 初始化失败但已被注销，结果丢弃: {e:#}");
                return;
            }
            tracing::warn!("[init] QQ {qq} 初始化失败（重新注册可恢复）: {e:#}");
        }
        Err(e) => {
            if !set_account_state_if_current(
                state,
                &qq,
                epoch,
                AccountStatus::Error,
                0,
                Some(format!("index task panicked: {e}")),
            ) {
                return;
            }
            tracing::error!("[init] QQ {qq} 初始化任务异常: {e}");
        }
    }
    update_ready(state);
}

/// Full startup: parse CLI args, load token, scan accounts (discovery only),
/// bind the server and wait for client-driven registrations. Runs until Ctrl-C.
pub async fn serve() -> Result<()> {
    let Some(cfg) = config::load()? else {
        return Ok(()); // help printed
    };
    if cfg.show_token {
        return match config::show_token()? {
            Some(t) => {
                println!("{t}");
                Ok(())
            }
            None => anyhow::bail!("尚未生成 API token（先启动一次服务以生成）"),
        };
    }
    crate::logging::init(&cfg.log);
    run_with(cfg).await
}

/// How long a graceful shutdown may take before the process exits anyway.
///
/// `with_graceful_shutdown` waits for every in-flight connection to finish,
/// but an SSE stream never ends on its own — without an upper bound, Ctrl+C
/// would hang for as long as a client stays subscribed. The SSE handler also
/// watches the shutdown channel and closes its own stream, so this is the
/// safety net rather than the normal path.
const SHUTDOWN_GRACE: std::time::Duration = std::time::Duration::from_secs(3);

pub async fn run_with(cfg: config::Config) -> Result<()> {
    run_with_shutdown(cfg, async {
        tokio::signal::ctrl_c().await.ok();
    })
    .await
}

/// `run_with`, with the shutdown trigger injected.
///
/// Exists so the shutdown path is testable: a real `CTRL_C_EVENT` cannot be
/// delivered to another process from a test on Windows. Tests drive this with
/// a channel instead of a signal.
pub async fn run_with_shutdown(
    cfg: config::Config,
    shutdown_signal: impl std::future::Future<Output = ()> + Send + 'static,
) -> Result<()> {
    let data_dir = config::data_dir()?;
    let token = config::load_or_create_token()?;

    // ---- accounts: platform scan for discovery only ----------------------
    // Zero accounts is a valid start state — a client will register them
    // with qq + key + db_path via POST /api/v1/accounts.
    let accounts = db::scan::scan_accounts(None)?;

    // ---- state -----------------------------------------------------------
    let store = Arc::new(RwLock::new(Store::default()));
    let (tx, _) = broadcast::channel::<Event>(1024);
    // Scanned accounts are listed as awaiting keys; initialization is
    // entirely client-driven via POST /api/v1/accounts.
    let accounts_state = Arc::new(RwLock::new(
        accounts
            .iter()
            .map(|a| AccountState {
                qq: a.qq.clone(),
                state: AccountStatus::AwaitingKey,
                message_count: 0,
                error: None,
            })
            .collect::<Vec<_>>(),
    ));
    let ready = Arc::new(AtomicBool::new(false));
    let sync_engine = Arc::new(sync::SyncEngine::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let watch_cfg = crate::sync::watch::WatchConfig {
        debounce: std::time::Duration::from_millis(cfg.watch_debounce_ms),
        fallback: (cfg.watch_fallback_ms > 0)
            .then(|| std::time::Duration::from_millis(cfg.watch_fallback_ms)),
    };
    let export_root = Arc::new(
        cfg.media_export_dir
            .clone()
            .unwrap_or_else(|| data_dir.join("api-media")),
    );
    // Exported-media URL base. `--base-url` overrides; otherwise derive
    // from host/port — but bind-all addresses (0.0.0.0 / ::) are not
    // reachable as URLs, so they fall back to 127.0.0.1 (LAN clients must
    // pass --base-url explicitly). IPv6 hosts are bracketed: [::1]:5032.
    let base_url = Arc::new(derive_base_url(&cfg.host, cfg.port, cfg.base_url.as_deref()));
    let state = Arc::new(AppState {
        store: store.clone(),
        events: tx.clone(),
        accounts: accounts_state.clone(),
        ready: ready.clone(),
        token: Arc::new(token.clone()),
        sync: sync_engine.clone(),
        init: AccountRegistry::new(accounts, watch_cfg, shutdown_rx.clone()),
        export_root,
        base_url,
        history: Arc::new(Mutex::new(HistoryBuf::default())),
        shutdown: shutdown_tx.clone(),
    });
    update_ready(&state);

    // ---- server (bind early; /health reports "starting") ------------------
    let app = build_router(state.clone());
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!("[init] 服务启动: http://{addr}  (API token 存于系统凭据库; 仅首次生成时打印; --show-token 获取)");
    tracing::info!("[init] 等待客户端注册账号: POST /api/v1/accounts {{\"qq\", \"key\", \"db_path\"}}");

    // ---- shutdown ----------------------------------------------------------
    // Signal the watchers (and the SSE streams) the moment Ctrl+C lands, then
    // let axum drain. `drain_tx` tells the grace timer when to start counting.
    // Previously the server ran in a detached `tokio::spawn` and this function
    // returned as soon as the signal arrived, so the process exited while
    // requests were still in flight: responses were truncated and SSE clients
    // saw a dropped socket rather than a clean end of stream.
    let (drain_tx, drain_rx) = tokio::sync::oneshot::channel::<()>();
    let server = axum::serve(listener, app).with_graceful_shutdown(async move {
        shutdown_signal.await;
        tracing::info!("收到退出信号，清理中…");
        // Stops the per-account watch tasks (releasing their database and
        // directory handles) and ends every live SSE stream.
        shutdown_tx.send(true).ok();
        let _ = drain_tx.send(());
    });

    tokio::select! {
        result = server => result.context("http server error")?,
        _ = async {
            // Only start the clock once shutdown was actually requested; if
            // the sender is dropped without a signal (server ended on its
            // own) this branch must never win the select.
            match drain_rx.await {
                Ok(()) => tokio::time::sleep(SHUTDOWN_GRACE).await,
                Err(_) => std::future::pending::<()>().await,
            }
        } => {
            tracing::warn!("退出宽限期 {:?} 已到，仍有连接未结束，强制退出", SHUTDOWN_GRACE);
        }
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `AppState` with one `10001` account in the given status.
    fn state_with_account(status: AccountStatus) -> Arc<AppState> {
        state_with_accounts(&[("10001", status)])
    }

    /// `AppState` with the given `(qq, status)` accounts.
    fn state_with_accounts(accounts: &[(&str, AccountStatus)]) -> Arc<AppState> {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Arc::new(AppState {
            store: Arc::new(RwLock::new(Store::default())),
            events: broadcast::channel::<Event>(16).0,
            accounts: Arc::new(RwLock::new(
                accounts
                    .iter()
                    .map(|(qq, state)| AccountState {
                        qq: (*qq).into(),
                        state: *state,
                        message_count: 0,
                        error: None,
                    })
                    .collect(),
            )),
            ready: Arc::new(AtomicBool::new(false)),
            token: Arc::new("t".into()),
            sync: Arc::new(sync::SyncEngine::new()),
            init: AccountRegistry::new(
                Vec::new(),
                crate::sync::watch::WatchConfig::default(),
                shutdown_rx,
            ),
            export_root: Arc::new(std::path::PathBuf::from(".")),
            base_url: Arc::new("http://127.0.0.1:5032".into()),
            history: Arc::new(Mutex::new(HistoryBuf::default())),
            shutdown: shutdown_tx,
        })
    }

    #[tokio::test]
    async fn install_index_rebaselines_subscribers() {
        let store = Arc::new(RwLock::new(Store::default()));
        let (tx, mut rx) = broadcast::channel::<Event>(16);
        let st = Store { watermark_group: 42, watermark_c2c: 7, ..Store::default() };
        install_index(&store, &tx, st);
        assert_eq!(store.read().watermark_group, 42, "store replaced");
        let ev = rx.try_recv().expect("build completion broadcasts a sync baseline");
        assert_eq!(ev.event, "sync");
        assert_eq!(ev.last_rowid_group, Some(42));
        assert_eq!(ev.last_rowid_c2c, Some(7));
    }

    #[test]
    fn update_ready_requires_all_registered_accounts_ready() {
        let state = state_with_account(AccountStatus::AwaitingKey);
        update_ready(&state);
        assert!(
            !state.ready.load(Ordering::SeqCst),
            "a scan result with no key registered is not readiness"
        );
        set_account_state(&state, "10001", AccountStatus::Indexing, 0, None);
        update_ready(&state);
        assert!(!state.ready.load(Ordering::SeqCst), "still indexing");
        set_account_state(&state, "10001", AccountStatus::Ready, 7, None);
        update_ready(&state);
        assert!(state.ready.load(Ordering::SeqCst), "all registered ready flips the flag");
    }

    /// A second scanned-but-unregistered account must not gate readiness.
    ///
    /// The startup scan seeds one `awaiting_key` entry per account directory
    /// found. Requiring *every* entry to be `ready` meant a machine with two
    /// QQ profiles could never report `ok`: the profile the client never
    /// registered stays `awaiting_key` forever, so `/health` was pinned to
    /// `starting` and readiness-gated endpoints returned 503 indefinitely.
    #[test]
    fn update_ready_ignores_unregistered_accounts() {
        let state = state_with_accounts(&[
            ("10001", AccountStatus::Ready),
            ("10002", AccountStatus::AwaitingKey),
        ]);
        update_ready(&state);
        assert!(
            state.ready.load(Ordering::SeqCst),
            "an unregistered second account must not gate readiness"
        );

        // But a registered one that failed still does.
        set_account_state(&state, "10002", AccountStatus::Error, 0, Some("bad key".into()));
        update_ready(&state);
        assert!(
            !state.ready.load(Ordering::SeqCst),
            "a registered account in error gates readiness"
        );
    }

    #[test]
    fn base_url_derivation() {
        assert_eq!(
            derive_base_url("127.0.0.1", 5032, None),
            "http://127.0.0.1:5032"
        );
        // Bind-all addresses are not reachable as URLs -> 127.0.0.1.
        assert_eq!(derive_base_url("0.0.0.0", 5032, None), "http://127.0.0.1:5032");
        assert_eq!(derive_base_url("::", 5032, None), "http://127.0.0.1:5032");
        // IPv6 hosts get brackets.
        assert_eq!(derive_base_url("::1", 5032, None), "http://[::1]:5032");
        // --base-url overrides everything verbatim.
        assert_eq!(
            derive_base_url("0.0.0.0", 5032, Some("http://192.168.1.10:5032")),
            "http://192.168.1.10:5032"
        );
    }

    #[test]
    fn begin_indexing_flips_once_and_guards_duplicates() {
        let state = state_with_account(AccountStatus::AwaitingKey);
        assert_eq!(begin_indexing(&state, "10001"), BindOutcome::Bound, "first registration proceeds");
        assert_eq!(
            begin_indexing(&state, "10001"),
            BindOutcome::SameQq(AccountStatus::Indexing),
            "duplicate registration observes indexing"
        );
        set_account_state(&state, "10001", AccountStatus::Ready, 7, None);
        assert_eq!(
            begin_indexing(&state, "10001"),
            BindOutcome::SameQq(AccountStatus::Ready),
            "ready accounts stay ready"
        );
    }

    /// The store has no account dimension, so a second qq must be rejected
    /// rather than silently overwriting the first one's index.
    #[test]
    fn begin_indexing_rejects_a_second_account() {
        let state = state_with_accounts(&[
            ("10001", AccountStatus::Ready),
            ("10002", AccountStatus::AwaitingKey),
        ]);
        assert_eq!(
            begin_indexing(&state, "10002"),
            BindOutcome::Occupied { qq: "10001".into(), status: AccountStatus::Ready },
            "a scanned second account cannot take the binding"
        );
        assert_eq!(
            state.accounts.read().iter().find(|a| a.qq == "10002").map(|a| a.state),
            Some(AccountStatus::AwaitingKey),
            "the rejected account's state is untouched"
        );
    }

    /// An `error` account keeps the binding: a transient decrypt failure must
    /// not let a different account take over. The same qq may retry.
    #[test]
    fn error_state_keeps_the_binding_but_allows_retry() {
        let state = state_with_accounts(&[
            ("10001", AccountStatus::Error),
            ("10002", AccountStatus::AwaitingKey),
        ]);
        assert_eq!(
            begin_indexing(&state, "10002"),
            BindOutcome::Occupied { qq: "10001".into(), status: AccountStatus::Error },
            "error does not free the binding"
        );
        assert_eq!(
            begin_indexing(&state, "10001"),
            BindOutcome::Bound,
            "the same qq retries after a failure"
        );
    }

    #[test]
    fn bound_account_ignores_scan_results() {
        let state = state_with_accounts(&[
            ("10001", AccountStatus::AwaitingKey),
            ("10002", AccountStatus::AwaitingKey),
        ]);
        assert!(
            bound_account(&state.accounts.read()).is_none(),
            "scanned-but-unregistered accounts are not a binding"
        );
        set_account_state(&state, "10002", AccountStatus::Indexing, 0, None);
        assert_eq!(
            bound_account(&state.accounts.read()).map(|a| a.qq.clone()),
            Some("10002".into())
        );
    }

    /// `/health` must never be able to say "a key is awaited", because the
    /// only way an account reaches that state is the startup scan finding it.
    #[test]
    fn account_phase_never_exposes_awaiting_key() {
        assert_eq!(AccountPhase::from(AccountStatus::AwaitingKey), AccountPhase::Unregistered);
        assert_eq!(AccountPhase::from(AccountStatus::Indexing), AccountPhase::Indexing);
        assert_eq!(AccountPhase::from(AccountStatus::Ready), AccountPhase::Ready);
        assert_eq!(AccountPhase::from(AccountStatus::Error), AccountPhase::Error);
    }

    /// `Last-Event-ID` resumes from event ids, so the counter must survive a
    /// clear. Restarting at 1 would leave a client holding `last-event-id:
    /// 500` receiving nothing until 500 new events had accumulated.
    #[test]
    fn clear_items_drops_events_but_keeps_the_id_counter() {
        let mut h = HistoryBuf::default();
        assert_eq!(h.append("message.new".into(), serde_json::json!({"a": 1})), 1);
        assert_eq!(h.append("message.new".into(), serde_json::json!({"a": 2})), 2);
        assert_eq!(h.replay_since(0).len(), 2);
        h.clear_items();
        assert!(h.replay_since(0).is_empty(), "buffered events are gone");
        assert_eq!(h.append("sync".into(), serde_json::json!({})), 3, "ids keep climbing");
    }

    #[tokio::test]
    async fn deregister_clears_the_index_and_unbinds() {
        let state = state_with_account(AccountStatus::Ready);
        state.init.accounts_db.lock().push(DbInfo {
            qq: "10001".into(),
            path: std::path::PathBuf::from("C:\\x\\nt_msg.db"),
        });
        {
            let mut st = state.store.write();
            st.watermark_group = 42;
            st.watermark_c2c = 7;
            st.convs.insert(
                "g:g1".into(),
                store::Conversation { talker: "g1".into(), ..Default::default() },
            );
        }
        update_ready(&state);
        assert!(state.ready.load(Ordering::SeqCst));
        let mut rx = state.events.subscribe();

        let outcome = deregister_account(&state, "10001", false);
        assert_eq!(
            outcome,
            DeregisterOutcome::Deregistered {
                previous: AccountStatus::Ready,
                index_cleared: true,
                purged_dirs: 0,
            }
        );
        assert!(state.store.read().convs.is_empty(), "index dropped");
        assert_eq!(state.store.read().watermark_group, 0, "watermarks reset");
        assert!(!state.ready.load(Ordering::SeqCst), "no longer ready");
        assert!(bound_account(&state.accounts.read()).is_none(), "nothing bound");

        // Subscribers are told their watermarks went back to zero.
        let ev = rx.try_recv().expect("deregistration broadcasts a reset baseline");
        assert_eq!(ev.event, "sync");
        assert_eq!(ev.last_rowid_group, Some(0));
        assert_eq!(ev.last_rowid_c2c, Some(0));

        // Not scanned -> the account and its db_path are forgotten entirely.
        assert!(state.accounts.read().is_empty(), "client-registered account removed");
        assert!(state.init.find_db("10001").is_none(), "db_path forgotten");
    }

    /// A scanned account keeps its entry and path: the platform will find it
    /// again on the next boot, so claiming it does not exist would be a lie.
    #[tokio::test]
    async fn deregister_resets_scanned_accounts_to_awaiting_key() {
        let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let info = DbInfo { qq: "10001".into(), path: std::path::PathBuf::from("C:\\x\\nt_msg.db") };
        let state = Arc::new(AppState {
            store: Arc::new(RwLock::new(Store::default())),
            events: broadcast::channel::<Event>(16).0,
            accounts: Arc::new(RwLock::new(vec![AccountState {
                qq: "10001".into(),
                state: AccountStatus::Ready,
                message_count: 9,
                error: None,
            }])),
            ready: Arc::new(AtomicBool::new(true)),
            token: Arc::new("t".into()),
            sync: Arc::new(sync::SyncEngine::new()),
            init: AccountRegistry::new(
                vec![info],
                crate::sync::watch::WatchConfig::default(),
                shutdown_rx,
            ),
            export_root: Arc::new(std::path::PathBuf::from(".")),
            base_url: Arc::new("http://127.0.0.1:5032".into()),
            history: Arc::new(Mutex::new(HistoryBuf::default())),
            shutdown: shutdown_tx,
        });

        assert!(state.init.is_scanned("10001"));
        let outcome = deregister_account(&state, "10001", false);
        assert!(matches!(outcome, DeregisterOutcome::Deregistered { .. }));
        let accs = state.accounts.read();
        assert_eq!(accs.len(), 1, "the scan result survives");
        assert_eq!(accs[0].state, AccountStatus::AwaitingKey);
        assert_eq!(accs[0].message_count, 0);
        assert!(state.init.find_db("10001").is_some(), "scanned db_path is kept");
    }

    #[tokio::test]
    async fn deregister_validates_the_qq_interlock() {
        let state = state_with_accounts(&[
            ("10001", AccountStatus::Ready),
            ("20002", AccountStatus::AwaitingKey),
        ]);
        assert_eq!(
            deregister_account(&state, "20002", false),
            DeregisterOutcome::QqMismatch {
                occupied_by: "10001".into(),
                status: AccountStatus::Ready,
            },
            "a scanned account is not the bound one"
        );
        assert_eq!(
            deregister_account(&state, "99999", false),
            DeregisterOutcome::QqMismatch {
                occupied_by: "10001".into(),
                status: AccountStatus::Ready,
            }
        );
        // The incumbent is untouched by either rejected call.
        assert_eq!(bound_account(&state.accounts.read()).map(|a| a.state), Some(AccountStatus::Ready));

        let empty = state_with_account(AccountStatus::AwaitingKey);
        assert_eq!(
            deregister_account(&empty, "10001", false),
            DeregisterOutcome::NotRegistered,
            "a scan result is not a registration"
        );
    }

    /// Deregistering mid-build is allowed and must not be undone by the build
    /// finishing afterwards.
    #[tokio::test]
    async fn deregister_during_indexing_invalidates_the_build() {
        let state = state_with_account(AccountStatus::Indexing);
        let epoch = state.init.epoch.load(Ordering::SeqCst);

        let outcome = deregister_account(&state, "10001", false);
        assert_eq!(
            outcome,
            DeregisterOutcome::Deregistered {
                previous: AccountStatus::Indexing,
                index_cleared: false,
                purged_dirs: 0,
            },
            "no index existed yet"
        );

        // The in-flight build now tries to publish its result.
        assert!(
            !set_account_state_if_current(&state, "10001", epoch, AccountStatus::Ready, 500, None),
            "a stale build must not resurrect the account"
        );
        assert!(state.accounts.read().is_empty(), "still unbound");
        // A registration started AFTER the deregistration still works.
        let fresh = state.init.epoch.load(Ordering::SeqCst);
        assert!(set_account_state_if_current(&state, "10001", fresh, AccountStatus::Ready, 3, None));
    }

    /// The purge removes only `<root>/<talker>/<kind>` for the four kinds the
    /// exporter writes; everything else under the export root survives,
    /// including files the operator put there (the export root may be a
    /// directory they also use for other things).
    #[test]
    fn purge_exported_media_stays_inside_the_known_layout() {
        let root = std::env::temp_dir().join(format!("qqflow_purge_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        for (dir, file) in [
            ("10001/images", "a.jpg"),
            ("10001/voices", "a.amr"),
            ("10001/notes", "keep.txt"),
            ("20002/images", "b.jpg"),
        ] {
            std::fs::create_dir_all(root.join(dir)).unwrap();
            std::fs::write(root.join(dir).join(file), b"x").unwrap();
        }
        std::fs::write(root.join("operator-notes.txt"), b"keep me").unwrap();

        let removed = purge_exported_media(&root, &["10001".into(), "../escape".into()]);
        assert_eq!(removed, 2, "images + voices for the one talker");
        assert!(!root.join("10001/images").exists());
        assert!(!root.join("10001/voices").exists());
        assert!(root.join("10001/notes/keep.txt").exists(), "unknown subdir untouched");
        assert!(root.join("10001").exists(), "non-empty talker dir survives");
        assert!(root.join("20002/images/b.jpg").exists(), "other talkers untouched");
        assert!(root.join("operator-notes.txt").exists(), "export root never wiped");

        // Now that only known-empty dirs remain for 20002, its dir goes too.
        assert_eq!(purge_exported_media(&root, &["20002".into()]), 1);
        assert!(!root.join("20002").exists(), "emptied talker dir removed");
        assert!(root.exists(), "the root itself is never removed");
        let _ = std::fs::remove_dir_all(&root);
    }
}
