//! HTTP layer: axum router with WeFlow-compatible endpoints, plus the
//! client-driven account initialization machinery.

pub mod auth;
pub mod dto;
pub mod error;
pub mod openapi;
pub mod routes;
pub(crate) mod chatlab;
pub mod handlers;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::Router;
use parking_lot::{Mutex, RwLock};
use serde::Serialize;

use crate::config;
use crate::db;
use crate::db::live::LiveReader;
use crate::db::scan::DbInfo;
use crate::sync;
use crate::sync::Event;
use crate::store::{index, Store};

/// Per-account readiness state (serialized as-is into the token-protected
/// `GET /api/v1/accounts`; `/health` reports the coarser [`AccountPhase`]).
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, utoipa::ToSchema)]
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

/// **事件基线代号**：注销账号时递增。
///
/// 为什么需要它：注销会清掉重放缓冲里的条目，而**事件 id 计数器保留**（否则新账号的事件 id 会
/// 从旧客户端已经见过的号段重新开始，它们会以为那些事件已经收过）。于是客户端带着旧的
/// `Last-Event-ID` 重连时，看到的是一个**空的重放**加**跳号的 id** —— 它无法区分「注销后新账号
/// 刚开始」与「自己漏收了」。基线里带上代号，客户端一比就知道该丢弃本地状态重新拉。
///
/// 进程级而不是每账号：事件总线与重放历史本来就是进程级的。
static GENERATION: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(1);

/// 当前的基线代号，见 [`GENERATION`]。
pub fn current_generation() -> u64 {
    GENERATION.load(std::sync::atomic::Ordering::SeqCst)
}

/// 推进基线代号（注销时调用）。
pub fn bump_generation() -> u64 {
    GENERATION.fetch_add(1, std::sync::atomic::Ordering::SeqCst) + 1
}

pub use crate::sync::history::{EventBus, HistoryBuf, HistoryItem, Stamped};


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

/// `GET /openapi.json` —— 由 DTO 的 `ToSchema` 生成的接口描述。
///
/// 每次请求重新生成：生成成本是一次内存遍历，而缓存会引入「改了 DTO 但描述是旧的」
/// 这一类只在部署后才暴露的问题。
async fn openapi_handler() -> axum::response::Response {
    use axum::response::IntoResponse;
    let doc = openapi::document();
    match serde_json::to_value(&doc) {
        Ok(v) => axum::Json(v).into_response(),
        Err(_) => crate::server::error::ApiError::internal("OpenAPI 描述生成失败")
            .into_response(),
    }
}

pub fn build_router(state: Arc<AppState>) -> Router {
    // **路由表是唯一事实源**（见 `routes`）：这里只负责把它挂上去。加路由改 routes.rs，
    // 不在这里 —— 于是「真实路由」与「接口描述」不可能各自漂移（对等测试在 api_smoke）。
    let mut app = Router::new();
    for r in routes::ROUTES {
        app = app.route(r.path, routes::method_router(r.kind));
    }
    app.fallback(unknown_path)
        .method_not_allowed_fallback(method_not_allowed)
        .with_state(state)
}

/// 未知路径：与其它错误走**同一个**信封。
///
/// axum 默认给的是**空响应体**的 404，于是「所有错误都带 `{success,code,message}`」这条
/// 契约恰好留下一个例外——而例外正是客户端最容易漏掉的那个：它只能按状态码特判，
/// 漏了就会把「路径打错了」显示成「服务器返回了无法解析的东西」。
async fn unknown_path() -> crate::server::error::ApiError {
    crate::server::error::ApiError::not_found("未知路径")
}

/// 路径存在但方法不对：同样是客户端要解析的错误，同样走信封。
///
/// 状态码用 405 而不是 404——「这个方法不存在」与「这个路径不存在」是两回事，
/// 合并会让调用方改不动自己的请求。
async fn method_not_allowed() -> crate::server::error::ApiError {
    crate::server::error::ApiError {
        status: axum::http::StatusCode::METHOD_NOT_ALLOWED,
        message: "方法不允许".into(),
    }
}

/// Replace the store with a freshly built index and re-baseline SSE
/// subscribers: a client that connected while we were indexing received a
/// `sync(0,0)` event and would otherwise never learn the real watermarks.
/// Install a freshly built index — **only if the registration that asked for it
/// is still current**. The epoch re-check lives inside the same write-lock
/// critical section as the store swap: `deregister_account` clears the store
/// under that same lock *after* bumping the epoch, so holding it here means
/// the two orders (bump→clear→install vs install→bump→clear) can no longer
/// interleave. Without this, an `init_account` whose `cancelled_in_build()`
/// check passed a few instructions ago could still land its index into the
/// just-emptied store — and then re-baseline every SSE subscriber from a
/// phantom account that was already deregistered (regression:
/// `cancelled_build_cannot_install_after_deregister`).
/// Returns false = the build lost its race against a deregistration.
///
/// `cancelled` 是调用方带来的 epoch 判据闭包：这个 helper 同时被初始化路径与测试用，
/// 把 AppState 拖进来只会让它更难组合，判据本身留在调用方。
fn install_index(
    store: &Arc<RwLock<Store>>,
    bus: &EventBus,
    st: Store,
    cancelled: impl Fn() -> bool,
) -> bool {
    let (wm_g, wm_c) = {
        let mut guard = store.write();
        // 重验发生在**换锁临界区内**：注销先在锁外 bump epoch、再进同一把锁清空 store，
        // 于是「先清空后安装」与「先安装后清空」两种交错都被这把锁挡住 —— 检查与交换
        // 若分成两步，被取消的构建仍可能把索引落进刚清空的 store，并向所有订阅者广播
        // 幽灵账号的水位。
        if cancelled() {
            return false;
        }
        *guard = st;
        (guard.watermark_group, guard.watermark_c2c)
    };
    bus.publish(Event::sync(wm_g, wm_c, chrono::Utc::now().timestamp()));
    true
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
/// Claim the single account binding and start the background index build.
///
/// `pub` because a **test harness** may need to restore this precondition: conformance cases do
/// not currently guarantee independence, and one of them deregisters the account — anything
/// after it then runs against a server with no account. `weflow-server` exposes its equivalent
/// (`start_account`) the same way, so this also removes an inconsistency between the two.
/// The library boundary (narrowing what is `pub`) is a separate, later concern.
pub fn begin_indexing(state: &AppState, qq: &str) -> BindOutcome {
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
        // a path segment — same containment rule as the media route, from the
        // shared module rather than a local copy that had drifted to a subset
        // of the checks (this one missed `:`, which makes `Path::join` discard
        // `root` outright, and the Windows trailing-dot case).
        if !crate::pathsafe::safe_segment(talker) {
            tracing::warn!("[deregister] 跳过异常 talker 目录名: {talker:?}");
            continue;
        }
        let dir = root.join(talker);
        for kind in EXPORT_KINDS {
            let sub = dir.join(kind);
            // This is a recursive delete, and it is the one path here that has
            // no read-side backstop to catch a mistake. Assert the parent really
            // resolves under the export root before removing anything: a
            // junction planted at `<root>/<talker>` would otherwise be followed
            // out of the export directory.
            if !crate::pathsafe::is_contained(root, &sub) {
                tracing::warn!("[deregister] 跳过越界导出目录: {}", sub.display());
                continue;
            }
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
    state.bus.history().lock().clear_items();
    // 代号与清条目**同时**推进：客户端据此区分「注销后新账号刚开始」与「自己漏收了」。
    // 事件 id 计数器**不动** —— 见 `GENERATION` 的说明。
    bump_generation();
    state.bus.publish(Event::sync(0, 0, chrono::Utc::now().timestamp()));

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
    let bus = state.bus.clone();
    let info_for_build = info.clone();
    let key_for_build = key.clone();
    let cancelled_in_build = cancelled.clone();
    // `Ok(None)` = cancelled mid-build (deregistered), as distinct from
    // `Err` = the build genuinely failed.
    let result = tokio::task::spawn_blocking(move || -> Result<Option<(Arc<Mutex<LiveReader>>, usize)>> {
        let mut reader = LiveReader::new(info_for_build.path.clone(), key_for_build.clone());
        reader.open()?; // verify the key now — bad key → error state (unchanged UX)
        let conn = reader.acquire()?;
        // 媒体根与群名映射都在 `build_with` 里处理（嵌入者走同一段）。
        let nt_db_dir = info_for_build
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        // 与嵌入者走**同一段**：建索引 + 载群名映射的次序有讲究，抄一份就会漂移。
        let st = index::build_with(conn, nt_db_dir, &key_for_build)?;
        let count: usize = st.convs.values().map(|c| c.msgs.len()).sum();
        // Cancelled during the build (decrypt + index is the slow part) —
        // drop the freshly built index instead of installing it. The check
        // and the swap are one critical section inside install_index, so no
        // deregistration can slip between them.
        if !install_index(&store, &bus, st, cancelled_in_build) {
            return Ok(None);
        }
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
                state.bus.clone(),
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
///
/// `cli` 打开时起服务走 [`run_with`]（配置由子命令面解析），所以这个便捷入口只在关掉 CLI 时
/// 被用到 —— 那不是「它没用了」。allow 只在那一种 feature 组合下生效。
#[cfg_attr(feature = "cli", allow(dead_code))]
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
    // Only the COUNT is logged, never the QQ numbers. `/health` pays a
    // type-level price to avoid enumerating accounts without a token
    // (`AccountPhase` has no `AwaitingKey` variant precisely so a scanned
    // account cannot leak through the unauthenticated endpoint) — printing the
    // list here would route around that for anyone who can read the log. The
    // numbers stay available to authenticated callers via
    // `GET /api/v1/accounts`.
    if accounts.is_empty() {
        tracing::info!("[init] 未发现本机 QQ 账号目录（客户端可显式传 db_path 注册）");
    } else {
        tracing::info!(
            "[init] 发现 {} 个账号目录，等待注册（清单见 GET /api/v1/accounts，需鉴权）",
            accounts.len()
        );
    }

    // ---- state -----------------------------------------------------------
    let store = Arc::new(RwLock::new(Store::default()));
    let bus = EventBus::new(1024);
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
        bus: bus.clone(),
        accounts: accounts_state.clone(),
        ready: ready.clone(),
        token: Arc::new(token.clone()),
        sync: sync_engine.clone(),
        init: AccountRegistry::new(accounts, watch_cfg, shutdown_rx.clone()),
        export_root,
        base_url,
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

// `AppState` 从 `store` 搬到这里。它本来就是**服务层**类型：字段引用的是 `AccountState` /
// `AccountRegistry` / `HistoryBuf`，全在 `server` 里 —— 之前放在 `store` 只是历史原因，
// 代价是「核心面依赖可选面」（`--no-default-features` 下必须为它加 cfg 才能编译）。
//
// 搬家之后那条 cfg 不再需要：它就在 `server` 里，而 `server` 本来就随 feature 门控。

/// Shared application state handed to the HTTP layer and poller tasks.
///
/// 不需要 `#[cfg(feature = "server")]`：本模块本身就随该 feature 门控（上面的注释解释了
/// 搬家之后那条 cfg 为什么是多余的）。
pub struct AppState {
    pub store: Arc<RwLock<Store>>,
    /// 进程级事件总线（weflow-server 同构）：重放历史 ＋ 广播通道，**绑成一件**。
    /// 历史与通道必须成对：把裸 `Sender` 交给生产者，它就会忘记写历史 —— 而忘记的后果
    /// （重放窗口里没有断线期间的事件）要等第一次重连才显形，见 `sync::history`。
    pub bus: EventBus,
    /// One entry per loaded account: qq number -> readiness state.
    pub accounts: Arc<RwLock<Vec<crate::server::AccountState>>>,
    /// True once all account indexes are built.
    pub ready: Arc<std::sync::atomic::AtomicBool>,
    /// Access token (Bearer header / access_token query / POST body).
    pub token: Arc<String>,
    /// Per-account sync engines; powers the manual-sync endpoint and the
    /// change-driven poll tasks.
    pub sync: Arc<sync::SyncEngine>,
    /// Client-driven account registry (paths, watch config, shutdown).
    pub init: crate::server::AccountRegistry,
    /// Media export root (`media=1` on /api/v1/messages copies here, WeFlow
    /// exportPath semantics); `--media-export-dir`, default `<data-dir>/api-media`.
    pub export_root: Arc<std::path::PathBuf>,
    /// Base URL for exported media links (`http://{host}:{port}`).
    pub base_url: Arc<String>,

    /// Shutdown broadcast. Live SSE streams subscribe so they can end
    /// themselves rather than holding the graceful drain open for the whole
    /// grace period.
    pub shutdown: tokio::sync::watch::Sender<bool>,
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
            bus: EventBus::new(16),
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
            shutdown: shutdown_tx,
        })
    }

    /// 取消与安装的**互斥性**：epoch 重验发生在写锁临界区内（检查与交换不可分两步）。
    ///
    /// 时序钉法：主线程占住 store 的写锁（模拟「注销已 bump epoch、还没清空」的半途），
    /// 安装线程此时必须被挡在锁外；主线程随后 bump 并放锁。若判据在**取锁前**求值
    /// （被删除的两步写法），它会看到尚未 bump 的旧 epoch 放行自己，把索引落进
    /// 刚被清空的 store 并向所有订阅者广播幽灵账号的水位。
    #[test]
    fn cancelled_build_cannot_install_after_deregister() {
        let store = Arc::new(RwLock::new(Store::default()));
        let bus = EventBus::new(16);
        let epoch = Arc::new(std::sync::atomic::AtomicU64::new(7));
        let (done_tx, done_rx) = std::sync::mpsc::channel::<bool>();
        // 主线程占住写锁。
        let guard = store.write();
        let store2 = store.clone();
        let bus2 = bus.clone();
        let epoch2 = epoch.clone();
        let builder = std::thread::spawn(move || {
            let st = Store { watermark_group: 42, ..Store::default() };
            let installed = install_index(&store2, &bus2, st, move || {
                epoch2.load(Ordering::SeqCst) != 7
            });
            let _ = done_tx.send(installed);
        });
        // 让 builder 确实走到锁上排队，再演「注销发生」。
        std::thread::sleep(std::time::Duration::from_millis(60));
        epoch.fetch_add(1, Ordering::SeqCst);
        drop(guard);
        let installed = done_rx.recv_timeout(std::time::Duration::from_secs(5)).expect("builder settles");
        builder.join().unwrap();
        assert!(!installed, "重验必须在锁内：拿到锁时 epoch 已变，安装必须被拒");
        assert_eq!(store.read().watermark_group, 0, "store 保持注销后的空态");
    }

    #[tokio::test]
    async fn install_index_rebaselines_subscribers() {
        let store = Arc::new(RwLock::new(Store::default()));
        let bus = EventBus::new(16);
        let mut rx = bus.subscribe();
        let st = Store { watermark_group: 42, watermark_c2c: 7, ..Store::default() };
        install_index(&store, &bus, st, || false);
        assert_eq!(store.read().watermark_group, 42, "store replaced");
        let ev = rx.try_recv().expect("build completion broadcasts a sync baseline").event;
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
    /// 造一个只关心 `event` 名的测试事件 —— 缓冲现在存的是**原始事件**（不是序列化后的载荷），
    /// 所以测试要构造事件而不是 JSON。
    fn test_event(name: &str) -> Event {
        let mut ev = Event::sync(1, 2, 1_700_000_000);
        ev.event = name.to_string();
        ev
    }

    #[test]
    fn clear_items_drops_events_but_keeps_the_id_counter() {
        let mut h = HistoryBuf::default();
        assert_eq!(h.append(test_event("message.new")), 1);
        assert_eq!(h.append(test_event("message.new")), 2);
        assert_eq!(h.replay_since(0).len(), 2);
        h.clear_items();
        assert!(h.replay_since(0).is_empty(), "buffered events are gone");
        assert_eq!(h.append(test_event("sync")), 3, "ids keep climbing");
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
                crate::store::Conversation { talker: "g1".into(), ..Default::default() },
            );
        }
        update_ready(&state);
        assert!(state.ready.load(Ordering::SeqCst));
        let mut rx = state.bus.subscribe();

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
        let ev = rx.try_recv().expect("deregistration broadcasts a reset baseline").event;
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
            bus: EventBus::new(16),
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
