//! HTTP layer: axum router with WeFlow-compatible endpoints, plus the
//! client-driven account initialization machinery.

pub mod auth;
pub mod cursor;
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
    /// 建索引期间的共享计数；`indexing` 之外没有观察意义。
    ///
    /// `#[serde(skip)]`：它不是响应形状的一部分——响应由 `AccountView` 投影，
    /// 计数只在这里存放一份，供 `begin_indexing` 创建、`init_account` 复用同一 `Arc`。
    #[serde(skip)]
    pub progress: Option<std::sync::Arc<crate::store::index::IndexProgress>>,
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
/// 一次注册的身份令牌：`(epoch, registration_id)` 二元组。
///
/// 只靠全局 `epoch` 不够——它是**注销**计数，旧任务在注销后启动会读到**新**
/// 值而全部通过取消检查（这正是复活缺陷的机制）。`registration_id` 由
/// `begin_indexing` 在取得绑定的**同一临界区**内铸造并随任务传递：注销时两个
/// 计数都被推进，任何在飞任务的旧令牌在两处校验点都必失败。任务内部**不得**
/// 重新推断自己属于哪次注册。
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct RegistrationToken {
    pub epoch: u64,
    pub registration_id: u64,
}

impl RegistrationToken {
    /// The cancellation gate: true when this token no longer describes the
    /// current registration (deregistered, or a later registration took over).
    fn is_cancelled(&self, registry: &AccountRegistry) -> bool {
        registry.epoch.load(Ordering::SeqCst) != self.epoch
            || registry.registration.load(Ordering::SeqCst) != self.registration_id
    }
}

#[derive(Debug, PartialEq)]
pub enum BindOutcome {
    /// The binding is now held by this qq and `indexing` was set. The token
    /// was minted inside the same critical section that claimed the binding;
    /// the background build must carry it and verify it at every point where
    /// its work becomes visible.
    Bound(RegistrationToken),
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

/// 进程级 nonce：游标里带上它，重启后旧游标一律按「需要重拉」处理。
///
/// 内存索引的行标识只在**本进程**内连续；跨进程沿用等于按一张新表的位置去读
/// 旧表。用随机值而不是 pid + 启动时间：后者在同一秒内重启会撞。
pub fn process_nonce() -> &'static str {
    static NONCE: std::sync::OnceLock<String> = std::sync::OnceLock::new();
    NONCE.get_or_init(|| {
        use rand::RngCore;
        let mut raw = [0u8; 8];
        rand::thread_rng().fill_bytes(&mut raw);
        let mut hex = String::with_capacity(16);
        for b in raw {
            hex.push_str(&format!("{b:02x}"));
        }
        hex
    })
}

pub use crate::sync::history::{EventBus, HistoryBuf, HistoryItem, Stamped};


/// Runtime per-account registration machinery (client-driven startup).
pub struct AccountRegistry {
    /// Serializes binding, index installation, sync/watch publication and removal.
    /// Lock order: lifecycle -> engine registry / accounts / store -> event history.
    /// Readers never acquire lifecycle while holding any of those inner locks.
    lifecycle: Mutex<()>,
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
    /// Bumped by every `begin_indexing` **inside the accounts write-lock
    /// critical section that claims the binding**. Together with `epoch`
    /// this forms the registration token handed to the background build:
    /// a queued `init_account` that starts after its registration was
    /// deregistered (and another one claimed the binding) reads the NEW
    /// counters and — under an epoch-only check — would pass every
    /// cancellation gate and resurrect the account. The token is created
    /// where the binding is claimed and verified everywhere the build
    /// becomes visible; it is never re-inferred inside the task.
    pub registration: std::sync::atomic::AtomicU64,
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
            lifecycle: Mutex::new(()),
            accounts_db: Mutex::new(accounts),
            scanned,
            epoch: std::sync::atomic::AtomicU64::new(0),
            registration: std::sync::atomic::AtomicU64::new(0),
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
        reason: None,
    }
}

/// Install a built index only while its registration is current. Lifecycle
/// serializes with binding changes; the store write lock also orders this
/// baseline against sync publication and the deregistration reset.
/// Regression: `cancelled_build_cannot_install_after_deregister`.
fn install_index(
    lifecycle: &Mutex<()>,
    store: &Arc<RwLock<Store>>,
    bus: &EventBus,
    st: Store,
    cancelled: impl Fn() -> bool,
) -> bool {
    let _lifecycle = lifecycle.lock();
    let mut guard = store.write();
    if !cancelled() {
        return false;
    }
    *guard = st;
    let (wm_g, wm_c) = (guard.watermark_group, guard.watermark_c2c);
    // publish 就在这把 store 写锁仍被持有时发出：guard 直到本函数返回才 drop。
    bus.publish(Event::sync(wm_g, wm_c, chrono::Utc::now().timestamp()));
    drop(guard);
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
            progress: None,
        }),
    }
}

/// `indexing` 态才给进度，其余态整块省略。
///
/// 读的是账号状态里那个 `Arc` 的原子量，不碰建索引路径的锁——否则「问一句到哪了」
/// 会一直等到建完。
pub(crate) fn indexing_progress(
    a: &AccountState,
) -> Option<crate::server::dto::IndexProgressDto> {
    if a.state == AccountStatus::Indexing {
        a.progress.as_ref().map(|p| progress_dto(p.snapshot()))
    } else {
        None
    }
}

pub(crate) fn progress_dto(
    s: crate::store::index::ProgressSnapshot,
) -> crate::server::dto::IndexProgressDto {
    crate::server::dto::IndexProgressDto {
        messages_indexed: s.messages_indexed,
        tables_done: s.tables_done,
        tables_total: s.tables_total,
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
    token: RegistrationToken,
    status: AccountStatus,
    count: usize,
    error: Option<String>,
) -> bool {
    let _lifecycle = state.init.lifecycle.lock();
    let mut accs = state.accounts.write();
    // 锁内双计数校验：epoch 或 registration 任一变化都拒绝——「注销后重注册」
    // 只推进 registration 时的旧任务同样不能借新注册的名义写状态。
    if token.is_cancelled(&state.init) {
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
    begin_indexing_with_source(state, qq, None)
}

fn begin_indexing_with_source(state: &AppState, qq: &str, source: Option<DbInfo>) -> BindOutcome {
    let _lifecycle = state.init.lifecycle.lock();
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
            a.progress = Some(std::sync::Arc::new(
                crate::store::index::IndexProgress::default(),
            ));
        }
        None => accs.push(AccountState {
            qq: qq.to_string(),
            state: AccountStatus::Indexing,
            message_count: 0,
            error: None,
            progress: Some(std::sync::Arc::new(
                crate::store::index::IndexProgress::default(),
            )),
        }),
    }
    // 铸造令牌：与「取得绑定」同一临界区——`registration` 推进发生在仍持有
    // accounts 写锁的时刻，此刻任何已派发的后台任务持有的都是旧令牌。
    let registration_id = state.init.registration.fetch_add(1, Ordering::SeqCst) + 1;
    let minted = RegistrationToken {
        epoch: state.init.epoch.load(Ordering::SeqCst),
        registration_id,
    };
    if let Some(info) = source {
        state.init.upsert_db(info);
    }
    state.ready.store(false, Ordering::SeqCst);
    BindOutcome::Bound(minted)
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
    let _lifecycle = state.init.lifecycle.lock();
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

    // Invalidate and clear under the store lock. A previously snapshotted sync
    // handle either finishes publication first, or sees stopped/current=false.
    // The outer lifecycle gate excludes a new binding until reset completes.
    let (talkers, index_cleared) = {
        let mut guard = state.store.write();
        state.init.epoch.fetch_add(1, Ordering::SeqCst);
        // registration 同步推进：任何还持有旧注册令牌的后台任务（含「排队中、
        // 尚未首次执行」的 init_account）在下一个校验点全部失效——只推 epoch
        // 时，注销后启动的旧任务会读到新 epoch 而通过全部检查（复活缺陷的根）。
        state.init.registration.fetch_add(1, Ordering::SeqCst);
        let talkers: Vec<String> = if purge_media {
            guard.convs.values().map(|c| c.talker.clone()).collect()
        } else {
            Vec::new()
        };
        let had_index = !guard.convs.is_empty();
        *guard = Store::default();
        (talkers, had_index)
    };
    // 清重放条目排在**清空索引之后**：install_index 的基线发布也在 store 写锁内，
    // 两种交错都被锁挡住（要么 install 先完成、这里连它的基线一起清掉，要么 install
    // 被 epoch 重验拒掉、根本没发布）—— 幽灵基线不可能留在归零基线之后。
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

/// Publish a built account, its watcher and readiness as one lifecycle commit.
/// Slow database work has already finished; no lock is held across an await.
fn activate_account(
    state: &Arc<AppState>,
    token: RegistrationToken,
    account: Arc<sync::AccountSync>,
    count: usize,
    spawn_watcher: impl FnOnce() -> tokio::task::JoinHandle<Result<()>>,
) -> bool {
    let current = {
        let weak = Arc::downgrade(state);
        move || weak.upgrade().is_some_and(|s| !token.is_cancelled(&s.init))
    };
    account.set_registration_guard(Box::new(current.clone()));
    // 换库处置：轮询发现数据源身份变化时，账号转 Error 待重注册（重注册 =
    // 全量重建，是唯一能兑现「新库、新水位线」的路径）。闭包持 Weak（理由同
    // registration_guard：持 Arc 会形成引用环），捕获的只有 qq 的 String、
    // 账号句柄的 Arc 与令牌副本。先验注册令牌再写状态：旧注册的换库发现不能
    // 改写新注册的状态。
    //
    // 处置是**整套退役**而不只是写状态：只把账号翻成 Error 时，旧句柄仍留在
    // SyncEngine 注册表里——客户端按提示重注册同一 QQ，activate_account 的
    // register_if_current 会因「已有同 qq」拒绝，重注册永远完不成。按句柄身份
    // （Arc::ptr_eq）从注册表移除旧条目并停掉 watcher，重注册才能装新句柄；
    // 身份比对保证这个退役只命中本句柄自己，绝不波及后来注册的新账号。
    //
    // 退役与状态写入**各自持锁**，不在一把大锁内做完：set_account_state_if_current
    // 自己拿 lifecycle 锁，先拿锁再调它会同线程重入死锁（parking_lot Mutex 不可
    // 重入）。原子性由令牌校验保证——本处置运行期间插入的重注册推进 registration，
    // 令牌校验让写状态与退役都不产生可见效果（退役按句柄身份，新注册的句柄不是
    // 这个 Arc，不会被误伤）。
    let on_replace_state = Arc::downgrade(state);
    let on_replace_qq = account.qq.clone();
    let on_replace_handle = account.clone();
    account.set_on_source_replaced(Box::new(move || {
        let Some(state) = on_replace_state.upgrade() else { return };
        if token.is_cancelled(&state.init) {
            tracing::debug!("[sync] 数据源替换发现来自已失效的注册，处置丢弃");
            return;
        }
        let retired = state.sync.unregister_if(&on_replace_handle);
        if set_account_state_if_current(
            &state,
            &on_replace_qq,
            token,
            AccountStatus::Error,
            0,
            Some(
                "数据源已被替换（库文件身份变化）：增量续读无法保证不漏，请重新注册以全量重建"
                    .to_string(),
            ),
        ) {
            // 账号离开 Ready 的同一次处置里刷新全局 readiness：只写状态不清 ready
            // 时，依赖该标志的请求仍会把旧索引当可用。
            update_ready(&state);
            tracing::warn!(
                retired = retired,
                "[sync] 数据源已被替换，账号转 Error 待重新注册（旧同步句柄已退役）"
            );
        }
    }));
    let _lifecycle = state.init.lifecycle.lock();
    if !state.sync.register_if_current(account.clone(), &current) {
        account.stop();
        return false;
    }
    if !state.sync.attach_watcher_if_current(&account, spawn_watcher(), &current) {
        state.sync.unregister_if(&account);
        return false;
    }
    write_account_state(&mut state.accounts.write(), &account.qq, AccountStatus::Ready, count, None);
    update_ready(state);
    true
}

/// Full per-account initialization: open the LIVE source read-only, verify
/// the key, build the index (blocking pool), SSE baseline broadcast,
/// `AccountSync` registration, watch task. No copies, no mirror dir.
/// On failure the account enters the `error` state with the reason —
/// recoverable by posting a corrected registration to /api/v1/accounts.
/// The caller (the registration handler) has already flipped the account
/// to `indexing` synchronously so /health shows it immediately.
pub async fn init_account(state: &Arc<AppState>, info: DbInfo, key: String, token: RegistrationToken) {
    init_account_inner(state, info, key, token, None).await;
}

/// `init_account` 的测试编排变体：`build_gate` 在构建闭包**采完基线之后、开始
/// 解密建索引之前**被调用（阻塞后台构建线程，直到测试放行）。测试用它把「换库
/// 覆写」精确插进「基线已采、install 未跑」的窗口——大语料拉长构建的编排
/// （`build_time_source_replacement_discards_the_index` 的旧形态）依赖时序运气，
/// CI 的 Windows 机器上覆写可能晚于 install 复查，回归等不到 Error 超时。
#[cfg(feature = "testing")]
pub async fn init_account_with_build_gate(
    state: &Arc<AppState>,
    info: DbInfo,
    key: String,
    token: RegistrationToken,
    build_gate: Box<dyn FnOnce() + Send>,
) {
    init_account_inner(state, info, key, token, Some(build_gate)).await;
}

async fn init_account_inner(
    state: &Arc<AppState>,
    info: DbInfo,
    key: String,
    token: RegistrationToken,
    build_gate: Option<Box<dyn FnOnce() + Send>>,
) {
    let qq = info.qq.clone();
    // 与 `begin_indexing` 创建的是同一份计数：从账号状态里取回 `Arc`，不要另起一份，
    // 否则 HTTP 侧永远读到零。
    let progress = state
        .accounts
        .read()
        .iter()
        .find(|a| a.qq == qq)
        .and_then(|a| a.progress.clone());
    // 取消判据只认**任务参数带来的令牌**：注销推进 epoch、下一次注册推进
    // registration——两者任一变化都让旧令牌失效。任务启动时重新读计数器的旧形态
    // （本函数曾经的形态）会让「排队中的初始化」在注销后启动时读到新值而全部通过，
    // 把刚被注销的账号复活成 ready（回归：
    // queued_init_cannot_resurrect_after_deregister）。
    // 这个闭包的语义是「**本次注册仍然有效**」（可继续、可写）：install_index 的
    // cancelled 判据、sync 引擎的令牌闸与 set_account_state_if_current 全部共用它。
    // 名字保留 cancelled 是历史形态——install_index 的参数名就叫 cancelled，
    // 「失效才拒绝」与「有效才放行」是同一个谓词的两种读法。
    //
    // 闭包持 **registry 的 Weak**：持 Arc 会形成 AppState → sync → account →
    // guard → AppState 的引用环，句柄与整个注册机器永不释放（Windows 上
    // LiveReader 的文件句柄跟着不关，后续对同一目录的清理重建全部踩在打开的
    // 旧文件上）。registry 死亡时谓词按「已失效」处理。
    let cancelled = {
        let state = Arc::downgrade(state);
        move || {
            match state.upgrade() {
                Some(state) => !token.is_cancelled(&state.init),
                None => false,
            }
        }
    };

    let store = state.store.clone();
    let bus = state.bus.clone();
    let info_for_build = info.clone();
    let key_for_build = key.clone();
    let cancelled_in_build = cancelled.clone();
    let state_for_build = state.clone();
    // `Ok(None)` = cancelled mid-build (deregistered), as distinct from
    // `Err` = the build genuinely failed.
    let result = tokio::task::spawn_blocking(move || -> Result<Option<(Arc<Mutex<LiveReader>>, usize)>> {
        let mut reader = LiveReader::new(info_for_build.path.clone(), key_for_build.clone());
        reader.open()?; // verify the key now — bad key → error state (unchanged UX)
        let conn = reader.acquire()?;
        // **身份基线绑定到构建时的数据源**：open 之后立刻采一次指纹。基线若在
        // 构建完成之后才采（旧形态），构建期间换库时 store 里是 A 的数据、基线
        // 却记成 B——后续采样永不推进世代，换库被整个吞掉（客户端拿不到任何
        // 失效信号）。
        let build_identity = crate::db::source_identity::fingerprint(&info_for_build.path)?;
        // 测试闸门：基线已采、解密建索引未开始的窗口交给测试（见
        // `init_account_with_build_gate`）。生产路径 None，零开销。
        if let Some(gate) = build_gate {
            gate();
        }
        // 媒体根与群名映射都在 `build_with` 里处理（嵌入者走同一段）。
        let nt_db_dir = info_for_build
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."));
        // 与嵌入者走**同一段**：建索引 + 载群名映射的次序有讲究，抄一份就会漂移。
        let st = index::build_with(conn, nt_db_dir, &key_for_build, progress.as_deref())?;
        let count: usize = st.convs.values().map(|c| c.msgs.len()).sum();
        // Cancelled during the build (decrypt + index is the slow part) —
        // drop the freshly built index instead of installing it. The check
        // and the swap are one critical section inside install_index, so no
        // deregistration can slip between them.
        //
        // 安装前在同一临界区内复查指纹：构建期间文件被替换（删库重建 / 备份
        // 还原成另一份）时，这份索引描述的是旧文件——不能装，更不能把替换后
        // 的文件当这份旧索引的基线。指纹失败按「无法证明未变」处理，同样丢弃。
        let identity_unchanged = || {
            match crate::db::source_identity::fingerprint(&info_for_build.path) {
                Ok(fp) => {
                    if fp.digest == build_identity.digest {
                        true
                    } else {
                        tracing::warn!("[init] 安装前指纹与构建时不一致（身份变化）");
                        false
                    }
                }
                Err(e) => {
                    tracing::warn!("[init] 安装前指纹采样失败: {e}；按无法证明未变处理");
                    false
                }
            }
        };
        let install_cancelled = cancelled_in_build.clone();
        let install_identity = identity_unchanged;
        // install_index 的判据参数沿用「cancelled」这一历史形态的名字，实际语义是
        // 「**本次注册仍然有效、且构建时的数据源还是当前文件**」——true 才安装。
        if !install_index(&state_for_build.init.lifecycle, &store, &bus, st, move || {
            let still_valid = install_cancelled();
            let identity_ok = install_identity();
            if !still_valid {
                tracing::warn!("[init] 构建期间注册已失效，丢弃本次索引");
                return false;
            }
            if !identity_ok {
                tracing::warn!(
                    "[init] 构建期间数据源被替换（库文件身份变化），丢弃本次索引；请重新注册以对当前文件重建"
                );
                return false;
            }
            true
        }) {
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
            // 全量构建的基线采样：`poll_once` 每轮都会采，但第一轮 poll 之前
            // 必须先有基线——否则首个新消息触发的 poll 会把「建索引时的文件」
            // 与「此刻的文件」都当首次采样，基线静默换成后者，换库窗口被吞掉。
            account.sample_source_baseline();
            if !activate_account(state, token, account.clone(), count, || {
                tokio::spawn(sync::watch::spawn(
                    account,
                    watch_dir,
                    state.init.watch_cfg.clone(),
                    state.init.shutdown.clone(),
                ))
            }) {
                tracing::info!("[init] QQ {qq} 注册已失效，初始化结果丢弃");
                return;
            }
            tracing::info!("[init] QQ {qq} 索引完成: {count} 条消息");
        }
        Ok(Ok(None)) => {
            // 两种来源都在 install 的判据里留下了日志（注册失效 / 构建期换库）；
            // 这里只确认状态面：换库丢弃时账号仍是 indexing，转 Error 待重注册。
            let still_indexing = state
                .accounts
                .read()
                .iter()
                .find(|a| a.qq == qq)
                .is_some_and(|a| a.state == AccountStatus::Indexing);
            if still_indexing
                && set_account_state_if_current(
                    state,
                    &qq,
                    token,
                    AccountStatus::Error,
                    0,
                    Some("构建期间数据源被替换：索引已丢弃，请重新注册以全量重建".to_string()),
                )
            {
                tracing::warn!("[init] QQ {qq} 构建期换库，账号转 Error 待重新注册");
            } else {
                tracing::info!("[init] QQ {qq} 初始化中被注销，已放弃本次构建");
            }
            return;
        }
        Ok(Err(e)) => {
            if !set_account_state_if_current(
                state,
                &qq,
                token,
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
                token,
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
    // serve 形态：输出 stdout（`cli` 关掉时的老路，判据同 weflow 侧 serve_logs_stay_on_stdout）。
    let _ = crate::logging::init_for_serve(&cfg.log, &cfg.log_format);
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
    // 显式给定的 token 直接用，不碰 OS 凭据库：凭据库是环境依赖而非逻辑依赖 ——
    // 无密钥环守护的环境里服务端会降级成会话级 token，而读方（测试探针、另开的
    // --show-token）在同样的环境里只会持续读到「无 token」，两侧永远对不上
    // （回归：shutdown_ends_a_live_sse_stream_within_the_grace_period 在 CI 的形态）。
    // 拿着 token 起服务的调用方（进程内测试、自带密钥管理的嵌入者）经 Config 传入。
    let token = match cfg.token.clone() {
        Some(t) => t,
        None => config::load_or_create_token()?,
    };

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
                progress: None,
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
    pub(super) fn state_with_accounts(accounts: &[(&str, AccountStatus)]) -> Arc<AppState> {
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
                        progress: None,
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
        let _token = match begin_indexing(&state, "10001") {
            BindOutcome::Bound(t) => t,
            other => panic!("first registration must claim the binding: {other:?}"),
        };
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
        assert!(
            matches!(begin_indexing(&state, "10001"), BindOutcome::Bound(_)),
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

    /// 排队中的 init_task 不得复活已注销的账号：注册令牌在 begin_indexing 的
    /// 临界区内铸造，任务带着它走到每个可见点复查；注销推进两个计数器，旧令牌
    /// 全部失效。修复前的机制：任务启动时**重新读取**全局 epoch——注销后启动
    /// 读到新值，全部取消检查通过，账号被写回 ready。
    #[test]
    fn queued_init_cannot_resurrect_after_deregister() {
        let state = state_with_account(AccountStatus::AwaitingKey);
        let qq = "10001";
        // 铸造令牌（begin_indexing 的 Bound 分支）。
        let token = match begin_indexing(&state, qq) {
            BindOutcome::Bound(t) => t,
            other => panic!("claim the binding first: {other:?}"),
        };
        // 注销完整执行：双计数推进、绑定移除。
        assert!(matches!(
            deregister_account(&state, qq, false),
            DeregisterOutcome::Deregistered { .. }
        ));
        assert!(bound_account(&state.accounts.read()).is_none());

        // 等价于「排队中的 init_task 此时才获得首次执行机会」：用**旧令牌**走
        // 全部可见点——每一个都必须拒绝。
        assert!(
            !set_account_state_if_current(&state, qq, token, AccountStatus::Ready, 8, None),
            "旧令牌不得把注销账号写回 ready"
        );
        assert!(bound_account(&state.accounts.read()).is_none(), "状态写入被拒后绑定仍为空");
        assert!(
            state.store.read().convs.is_empty(),
            "store 不得被旧令牌的索引填充"
        );

        // 注销后立即注册**别的账号**：旧令牌同样失效（registration 已被重注册推进）。
        state.accounts.write().push(AccountState {
            qq: "10002".into(),
            state: AccountStatus::AwaitingKey,
            message_count: 0,
            error: None,
            progress: None,
        });
        assert!(matches!(
            begin_indexing(&state, "10002"),
            BindOutcome::Bound(_)
        ));
        assert!(
            !set_account_state_if_current(&state, "10002", token, AccountStatus::Ready, 8, None),
            "旧令牌不得借新注册的名义写状态"
        );
        let accs = state.accounts.read();
        let bound = bound_account(&accs).unwrap();
        assert_eq!(bound.qq, "10002");
        assert_ne!(bound.state, AccountStatus::Ready, "新账号不被旧令牌写成 ready");
        drop(accs);
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
                progress: None,
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
            !set_account_state_if_current(
                &state,
                "10001",
                RegistrationToken { epoch, registration_id: state.init.registration.load(Ordering::SeqCst) },
                AccountStatus::Ready,
                500,
                None,
            ),
            "a stale build must not resurrect the account"
        );
        assert!(state.accounts.read().is_empty(), "still unbound");
        // A registration started AFTER the deregistration still works: mint a
        // fresh token the way begin_indexing would (both counters advanced).
        let fresh = RegistrationToken {
            epoch: state.init.epoch.load(Ordering::SeqCst),
            registration_id: state.init.registration.load(Ordering::SeqCst),
        };
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

#[cfg(all(test, feature = "testing"))]
mod lifecycle_tests;
