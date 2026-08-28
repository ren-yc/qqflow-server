//! HTTP layer: axum router with WeFlow-compatible endpoints, plus the
//! client-driven account initialization machinery.

pub mod auth;
pub mod error;
pub mod handlers;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::routing::{get, post};
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

/// Per-account readiness state (serialized as-is into /health).
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

/// Per-account readiness (exposed via /health and used for startup gating).
#[derive(Debug, Clone, Serialize)]
pub struct AccountState {
    pub qq: String,
    pub state: AccountStatus,
    pub message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
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
        Self { accounts_db: Mutex::new(accounts), watch_cfg, shutdown }
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
        .route("/api/v1/accounts", post(accounts::handler))
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

/// Insert or update one account's health state entry.
fn set_account_state(state: &AppState, qq: &str, status: AccountStatus, count: usize, error: Option<String>) {
    let mut accs = state.accounts.write();
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

/// Flip an account to `indexing` atomically with the idempotency guard: a
/// concurrent duplicate registration serializes here and observes the new
/// state instead of spawning a second initialization. Returns the status
/// that blocked (already ready / already indexing).
fn begin_indexing(state: &AppState, qq: &str) -> Option<AccountStatus> {
    let mut accs = state.accounts.write();
    match accs.iter_mut().find(|a| a.qq == qq) {
        Some(a) if a.state.is_ready() => Some(AccountStatus::Ready),
        Some(a) if matches!(a.state, AccountStatus::Indexing) => Some(AccountStatus::Indexing),
        Some(a) => {
            a.state = AccountStatus::Indexing;
            a.message_count = 0;
            a.error = None;
            None
        }
        None => {
            accs.push(AccountState {
                qq: qq.to_string(),
                state: AccountStatus::Indexing,
                message_count: 0,
                error: None,
            });
            None
        }
    }
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

/// Full per-account initialization: open the LIVE source read-only, verify
/// the key, build the index (blocking pool), SSE baseline broadcast,
/// `AccountSync` registration, watch task. No copies, no mirror dir.
/// On failure the account enters the `error` state with the reason —
/// recoverable by posting a corrected registration to /api/v1/accounts.
/// The caller (the registration handler) has already flipped the account
/// to `indexing` synchronously so /health shows it immediately.
pub async fn init_account(state: &Arc<AppState>, info: DbInfo, key: String) {
    let qq = info.qq.clone();

    let store = state.store.clone();
    let tx = state.events.clone();
    let info_for_build = info.clone();
    let key_for_build = key.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<(Arc<Mutex<LiveReader>>, usize)> {
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
        install_index(&store, &tx, st);
        Ok((Arc::new(Mutex::new(reader)), count))
    })
    .await;

    match result {
        Ok(Ok((reader, count))) => {
            let watch_dir = info
                .path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf();
            let account = Arc::new(sync::AccountSync::new(
                reader,
                state.store.clone(),
                state.events.clone(),
                info.path.clone(),
                watch_dir.clone(),
                key.clone(),
            ));
            state.sync.register(account.clone());
            tokio::spawn(sync::watch::spawn(
                account,
                watch_dir,
                state.init.watch_cfg.clone(),
                state.init.shutdown.clone(),
            ));
            set_account_state(state, &qq, AccountStatus::Ready, count, None);
            tracing::info!("[init] QQ {qq} 索引完成: {count} 条消息");
        }
        Ok(Err(e)) => {
            set_account_state(state, &qq, AccountStatus::Error, 0, Some(format!("{e:#}")));
            tracing::warn!("[init] QQ {qq} 初始化失败（重新注册可恢复）: {e:#}");
        }
        Err(e) => {
            set_account_state(state, &qq, AccountStatus::Error, 0, Some(format!("index task panicked: {e}")));
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
        assert_eq!(begin_indexing(&state, "10001"), None, "first registration proceeds");
        assert_eq!(
            begin_indexing(&state, "10001"),
            Some(AccountStatus::Indexing),
            "duplicate registration observes indexing"
        );
        set_account_state(&state, "10001", AccountStatus::Ready, 7, None);
        assert_eq!(
            begin_indexing(&state, "10001"),
            Some(AccountStatus::Ready),
            "ready accounts stay ready"
        );
    }
}
