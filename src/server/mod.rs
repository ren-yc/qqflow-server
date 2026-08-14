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

/// Global readiness = at least one account and every account `ready`.
pub fn update_ready(state: &AppState) {
    let accs = state.accounts.read();
    let all_ready = !accs.is_empty() && accs.iter().all(|a| a.state.is_ready());
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
        let media_root = nt_db_dir.parent().map(|p| p.join("nt_data"));
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
    crate::logging::init(&cfg.log);
    run_with(cfg).await
}

pub async fn run_with(cfg: config::Config) -> Result<()> {
    let data_dir = config::data_dir()?;
    let token = config::load_or_create_token(&data_dir)?;

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
    });
    update_ready(&state);

    // ---- server (bind early; /health reports "starting") ------------------
    let app = build_router(state.clone());
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!("[init] 服务启动: http://{addr}  (token 已生成/加载)");
    tracing::info!("[init] 等待客户端注册账号: POST /api/v1/accounts {{\"qq\", \"key\", \"db_path\"}}");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server error: {e}");
        }
    });

    // ---- shutdown ----------------------------------------------------------
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("收到退出信号，清理中…");
    shutdown_tx.send(true).ok();
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

    /// `AppState` with one `10001` account in the given status.
    fn state_with_account(status: AccountStatus) -> Arc<AppState> {
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        Arc::new(AppState {
            store: Arc::new(RwLock::new(Store::default())),
            events: broadcast::channel::<Event>(16).0,
            accounts: Arc::new(RwLock::new(vec![AccountState {
                qq: "10001".into(),
                state: status,
                message_count: 0,
                error: None,
            }])),
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
    fn update_ready_requires_all_accounts_ready() {
        let state = state_with_account(AccountStatus::AwaitingKey);
        assert!(!state.ready.load(Ordering::SeqCst), "awaiting_key is not ready");
        set_account_state(&state, "10001", AccountStatus::Ready, 7, None);
        update_ready(&state);
        assert!(state.ready.load(Ordering::SeqCst), "all ready flips the flag");
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
