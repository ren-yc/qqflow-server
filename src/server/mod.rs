//! HTTP layer: axum router with WeFlow-compatible endpoints, plus the
//! client-driven account initialization machinery.

pub mod auth;
pub mod error;
pub mod handlers;

use std::path::PathBuf;
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
use crate::db::mirror::Mirror;
use crate::db::scan::DbInfo;
use crate::keystore::KeyStore;
use crate::sync;
use crate::sync::Event;
use crate::store::index;
use crate::store::{AppState, Store};

/// Per-account readiness (exposed via /health and used for startup gating).
/// States: `awaiting_key` (scanned, no key yet) | `indexing` | `ready` |
/// `error` (initialization failed — a corrected registration recovers).
#[derive(Debug, Clone, Serialize)]
pub struct AccountState {
    pub qq: String,
    pub state: String,
    pub message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

/// Runtime per-account registration machinery (client-driven startup).
pub struct AccountRegistry {
    /// All known accounts: platform-scan results plus client registrations.
    pub accounts_db: Mutex<Vec<DbInfo>>,
    /// Client-supplied SQLCipher keys (memory only, never persisted).
    pub key_store: Mutex<KeyStore>,
    /// Mirror workspace root (`<data-dir>/mirror`).
    pub mirror_root: PathBuf,
    /// Watch behavior handed to deferred watch tasks.
    pub watch_cfg: crate::sync::watch::WatchConfig,
    /// Shutdown signal receiver (cloned per deferred watch task).
    pub shutdown: tokio::sync::watch::Receiver<bool>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    use handlers::*;
    Router::new()
        .route("/health", get(health::handler).post(health::handler))
        .route("/api/v1/health", get(health::handler).post(health::handler))
        .route("/api/v1/accounts", post(accounts::handler))
        .route("/api/v1/messages", get(messages::handler).post(messages::handler))
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
fn set_account_state(state: &AppState, qq: &str, status: &str, count: usize, error: Option<String>) {
    let mut accs = state.accounts.write();
    match accs.iter_mut().find(|a| a.qq == qq) {
        Some(a) => {
            a.state = status.into();
            a.message_count = count;
            a.error = error;
        }
        None => accs.push(AccountState {
            qq: qq.into(),
            state: status.into(),
            message_count: count,
            error,
        }),
    }
}

/// Global readiness = at least one account and every account `ready`.
pub fn update_ready(state: &AppState) {
    let accs = state.accounts.read();
    let all_ready = !accs.is_empty() && accs.iter().all(|a| a.state == "ready");
    state.ready.store(all_ready, Ordering::SeqCst);
}

/// Full per-account initialization: mirror + decrypt + index build (blocking
/// pool), SSE baseline broadcast, `AccountSync` registration, watch task.
/// On failure the account enters the `error` state with the reason —
/// recoverable by posting a corrected registration to /api/v1/accounts.
pub async fn init_account(state: &Arc<AppState>, info: DbInfo, key: String) {
    let qq = info.qq.clone();
    set_account_state(state, &qq, "indexing", 0, None);

    let store = state.store.clone();
    let tx = state.events.clone();
    let mirror_root = state.init.mirror_root.clone();
    let info_for_build = info.clone();
    let key_for_build = key.clone();
    let result = tokio::task::spawn_blocking(move || -> Result<(Mirror, usize)> {
        let mirror = Mirror::new(&info_for_build, &mirror_root)?;
        let conn = db::decrypt::open_decrypted(&mirror.main_path, &key_for_build)?;
        let st = index::build_index(&conn)?;
        let count: usize = st.convs.values().map(|c| c.msgs.len()).sum();
        install_index(&store, &tx, st);
        Ok((mirror, count))
    })
    .await;

    match result {
        Ok(Ok((mirror, count))) => {
            let account = Arc::new(sync::AccountSync::new(
                Arc::new(Mutex::new(mirror)),
                key,
                state.store.clone(),
                state.events.clone(),
            ));
            state.sync.register(account.clone());
            let watch_dir = info
                .path
                .parent()
                .unwrap_or_else(|| std::path::Path::new("."))
                .to_path_buf();
            tokio::spawn(sync::watch::spawn(
                account,
                watch_dir,
                state.init.watch_cfg.clone(),
                state.init.shutdown.clone(),
            ));
            set_account_state(state, &qq, "ready", count, None);
            tracing::info!("[init] QQ {qq} 索引完成: {count} 条消息");
        }
        Ok(Err(e)) => {
            set_account_state(state, &qq, "error", 0, Some(format!("{e:#}")));
            tracing::warn!("[init] QQ {qq} 初始化失败（重新注册可恢复）: {e:#}");
        }
        Err(e) => {
            set_account_state(state, &qq, "error", 0, Some(format!("index task panicked: {e}")));
            tracing::error!("[init] QQ {qq} 初始化任务异常: {e}");
        }
    }
    update_ready(state);
}

/// Full startup: parse CLI args, load token, scan accounts (discovery only),
/// bind the server and wait for client-driven registrations. Runs until Ctrl-C.
pub async fn serve() -> Result<()> {
    let cfg = config::load()?;
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
    let accounts_state = Arc::new(RwLock::new(Vec::<AccountState>::new()));
    let ready = Arc::new(AtomicBool::new(false));
    let sync_engine = Arc::new(sync::SyncEngine::new());
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let watch_cfg = crate::sync::watch::WatchConfig {
        debounce: std::time::Duration::from_millis(cfg.watch_debounce_ms),
        fallback: (cfg.watch_fallback_ms > 0)
            .then(|| std::time::Duration::from_millis(cfg.watch_fallback_ms)),
    };
    let init_registry = Arc::new(AccountRegistry {
        accounts_db: Mutex::new(accounts),
        key_store: Mutex::new(KeyStore::default()),
        mirror_root: data_dir.join("mirror"),
        watch_cfg,
        shutdown: shutdown_rx.clone(),
    });
    let state = Arc::new(AppState {
        store: store.clone(),
        events: tx.clone(),
        accounts: accounts_state.clone(),
        ready: ready.clone(),
        token: Arc::new(token.clone()),
        sync: sync_engine.clone(),
        init: init_registry.clone(),
    });

    // Scanned accounts are listed as awaiting keys; initialization is
    // entirely client-driven via POST /api/v1/accounts.
    for a in init_registry.accounts_db.lock().iter() {
        accounts_state.write().push(AccountState {
            qq: a.qq.clone(),
            state: "awaiting_key".into(),
            message_count: 0,
            error: None,
        });
    }
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
    // Remove mirror dirs (they only hold SQLCipher ciphertext, but keep
    // the workspace tidy).
    let _ = std::fs::remove_dir_all(data_dir.join("mirror"));
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;

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
        let store = Arc::new(RwLock::new(Store::default()));
        let (tx, _) = broadcast::channel::<Event>(16);
        let accounts = Arc::new(RwLock::new(vec![AccountState {
            qq: "10001".into(),
            state: "awaiting_key".into(),
            message_count: 0,
            error: None,
        }]));
        let ready = Arc::new(AtomicBool::new(false));
        let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
        let state = AppState {
            store: store.clone(),
            events: tx.clone(),
            accounts: accounts.clone(),
            ready: ready.clone(),
            token: Arc::new("t".into()),
            sync: Arc::new(sync::SyncEngine::new()),
            init: Arc::new(AccountRegistry {
                accounts_db: Mutex::new(Vec::new()),
                key_store: Mutex::new(KeyStore::default()),
                mirror_root: std::env::temp_dir().join("qqflow_server_test_mirror"),
                watch_cfg: crate::sync::watch::WatchConfig {
                    debounce: std::time::Duration::from_millis(350),
                    fallback: None,
                },
                shutdown: shutdown_rx,
            }),
        };
        assert!(!ready.load(Ordering::SeqCst), "awaiting_key is not ready");
        set_account_state(&state, "10001", "ready", 7, None);
        update_ready(&state);
        assert!(ready.load(Ordering::SeqCst), "all ready flips the flag");
    }
}
