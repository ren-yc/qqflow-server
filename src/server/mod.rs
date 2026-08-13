//! HTTP layer: axum router with WeFlow-compatible endpoints.

pub mod auth;
pub mod error;
pub mod handlers;

use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::Arc;

use anyhow::{Context, Result};
use axum::routing::get;
use axum::Router;
use parking_lot::RwLock;
use serde::Serialize;
use tokio::sync::broadcast;

use crate::config;
use crate::db;
use crate::db::mirror::Mirror;
use crate::keystore::KeyStore;
use crate::sync;
use crate::sync::Event;
use crate::store::index;
use crate::store::{AppState, Store};

/// Per-account readiness (exposed via /health and used for startup gating).
#[derive(Debug, Clone, Serialize)]
pub struct AccountState {
    pub qq: String,
    pub state: String, // "indexing" | "ready" | "error"
    pub message_count: usize,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
}

pub fn build_router(state: Arc<AppState>) -> Router {
    use handlers::*;
    Router::new()
        .route("/health", get(health::handler).post(health::handler))
        .route("/api/v1/health", get(health::handler).post(health::handler))
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

/// Full startup: load config, load keys, scan accounts, build indexes,
/// start pollers, bind the server. Runs until Ctrl-C.
pub async fn serve() -> Result<()> {
    let cfg = config::load()?;
    crate::logging::init(&cfg.log);
    run_with(cfg).await
}

pub async fn run_with(cfg: config::Config) -> Result<()> {
    let data_dir = config::data_dir(cfg.data_dir.as_deref())?;
    let token = config::load_or_create_token(&data_dir, cfg.token.as_deref())?;

    // ---- accounts & keys -------------------------------------------------
    let mut accounts = db::scan::scan_accounts(cfg.db_path.as_deref())?;
    if !cfg.qq.is_empty() {
        accounts.retain(|a| cfg.qq.contains(&a.qq));
    }
    if accounts.is_empty() {
        anyhow::bail!("未找到 QQ 数据库（nt_msg.db）。请确认 QQ 已安装并登录过，或在配置文件中设置 db_path。");
    }
    let qq_list: Vec<String> = accounts.iter().map(|a| a.qq.clone()).collect();
    let keys = KeyStore::load(&cfg.keys, cfg.keys_file.as_deref(), cfg.ask_key, &qq_list)?;
    for a in &accounts {
        if keys.get(&a.qq).is_none() {
            anyhow::bail!(
                "缺少 QQ {} 的数据库密钥。请先用 qq-win-db-key 提取，再在配置文件中设置 keys 或 keys_file。",
                a.qq
            );
        }
    }
    keys.save(&data_dir)?;

    // ---- state -----------------------------------------------------------
    let store = Arc::new(RwLock::new(Store::default()));
    let (tx, _) = broadcast::channel::<Event>(1024);
    let accounts_state = Arc::new(RwLock::new(Vec::<AccountState>::new()));
    let ready = Arc::new(AtomicBool::new(false));
    let sync_engine = Arc::new(sync::SyncEngine::new());
    let state = Arc::new(AppState {
        store: store.clone(),
        events: tx.clone(),
        accounts: accounts_state.clone(),
        ready: ready.clone(),
        token: Arc::new(token.clone()),
        sync: sync_engine.clone(),
    });

    // ---- server (bind early; /health reports "starting") ------------------
    let app = build_router(state.clone());
    let addr = format!("{}:{}", cfg.host, cfg.port);
    let listener = tokio::net::TcpListener::bind(&addr)
        .await
        .with_context(|| format!("bind {addr}"))?;
    tracing::info!("[init] 服务启动: http://{addr}  (token 已生成/加载)");
    tokio::spawn(async move {
        if let Err(e) = axum::serve(listener, app).await {
            tracing::error!("http server error: {e}");
        }
    });

    // ---- per-account index build + poller ---------------------------------
    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let mut poll_tasks = Vec::new();
    for info in accounts {
        let key = keys.get(&info.qq).unwrap().to_string();
        let store = store.clone();
        let tx = tx.clone();
        let accounts_state = accounts_state.clone();
        let mirror_root = data_dir.join("mirror");
        let qq = info.qq.clone();
        let watch_dir = info
            .path
            .parent()
            .unwrap_or_else(|| std::path::Path::new("."))
            .to_path_buf();

        // Index build is CPU-bound (decrypt + full scan): run in blocking pool.
        let key_for_index = key.clone();
        let store_for_index = store.clone();
        let tx_for_build = tx.clone();
        let handle = tokio::task::spawn_blocking(move || -> Result<Mirror> {
            {
                let mut accs = accounts_state.write();
                accs.push(AccountState {
                    qq: qq.clone(),
                    state: "indexing".into(),
                    message_count: 0,
                    error: None,
                });
            }
            let mirror = db::mirror::Mirror::new(&info, &mirror_root)?;
            let conn = db::decrypt::open_decrypted(&mirror.main_path, &key_for_index)?;
            let st = index::build_index(&conn)?;
            let count: usize = st.convs.values().map(|c| c.msgs.len()).sum();
            // Replace the store and re-baseline SSE subscribers in one step
            // (clients connected during indexing hold a sync(0,0) baseline).
            install_index(&store_for_index, &tx_for_build, st);
            {
                let mut accs = accounts_state.write();
                if let Some(a) = accs.iter_mut().find(|a| a.qq == qq) {
                    a.state = "ready".into();
                    a.message_count = count;
                }
            }
            tracing::info!("[init] QQ {qq} 索引完成: {count} 条消息");
            Ok(mirror)
        });
        let mirror = handle.await.map_err(|e| anyhow::anyhow!("index task panicked: {e}"))??;

        // Share the mirror between the change-driven poll task and the
        // manual-sync endpoint (both call AccountSync::poll_once).
        let account = Arc::new(sync::AccountSync::new(
            Arc::new(parking_lot::Mutex::new(mirror)),
            key.clone(),
            store.clone(),
            tx.clone(),
        ));
        sync_engine.register(account.clone());

        // File-system-event-driven trigger (notify) + slow fallback poll.
        let watch_cfg = crate::sync::watch::WatchConfig {
            debounce: std::time::Duration::from_millis(cfg.watch_debounce_ms),
            fallback: (cfg.watch_fallback_ms > 0)
                .then(|| std::time::Duration::from_millis(cfg.watch_fallback_ms)),
        };
        let task = tokio::spawn(crate::sync::watch::spawn(
            account,
            watch_dir,
            watch_cfg,
            shutdown_rx.clone(),
        ));
        poll_tasks.push(task);
    }

    ready.store(true, Ordering::SeqCst);
    tracing::info!("[init] 全部就绪");

    // ---- shutdown ----------------------------------------------------------
    tokio::signal::ctrl_c().await.ok();
    tracing::info!("收到退出信号，清理中…");
    shutdown_tx.send(true).ok();
    for t in poll_tasks {
        t.abort();
    }
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
}
