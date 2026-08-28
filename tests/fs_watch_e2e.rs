//! E2E: file-system events (notify) drive sync -> SSE broadcast.
//!
//! A persistent fake-QQ writer appends a new message (into its WAL, like a
//! running QQ client); the source pair is materialized in place and the
//! watch task triggers a sync whose SSE events arrive on the broadcast
//! channel within a timeout window. The reader is the production LIVE
//! connection — no mirror, no copies.

use std::sync::Arc;
use std::time::Duration;

use qqflow_server::db::live::LiveReader;
use qqflow_server::sync::watch::{self, WatchConfig};
use qqflow_server::sync::{AccountSync, Event};

mod common;
use common::{FAKE_KEY, FAKE_QQ, write_fake_source};

#[tokio::test]
async fn watch_event_drives_sse_push() {
    let dir = std::env::temp_dir().join(format!("qqflow_watch_{}", std::process::id()));
    let nt_db = dir.join("nt_db");
    // Persistent writer: new rows land in its WAL; materialize_source makes
    // them visible to the live reader (in-place, like QQ's appends).
    let (writer, _raw) = common::open_fake_source(&nt_db, 0); // standard 8-row dataset
    let src = nt_db.join("nt_msg.db");

    let store = Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default()));
    let (tx, mut rx) = tokio::sync::broadcast::channel::<Event>(64);
    let reader = Arc::new(parking_lot::Mutex::new(LiveReader::new(src.clone(), FAKE_KEY.into())));
    reader.lock().open().unwrap();
    let account = Arc::new(AccountSync::new(
        FAKE_QQ.into(),
        reader,
        store,
        tx,
        src.clone(),
        nt_db.clone(),
        FAKE_KEY.into(),
    ));

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let task = tokio::spawn(watch::spawn(
        account.clone(),
        nt_db.clone(),
        WatchConfig { debounce: Duration::from_millis(100), fallback: None },
        shutdown_rx,
    ));
    // Give the watcher backend thread time to attach.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Baseline: initial poll sees all rows; drain its events.
    assert_eq!(account.poll_once().unwrap().len(), 8);
    while rx.try_recv().is_ok() {}

    // Simulate QQ writing a new message: append via the live writer, then
    // refresh the source pair (WAL snapshot -> file event -> sync).
    common::append_group_row(&writer, 7, "事件驱动新增-7");
    common::materialize_source(&nt_db);

    let ev = tokio::time::timeout(Duration::from_secs(5), rx.recv())
        .await
        .expect("watch-driven SSE event within 5 s")
        .unwrap();
    assert_eq!(ev.event, "message.new");
    assert!(ev.content.contains("事件驱动新增"), "got: {}", ev.content);

    // Idempotent: the row was consumed; nothing new afterwards.
    assert_eq!(account.poll_once().unwrap().len(), 0);

    shutdown_tx.send(true).ok();
    task.await.unwrap().unwrap();
    drop(writer);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Fallback poll regression (review finding #1): `changed()` must detect
/// WAL-only writes even when no watch event arrives — the 30 s fallback is
/// the insurance against silently dropped watch events. It stats the WAL
/// (metadata only), so a write that is never observed by the watcher still
/// triggers the next fallback sync.
#[test]
fn fallback_changed_detects_wal_writes() {
    // Distinct temp dir from watch_event_drives_sse_push: tests in the same
    // binary run in parallel and share the PID-named directory.
    let dir = std::env::temp_dir().join(format!("qqflow_fallback_{}", std::process::id()));
    let nt_db = dir.join("nt_db");
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let src = nt_db.join("nt_msg.db");

    let store = Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default()));
    let (tx, _rx) = tokio::sync::broadcast::channel::<Event>(64);
    let reader = Arc::new(parking_lot::Mutex::new(LiveReader::new(src.clone(), FAKE_KEY.into())));
    reader.lock().open().unwrap();
    let account = Arc::new(AccountSync::new(
        FAKE_QQ.into(),
        reader,
        store,
        tx,
        src.clone(),
        nt_db.clone(),
        FAKE_KEY.into(),
    ));

    // Baseline: the first check initializes the snapshot (reports a change);
    // the second, with nothing new on disk, reports none.
    assert!(account.changed(), "first check initializes the WAL snapshot");
    assert!(!account.changed(), "no change after baseline");
    assert_eq!(account.poll_once().unwrap().len(), 8);

    // A WAL-only write (new row in the writer's WAL, never watched by any
    // watcher) must flip changed() to true, exactly once per write.
    common::append_group_row(&writer, 8, "兜底轮询新增");
    common::materialize_source(&nt_db);
    assert!(account.changed(), "fallback must see WAL-only writes");
    assert!(!account.changed(), "snapshot advances after the check");
    let rows = account.poll_once().unwrap();
    assert_eq!(rows.len(), 1);
    assert_eq!(rows[0].content, "兜底轮询新增");

    drop(writer);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Deregistration must actually stop the watch task, not just unbind the
/// account. Registering spawns a watch task that holds an `Arc<AccountSync>`
/// pointing at the store; if it survived, a later write to the source would
/// still drive a sync into the store the deregistration had just cleared, and
/// SSE subscribers would keep receiving messages for an account the server
/// reports as unregistered.
///
/// Goes through the HTTP surface deliberately: the task under test is the one
/// `POST /api/v1/accounts` spawned, so nothing here can accidentally test a
/// hand-built watcher instead.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn deregister_stops_the_watch_task() {
    let dir = std::env::temp_dir().join(format!("qqflow_dereg_watch_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    let nt_db = dir.join("nt_db");
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let src = nt_db.join("nt_msg.db");

    let (shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = Arc::new(qqflow_server::store::AppState {
        store: Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default())),
        events: tokio::sync::broadcast::channel::<Event>(256).0,
        accounts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        token: Arc::new("test-token-123456".into()),
        sync: Arc::new(qqflow_server::sync::SyncEngine::new()),
        init: qqflow_server::server::AccountRegistry::new(
            Vec::new(),
            // Short debounce, no fallback poll: a surviving watch task must be
            // caught by the file event, and the fallback timer would otherwise
            // muddy which mechanism fired.
            WatchConfig { debounce: Duration::from_millis(100), fallback: None },
            shutdown_rx,
        ),
        export_root: Arc::new(dir.join("export")),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: shutdown_tx,
    });
    let app = qqflow_server::server::build_router(state.clone());
    let auth: &[(&str, &str)] = &[("Authorization", "Bearer test-token-123456")];

    let (s, v) = common::post_json(
        app.clone(),
        "/api/v1/accounts",
        auth,
        serde_json::json!({"qq": FAKE_QQ, "key": FAKE_KEY, "db_path": src.to_string_lossy()}),
    )
    .await;
    assert_eq!(s, axum::http::StatusCode::OK, "registration: {v}");
    assert_eq!(v["state"], "accepted", "registration: {v}");
    common::wait_account_state(&app, "test-token-123456", FAKE_QQ, "ready", Duration::from_secs(60))
        .await;
    // Let the watcher backend thread attach before the first write.
    tokio::time::sleep(Duration::from_millis(300)).await;

    // Prove the watch is live first, so the negative assertion below cannot
    // pass just because the plumbing never worked.
    let mut rx = state.events.subscribe();
    common::append_group_row(&writer, 7, "注销前新增-7");
    common::materialize_source(&nt_db);
    let ev = tokio::time::timeout(Duration::from_secs(10), async {
        loop {
            let ev = rx.recv().await.unwrap();
            if ev.event == "message.new" {
                return ev;
            }
        }
    })
    .await
    .expect("the watch task is live before deregistration");
    assert!(ev.content.contains("注销前新增"), "got: {}", ev.content);

    let (s, v) = common::delete_json(
        app.clone(),
        &format!("/api/v1/accounts/{FAKE_QQ}"),
        auth,
    )
    .await;
    assert_eq!(s, axum::http::StatusCode::OK, "deregistration: {v}");
    assert_eq!(v["state"], "deregistered");
    assert_eq!(v["index_cleared"], true, "a ready account had an index: {v}");
    assert!(state.store.read().convs.is_empty(), "index dropped");

    // A fresh subscriber, so the reset baseline the deregistration already
    // broadcast is not in this receiver's queue.
    let mut rx = state.events.subscribe();
    common::append_group_row(&writer, 8, "注销后新增-8");
    common::materialize_source(&nt_db);
    // 2 s is ~20x the debounce: a surviving watcher would have fired by now.
    let leaked = tokio::time::timeout(Duration::from_secs(2), async {
        loop {
            let ev = rx.recv().await.unwrap();
            if ev.event == "message.new" {
                return ev;
            }
        }
    })
    .await;
    assert!(
        leaked.is_err(),
        "a deregistered account still pushed messages: {:?}",
        leaked.map(|e| e.content)
    );
    assert!(state.store.read().convs.is_empty(), "and wrote nothing into the cleared store");

    drop(writer);
    let _ = std::fs::remove_dir_all(&dir);
}

/// Manual E2E helper (run explicitly): writes the fake server DB to a
/// stable path under %TEMP%\qqflow_watch_server. With QQFLOW_WATCH_PLUS=1
/// the file is rewritten with one extra row (simulating a new message).
///
///   powershell -File scripts\build.ps1 test --test fs_watch_e2e write_fake_server_db -- --ignored --nocapture
///   $env:QQFLOW_WATCH_PLUS = "1"; ... (same command again)
#[test]
#[ignore]
fn write_fake_server_db() {
    // Tencent Files-style layout so the server can discover it via db_path:
    // <root>/<qq>/nt_qq/nt_db/nt_msg.db (root = %TEMP%\qqflow_watch_server)
    let nt_db = std::env::temp_dir()
        .join("qqflow_watch_server")
        .join(FAKE_QQ)
        .join("nt_qq")
        .join("nt_db");
    let extra = if std::env::var("QQFLOW_WATCH_PLUS").is_ok() { 1 } else { 0 };
    let p = write_fake_source(&nt_db, extra);
    println!("fake server db at {}", p.display());
}
