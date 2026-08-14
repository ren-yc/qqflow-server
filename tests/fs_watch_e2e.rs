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
