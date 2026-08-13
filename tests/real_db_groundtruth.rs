//! Ground-truth queries against a REAL QQ database (SQLCipher-encrypted
//! nt_msg.db), plus a fake-DB builder used for behavioral reproduction.
//!
//! The `real_db_groundtruth` test is `#[ignore]`d by default: it needs a real
//! database and its SQLCipher key, both supplied via environment variables:
//!   QQFLOW_TEST_DB_ROOT  - Tencent Files-style root (<dir>/<qq>/nt_qq/nt_db/nt_msg.db)
//!   QQFLOW_TEST_DB_KEY   - 16-byte printable ASCII SQLCipher key
//!
//! Run: powershell -File scripts\build.ps1 test --test real_db_groundtruth -- --ignored --nocapture
//!
//! Everything goes through the SAME pipeline the server uses
//! (`db::live::LiveReader` — the offset VFS + read-only live connection),
//! so "opens at all" doubles as the arbitration experiment for the offset
//! VFS against the real file layout.

use std::collections::BTreeSet;
use std::path::Path;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use qqflow_server::db::live::LiveReader;
use qqflow_server::db::scan;
use qqflow_server::parser::types::{seq_to_time, ChatType};
use qqflow_server::server::{build_router, AccountRegistry, AccountState, AccountStatus};
use qqflow_server::store::AppState;
use qqflow_server::sync::SyncEngine;
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;
use common::{FAKE_KEY, FAKE_QQ};

/// Fake DB location (also used by the behavioral-repro runbook).
pub fn fake_db_path() -> std::path::PathBuf {
    std::env::temp_dir()
        .join("qqflow_fake")
        .join(FAKE_QQ)
        .join("nt_qq")
        .join("nt_db")
        .join("nt_msg.db")
}

/// Rebuild the shared fake DB fixture (serialized against parallel tests).
fn build_fake_db() -> std::path::PathBuf {
    let fake_root = std::env::temp_dir().join("qqflow_fake");
    let _ = std::fs::remove_dir_all(&fake_root);
    common::write_fake_source(fake_db_path().parent().unwrap(), 0)
}

/// Both fake-db tests rebuild the same fixture directory — serialize them
/// (integration tests run in parallel threads of one process).
static FAKE_DB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

/// Verify the fake DB is readable through the server's pipeline (live
/// reader + direct row counts). The behavioral-repro runbook depends on it.
#[test]
fn fake_db_for_behavioral_repro() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let path = build_fake_db();
    let mut reader = LiveReader::new(path.clone(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM group_msg_table", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 6, "fake group rows readable through the live pipeline");
    let n: i64 = conn
        .query_row("SELECT count(*) FROM c2c_msg_table", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "fake c2c rows readable through the live pipeline");
    drop(reader);

    println!("[GT] fake db ready at {} (key={FAKE_KEY}, qq={FAKE_QQ})", path.display());
}

/// Regression check for the c2c index bug found in behavioral reproduction:
/// `store::index` silently dropped ALL c2c rows (shared 6-column row closure
/// used against the 5-column c2c query; row.get(5) errors were swallowed by
/// rows.flatten()). Now a permanent guard: both c2c rows must be indexed.
#[test]
fn fake_db_indexes_c2c_rows() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let path = build_fake_db();
    let mut reader = LiveReader::new(path, FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let store = qqflow_server::store::index::build_index(conn).unwrap();
    drop(reader);

    println!(
        "[GT] build_index: groupWatermark={} c2cWatermark={} convs={}",
        store.watermark_group,
        store.watermark_c2c,
        store.convs.len()
    );
    assert_eq!(store.watermark_group, 6, "group rows must be indexed");
    assert_eq!(store.watermark_c2c, 2, "c2c rows must be indexed (BUG: currently 0)");
    let c2c = store
        .conversation(qqflow_server::parser::types::ChatType::C2c, "u_12345")
        .expect("c2c conversation must exist (BUG: currently missing)");
    assert_eq!(c2c.msgs.len(), 2, "both c2c messages must be present");
}

/// Manual-sync path: `AccountSync::poll_once` picks up rows appended to
/// the database between calls and broadcasts SSE events for them (this is
/// what `POST /api/v1/sync` drives).
#[test]
fn manual_sync_picks_up_new_rows() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let path = fake_db_path();
    let reader = std::sync::Arc::new(parking_lot::Mutex::new(
        LiveReader::new(path, FAKE_KEY.into()),
    ));
    reader.lock().open().unwrap();
    let store = std::sync::Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default()));
    let (tx, mut rx) = tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(16);
    let account = qqflow_server::sync::AccountSync::new(reader, store, tx, fake_db_path());

    let first = account.poll_once().unwrap();
    assert_eq!(first.len(), 8, "initial poll returns all rows (6 group + 2 c2c)");
    // The receiver was subscribed before the first poll: drain the initial
    // batch of events so try_recv below sees only the new row's event.
    while rx.try_recv().is_ok() {}

    // Append a new group row via the live writer (simulates QQ writing a
    // new message between polls) and materialize the source pair.
    common::append_group_row(&writer, 7, "手动同步新增");
    common::materialize_source(&nt_db);

    let second = account.poll_once().unwrap();
    assert_eq!(second.len(), 1, "second poll returns only the new row");
    assert_eq!(second[0].parsed.content, "手动同步新增");

    // The new row must also be broadcast as an SSE event.
    let ev = rx.try_recv().unwrap();
    assert_eq!(ev.event, "message.new");
    assert_eq!(ev.content, "手动同步新增");

    drop(writer);
}

/// Regression: the sync read phase must not mutate the store. When the c2c
/// read fails AFTER the group read, nothing may be applied (no group rows,
/// no watermark advance), and the retry after repair must deliver every row
/// exactly once — the old combined read+apply pass duplicated rows here.
#[test]
fn failed_sync_leaves_store_untouched() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let reader = std::sync::Arc::new(parking_lot::Mutex::new(
        LiveReader::new(fake_db_path(), FAKE_KEY.into()),
    ));
    reader.lock().open().unwrap();
    let store = std::sync::Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default()));
    let (tx, _rx) = tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(16);
    let account = qqflow_server::sync::AccountSync::new(reader, store.clone(), tx, fake_db_path());

    // Break the c2c read by renaming its table away (via the live writer,
    // then materialize so the reader's next query hits the broken schema).
    writer.execute_batch("ALTER TABLE c2c_msg_table RENAME TO c2c_broken;")
        .unwrap();
    common::materialize_source(&nt_db);
    let err = account.poll_once().unwrap_err();
    println!("[GT] expected sync failure: {err:#}");
    {
        let g = store.read();
        assert!(g.convs.is_empty(), "failed sync must not apply group rows");
        assert_eq!((g.watermark_group, g.watermark_c2c), (0, 0), "failed sync must not advance watermarks");
    }

    // Repair and retry: every row arrives exactly once.
    writer.execute_batch("ALTER TABLE c2c_broken RENAME TO c2c_msg_table;")
        .unwrap();
    common::materialize_source(&nt_db);
    let records = account.poll_once().unwrap();
    assert_eq!(records.len(), 8, "retry returns all rows (6 group + 2 c2c)");
    let g = store.read();
    // The fake fixture spreads group rows over two groups: 5 in 10001, 1 in 20002.
    let group = g
        .conversation(ChatType::Group, "10001")
        .expect("group conversation exists");
    assert_eq!(group.msgs.len(), 5, "group rows applied exactly once (10001)");
    let other = g
        .conversation(ChatType::Group, "20002")
        .expect("second group conversation exists");
    assert_eq!(other.msgs.len(), 1, "group rows applied exactly once (20002)");
    let c2c = g
        .conversation(ChatType::C2c, "u_12345")
        .expect("c2c conversation exists");
    assert_eq!(c2c.msgs.len(), 2, "c2c rows applied exactly once");

    drop(writer);
}

/// Client-driven registration e2e: `POST /api/v1/accounts` with qq + key +
/// db_path initializes the account in the background. A wrong key lands the
/// account in `error` (recoverable); the corrected key reaches `ready` and
/// the account serves messages — the process never exits.
#[tokio::test]
// The fake fixture must stay exclusive for the whole test — parallel tests
// would rebuild the shared file underneath the in-flight initialization.
#[allow(clippy::await_holding_lock)]
async fn client_registers_account_with_key_and_db_path() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let path = build_fake_db();
    let db_path = path.to_string_lossy().to_string();

    let state = Arc::new(AppState {
        store: Arc::new(parking_lot::RwLock::new(qqflow_server::store::Store::default())),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(64).0,
        accounts: Arc::new(parking_lot::RwLock::new(vec![AccountState {
            qq: FAKE_QQ.into(),
            state: AccountStatus::AwaitingKey,
            message_count: 0,
            error: None,
        }])),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(false)),
        token: Arc::new("test-token".into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(
            Vec::new(),
            qqflow_server::sync::watch::WatchConfig::default(),
            tokio::sync::watch::channel(false).1,
        ),
    });
    let app = build_router(state.clone());

    // Boot state: account discovered, awaiting a key.
    let v = common::wait_account_state(&app, FAKE_QQ, "awaiting_key", Duration::from_secs(15)).await;
    assert_eq!(v["status"], "starting");

    // Wrong key (valid format, wrong content) -> accepted, then error.
    let (s, v) = common::post_json(
        app.clone(),
        "/api/v1/accounts",
        &[],
        json!({"access_token": "test-token", "qq": FAKE_QQ, "key": "0123456789abcdeX", "db_path": db_path}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "accepted");
    let v = common::wait_account_state(&app, FAKE_QQ, "error", Duration::from_secs(15)).await;
    let err = v["accounts"].as_array().unwrap()[0]["error"].as_str().unwrap().to_string();
    println!("[GT] expected init failure: {err}");
    assert!(err.contains("解密") || err.contains("密钥"), "error must explain: {err}");

    // Corrected key -> accepted, then ready and serving.
    let (s, v) = common::post_json(
        app.clone(),
        "/api/v1/accounts",
        &[],
        json!({"access_token": "test-token", "qq": FAKE_QQ, "key": FAKE_KEY, "db_path": db_path}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "accepted");
    let v = common::wait_account_state(&app, FAKE_QQ, "ready", Duration::from_secs(15)).await;
    assert_eq!(v["status"], "ok");
    assert_eq!(v["accounts"].as_array().unwrap()[0]["message_count"], 8);

    // The registered account serves queries.
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/messages?talker=10001&access_token=test-token")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let v: Value = serde_json::from_slice(&bytes).unwrap();
    assert_eq!(v["success"], true);
    assert_eq!(v["messages"].as_array().unwrap().len(), 5, "group 10001 has 5 rows");
}

/// Ground truth over a REAL QQ database. Ignored by default; requires
/// QQFLOW_TEST_DB_ROOT + QQFLOW_TEST_DB_KEY env vars.
#[test]
#[ignore]
fn real_db_groundtruth() {
    let root = match std::env::var("QQFLOW_TEST_DB_ROOT") {
        Ok(v) => v,
        Err(_) => {
            println!("[GT] SKIPPED: QQFLOW_TEST_DB_ROOT not set");
            return;
        }
    };
    let key = match std::env::var("QQFLOW_TEST_DB_KEY") {
        Ok(v) => v,
        Err(_) => {
            println!("[GT] SKIPPED: QQFLOW_TEST_DB_KEY not set");
            return;
        }
    };

    let accounts = scan::scan_accounts(Some(Path::new(&root))).expect("scan custom root");
    println!("[GT] accounts under {root}: {:?}", accounts.iter().map(|a| &a.qq).collect::<Vec<_>>());

    for info in &accounts {
        let now_ts = chrono::Utc::now().timestamp();
        let t0 = std::time::Instant::now();
        // The LIVE read-only open through the offset VFS — arbitrates the
        // whole no-copy design against the real on-disk layout while a
        // real QQ client may hold the database.
        let mut reader = LiveReader::new(info.path.clone(), key.clone());
        reader
            .open()
            .expect("real DB must open read-only through the offset VFS (arbitrates the VFS)");
        let conn = reader.acquire().unwrap();

        for (table, id_col) in [("group_msg_table", "40021"), ("c2c_msg_table", "40020")] {
            let (cnt, max_rowid): (i64, i64) = conn
                .query_row(&format!("SELECT count(*), max(rowid) FROM {table}"), [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            // Timestamps derive from the raw seq via the PRODUCTION
            // `seq_to_time` (seq >> 32), never an ad-hoc SQL shift — a
            // hand-written shift here once drifted from the code (>>16) and
            // silently printed garbage. NULLs (empty table) are skipped.
            let (min_seq, max_seq): (Option<i64>, Option<i64>) = conn
                .query_row(&format!("SELECT min(\"40001\"), max(\"40001\") FROM {table}"), [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            let (min_ts, max_ts) = match (min_seq, max_seq) {
                (Some(lo), Some(hi)) => (seq_to_time(lo), seq_to_time(hi)),
                _ => {
                    println!("[GT] {table}: empty (no seq rows), skipping ts checks");
                    continue;
                }
            };
            // Out-of-order rows: seq_to_time decreasing along rowid order
            // (backfill etc.), counted in Rust with the same extraction.
            // future_ts counts rows dated after `now` — a small fraction is
            // expected (senders with wrong clocks); wholesale future dates
            // mean the seq layout changed.
            let mut out_of_order = 0i64;
            let mut future_ts = 0i64;
            {
                let mut prev: Option<i64> = None;
                let mut stmt = conn
                    .prepare(&format!("SELECT \"40001\" FROM {table} ORDER BY rowid"))
                    .unwrap();
                let rows = stmt.query_map([], |r| r.get::<_, i64>(0)).unwrap();
                for r in rows.flatten() {
                    let t = seq_to_time(r);
                    if let Some(p) = prev
                        && t < p
                    {
                        out_of_order += 1;
                    }
                    if t > now_ts {
                        future_ts += 1;
                    }
                    prev = Some(t);
                }
            }
            println!(
                "[GT] {table}: tsRange(seq_to_time)=[{min_ts},{max_ts}] outOfOrder={out_of_order} futureTs={future_ts}"
            );
            // Arbitration: a plausible message time is 2000..now+1y (sender
            // clocks drift well past a day); anything beyond that means the
            // seq layout changed and seq_to_time must be reworked.
            assert!(min_ts > 946_684_800, "{table}: min ts implausible: {min_ts}");
            assert!(
                max_ts < now_ts + 366 * 86_400,
                "{table}: max ts implausibly far in the future: {max_ts}"
            );
            let (b_min, b_max): (i64, i64) = conn
                .query_row(&format!("SELECT min(length(\"40800\")), max(length(\"40800\")) FROM {table}"), [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            let null_blob: i64 = conn
                .query_row(&format!("SELECT count(*) FROM {table} WHERE \"40800\" IS NULL"), [], |r| r.get(0))
                .unwrap();
            println!("[GT] {table} nullBlob={null_blob}");
            let big: i64 = conn
                .query_row(&format!("SELECT count(*) FROM {table} WHERE length(\"40800\") > 65536"), [], |r| r.get(0))
                .unwrap();
            let distinct: i64 = conn
                .query_row(&format!("SELECT count(DISTINCT \"{id_col}\") FROM {table}"), [], |r| r.get(0))
                .unwrap();
            println!(
                "[GT] {table}: count={cnt} maxRowid={max_rowid} distinctTalkers={distinct} \
                 tsRange(seq_to_time)=[{min_ts},{max_ts}] outOfOrder={out_of_order} blobLen=[{b_min},{b_max}] >64KB={big}"
            );
            for (label, phrase) in [
                ("recall", "你猜猜撤回了什么"),
                ("system-pai", "拍了拍"),
                ("system-rename", "修改群名"),
                ("system-rename2", "已将群名修改为"),
            ] {
                let c: i64 = conn
                    .query_row(
                        &format!("SELECT count(*) FROM {table} WHERE CAST(\"40800\" AS TEXT) LIKE ?1"),
                        [format!("%{phrase}%")],
                        |r| r.get(0),
                    )
                    .unwrap();
                println!("[GT] {table} {label}Phrases={c}");
            }
        }

        // c2c talker prefix statistics (arbitrates classify_talker).
        let (mut u_prefix, mut digit, mut other) = (0i64, 0i64, 0i64);
        {
            let mut stmt = conn
                .prepare("SELECT DISTINCT \"40020\" FROM c2c_msg_table")
                .unwrap();
            let rows = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap();
            for r in rows.flatten() {
                if r.starts_with("u_") {
                    u_prefix += 1;
                } else if !r.is_empty() && r.chars().all(|c| c.is_ascii_digit()) {
                    digit += 1;
                } else {
                    other += 1;
                }
            }
        }
        println!("[GT] c2c talker prefixes: u_={u_prefix} allDigits={digit} other={other}");

        // Candidate-table presence (reference probing lists).
        let mut tables: BTreeSet<String> = BTreeSet::new();
        {
            let mut stmt = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'").unwrap();
            for r in stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().flatten() {
                tables.insert(r);
            }
        }
        for cand in [
            "nt_uid_mapping_table", "uid_mapping", "buddy_mapping", // uid map strategy 1
            "nt_group_info", "group_info", "troop_info", "nt_troop_info", "nt_group_table",
            "troop_member_list", "group_member_list", "recent_contact_table",
            "nt_recent_contact_table", "aio_recent_contact_table", "contact_table", "nt_buddylist",
        ] {
            println!("[GT] table {cand}: {}", tables.contains(cand));
        }
        println!("[GT] totalTables={}", tables.len());

        drop(reader);
        println!("[GT] qq {} done: live open+queries {:.1}s", info.qq, t0.elapsed().as_secs_f64());
    }
}
