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
use qqflow_server::db::decrypt::open_live_mode;
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

/// [GT] per-column value statistics for one table — the arbitration data
/// for the loader's value-driven classification (store::names): a `u_`
/// ratio > 0.8 marks the uid key column, all-digit 5..=12 marks QQ
/// numbers / group ids, CJK marks remark/name columns.
fn probe_columns(conn: &rusqlite::Connection, table: &str) {
    let names: Vec<String> = conn
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .unwrap()
        .query_map([], |r| r.get::<_, String>(1))
        .unwrap()
        .flatten()
        .collect();
    if names.is_empty() {
        println!("[GT] {table}: no columns");
        return;
    }
    let mut stats: Vec<Vec<String>> = names.iter().map(|_| Vec::new()).collect();
    let mut stmt = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 1000")).unwrap();
    let rows = stmt
        .query_map([], |row| {
            let mut vals = Vec::new();
            for i in 0..names.len() {
                vals.push(match row.get_ref(i) {
                    Ok(rusqlite::types::ValueRef::Text(t)) => std::str::from_utf8(t).ok().map(String::from),
                    Ok(rusqlite::types::ValueRef::Integer(n)) => Some(n.to_string()),
                    _ => None,
                });
            }
            Ok(vals)
        })
        .unwrap();
    for r in rows.flatten() {
        for (i, v) in r.iter().enumerate() {
            if let Some(s) = v {
                stats[i].push(s.clone());
            }
        }
    }
    for (i, name) in names.iter().enumerate() {
        let vals = &stats[i];
        let nonempty: Vec<&String> = vals.iter().filter(|v| !v.is_empty()).collect();
        let n = nonempty.len();
        if n == 0 {
            println!("[GT] {table} col {name}: EMPTY");
            continue;
        }
        let u = nonempty.iter().filter(|v| v.starts_with("u_")).count();
        let digit = nonempty
            .iter()
            .filter(|v| (5..=12).contains(&v.len()) && v.bytes().all(|b| b.is_ascii_digit()))
            .count();
        let cjk = nonempty
            .iter()
            .filter(|v| {
                let total = v.chars().count();
                let han = v.chars().filter(|c| ('\u{4e00}'..='\u{9fa5}').contains(c)).count();
                total >= 2 && han as f64 / total as f64 >= 0.6
            })
            .count();
        let distinct: BTreeSet<&String> = nonempty.iter().copied().collect();
        let sample = nonempty.first().map(|s| s.as_str()).unwrap_or("");
        println!(
            "[GT] {table} col {name}: total={} u={u} digit={digit} cjk={cjk} distinct={} sample={sample:?}",
            vals.len(),
            distinct.len()
        );
    }
}

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

/// The name-maps loader reads uid→remark/nick/QQ from the mapping table
/// inside nt_msg.db + the sibling profile_info.db (headerless — exercises
/// the offset→plain open retry), and group names from the sibling
/// `group_info.db` (headed — exercises the offset VFS on a non-nt_msg file).
#[test]
fn fake_db_names_loaded() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let path = build_fake_db();
    let nt_db = path.parent().unwrap().to_path_buf();
    common::write_fake_group_info_headed(&nt_db);
    common::write_fake_profile_info(&nt_db);

    let mut reader = LiveReader::new(path, FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let mut store = qqflow_server::store::index::build_index(conn).unwrap();
    let known = qqflow_server::store::names::KnownKeys::from_store(&store);
    let maps = qqflow_server::store::names::load_names(conn, &nt_db, FAKE_KEY, &known);
    drop(reader);

    // profile_info.db (authoritative, probed first): nick per uid.
    assert_eq!(maps.uid_nick.get("u_12345").map(String::as_str), Some("档案昵称"), "profile nick wins");
    assert_eq!(maps.uid_nick.get("u_c").map(String::as_str), Some("王五档案"), "profile-only uid");
    // uid mapping table (inside nt_msg.db): remark + qq per uid.
    assert_eq!(maps.uid_remark.get("u_12345").map(String::as_str), Some("李四他哥"));
    assert_eq!(maps.uid_remark.get("u_a").map(String::as_str), Some("张三备注"));
    assert_eq!(maps.uid_remark.get("u_b"), None, "empty remark stays absent");
    assert_eq!(maps.uid_qq.get("u_a").map(String::as_str), Some("10001"));
    // sibling group_info.db (headed): group id -> name.
    assert_eq!(maps.group_name.get("10001").map(String::as_str), Some("测试群"));
    assert_eq!(maps.group_name.get("20002").map(String::as_str), Some("第二群"));

    // Wire the maps into the store, exactly like init_account does, then
    // check display resolution through the store.
    store.names = maps;

    assert_eq!(store.display_uid("u_12345"), "李四他哥", "remark wins over profile nick");
    assert_eq!(store.display_uid("u_a"), "张三备注", "remark wins");
    assert_eq!(store.display_uid("u_c"), "王五档案", "profile nick wins over message nick 王五");
    assert_eq!(store.display_uid("u_b"), "李四", "no remark/profile row -> message nick");
    assert_eq!(
        store.display_name(ChatType::Group, "10001"),
        "测试群",
        "group-info name"
    );
    assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三备注", "c2c remark wins");
    assert_eq!(store.display_name(ChatType::C2c, "u_c"), "王五档案", "c2c profile nick wins");
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
    let account = qqflow_server::sync::AccountSync::new(
        reader,
        store,
        tx,
        fake_db_path(),
        fake_db_path().parent().unwrap().to_path_buf(),
        FAKE_KEY.into(),
    );

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
    let account = qqflow_server::sync::AccountSync::new(
        reader,
        store.clone(),
        tx,
        fake_db_path(),
        fake_db_path().parent().unwrap().to_path_buf(),
        FAKE_KEY.into(),
    );

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
    // Loader decision tracing (RUST_LOG=debug shows [names] lines); the
    // probe is a diagnostic tool, so a subscriber helps arbitrate the
    // value-driven classification against the real layout.
    let _sub = tracing_subscriber::fmt()
        .with_env_filter(tracing_subscriber::EnvFilter::from_env("RUST_LOG"))
        .with_test_writer()
        .try_init();
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

        // ---- name-map sources (arbitrates store::names) -----------------
        // 1) Column introspection + value stats for candidate mapping
        //    tables inside nt_msg.db — the loader's value-driven column
        //    classification must be able to read what this prints.
        for cand in [
            "nt_uid_mapping_table", "uid_mapping", "buddy_mapping", "nt_buddylist",
            "nt_group_info", "group_info", "troop_info", "nt_troop_info", "nt_group_table",
            "contact_table",
        ] {
            if tables.contains(cand) {
                probe_columns(conn, cand);
            }
        }
        // 2) Sibling databases in the same nt_db directory (public docs put
        //    group names in group_info.db) — the file header arbitrates the
        //    offset layout, and both open modes are tried so the loader's
        //    offset→plain retry is validated against the real layout.
        let Some(nt_db_dir) = info.path.parent() else {
            continue;
        };
        for entry in std::fs::read_dir(nt_db_dir).into_iter().flatten().flatten() {
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with("-wal") || name.ends_with("-shm") || name.ends_with("-journal") {
                continue;
            }
            let size = entry.metadata().map(|m| m.len()).unwrap_or(0);
            println!("[GT] nt_db file {name}: size={size}");
        }
        for entry in std::fs::read_dir(nt_db_dir).into_iter().flatten().flatten() {
            let path = entry.path();
            let name = entry.file_name().to_string_lossy().into_owned();
            if !name.ends_with(".db") || name.starts_with("nt_msg") || name == "raw.db" {
                continue;
            }
            let head = std::fs::read(&path)
                .ok()
                .map(|b| b[..b.len().min(16)].iter().map(|x| format!("{x:02x}")).collect::<Vec<_>>().join(" "))
                .unwrap_or_default();
            println!("[GT] sibling {name}: header=[{head}]");
            for (label, offset) in [("offset", true), ("plain", false)] {
                match open_live_mode(&path, &key, offset) {
                    Ok(c) => {
                        let mut st = c.prepare("SELECT name FROM sqlite_master WHERE type='table'").unwrap();
                        let mut names: Vec<String> =
                            st.query_map([], |r| r.get(0)).unwrap().flatten().collect();
                        names.sort();
                        println!("[GT] sibling {name} {label}=ok tables: {}", names.join(", "));
                        for t in &names {
                            let l = t.to_lowercase();
                            if l.contains("group") || l.contains("uid") || l.contains("buddy")
                                || l.contains("contact") || l.contains("mapping")
                                || l.contains("troop") || l.contains("profile")
                            {
                                let cnt: i64 = c
                                    .query_row(&format!("SELECT count(*) FROM \"{t}\""), [], |r| r.get(0))
                                    .unwrap_or(-1);
                                println!("[GT] sibling {name} {t}: count={cnt}");
                                probe_columns(&c, t);
                            }
                        }
                    }
                    Err(e) => println!("[GT] sibling {name} {label}=err: {e:#}"),
                }
            }
        }

        // 3) End-to-end arbitration of the PRODUCTION loader against the
        //    real layout. A full index build on a 1.2 GB DB is slow, so the
        //    known keys are empty — the loader's value-driven classification
        //    (u_ ratio, digit, CJK, known numeric field ids) must still land.
        {
            let known = qqflow_server::store::names::KnownKeys::default();
            let maps = qqflow_server::store::names::load_names(conn, nt_db_dir, &key, &known);
            println!(
                "[GT] load_names: uid_remark={} uid_nick={} uid_qq={} group_name={} group_remark={}",
                maps.uid_remark.len(),
                maps.uid_nick.len(),
                maps.uid_qq.len(),
                maps.group_name.len(),
                maps.group_remark.len()
            );
            for (uid, r) in maps.uid_remark.iter().take(3) {
                println!("[GT] load_names remark sample: {uid} -> {r}");
            }
            for (uid, n) in maps.uid_nick.iter().take(3) {
                println!("[GT] load_names nick sample: {uid} -> {n}");
            }
            for (gid, n) in maps.group_name.iter().take(3) {
                println!("[GT] load_names group sample: {gid} -> {n}");
            }
            // 4) 档案昵称 vs 消息昵称 for the same uids: 消息昵称 = the
            // "40093" column embedded in message rows (sender's nick AT
            // SEND TIME — stale when the profile changed); 档案昵称 = the
            // profile lookup for that uid.
            let mut mismatched = 0usize;
            let mut shown = 0usize;
            for (uid, pnick) in maps.uid_nick.iter() {
                let mut msg_nick = String::new();
                for tbl in ["group_msg_table", "c2c_msg_table"] {
                    if let Ok(mut st) = conn.prepare(&format!(
                        "SELECT \"40093\" FROM {tbl} WHERE \"40020\" = ?1 AND \"40093\" != '' LIMIT 1"
                    )) {
                        if let Ok(mut rows) = st.query_map([uid], |r| r.get::<_, String>(0)) {
                            if let Some(Ok(n)) = rows.next() {
                                msg_nick = n;
                                break;
                            }
                        }
                    }
                }
                if msg_nick.is_empty() {
                    continue;
                }
                let differ = msg_nick != *pnick;
                if shown < 6 || (differ && mismatched < 3) {
                    println!(
                        "[GT] nick contrast: {uid} message={msg_nick:?} profile={pnick:?}{}",
                        if differ { " <== 不一致" } else { "" }
                    );
                }
                if differ {
                    mismatched += 1;
                }
                shown += 1;
                if shown >= 30 && mismatched >= 3 {
                    break;
                }
            }
        }

        drop(reader);
        println!("[GT] qq {} done: live open+queries {:.1}s", info.qq, t0.elapsed().as_secs_f64());
    }
}
