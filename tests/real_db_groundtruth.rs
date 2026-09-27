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
use qqflow_server::parser::types::{seq_to_time, ChatType, MsgType};
use qqflow_server::server::{build_router, AccountRegistry, AccountState, AccountStatus};
use qqflow_server::store::query::{query_messages, MessageOut, MessageQuery};
use qqflow_server::server::AppState;
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
    let store = qqflow_server::store::index::build_index(conn, None).unwrap();
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
    let mut store = qqflow_server::store::index::build_index(conn, None).unwrap();
    let known = qqflow_server::store::names::KnownKeys::from_store(&store);
    let maps = qqflow_server::store::names::load_names(conn, &nt_db, FAKE_KEY, &known);
    drop(reader);

    // profile_info.db (authoritative, probed first): nick per uid.
    assert_eq!(maps.uid_nick.get("u_12345").map(String::as_str), Some("档案昵称"), "profile nick wins");
    assert_eq!(maps.uid_nick.get("u_c").map(String::as_str), Some("王五档案"), "profile-only uid");
    assert_eq!(
        maps.uid_remark.get("u_c").map(String::as_str),
        Some("王五备注"),
        "20009 remark column harvested (field-id hint)"
    );
    // uid mapping table (inside nt_msg.db): remark + qq per uid.
    assert_eq!(maps.uid_remark.get("u_12345").map(String::as_str), Some("李四他哥"));
    assert_eq!(maps.uid_remark.get("u_a").map(String::as_str), Some("张三备注"));
    assert_eq!(maps.uid_remark.get("u_b"), None, "empty remark stays absent");
    assert_eq!(maps.uid_qq.get("u_a").map(String::as_str), Some("10001"));
    // sibling group_info.db (headed): group id -> name.
    assert_eq!(
        maps.group_name.get("10001").map(String::as_str),
        Some("测试群a")
    );
    assert_eq!(maps.group_name.get("20002").map(String::as_str), Some("第二群"));

    // Wire the maps into the store, exactly like init_account does, then
    // check display resolution through the store.
    store.names = maps;

    assert_eq!(store.display_uid("u_12345"), "李四他哥", "remark wins over message nick");
    assert_eq!(store.display_uid("u_a"), "张三备注", "remark wins");
    assert_eq!(
        store.display_uid("u_c"),
        "王五备注",
        "20009 remark wins over message nick 王五 and profile nick 王五档案"
    );
    assert_eq!(store.display_uid("u_b"), "李四", "no remark/profile row -> message nick");
    assert_eq!(
        store.display_name(ChatType::Group, "10001"),
        "测试群a",
        "group-info name"
    );
    assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三备注", "c2c remark wins");
    assert_eq!(
        store.display_name(ChatType::C2c, "u_12345"),
        "李四他哥",
        "c2c remark wins over 会话名(首行 40093 王五)"
    );
    assert_eq!(
        store.display_name(ChatType::C2c, "u_c"),
        "王五备注",
        "c2c remark wins over profile nick (no conversation)"
    );
}

/// Structured image rows flow through the whole pipeline: the spec-shaped
/// 40800 blob (45002=2 + media fields) is decoded by parser::proto, the
/// media map registers the md5 key, direction/ts come from 40013/40050,
/// and MessageOut exposes is_send + media + mediaId.
#[test]
fn fake_db_index_media_metadata() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let md5 = common::append_image_row(&writer, 7, &nt_db);
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    // Media registration needs the account's nt_data root: `resolve_local_path`
    // contains its result to it, so passing None registers nothing at all.
    let media_root = qqflow_server::store::media::media_root_of(&nt_db);
    let store = qqflow_server::store::index::build_index(conn, media_root.as_deref()).unwrap();
    drop(reader);

    // Media map: md5 key -> local cache file.
    let entry = store.media.get(&md5).expect("media entry registered");
    assert!(entry.local_path.ends_with("fake_image_01.jpg"));
    assert_eq!(entry.file_name.as_deref(), Some("fake_image_01.jpg"));

    // The image row carries the full structured metadata.
    let conv = store.conversation(ChatType::Group, "10001").unwrap();
    let m = conv.msgs.iter().find(|m| m.rowid == 7).expect("image row indexed");
    assert_eq!(m.parsed.msg_type, MsgType::Image);
    assert_eq!(m.parsed.content, "[image]");
    let media = m.parsed.media.as_ref().expect("structured media");
    assert_eq!(media.md5.as_deref(), Some(md5.as_str()));
    assert_eq!(media.uuid.as_deref(), Some("fake-uuid-0001"));
    assert_eq!(media.file_name.as_deref(), Some("fake_image_01.jpg"));
    assert_eq!(media.size, Some(12345));
    assert_eq!((media.width, media.height), (Some(640), Some(480)));
    assert_eq!(m.ts, 1782864000, "40050 authoritative over seq>>32");
    assert_eq!(m.direction, Some(0), "image row dir=0");

    // MessageOut: is_send mapping (0 other / 1 self / 3 system), media id.
    assert_eq!(MessageOut::from_record(m).is_send, 0);
    assert_eq!(MessageOut::from_record(m).media_id.as_deref(), Some(md5.as_str()));
    let self_rec = conv.msgs.iter().find(|m| m.rowid == 1).unwrap();
    assert_eq!(MessageOut::from_record(self_rec).is_send, 1, "40013=1 -> self");
    let sys_rec = conv.msgs.iter().find(|m| m.rowid == 4).unwrap();
    assert_eq!(MessageOut::from_record(sys_rec).is_send, 0, "40013=3 system -> 0");
    // Group card (40090) stays per-conversation: the global nick keeps
    // 40093, the card only surfaces inside its own group.
    assert_eq!(self_rec.from_nick, "张三", "40093 nickname stays global");
    assert_eq!(self_rec.card.as_deref(), Some("张三群名片"), "40090 card kept");
    assert_eq!(
        store.display_sender(ChatType::Group, "10001", "u_a"),
        "张三群名片",
        "in-group display prefers the card"
    );
    assert_eq!(store.display_uid("u_a"), "张三", "global display never shows the card");

    drop(writer);
}

/// The media endpoint serves bytes from the local cache path registered at
/// index time — 200 with exact content, 404 for unknown ids and for files
/// QQ cleared from its cache, 401 without a token.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn fake_db_media_endpoint_serves_bytes() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let md5 = common::append_image_row(&writer, 7, &nt_db);
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    // See `fake_db_index_media_metadata`: without the nt_data root the local
    // path is contained out and no media registers, so there is nothing to serve.
    let media_root = qqflow_server::store::media::media_root_of(&nt_db);
    let store = qqflow_server::store::index::build_index(conn, media_root.as_deref()).unwrap();
    drop(reader);
    drop(writer);

    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = Arc::new(AppState {
        store: Arc::new(parking_lot::RwLock::new(store)),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(16).0,
        accounts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        token: Arc::new("test-token".into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(Vec::new(), qqflow_server::sync::watch::WatchConfig::default(), shutdown_rx),
        export_root: Arc::new(std::env::temp_dir().join("qqflow_fake_export")),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    let app = build_router(state.clone());

    // 200 + exact file bytes + jpeg content type.
    let expected = std::fs::read(common::fake_media_path(&nt_db)).unwrap();
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/media/{md5}?access_token=test-token"))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("content-type").unwrap(), "image/jpeg");
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert_eq!(bytes.to_vec(), expected, "exact file bytes");

    // Unknown id -> 404.
    let (s, _v) = common::get_json(app.clone(), "/api/v1/media/deadbeef?access_token=test-token", &[]).await;
    assert_eq!(s, StatusCode::NOT_FOUND);

    // No token -> 401.
    let (s, _v) = common::get_json(app.clone(), &format!("/api/v1/media/{md5}"), &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);

    // File deleted (QQ cleared cache) -> 404 with a clear message.
    std::fs::remove_file(common::fake_media_path(&nt_db)).unwrap();
    let (s, v) = common::get_json(app.clone(), &format!("/api/v1/media/{md5}?access_token=test-token"), &[]).await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert!(
        v["message"].as_str().unwrap().contains("缓存"),
        "cache-cleared message: {v}"
    );
}

/// Cache-index fallback: an image row whose "45812" points at a DELETED
/// file still registers when the fake nt_data cache holds a file named by
/// the row's md5 — and the media endpoint serves its bytes. Real-machine
/// probe: "45812" survives on disk for ~0.3% of media rows while the
/// fallback rescues ~63% of the rest.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn fake_db_media_fallback_registers_and_serves() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    // Row 7's 45812 points at the standard fake media file — delete it so
    // the exact path is dead and only the cache fallback can rescue it.
    let md5 = common::append_image_row(&writer, 7, &nt_db);
    std::fs::remove_file(common::fake_media_path(&nt_db)).unwrap();
    let cache_file = common::write_fake_cache_file(&nt_db, &format!("Pic/2026-08/Ori/{md5}.jpg"));
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let media_root = qqflow_server::store::media::media_root_of(&nt_db);
    let store = qqflow_server::store::index::build_index(conn, media_root.as_deref()).unwrap();
    drop(reader);
    drop(writer);

    let entry = store.media.get(&md5).expect("fallback registered the row");
    assert!(
        entry.local_path.ends_with(&format!("{md5}.jpg")),
        "entry points at the md5-named cache file: {}",
        entry.local_path
    );
    assert_eq!(entry.file_name.as_deref(), Some("fake_image_01.jpg"));

    // mediaId survives the fetchability filter.
    let conv = store.conversation(ChatType::Group, "10001").unwrap();
    let m = conv.msgs.iter().find(|m| m.rowid == 7).expect("row indexed");
    let out = qqflow_server::store::query::with_fetchable_media_id(&store, MessageOut::from_record(m));
    assert_eq!(out.media_id.as_deref(), Some(md5.as_str()));

    // Full chain: /api/v1/media/{id} serves the cache file bytes.
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = Arc::new(AppState {
        store: Arc::new(parking_lot::RwLock::new(store)),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(16).0,
        accounts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        token: Arc::new("test-token".into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(Vec::new(), qqflow_server::sync::watch::WatchConfig::default(), shutdown_rx),
        export_root: Arc::new(std::env::temp_dir().join("qqflow_fake_export")),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    let app = build_router(state.clone());
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/media/{md5}?access_token=test-token"))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert_eq!(bytes.to_vec(), b"fallback cache bytes", "exact cache file bytes");
    assert!(cache_file.exists(), "cache file still in place");
}

/// Fallback interplay: a live "45812" entry is never displaced by a later
/// fallback row (first-wins); a key with no file anywhere stays
/// unregistered and the fetchability filter omits its mediaId.
#[test]
fn fake_db_media_fallback_first_wins_and_no_match() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    // md5_a: row 7 carries a LIVE 45812; row 8 repeats the same key with no
    // 45812 — a cache file exists, but the live exact entry must win.
    let md5_a = common::append_image_row(&writer, 7, &nt_db);
    common::append_image_row_no_local(&writer, 8, &md5_a, "fake_image_01.jpg");
    let cache_file = common::write_fake_cache_file(&nt_db, &format!("Pic/2026-08/Ori/{md5_a}.jpg"));
    // md5_b: no 45812 and no cache file anywhere -> never registered.
    let md5_b = "ffffffffffffffffffffffffffffffff";
    common::append_image_row_no_local(&writer, 9, md5_b, "ghost.png");
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let media_root = qqflow_server::store::media::media_root_of(&nt_db);
    let store = qqflow_server::store::index::build_index(conn, media_root.as_deref()).unwrap();
    drop(reader);
    drop(writer);

    let entry = store.media.get(&md5_a).expect("registered");
    assert!(
        entry.local_path.ends_with("fake_image_01.jpg"),
        "live exact 45812 wins over the fallback: {}",
        entry.local_path
    );
    assert!(!entry.local_path.ends_with(&format!("{md5_a}.jpg")));
    assert!(!store.media.contains_key(md5_b), "no file anywhere -> unregistered");

    let conv = store.conversation(ChatType::Group, "10001").unwrap();
    let rescued = conv.msgs.iter().find(|m| m.rowid == 8).unwrap();
    let out8 = qqflow_server::store::query::with_fetchable_media_id(&store, MessageOut::from_record(rescued));
    assert_eq!(out8.media_id.as_deref(), Some(md5_a.as_str()), "registered key keeps mediaId");
    let ghost = conv.msgs.iter().find(|m| m.rowid == 9).expect("ghost row indexed");
    let out9 = qqflow_server::store::query::with_fetchable_media_id(&store, MessageOut::from_record(ghost));
    assert!(out9.media_id.is_none(), "unregistered key omits mediaId");
    assert!(cache_file.exists(), "cache file still in place");
}

/// WeFlow-shaped media export end to end: `media=1` on /api/v1/messages
/// exports the page's image into the export root, the response carries
/// mediaFileName/mediaUrl/mediaLocalPath, and the three-segment media URL
/// serves the exact bytes — with traversal attacks rejected.
#[tokio::test]
#[allow(clippy::await_holding_lock)]
async fn fake_db_media_export_serves_exported_bytes() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    let md5 = common::append_image_row(&writer, 7, &nt_db);
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    // See `fake_db_index_media_metadata`: the export copies FROM the local
    // cache, so it needs the nt_data root that contains those paths.
    let media_root = qqflow_server::store::media::media_root_of(&nt_db);
    let store = qqflow_server::store::index::build_index(conn, media_root.as_deref()).unwrap();
    drop(reader);
    drop(writer);

    let export_root = std::env::temp_dir().join("qqflow_fake_export");
    let _ = std::fs::remove_dir_all(&export_root);
    let (_shutdown_tx, shutdown_rx) = tokio::sync::watch::channel(false);
    let state = Arc::new(AppState {
        store: Arc::new(parking_lot::RwLock::new(store)),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(16).0,
        accounts: Arc::new(parking_lot::RwLock::new(Vec::new())),
        ready: Arc::new(std::sync::atomic::AtomicBool::new(true)),
        token: Arc::new("test-token".into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(Vec::new(), qqflow_server::sync::watch::WatchConfig::default(), shutdown_rx),
        export_root: Arc::new(export_root.clone()),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    let app = build_router(state.clone());

    // 1) Export via media=1: envelope + per-message export fields.
    let (s, v) = common::get_json(
        app.clone(),
        "/api/v1/messages?talker=10001&media=1&access_token=test-token",
        &[],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["enabled"], true);
    assert_eq!(v["media"]["exportPath"], export_root.to_string_lossy().as_ref());
    assert_eq!(v["media"]["count"], 1, "the image row exported");
    let m = v["messages"].as_array().unwrap().iter().find(|m| m["mediaId"] == md5).expect("image row");
    // Export names are key-derived (<md5>.jpg): unique per content.
    let exported_name = format!("{md5}.jpg");
    assert_eq!(m["mediaFileName"], exported_name);
    // 根相对路径、不含凭据：调用方按自己的基址拼接。
    assert_eq!(m["mediaUrl"], format!("/api/v1/media/10001/images/{exported_name}"));
    let local = m["mediaLocalPath"].as_str().unwrap();
    assert!(local.starts_with(export_root.to_string_lossy().as_ref()));

    // 2) The exported file exists with the fake JPEG bytes.
    let expected = std::fs::read(common::fake_media_path(&nt_db)).unwrap();
    assert_eq!(std::fs::read(local).unwrap(), expected);

    // 3) GET the three-segment URL -> exact bytes + image/jpeg.
    let resp = app
        .clone()
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/media/10001/images/{exported_name}?access_token=test-token"))
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(resp.headers().get("content-type").unwrap(), "image/jpeg");
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    assert_eq!(bytes.to_vec(), expected);

    // 4) POST variant works too (WeFlow GET|POST).
    let (s, _v) = common::post_json(
        app.clone(),
        &format!("/api/v1/media/10001/images/{exported_name}"),
        &[],
        serde_json::json!({"access_token": "test-token"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);

    // 5) Traversal attacks -> 404, never a file outside the export root.
    for path in [
        "/api/v1/media/../token.txt",
        "/api/v1/media/10001/images/..%2F..%2Fsecret",
        "/api/v1/media/..%2F..%2F..%2Fqqflow-server.json",
    ] {
        let (s, _v) = common::get_json(
            app.clone(),
            &format!("{path}?access_token=test-token"),
            &[],
        )
        .await;
        assert_eq!(s, StatusCode::NOT_FOUND, "traversal blocked: {path}");
    }

    // 6) Unknown media_type -> 404.
    let (s, _v) = common::get_json(
        app.clone(),
        &format!("/api/v1/media/10001/other/{exported_name}?access_token=test-token"),
        &[],
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
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
        FAKE_QQ.into(),
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

    // Sync rows carry senderName like the messages query does — the row is
    // from u_a in group 10001, whose card ("40090") this batch itself just
    // registered, so the field proves resolution happens after the apply
    // phase and not against a stale store.
    let group_row = first.iter().find(|m| m.sender_username == "u_a").unwrap();
    assert_eq!(
        group_row.sender_name, "张三群名片",
        "sync rows resolve the in-group card, same as /api/v1/messages"
    );
    let c2c_row = first.iter().find(|m| m.sender_username == "u_12345").unwrap();
    assert_eq!(
        c2c_row.sender_name, "王五",
        "c2c has no card in scope -> the message nick (40093)"
    );

    let second = account.poll_once().unwrap();
    assert_eq!(second.len(), 1, "second poll returns only the new row");
    assert_eq!(second[0].content, "手动同步新增");
    assert_eq!(second[0].sender_name, "张三群名片", "incremental rows too");

    // The new row must also be broadcast as an SSE event.
    let ev = rx.try_recv().unwrap();
    assert_eq!(ev.event, "message.new");
    assert_eq!(ev.content, "手动同步新增");
    // SSE `sourceName` and the response row's `senderName` are the same
    // resolution — a client mixing the two channels sees one name per sender.
    assert_eq!(
        ev.source_name.as_deref(),
        Some(second[0].sender_name.as_str()),
        "SSE sourceName agrees with the sync row's senderName"
    );

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
        FAKE_QQ.into(),
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
        export_root: Arc::new(std::env::temp_dir().join("qqflow_fake_export")),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    });
    let app = build_router(state.clone());

    // Boot state: account discovered, awaiting a key. A scan result is not a
    // binding, so /health still reports `unregistered`.
    let v =
        common::wait_account_state(&app, "test-token", FAKE_QQ, "awaiting_key", Duration::from_secs(15))
            .await;
    assert_eq!(v["success"], true);
    let (s, h) = common::get_json(app.clone(), "/health", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(h["status"], "starting");
    assert_eq!(h["account"], "unregistered");

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
    // accepted always reports the freshly-set indexing status plus the
    // resolved database — `indexing` is NOT a claim that the key is good
    // (this very registration is about to fail its decrypt check).
    assert_eq!(v["status"], "indexing");
    assert_eq!(v["db_path"], db_path);
    let v =
        common::wait_account_state(&app, "test-token", FAKE_QQ, "error", Duration::from_secs(15)).await;
    let err = common::account_entry(&v, FAKE_QQ)["error"].as_str().unwrap().to_string();
    println!("[GT] expected init failure: {err}");
    assert!(err.contains("解密") || err.contains("密钥"), "error must explain: {err}");
    // The reason is behind the token; /health only admits the phase.
    let (_, h) = common::get_json(app.clone(), "/health", &[]).await;
    assert_eq!(h["account"], "error");
    assert!(h.get("error").is_none(), "/health must not carry the failure reason");

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
    assert_eq!(v["status"], "indexing");
    assert_eq!(v["db_path"], db_path);
    let v =
        common::wait_account_state(&app, "test-token", FAKE_QQ, "ready", Duration::from_secs(15)).await;
    assert_eq!(common::account_entry(&v, FAKE_QQ)["message_count"], 8);
    assert_eq!(common::account_entry(&v, FAKE_QQ)["db_path"], db_path);

    let (_, h) = common::get_json(app.clone(), "/health", &[]).await;
    assert_eq!(h["status"], "ok");
    assert_eq!(h["account"], "ready");

    // Idempotent re-registration of the now-ready account: status mirrors
    // /health and db_path echoes the running account's own path (this
    // request omits db_path, so the echo proves it came from the registry).
    let (s, v) = common::post_json(
        app.clone(),
        "/api/v1/accounts",
        &[],
        json!({"access_token": "test-token", "qq": FAKE_QQ, "key": FAKE_KEY}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "already_ready");
    assert_eq!(v["status"], "ready");
    assert_eq!(v["db_path"], db_path);

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


/// A reply exposes the quoted message's id — but only when the target is
/// unambiguous.
///
/// `(conversation, "40003")` is NOT unique in a real database (1371 repeating
/// key groups were measured), so the obvious lookup can land on several rows.
/// The contract this endpoint makes is "the id, when present, is correct";
/// guessing would break it silently, because a client matches the id against
/// another message in the same conversation and cannot tell a wrong match from
/// a right one.
#[test]
fn fake_db_reply_is_stated_only_when_unambiguous() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let nt_db = fake_db_path().parent().unwrap().to_path_buf();
    let (writer, _raw) = common::open_fake_source(&nt_db, 0);
    common::seed_reply_rows(&writer);
    common::materialize_source(&nt_db);

    let mut reader = LiveReader::new(fake_db_path(), FAKE_KEY.into());
    reader.open().unwrap();
    let conn = reader.acquire().unwrap();
    let store = qqflow_server::store::index::build_index(conn, None).unwrap();
    drop(reader);

    let q = MessageQuery {
        talker: "10001",
        limit: 50,
        offset: 0,
        start: None,
        end: None,
        keyword: None,
    };
    let (rows, _) = query_messages(&store, &q);
    let seq_of = |rowid: i64| {
        store
            .conversation(ChatType::Group, "10001")
            .unwrap()
            .msgs
            .iter()
            .find(|m| m.rowid == rowid)
            .expect("row indexed")
            .seq
            .to_string()
    };
    let served = |rowid: i64| {
        let seq = seq_of(rowid);
        rows.iter().find(|r| r.server_id == seq).expect("row served")
    };

    // The quoted row is on the page, so the id is checkable rather than
    // merely plausible.
    assert_eq!(
        served(7).reply_to_message_id.as_deref(),
        Some(seq_of(2).as_str()),
        "a unique target becomes the quoted row's platformMessageId"
    );

    // Two rows carry the wanted number: stating either one would be a coin
    // flip, so the key is absent instead.
    assert_eq!(served(8).reply_to_message_id, None, "ambiguous target -> omitted");
    // No reply at all is absent too — never null.
    assert_eq!(served(9).reply_to_message_id, None, "no reply -> omitted");
}

/// Ground truth for the reply relation: does `40850` point at a message in the
/// same conversation, and at which column of it?
///
/// Ignored by default; requires QQFLOW_TEST_DB_ROOT + QQFLOW_TEST_DB_KEY.
///
/// Prints statistics only — never a platform id, a group number or a message
/// body. It exists to settle what the format notes cannot: whether the upstream
/// field documentation matches **this** database.
#[test]
#[ignore]
fn real_db_reply_relation() {
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
    for info in &accounts {
        let mut reader = LiveReader::new(info.path.clone(), key.clone());
        reader.open().expect("real DB must open read-only through the offset VFS");
        let conn = reader.acquire().unwrap();

        // ① The local shape decides: check the columns actually present before
        // trusting any upstream note.
        let cols: Vec<String> = conn
            .prepare("PRAGMA table_info(group_msg_table)")
            .unwrap()
            .query_map([], |r| r.get::<_, String>(1))
            .unwrap()
            .flatten()
            .collect();
        let has = |c: &str| cols.iter().any(|x| x == c);
        println!("[GT] reply: 列数 {}，40850={} 40003={} 40027={} 40001={}",
            cols.len(), has("40850"), has("40003"), has("40027"), has("40001"));
        if !has("40850") || !has("40003") {
            println!("[GT] reply: 本库缺 40850 或 40003 —— 上游文档与本库形态不符，到此为止");
            continue;
        }

        let total: i64 = conn
            .query_row("SELECT count(*) FROM group_msg_table", [], |r| r.get(0))
            .unwrap();
        let nonzero: i64 = conn
            .query_row(
                "SELECT count(*) FROM group_msg_table WHERE \"40850\" IS NOT NULL AND \"40850\" != 0",
                [],
                |r| r.get(0),
            )
            .unwrap();
        println!("[GT] reply: 总行 {total}；40850 非零 {nonzero}（{:.2}%）",
            100.0 * nonzero as f64 / total.max(1) as f64);

        // ② Resolution and direction in one pass over a bounded sample: the
        // table is large and unindexed on 40003, so an unbounded correlated
        // subquery would scan it per row.
        let sample = nonzero.min(2000);
        let mut stmt = conn
            .prepare(
                "SELECT a.\"40850\", a.\"40001\", \
                 (SELECT b.\"40001\" FROM group_msg_table b \
                  WHERE b.\"40027\" = a.\"40027\" AND b.\"40003\" = a.\"40850\" LIMIT 1) \
                 FROM group_msg_table a WHERE a.\"40850\" != 0 LIMIT ?1",
            )
            .unwrap();
        let rows: Vec<(i64, Option<i64>)> = stmt
            .query_map([sample], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .flatten()
            .collect();
        let resolved = rows.iter().filter(|(_, t)| t.is_some()).count();
        println!("[GT] reply: 抽样 {sample} 行，能在同会话内解析到 40003 的 {resolved}（{:.2}%）",
            100.0 * resolved as f64 / sample.max(1) as f64);

        // ③ A reply must point BACKWARDS: the target has to be older than the
        // message quoting it. If it does not, 40850 means something else here.
        let mut stmt2 = conn
            .prepare(
                "SELECT a.\"40001\", b.\"40001\" FROM group_msg_table a \
                 JOIN group_msg_table b ON b.\"40027\" = a.\"40027\" AND b.\"40003\" = a.\"40850\" \
                 WHERE a.\"40850\" != 0 LIMIT ?1",
            )
            .unwrap();
        let joins: Vec<(i64, i64)> = stmt2
            .query_map([sample], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .flatten()
            .collect();
        let earlier = joins.iter().filter(|(a, b)| b < a).count();
        println!("[GT] reply: 连接抽样 {} 对，目标更早（40001 更小）的 {earlier}（{:.2}%）",
            joins.len(), 100.0 * earlier as f64 / joins.len().max(1) as f64);

        // ④ Which outer types carry a reply — the set the parser must cover.
        let mut stmt3 = conn
            .prepare(
                "SELECT \"40011\", count(*) FROM group_msg_table \
                 WHERE \"40850\" != 0 GROUP BY \"40011\" ORDER BY 2 DESC LIMIT 6",
            )
            .unwrap();
        let types: Vec<(i64, i64)> = stmt3
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .flatten()
            .collect();
        println!("[GT] reply: 带引用的行按 40011 分布（前 6）：{types:?}");
    }
}


/// Can the reply target be disambiguated? `(40027, 40003)` repeats, so the
/// documented lookup can land on more than one row. This measures whether
/// bounding the target by TIME (`40050` not after the reply) leaves exactly one
/// candidate — the disambiguation any implementation would have to rely on.
#[test]
#[ignore]
fn real_db_reply_relation_disambiguation() {
    let root = std::env::var("QQFLOW_TEST_DB_ROOT").expect("QQFLOW_TEST_DB_ROOT");
    let key = std::env::var("QQFLOW_TEST_DB_KEY").expect("QQFLOW_TEST_DB_KEY");
    let accounts = scan::scan_accounts(Some(Path::new(&root))).expect("scan custom root");
    for info in &accounts {
        let mut reader = LiveReader::new(info.path.clone(), key.clone());
        reader.open().expect("real DB must open");
        let conn = reader.acquire().unwrap();

        // ① How many duplicate key groups are duplicates IN TIME (the same
        // message stored twice) versus genuinely distinct rows?
        let (dup_total, dup_same_time): (i64, i64) = conn
            .query_row(
                "SELECT count(*), sum(CASE WHEN n_ts = 1 THEN 1 ELSE 0 END) FROM (\
                 SELECT \"40027\", \"40003\", count(DISTINCT \"40050\") AS n_ts \
                 FROM group_msg_table GROUP BY 1, 2 HAVING count(*) > 1)",
                [],
                |r| Ok((r.get(0)?, r.get(1)?)),
            )
            .unwrap();
        println!("[GT] reply3: 重复键组 {dup_total}，其中同一时间的 {dup_same_time}");

        // ② Candidate counts once the target is bounded by time.
        let mut stmt = conn
            .prepare(
                "SELECT cnt, count(*) FROM (SELECT a.rowid AS rid, \
                 (SELECT count(*) FROM group_msg_table b WHERE b.\"40027\" = a.\"40027\" \
                  AND b.\"40003\" = a.\"40850\" AND b.\"40050\" <= a.\"40050\") AS cnt \
                 FROM group_msg_table a WHERE a.\"40850\" != 0) GROUP BY cnt ORDER BY cnt",
            )
            .unwrap();
        let dist: Vec<(i64, i64)> = stmt
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .flatten()
            .collect();
        println!("[GT] reply3: 时间上界内候选数分布（候选数, 行数）：{dist:?}");

        // ③ Same, but also requiring the target to be strictly older, which is
        // what a reply means: the newest match strictly before the reply.
        let mut stmt2 = conn
            .prepare(
                "SELECT cnt, count(*) FROM (SELECT a.rowid AS rid, \
                 (SELECT count(*) FROM group_msg_table b WHERE b.\"40027\" = a.\"40027\" \
                  AND b.\"40003\" = a.\"40850\" AND b.\"40050\" < a.\"40050\") AS cnt \
                 FROM group_msg_table a WHERE a.\"40850\" != 0) GROUP BY cnt ORDER BY cnt",
            )
            .unwrap();
        let dist2: Vec<(i64, i64)> = stmt2
            .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
            .unwrap()
            .flatten()
            .collect();
        println!("[GT] reply3: 严格更早的候选数分布：{dist2:?}");
    }
}


/// Last look at the ambiguous rows: what DOES `40900` contain, and is there a
/// sender-level tie-break?
///
/// The previous probe only checked one encoding of one column, so "the blob has
/// no candidate" was not yet a safe conclusion. This checks both columns in both
/// encodings (protobuf varint and decimal text), plus whether the candidates
/// differ by sender.
#[test]
#[ignore]
fn real_db_reply_relation_ambiguous_detail() {
    fn varint(mut v: u64) -> Vec<u8> {
        let mut out = Vec::new();
        loop {
            let mut b = (v & 0x7f) as u8;
            v >>= 7;
            if v != 0 {
                b |= 0x80;
            }
            out.push(b);
            if v == 0 {
                return out;
            }
        }
    }
    fn contains(hay: &[u8], needle: &[u8]) -> bool {
        !needle.is_empty() && hay.len() >= needle.len() && hay.windows(needle.len()).any(|w| w == needle)
    }

    let root = std::env::var("QQFLOW_TEST_DB_ROOT").expect("QQFLOW_TEST_DB_ROOT");
    let key = std::env::var("QQFLOW_TEST_DB_KEY").expect("QQFLOW_TEST_DB_KEY");
    let accounts = scan::scan_accounts(Some(Path::new(&root))).expect("scan custom root");
    for info in &accounts {
        let mut reader = LiveReader::new(info.path.clone(), key.clone());
        reader.open().expect("real DB must open");
        let conn = reader.acquire().unwrap();

        /// One ambiguous row: (rowid, "40900" blob, conversation, reply
        /// target number, sender uid).
        type AmbiguousRow = (i64, Option<Vec<u8>>, i64, i64, String);
        let rows: Vec<AmbiguousRow> = {
            let mut stmt = conn
                .prepare(
                    "SELECT a.rowid, a.\"40900\", a.\"40027\", a.\"40850\", coalesce(a.\"40020\", \"\") \
                     FROM group_msg_table a WHERE a.\"40850\" != 0 AND (SELECT count(*) FROM group_msg_table b \
                       WHERE b.\"40027\" = a.\"40027\" AND b.\"40003\" = a.\"40850\" \
                       AND b.\"40050\" <= a.\"40050\") > 1",
                )
                .unwrap();
            stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?, r.get(3)?, r.get(4)?)))
                .unwrap()
                .flatten()
                .collect()
        };
        let (mut blob_hits_seq, mut blob_hits_no, mut blob_text_hits, mut sender_unique) = (0, 0, 0, 0);
        for (rowid, blob, sess, target, sender) in &rows {
            let mut cs = conn
                .prepare(
                    "SELECT \"40001\", \"40003\", coalesce(\"40020\", \"\") FROM group_msg_table \
                     WHERE \"40027\" = ?1 AND \"40003\" = ?2 AND \"40050\" <= \
                       (SELECT \"40050\" FROM group_msg_table WHERE rowid = ?3)",
                )
                .unwrap();
            let cands: Vec<(i64, i64, String)> = cs
                .query_map([sess, target, rowid], |r| Ok((r.get(0)?, r.get(1)?, r.get(2)?)))
                .unwrap()
                .flatten()
                .collect();
            if cands.iter().filter(|(_, _, s)| s == sender).count() == 1 {
                sender_unique += 1;
            }
            let Some(blob) = blob else { continue };
            let by_seq = cands.iter().filter(|(s, _, _)| contains(blob, &varint(*s as u64))).count();
            let by_no = cands.iter().filter(|(_, n, _)| contains(blob, &varint(*n as u64))).count();
            let by_text = cands
                .iter()
                .filter(|(s, n, _)| {
                    contains(blob, s.to_string().as_bytes()) || contains(blob, n.to_string().as_bytes())
                })
                .count();
            if by_seq == 1 {
                blob_hits_seq += 1;
            }
            if by_no == 1 {
                blob_hits_no += 1;
            }
            if by_text == 1 {
                blob_text_hits += 1;
            }
        }
        println!("[GT] reply5: 多候选 {} 行；blob 唯一命中 40001={blob_hits_seq} 40003={blob_hits_no} 文本形态={blob_text_hits}；候选里发送者唯一={sender_unique}",
            rows.len());
    }
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
            // Structured 40800 decode on real blobs (arbitrates
            // parser::proto): media segments (content type 2/4/5) with
            // their metadata, and how often the "45812" local path exists
            // on disk (absolute vs relative tells us the serving strategy).
            {
                let mut media_segs = 0i64;
                let mut with_local = 0i64;
                let mut local_exists = 0i64;
                let mut samples = 0usize;
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT \"40800\" FROM {table} WHERE \"40800\" IS NOT NULL ORDER BY rowid DESC LIMIT 500"
                    ))
                    .unwrap();
                let blobs = stmt.query_map([], |r| r.get::<_, Vec<u8>>(0)).unwrap();
                for b in blobs.flatten() {
                    for seg in qqflow_server::parser::proto::parse_msg_body(&b) {
                        if matches!(seg.content_type, Some(2) | Some(4) | Some(5))
                            && let Some(m) = &seg.media
                        {
                            media_segs += 1;
                            if let Some(p) = &m.local_path {
                                    with_local += 1;
                                    if std::path::Path::new(p).exists() {
                                        local_exists += 1;
                                    }
                                }
                                if samples < 5 {
                                    println!(
                                        "[GT] {table} media sample: ct={:?} subtype={:?} uuid={:?} md5={:?} name={:?} size={:?} dims=({:?}x{:?}) localPath={:?} urls={:?}",
                                        seg.content_type, seg.media_subtype, m.uuid, m.md5_hex, m.file_name, m.size, m.width, m.height, m.local_path, m.urls
                                    );
                                    samples += 1;
                                }
                        }
                    }
                }
                println!(
                    "[GT] {table} 40800 structured: mediaSegments={media_segs} withLocalPath={with_local} localExistsOnDisk={local_exists}"
                );
            }
        }

        // ---- spec-derived columns (arbitrates store::index 40013/40050/40090)
        // QQDecrypt/nt_msg_db_util field analysis: 40013 = message direction
        // (0 other / 1,2 self / 3 system), 40050 = unix send time (seconds),
        // 40090 = sender group card (group table). Each statistic gates the
        // corresponding column in store::index — absent columns degrade.
        for (table, is_group) in [("group_msg_table", true), ("c2c_msg_table", false)] {
            let cols: BTreeSet<String> = conn
                .prepare(&format!("PRAGMA table_info(\"{table}\")"))
                .unwrap()
                .query_map([], |r| r.get::<_, String>(1))
                .unwrap()
                .flatten()
                .collect();
            for cand in ["40013", "40050", "40090"] {
                println!("[GT] {table} has {cand}: {}", cols.contains(cand));
            }
            if !cols.contains("40013") {
                continue;
            }
            // 40013 distribution: must be ⊆ {0,1,2,3} with 0 or 1 present —
            // arbitrates the is_send mapping (0->0, 1/2->1, 3->0).
            let mut hist: std::collections::BTreeMap<i64, i64> = std::collections::BTreeMap::new();
            {
                let mut stmt = conn
                    .prepare(&format!(
                        "SELECT \"40013\", count(*) FROM {table} WHERE \"40013\" IS NOT NULL GROUP BY \"40013\""
                    ))
                    .unwrap();
                for r in stmt
                    .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                    .unwrap()
                    .flatten()
                {
                    hist.insert(r.0, r.1);
                }
            }
            let total_dir: i64 = hist.values().sum();
            println!("[GT] {table} 40013 histogram: {hist:?}");
            if total_dir > 0 {
                // Spec pins 0/1/2/3; the real DB also shows a bitmask-like
                // value (32761 = 0x7FF9, observed with system messages) —
                // production maps anything not 1/2 to is_send 0, so unknown
                // values are printed here, not asserted away.
                let unknown: Vec<i64> = hist
                    .keys()
                    .filter(|k| !(0..=3).contains(*k))
                    .copied()
                    .collect();
                if !unknown.is_empty() {
                    println!("[GT] {table} 40013 unknown values (mapped to is_send 0): {unknown:?}");
                }
                assert!(
                    hist.contains_key(&0) || hist.contains_key(&1),
                    "{table}: 40013 lacks 0/1 values: {hist:?}"
                );
            }
            // 40050 vs seq>>32 agreement: the explicit column must match the
            // packed timestamp; wholesale mismatch means 40050 has a
            // different semantic and must NOT be adopted as authoritative.
            if cols.contains("40050") {
                let mut max_diff = 0i64;
                let mut over = 0i64;
                let mut n = 0i64;
                {
                    let mut stmt = conn
                        .prepare(&format!(
                            "SELECT \"40001\", \"40050\" FROM {table} \
                             WHERE \"40050\" IS NOT NULL AND \"40050\" > 0 ORDER BY rowid LIMIT 2000"
                        ))
                        .unwrap();
                    let rows = stmt
                        .query_map([], |r| Ok((r.get::<_, i64>(0)?, r.get::<_, i64>(1)?)))
                        .unwrap();
                    for r in rows.flatten() {
                        let diff = (seq_to_time(r.0) - r.1).abs();
                        max_diff = max_diff.max(diff);
                        if diff > 2 {
                            over += 1;
                        }
                        n += 1;
                    }
                }
                println!("[GT] {table} 40050-vs-seq>>32: n={n} maxDiff={max_diff} diff>2={over}");
            }
            // 40090: sender group card (group table only) — non-empty rate,
            // plus the SAME rows' 40093 nickname so a noisy 40090 (e.g.
            // synthesized "name(qq)" strings) can be told apart from a real
            // card before it wins the display preference.
            if is_group && cols.contains("40090") {
                let (total, nonempty): (i64, i64) = conn
                    .query_row(
                        &format!("SELECT count(*), count(CASE WHEN \"40090\" != '' THEN 1 END) FROM {table}"),
                        [],
                        |r| Ok((r.get(0)?, r.get(1)?)),
                    )
                    .unwrap();
                println!("[GT] {table} 40090 nonEmpty={nonempty}/{total}");
                if nonempty > 0 {
                    let mut stmt = conn
                        .prepare(&format!(
                            "SELECT DISTINCT \"40090\", \"40093\" FROM {table} WHERE \"40090\" != '' LIMIT 5"
                        ))
                        .unwrap();
                    let samples: Vec<(String, String)> = stmt
                        .query_map([], |r| Ok((r.get(0)?, r.get(1)?)))
                        .unwrap()
                        .flatten()
                        .collect();
                    for (card, nick) in &samples {
                        println!("[GT] {table} 40090-vs-40093: card={card:?} nick={nick:?}");
                    }
                    // Decisive test: 40090 is the SENDER's card iff one group
                    // has multiple distinct values (a group name would be
                    // one value per group).
                    let max_distinct_per_group: i64 = conn
                        .query_row(
                            &format!(
                                "SELECT max(per_group) FROM (SELECT \"40021\", \
                                 count(DISTINCT \"40090\") AS per_group FROM {table} \
                                 WHERE \"40090\" != '' GROUP BY \"40021\")"
                            ),
                            [],
                            |r| r.get(0),
                        )
                        .unwrap();
                    println!("[GT] {table} 40090 maxDistinctPerGroup={max_distinct_per_group}");
                }
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
                    )) && let Ok(mut rows) = st.query_map([uid], |r| r.get::<_, String>(0))
                        && let Some(Ok(n)) = rows.next()
                    {
                        msg_nick = n;
                        break;
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

/// [45812] Trust-boundary probe for the local cache path.
///
/// `store::media::resolve_local_path` takes an ABSOLUTE "45812" as-is, with no
/// containment check, on the stated grounds that it "comes from QQ's own DB".
/// That is an assumption about NTQQ, not something the server enforces: 45812 is
/// decoded from the same 40800 blob as 45402 (file name) and 45424 (md5), so if a
/// remote sender's value could survive into the receiver's database, then
/// `/api/v1/media/{id}` would read, and `media=1` would additionally COPY, an
/// attacker-chosen absolute path.
///
/// This measures the real distribution instead of guessing. Per direction
/// ("40013": 0 received / 1,2 sent / 3 system) it counts absolute vs relative
/// values, how many resolve under this account's own `nt_data`, and how many
/// carry a hostile shape (`..`, a control character, or a `:` past the drive
/// letter). Received rows are the ones that matter — a value there that points
/// outside this machine's own cache is the smoking gun.
///
/// What this can and cannot establish: it proves what is (or is not) present in
/// a real database, so a non-zero hostile count is conclusive. Zero is strong
/// evidence that NTQQ rewrites the field locally, but it is not a protocol
/// proof — only injecting a crafted message from a peer would be that. Treat a
/// clean result as "no observed exposure on real data", not "unreachable".
///
/// Ignored by default; same env vars as [`real_db_groundtruth`].
#[test]
#[ignore]
fn real_db_local_path_trust_boundary() {
    let (root, key) = match (std::env::var("QQFLOW_TEST_DB_ROOT"), std::env::var("QQFLOW_TEST_DB_KEY")) {
        (Ok(r), Ok(k)) => (r, k),
        _ => {
            println!("[45812] SKIPPED: QQFLOW_TEST_DB_ROOT / QQFLOW_TEST_DB_KEY not set");
            return;
        }
    };

    #[derive(Default)]
    struct Stat {
        rows: u64,
        with_media: u64,
        with_path: u64,
        absolute: u64,
        relative: u64,
        under_own_nt_data: u64,
        outside_own_nt_data: u64,
        hostile: u64,
        dotdot: u64,
        ctrl: u64,
        stray_colon: u64,
        ntos_prefix: u64,
        resolvable: u64,
        resolvable_outside: u64,
    }
    /// QQ's own marker for "the full-resolution original", written by NTQQ ahead
    /// of an absolute path. It is not attacker syntax, and it is inert: the
    /// leading `::` means [`Path::is_absolute`] is false, so the value takes the
    /// RELATIVE branch of `resolve_local_path`, gets joined under `nt_data`
    /// (no escape — `::NTOSFull::D:\...` is a single ordinary component to
    /// `Path::join`, not a drive prefix), and then fails closed at `canonicalize`
    /// with `InvalidFilename` (Windows os error 123). Measured, not assumed.
    const NTOS_PREFIX: &str = "::NTOSFull::";

    /// A hostile shape for a value that will be fed to the filesystem with no
    /// containment check. `:` is only legitimate as the drive colon at index 1.
    /// Returns the reasons separately — an aggregate count cannot distinguish a
    /// traversal from a benign shape that merely trips one of the tests.
    ///
    /// [`NTOS_PREFIX`] is stripped before the colon test: leaving it in makes
    /// every thumbnail row on a real database look hostile (86 of 1170 here),
    /// which drowns the signal we are actually looking for. `..` and control
    /// characters are still tested on the FULL string, so the strip cannot be
    /// used to smuggle either one past this check.
    fn hostile(p: &str) -> (bool, bool, bool) {
        let dotdot = std::path::Path::new(p)
            .components()
            .any(|c| matches!(c, std::path::Component::ParentDir));
        let ctrl = p.contains(|c: char| c.is_control());
        let colon_scan = p.strip_prefix(NTOS_PREFIX).unwrap_or(p);
        let stray_colon = colon_scan.char_indices().any(|(i, c)| c == ':' && i != 1);
        (dotdot, ctrl, stray_colon)
    }
    /// Case-insensitive prefix test: Windows paths differ only in case here, and
    /// the real values mix `D:\AppData\...` with QQ's own capitalization.
    fn under(path: &str, root: &std::path::Path) -> bool {
        let norm = |s: &str| s.to_ascii_lowercase().replace('/', "\\");
        norm(path).starts_with(&norm(&root.to_string_lossy()))
    }

    let accounts = scan::scan_accounts(Some(Path::new(&root))).expect("scan custom root");
    let mut grand_hostile = 0u64;
    let mut grand_outside = 0u64;
    let mut grand_resolvable_outside = 0u64;

    for info in &accounts {
        let own_nt_data = info
            .path
            .parent()
            .and_then(qqflow_server::store::media::media_root_of)
            .expect("nt_data root derives from the db dir");
        // `resolve_local_path` returns canonicalized paths, so containment has to
        // be judged against a canonicalized root too. Falls back to the plain root
        // if nt_data does not exist (an account with no cached media yet).
        let canon_nt_data = own_nt_data.canonicalize().unwrap_or_else(|_| own_nt_data.clone());
        println!(
            "[45812] qq {} nt_data root = {} (canonical {})",
            info.qq,
            own_nt_data.display(),
            canon_nt_data.display()
        );

        let mut reader = LiveReader::new(info.path.clone(), key.clone());
        reader.open().expect("real DB must open read-only through the offset VFS");
        let conn = reader.acquire().unwrap();

        // 0 received / 1,2 sent / 3 system / None unknown -> one bucket each.
        let mut buckets: std::collections::BTreeMap<&'static str, Stat> = Default::default();
        let mut samples: Vec<String> = Vec::new();
        let mut hostile_samples: Vec<String> = Vec::new();

        for table in ["group_msg_table", "c2c_msg_table"] {
            // "40013" and "40800" are not guaranteed present on every schema
            // revision — introspect before selecting, like the loader does.
            let cols: BTreeSet<String> = {
                let mut stmt = conn.prepare(&format!("PRAGMA table_info(\"{table}\")")).unwrap();
                let rows = stmt.query_map([], |r| r.get::<_, String>(1)).unwrap();
                rows.flatten().collect()
            };
            if !cols.contains("40800") {
                println!("[45812] {table}: no 40800 column, skipping");
                continue;
            }
            let has_dir = cols.contains("40013");
            let sql = if has_dir {
                format!("SELECT \"40800\", \"40013\" FROM {table}")
            } else {
                format!("SELECT \"40800\", NULL FROM {table}")
            };
            let mut stmt = conn.prepare(&sql).unwrap();
            let rows = stmt
                .query_map([], |r| {
                    Ok((r.get::<_, Option<Vec<u8>>>(0)?, r.get::<_, Option<i64>>(1)?))
                })
                .unwrap();

            for (blob, dir) in rows.flatten() {
                let bucket = match dir {
                    Some(0) => "received",
                    Some(1) | Some(2) => "sent",
                    Some(3) => "system",
                    _ => "unknown",
                };
                let st = buckets.entry(bucket).or_default();
                st.rows += 1;
                let Some(blob) = blob else { continue };
                let parsed = qqflow_server::parser::extract_message(&blob);
                let Some(media) = parsed.media.as_ref() else { continue };
                st.with_media += 1;
                let Some(lp) = media.local_path.as_deref().filter(|s| !s.is_empty()) else {
                    continue;
                };
                st.with_path += 1;
                let is_abs = Path::new(lp).is_absolute();
                if is_abs {
                    st.absolute += 1;
                } else {
                    st.relative += 1;
                }
                // Relative values resolve against own nt_data by construction,
                // so containment only has meaning for the absolute ones.
                if !is_abs || under(lp, &own_nt_data) {
                    st.under_own_nt_data += 1;
                } else {
                    st.outside_own_nt_data += 1;
                    if samples.len() < 8 {
                        samples.push(format!("[{bucket}] OUTSIDE own nt_data: {lp:?}"));
                    }
                }
                if lp.starts_with(NTOS_PREFIX) {
                    st.ntos_prefix += 1;
                }
                // What the production resolver actually returns today. This is
                // the number that decides how a containment fix must be shaped:
                // rows that do not resolve now cannot be broken by tightening,
                // and `resolvable_outside` is exactly what a naive
                // "must live under nt_data" rule would start rejecting.
                // Deliberately NOT `resolve_local_path`: that function now
                // enforces containment, so asking it would make the count 0 by
                // construction and measure the fix instead of the data. This
                // replicates the OLD permissive resolution — join-or-take-as-is,
                // then canonicalize — which is what tells us how much real media
                // the containment rule actually costs.
                let raw_joined = if is_abs {
                    Some(Path::new(lp).to_path_buf())
                } else if Path::new(lp)
                    .components()
                    .any(|c| matches!(c, std::path::Component::ParentDir))
                {
                    None
                } else {
                    Some(own_nt_data.join(lp))
                };
                if let Some(got) = raw_joined.and_then(|p| p.canonicalize().ok()) {
                    st.resolvable += 1;
                    // Compare against the CANONICAL root, with `Path::starts_with`
                    // (component-wise) rather than the string `under()` helper:
                    // canonicalize returns a `\\?\`-prefixed verbatim path on
                    // Windows that no raw-string prefix test can ever match.
                    // Getting this wrong reports 100% of rows as "outside".
                    if !got.starts_with(&canon_nt_data) {
                        st.resolvable_outside += 1;
                    }
                }
                let (dotdot, ctrl, stray_colon) = hostile(lp);
                if dotdot || ctrl || stray_colon {
                    st.hostile += 1;
                    st.dotdot += u64::from(dotdot);
                    st.ctrl += u64::from(ctrl);
                    st.stray_colon += u64::from(stray_colon);
                    // Separate buffer: the OUTSIDE samples would otherwise fill
                    // the quota before a single hostile value was ever shown.
                    if hostile_samples.len() < 10 {
                        let why = [("..", dotdot), ("ctrl", ctrl), (":", stray_colon)]
                            .iter()
                            .filter(|(_, hit)| *hit)
                            .map(|(n, _)| *n)
                            .collect::<Vec<_>>()
                            .join("+");
                        hostile_samples.push(format!("[{bucket}] why={why} abs={is_abs} {lp:?}"));
                    }
                }
            }
        }

        for (name, s) in &buckets {
            println!(
                "[45812] qq {} {name}: rows={} media={} path={} abs={} rel={} underOwn={} OUTSIDE={} HOSTILE={} (..={} ctrl={} colon={}) ntos={} resolvable={} resolvableOutside={}",
                info.qq,
                s.rows,
                s.with_media,
                s.with_path,
                s.absolute,
                s.relative,
                s.under_own_nt_data,
                s.outside_own_nt_data,
                s.hostile,
                s.dotdot,
                s.ctrl,
                s.stray_colon,
                s.ntos_prefix,
                s.resolvable,
                s.resolvable_outside
            );
            grand_hostile += s.hostile;
            grand_outside += s.outside_own_nt_data;
            grand_resolvable_outside += s.resolvable_outside;
        }
        for s in &samples {
            println!("[45812] sample {s}");
        }
        for s in &hostile_samples {
            println!("[45812] HOSTILE {s}");
        }
        drop(reader);
    }

    println!(
        "[45812] TOTAL outside-own-nt_data={grand_outside} resolvable-outside={grand_resolvable_outside} hostile={grand_hostile}"
    );
    // Not asserted as a hard invariant: a legitimate account migration or a QQ
    // reinstall can leave absolute paths naming an older root, so "outside" is
    // reported for judgement rather than failed on. A hostile SHAPE has no
    // benign explanation, and is what an exploit would have to produce.
    assert_eq!(
        grand_hostile, 0,
        "a hostile 45812 shape on real data means the absolute branch of resolve_local_path is reachable with attacker input"
    );
    // The measurement that justifies containing the absolute branch to
    // `media_root`, computed against the OLD permissive resolution so it keeps
    // measuring the data rather than the fix.
    //
    // `outside` counts values that merely NAME another root (984 on this
    // account, all under a dead `QQ Files` install that no longer exists on
    // disk); those resolve to nothing either way, so containment cannot regress
    // them. `resolvable_outside` counts values that WOULD have resolved under
    // the old rule yet land outside the root — exactly the media containment
    // newly rejects. Zero means the rule is free here; a non-zero count on some
    // other account means containment costs real media on that machine and the
    // rule needs widening to an allowlist of QQ-owned roots instead.
    assert_eq!(
        grand_resolvable_outside, 0,
        "the pre-fix resolution reached files outside media_root, so containment costs real media on this account and needs a QQ-root allowlist"
    );
}

/// 真库表调研：列出 nt_db 下每个库的表名与行数，找出**群成员**的来源。
///
/// 用法与其它真库探针一致（`#[ignore]` ＋ 环境变量门控）：
///   QQFLOW_TEST_DB_ROOT  QQFLOW_TEST_DB_KEY
#[test]
#[ignore]
fn probe_real_db_tables() {
    let root = std::env::var("QQFLOW_TEST_DB_ROOT").expect("需要 QQFLOW_TEST_DB_ROOT");
    let key = std::env::var("QQFLOW_TEST_DB_KEY").expect("需要 QQFLOW_TEST_DB_KEY");
    // 三种传法都接受：直接给 `nt_msg.db`、给 `nt_db` 目录、或给 `<Tencent Files 根>`
    // （配置文件里的 `db_path` 是**根**，真实布局是 `<根>/<qq>/nt_qq/nt_db`）。
    // 猜错路径只会得到「目录不存在」，而那是这个探针最容易踩的坑。
    let given = Path::new(&root);
    let nt_db = if given.extension().is_some_and(|x| x == "db") {
        given.parent().expect("nt_msg.db 的父目录").to_path_buf()
    } else if given.join("nt_msg.db").exists() {
        given.to_path_buf()
    } else {
        // 根目录：往下找一层 <qq>/nt_qq/nt_db。
        std::fs::read_dir(given)
            .expect("根目录")
            .flatten()
            .map(|e| e.path().join("nt_qq").join("nt_db"))
            .find(|d| d.join("nt_msg.db").exists())
            .unwrap_or_else(|| panic!("在 {given:?} 下没找到 <qq>/nt_qq/nt_db/nt_msg.db"))
    };
    let mut names: Vec<std::path::PathBuf> = std::fs::read_dir(&nt_db)
        .expect("nt_db 目录")
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.extension().is_some_and(|x| x == "db"))
        .collect();
    names.sort();
    println!("[表调研] nt_db 下 {} 个库", names.len());
    for path in &names {
        let label = path.file_name().unwrap().to_string_lossy().to_string();
        // 与服务器同一条打开路径（偏移 VFS ＋ 只读活连接）。
        let mut reader = LiveReader::new(path.clone(), key.clone());
        let Ok(()) = reader.open() else {
            println!("  {label}: 打不开（可能不是 SQLCipher 或键不同）");
            continue;
        };
        let Ok(conn) = reader.acquire() else { continue };
        let mut stmt = match conn.prepare("SELECT name FROM sqlite_master WHERE type='table' ORDER BY name") {
            Ok(s) => s,
            Err(e) => { println!("  {label}: 列表失败 {e}"); continue }
        };
        let tables: Vec<String> = stmt
            .query_map([], |r| r.get::<_, String>(0))
            .map(|it| it.flatten().collect())
            .unwrap_or_default();
        let hits: Vec<&String> = tables
            .iter()
            .filter(|t| {
                let l = t.to_lowercase();
                l.contains("member") || l.contains("troop") || l.contains("group")
            })
            .collect();
        println!("  {label}: {} 张表；含 member/troop/group 的 {} 张", tables.len(), hits.len());
        for t in hits {
            let n: i64 = conn
                .query_row(&format!("SELECT COUNT(*) FROM \"{t}\""), [], |r| r.get(0))
                .unwrap_or(-1);
            println!("      {t}  ({n} 行)");
        }
    }
}

/// 追查 `group_member3` 的结构与样例行（表调研把它指出来了）。
#[test]
#[ignore]
fn probe_group_member3_shape() {
    let root = std::env::var("QQFLOW_TEST_DB_ROOT").expect("需要 QQFLOW_TEST_DB_ROOT");
    let key = std::env::var("QQFLOW_TEST_DB_KEY").expect("需要 QQFLOW_TEST_DB_KEY");
    let given = Path::new(&root);
    let nt_db = if given.extension().is_some_and(|x| x == "db") {
        given.parent().unwrap().to_path_buf()
    } else if given.join("nt_msg.db").exists() {
        given.to_path_buf()
    } else {
        std::fs::read_dir(given).unwrap().flatten()
            .map(|e| e.path().join("nt_qq").join("nt_db"))
            .find(|d| d.join("nt_msg.db").exists())
            .expect("找到 nt_db")
    };
    let path = nt_db.join("group_info.db");
    let mut reader = LiveReader::new(path, key.clone());
    reader.open().expect("打开 group_info.db");
    let conn = reader.acquire().unwrap();
    for table in ["group_member3", "group_member_new_ext_info_table", "group_list", "group_detail_info_ver1"] {
        let sql: String = conn
            .query_row("SELECT sql FROM sqlite_master WHERE name = ?1", [table], |r| r.get(0))
            .unwrap_or_else(|e| format!("（拿不到 {table}: {e}）"));
        println!("=== {table} ===");
        println!("{sql}");
        let n: i64 = conn.query_row(&format!("SELECT COUNT(*) FROM \"{table}\""), [], |r| r.get(0)).unwrap_or(-1);
        println!("-- {n} 行；前 3 行：");
        if let Ok(mut stmt) = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 3")) {
            let cols: Vec<String> = stmt.column_names().iter().map(|s| s.to_string()).collect();
            println!("-- 列: {}", cols.join(", "));
            let rows = stmt.query_map([], |r| {
                let mut v: Vec<String> = Vec::new();
                for i in 0..cols.len() {
                    v.push(match r.get_ref(i) {
                        Ok(rusqlite::types::ValueRef::Text(b)) => String::from_utf8_lossy(b).chars().take(30).collect(),
                        Ok(rusqlite::types::ValueRef::Integer(n)) => n.to_string(),
                        Ok(rusqlite::types::ValueRef::Real(f)) => f.to_string(),
                        Ok(rusqlite::types::ValueRef::Null) => "NULL".into(),
                        Ok(rusqlite::types::ValueRef::Blob(b)) => format!("<blob {}>", b.len()),
                        Err(_) => "?".into(),
                    });
                }
                Ok(v.join(" | "))
            });
            if let Ok(rows) = rows {
                for r in rows.flatten() {
                    println!("--   {r}");
                }
            }
        }
    }
}

/// 每群人数 ＋ 群号与会话 id 的对齐情况。
#[test]
#[ignore]
fn probe_group_roster_counts() {
    let root = std::env::var("QQFLOW_TEST_DB_ROOT").expect("root");
    let key = std::env::var("QQFLOW_TEST_DB_KEY").expect("key");
    let given = Path::new(&root);
    let nt_db = if given.extension().is_some_and(|x| x == "db") {
        given.parent().unwrap().to_path_buf()
    } else if given.join("nt_msg.db").exists() { given.to_path_buf() }
    else {
        std::fs::read_dir(given).unwrap().flatten()
            .map(|e| e.path().join("nt_qq").join("nt_db"))
            .find(|d| d.join("nt_msg.db").exists()).expect("nt_db")
    };
    let mut reader = LiveReader::new(nt_db.join("group_info.db"), key.clone());
    reader.open().expect("group_info.db");
    let conn = reader.acquire().unwrap();
    println!("=== group_list 的群号 ===");
    let mut stmt = conn.prepare("SELECT [60001], [60007] FROM group_list ORDER BY [60001]").unwrap();
    let groups: Vec<(i64, String)> = stmt
        .query_map([], |r| Ok((r.get(0)?, r.get::<_, Option<String>>(1)?.unwrap_or_default())))
        .unwrap().flatten().collect();
    for (id, name) in &groups { println!("  {id}  {name}"); }
    println!("=== group_member3 每群人数 ===");
    let mut stmt = conn
        .prepare("SELECT [60001], COUNT(*) FROM group_member3 GROUP BY [60001] ORDER BY 2 DESC")
        .unwrap();
    let counts: Vec<(i64, i64)> = stmt.query_map([], |r| Ok((r.get(0)?, r.get(1)?))).unwrap().flatten().collect();
    for (id, n) in &counts { println!("  {id}  {n} 人"); }
    let with_roster = counts.iter().filter(|(id, _)| groups.iter().any(|(g, _)| g == id)).count();
    println!("[小结] group_list {} 个群；group_member3 覆盖 {} 个", groups.len(), with_roster);
    // 会话 id 是否就是群号
    let mut reader2 = LiveReader::new(nt_db.join("nt_msg.db"), key.clone());
    reader2.open().expect("nt_msg.db");
    let conn2 = reader2.acquire().unwrap();
    let mut stmt = conn2
        .prepare("SELECT DISTINCT [40021] FROM group_msg_table")
        .unwrap();
    let talkers: Vec<String> = stmt.query_map([], |r| r.get::<_, String>(0)).unwrap().flatten().collect();
    let matched = talkers.iter().filter(|t| groups.iter().any(|(g, _)| g.to_string() == **t)).count();
    println!("[小结] 会话里的群 talker {} 个；其中 {} 个在 group_list 里", talkers.len(), matched);
}
