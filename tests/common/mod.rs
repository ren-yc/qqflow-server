//! Shared fake-DB fixtures for integration tests.
//!
//! A fake QQ-style source database: SQLCipher (QQ's exact PRAGMA suite) +
//! 1024-byte custom header + WAL journal mode, with a known dataset
//! (6 group rows incl. recall/system/media/miniapp + 2 c2c rows) plus an
//! optional number of extra plain group rows. Never touches real QQ data.

//! Not every test binary uses every helper below — that is expected for a
//! shared support module.
#![allow(dead_code)]

use std::path::Path;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use qqflow_server::db::scan::CUSTOM_HEADER_LEN;
use rusqlite::Connection;
use serde_json::Value;
use tower::ServiceExt;

/// Fabricated account number (random, not a real QQ).
pub const FAKE_QQ: &str = "335663881";
pub const FAKE_KEY: &str = "0123456789abcdef";

pub fn pragma_suite(key: &str) -> String {
    format!(
        "PRAGMA cipher_page_size = 4096;\n\
         PRAGMA key = '{key}';\n\
         PRAGMA kdf_iter = 4000;\n\
         PRAGMA cipher_hmac_algorithm = HMAC_SHA1;\n\
         PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;\n\
         PRAGMA cipher = 'aes-256-cbc';\n"
    )
}

pub fn make_schema(conn: &Connection) {
    // 40013 = message direction (0 other / 1,2 self / 3 system), 40050 =
    // unix-send-time, 40090 = group card. c2c deliberately lacks 40090 —
    // exercises the missing-column degrade path.
    conn.execute_batch(
        "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \"40093\" TEXT, \"40800\" BLOB, \"40013\" INTEGER, \"40050\" INTEGER, \"40090\" TEXT);\
         CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB, \"40013\" INTEGER, \"40050\" INTEGER);\
         CREATE TABLE nt_uid_mapping_table (nt_uid TEXT, remark TEXT, nickname TEXT, qq TEXT);\
         INSERT INTO nt_uid_mapping_table VALUES ('u_12345', '李四他哥', '王五', '12345');\
         INSERT INTO nt_uid_mapping_table VALUES ('u_a', '张三备注', '张三', '10001');\
         INSERT INTO nt_uid_mapping_table VALUES ('u_b', '', '李四', '10002');",
    )
    .unwrap();
}

/// Open a persistent fake-QQ writer on `raw.db` (headerless SQLCipher,
/// WAL mode, schema seeded). Keep the connection alive — rows then land in
/// `raw.db-wal` only, exactly like a running QQ client writing live.
pub fn open_fake_writer(nt_db_dir: &Path) -> (Connection, std::path::PathBuf) {
    std::fs::create_dir_all(nt_db_dir).unwrap();
    let raw = nt_db_dir.join("raw.db");
    // Clean slate: also drop any nt_msg.* leftovers (stale hard links to
    // dead WAL/shm inodes from a previous test run would break the salt
    // check). No reader is alive when a fixture is (re)built.
    for name in [
        "raw.db",
        "raw.db-wal",
        "raw.db-shm",
        "nt_msg.db",
        "nt_msg.db-wal",
        "nt_msg.db-shm",
    ] {
        let _ = std::fs::remove_file(nt_db_dir.join(name));
    }
    let conn = Connection::open(&raw).unwrap();
    conn.execute_batch(&pragma_suite(FAKE_KEY)).unwrap();
    conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    make_schema(&conn);
    ensure_fake_media_file(nt_db_dir);
    (conn, raw)
}

/// Fake media file path under the fake account's `nt_data` (mirrors the
/// real QQ layout `<root>/<qq>/nt_qq/nt_data/Pic/...`).
pub fn fake_media_path(nt_db_dir: &Path) -> std::path::PathBuf {
    nt_db_dir
        .parent()
        .unwrap()
        .join("nt_data")
        .join("Pic")
        .join("2026-08")
        .join("fake_image_01.jpg")
}

/// Ensure the fake media file exists on disk (small JPEG placeholder).
pub fn ensure_fake_media_file(nt_db_dir: &Path) -> std::path::PathBuf {
    let path = fake_media_path(nt_db_dir);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    // A minimal valid JPEG SOI..EOI placeholder (never a real photo).
    let jpeg = [
        0xFF, 0xD8, 0xFF, 0xE0, 0x00, 0x10, b'J', b'F', b'I', b'F', 0x00, 0x01, // JFIF\0
        0x01, 0x01, 0x00, 0x00, 0x01, 0x00, 0x01, 0x00, 0x00, // header
        0xFF, 0xDB, 0x00, 0x43, // DQT
        0x00, 0x10, 0x0B, 0x0C, 0x0E, 0x0C, 0x0A, 0x10, 0x0E, 0x0D, 0x0E, 0x12,
        0x11, 0x10, 0x13, 0x18, 0x28, 0x1A, 0x18, 0x16, 0x16, 0x18, 0x31, 0x23,
        0x25, 0x1D, 0x28, 0x3A, 0x33, 0x3D, 0x3C, 0x39, 0x33, 0x38, 0x37, 0x40,
        0x48, 0x5C, 0x4E, 0x40, 0x44, 0x57, 0x45, 0x37, 0x38, 0x50, 0x6D, 0x51,
        0x57, 0x56, 0x5D, 0x63, 0x67, 0x6E, 0x67, 0x66, 0x6E, 0x72, 0x79, 0x7E,
        0x7C, 0x8A, 0x8F, 0x93, 0x9A, 0xA6, 0xA9, 0x9E, 0x93, 0x92, 0xA0, 0xB0,
        0xB5, 0x9F, 0x96, 0xA8, 0xAD, 0xBD, 0xC2, 0xD4, 0xC4, 0xCE, 0xE4, 0xEA,
        0xDB, 0xE6, 0xF0, 0xF8, 0xFD, 0xF2, 0xF3, 0xF0, 0xFC, 0xFE, 0xFA, 0xFF,
        0xFF, 0xD9, // EOI
    ];
    std::fs::write(&path, jpeg).unwrap();
    path
}

/// Seed the standard dataset: 6 group rows (normal text, recall, system
/// 群名修改, large media blob, miniapp JSON) + 2 c2c rows, plus `extra`
/// plain group rows.
pub fn seed_dataset(conn: &Connection, extra: u32) {
    // Groups: normal text, recall, system (群名修改), large media blob, miniapp JSON.
    // seq = (ts << 32) | seqno (real QQ layout), ts ≈ 2026-07-01 (unix 1782864000).
    // dir: u_a rows are self-sent (1), u_b/u_c other (0), rename row system (3).
    let ts: i64 = 1782864000;
    let g = |rowid: i64, group: &str, seq: i64, uid: &str, nick: &str, dir: i64, card: &str, blob: &[u8]| {
        conn.execute(
            "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params![group, seq, uid, nick, blob, dir, ts, card],
        )
        .unwrap();
        assert_eq!(rowid, conn.last_insert_rowid());
    };
    g(1, "10001", (ts << 32) | 1, "u_a", "张三", 1, "张三群名片", "你好，欢迎加入".as_bytes());
    g(2, "10001", (ts << 32) | 2, "u_b", "李四", 0, "", "收到".as_bytes());
    g(3, "10001", (ts << 32) | 3, "u_a", "张三", 1, "张三群名片", "李四撤回了一条消息\n你猜猜撤回了什么".as_bytes());
    g(4, "10001", (ts << 32) | 4, "u_b", "李四", 3, "", "群主已将群名修改为「测试群」".as_bytes());
    let mut media = vec![0u8; 70_000];
    media[5000..5008].copy_from_slice(b".jpg.exe");
    g(5, "10001", (ts << 32) | 5, "u_c", "王五", 0, "", &media);
    g(6, "20002", (ts << 32) | 1, "u_a", "张三", 1, "张三群名片",
        "{\"appID\":\"x\",\"prompt\":\"分享一个链接\",\"desc\":\"有趣内容\",\"title\":\"标题\"}".as_bytes());
    conn.execute(
        "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params!["u_12345", (ts << 32) | 1, "王五", "在吗？".as_bytes(), 0, ts],
    )
    .unwrap();
    conn.execute(
        "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6)",
        rusqlite::params!["u_12345", (ts << 32) | 2, "王五", "明天见".as_bytes(), 0, ts],
    )
    .unwrap();

    for i in 0..extra {
        append_group_row(conn, 7 + i as i64, &format!("事件驱动新增-{}", 7 + i));
    }
}

/// Append one group row into group 10001 (simulating a new QQ message
/// written by the live client).
pub fn append_group_row(conn: &Connection, n: i64, text: &str) {
    let ts: i64 = 1782864000;
    conn.execute(
        "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params!["10001", (ts << 32) | n, "u_a", "张三", text.as_bytes(), 1, ts, "张三群名片"],
    )
    .unwrap();
}

// ---- structured 40800 blob builder (test-side protobuf encoder) ----------
// Encodes the spec-confirmed MsgBody layout (see db_docs 40800.md):
//   MsgBody { repeated MsgContent content = 40800; }
//   MsgContent: 45002 content type, 45003 media subtype, 45101 text,
//   image fields 45503 uuid / 45402 name / 45424 md5hex / 45405 size /
//   45411,45412 dims / 45812 local cache path.

fn enc_varint(mut v: u64, out: &mut Vec<u8>) {
    loop {
        let b = (v & 0x7f) as u8;
        v >>= 7;
        if v == 0 {
            out.push(b);
            break;
        }
        out.push(b | 0x80);
    }
}

fn enc_field(field: u64, wire: u64, payload: &[u8], out: &mut Vec<u8>) {
    enc_varint((field << 3) | wire, out);
    out.extend_from_slice(payload);
}

fn enc_varint_field(field: u64, v: u64, out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    enc_varint(v, &mut payload);
    enc_field(field, 0, &payload, out);
}

fn enc_bytes_field(field: u64, bytes: &[u8], out: &mut Vec<u8>) {
    let mut payload = Vec::new();
    enc_varint(bytes.len() as u64, &mut payload);
    payload.extend_from_slice(bytes);
    enc_field(field, 2, &payload, out);
}

/// Build a spec-shaped structured image message blob (45002=2): one
/// MsgContent segment with uuid/name/md5/size/dims/localPath.
pub fn image_blob(md5_hex: &str, local_path: &Path) -> Vec<u8> {
    let mut seg = Vec::new();
    enc_varint_field(45001, 1, &mut seg);
    enc_varint_field(45002, 2, &mut seg); // T_Image
    enc_varint_field(45003, 1, &mut seg); // media subtype image
    enc_bytes_field(45503, b"fake-uuid-0001", &mut seg);
    enc_bytes_field(45402, b"fake_image_01.jpg", &mut seg);
    enc_bytes_field(45424, md5_hex.as_bytes(), &mut seg);
    enc_varint_field(45405, 12345, &mut seg);
    enc_varint_field(45411, 640, &mut seg);
    enc_varint_field(45412, 480, &mut seg);
    enc_bytes_field(45812, local_path.to_string_lossy().as_bytes(), &mut seg);
    let mut body = Vec::new();
    enc_bytes_field(40800, &seg, &mut body);
    body
}

/// Append one structured image row into group 10001 (dir=0, others' image),
/// its 45812 local path pointing at the fake media file. Returns the row's
/// media key (md5 hex) for lookup assertions.
pub fn append_image_row(conn: &Connection, n: i64, nt_db_dir: &Path) -> String {
    let md5 = "aabbccddeeff00112233445566778899";
    let blob = image_blob(md5, &fake_media_path(nt_db_dir));
    let ts: i64 = 1782864000;
    conn.execute(
        "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params!["10001", (ts << 32) | n, "u_c", "王五", blob, 0, ts, ""],
    )
    .unwrap();
    md5.to_string()
}

/// Spec-shaped structured image blob WITHOUT the "45812" local path — the
/// exact case the cache-index fallback must rescue. `file_name` is the 45402
/// value.
pub fn image_blob_no_local(md5_hex: &str, file_name: &str) -> Vec<u8> {
    let mut seg = Vec::new();
    enc_varint_field(45001, 1, &mut seg);
    enc_varint_field(45002, 2, &mut seg); // T_Image
    enc_varint_field(45003, 1, &mut seg); // media subtype image
    enc_bytes_field(45503, b"fake-uuid-0001", &mut seg);
    enc_bytes_field(45402, file_name.as_bytes(), &mut seg);
    enc_bytes_field(45424, md5_hex.as_bytes(), &mut seg);
    enc_varint_field(45405, 12345, &mut seg);
    enc_varint_field(45411, 640, &mut seg);
    enc_varint_field(45412, 480, &mut seg);
    let mut body = Vec::new();
    enc_bytes_field(40800, &seg, &mut body);
    body
}

/// Append one structured image row WITHOUT a "45812" path (the
/// cache-fallback case). Returns the row's media key (md5 hex).
pub fn append_image_row_no_local(conn: &Connection, n: i64, md5: &str, file_name: &str) -> String {
    let blob = image_blob_no_local(md5, file_name);
    let ts: i64 = 1782864000;
    conn.execute(
        "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
        rusqlite::params!["10001", (ts << 32) | n, "u_c", "王五", blob, 0, ts, ""],
    )
    .unwrap();
    md5.to_string()
}

/// Write a fake cache file with an arbitrary relative path under the fake
/// account's `nt_data` (mirrors QQ's layout
/// `<root>/<qq>/nt_qq/nt_data/<rel>`). Returns its path.
pub fn write_fake_cache_file(nt_db_dir: &Path, rel: &str) -> std::path::PathBuf {
    let path = nt_db_dir.parent().unwrap().join("nt_data").join(rel);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();
    std::fs::write(&path, b"fallback cache bytes").unwrap();
    path
}

/// Materialize the writer's current state as a real QQ-style file pair:
/// `nt_msg.db` = raw.db + 1024-byte fake header (snapshot, rewritten in
/// place); `nt_msg.db-wal` and `nt_msg.db-shm` = HARD LINKS to the writer's
/// LIVE files. Sharing the live WAL + wal-index is exactly what production
/// does (our reader shares QQ's files), so a still-open reader sees appended
/// frames and checkpoint resets without reopening.
///
/// A copied -shm would not work: it would be a stale index (the reader
/// caches mxFrame) and, on Windows, a mapped section (the writer's -shm)
/// cannot be re-opened by a second handle anyway (ERROR_USER_MAPPED_FILE).
/// Returns the `nt_msg.db` path.
pub fn materialize_source(nt_db_dir: &Path) -> std::path::PathBuf {
    let raw = nt_db_dir.join("raw.db");
    let main = nt_db_dir.join("nt_msg.db");
    let mut bytes = std::fs::read(&raw).unwrap();
    let mut all = vec![0u8; CUSTOM_HEADER_LEN as usize];
    all[0..8].copy_from_slice(b"QQNTDB!1");
    all.append(&mut bytes);
    std::fs::write(&main, all).unwrap();

    for ext in ["db-wal", "db-shm"] {
        let raw_side = raw.with_extension(ext);
        let side = nt_db_dir.join(format!("nt_msg.{ext}"));
        if side.exists() {
            // Already hard-linked: while the writer stays alive its WAL and
            // wal-index keep the same inode (appends/checkpoints are in
            // place), so the existing link stays valid — and a reader may
            // have it open/mapped, so removing it would fail on Windows
            // (ERROR_USER_MAPPED_FILE).
            continue;
        }
        if raw_side.exists() {
            std::fs::hard_link(&raw_side, &side).unwrap();
        }
    }
    main
}

/// Write a fake sibling `group_info.db` into `nt_db_dir` (headerless
/// SQLCipher, same PRAGMA suite + key as the message DB — how QQ's sibling
/// databases are expected to open). Group ids match the seeded dataset
/// (10001 / 20002). Returns its path.
pub fn write_fake_group_info(nt_db_dir: &Path) -> std::path::PathBuf {
    std::fs::create_dir_all(nt_db_dir).unwrap();
    let path = nt_db_dir.join("group_info.db");
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(&pragma_suite(FAKE_KEY)).unwrap();
    conn.execute_batch(
        "CREATE TABLE group_list (id TEXT PRIMARY KEY, name TEXT, remark TEXT);\
         INSERT INTO group_list VALUES ('10001', '测试群', '');\
         INSERT INTO group_list VALUES ('20002', '第二群', '');",
    )
    .unwrap();
    drop(conn);
    path
}

/// Header-prefixed variant of `write_fake_group_info`: the same DB with the
/// fake QQ 1024-byte header prepended, so the offset-VFS open path is
/// exercised on a non-`nt_msg.db` file (siblings may carry the header).
pub fn write_fake_group_info_headed(nt_db_dir: &Path) -> std::path::PathBuf {
    let path = write_fake_group_info(nt_db_dir);
    let mut raw = std::fs::read(&path).unwrap();
    let mut all = vec![0u8; CUSTOM_HEADER_LEN as usize];
    all[0..8].copy_from_slice(b"QQNTDB!1");
    all.append(&mut raw);
    std::fs::write(&path, all).unwrap();
    path
}

/// Write a fake sibling `profile_info.db` — HEADERLESS SQLCipher, so the
/// loader's offset→plain open retry is exercised (real QQ siblings carry
/// the header; headerless ones must still open). Real layout shape:
/// `"1000"` uid / `"20002"` nick / `"20009"` remark / `"1002"` QQ number.
pub fn write_fake_profile_info(nt_db_dir: &Path) -> std::path::PathBuf {
    std::fs::create_dir_all(nt_db_dir).unwrap();
    let path = nt_db_dir.join("profile_info.db");
    let _ = std::fs::remove_file(&path);
    let conn = Connection::open(&path).unwrap();
    conn.execute_batch(&pragma_suite(FAKE_KEY)).unwrap();
    conn.execute_batch(
        "CREATE TABLE profile_info_v2 (\"1000\" TEXT, \"20002\" TEXT, \"20009\" TEXT, \"1002\" TEXT);\
         INSERT INTO profile_info_v2 VALUES ('u_12345', '档案昵称', '', '12345');\
         INSERT INTO profile_info_v2 VALUES ('u_c', '王五档案', '王五备注', '10003');",
    )
    .unwrap();
    drop(conn);
    path
}

/// Write a fresh fake source DB into `nt_db_dir` (created if needed) and
/// return the path to `nt_msg.db`. The DB is regenerated from scratch on
/// every call (writer dropped -> WAL auto-checkpointed into the main file),
/// so re-running with a different `extra` simulates QQ writing new
/// messages (file size changes -> watcher/checkpoint detection).
pub fn write_fake_source(nt_db_dir: &Path, extra: u32) -> std::path::PathBuf {
    let (conn, _raw) = open_fake_writer(nt_db_dir);
    seed_dataset(&conn, extra);
    drop(conn); // writer dropped -> WAL auto-checkpointed
    materialize_source(nt_db_dir)
}

/// Persistent-writer variant: the writer stays alive (rows land in its WAL)
/// and the source pair is materialized. Call `materialize_source` after
/// each writer op so a live reader sees the change.
pub fn open_fake_source(nt_db_dir: &Path, extra: u32) -> (Connection, std::path::PathBuf) {
    let (conn, _raw) = open_fake_writer(nt_db_dir);
    seed_dataset(&conn, extra);
    let main = materialize_source(nt_db_dir);
    (conn, main)
}

// ---- HTTP layer helpers (axum oneshot) ---------------------------------

/// GET through `app` with optional extra headers (e.g. Bearer auth);
/// returns (status, json).
pub async fn get_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("GET");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// POST a JSON body through `app` with optional extra headers; returns
/// (status, json).
pub async fn post_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("POST");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app
        .oneshot(
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// DELETE through `app` with optional extra headers; returns (status, json).
/// No body — the deregistration route takes its parameters from the path and
/// the query string (its POST alias is what carries a JSON body).
pub async fn delete_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("DELETE");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// Poll `GET /api/v1/accounts` until account `qq` reports state `want`;
/// returns the whole detail JSON. Panics when the account hits `error`
/// (with its reason) or the deadline passes.
///
/// Per-account state lives behind the token: `/health` reports only a
/// coarse `account` phase and never names an account.
pub async fn wait_account_state(
    app: &axum::Router,
    token: &str,
    qq: &str,
    want: &str,
    timeout: Duration,
) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let (status, v) =
            get_json(app.clone(), "/api/v1/accounts", &[("authorization", &format!("Bearer {token}"))])
                .await;
        assert_eq!(status, StatusCode::OK, "account detail: {v}");
        for a in v["accounts"].as_array().expect("accounts array") {
            if a["qq"] != qq {
                continue;
            }
            if a["state"] == want {
                return v;
            }
            if a["state"] == "error" {
                panic!("account {qq} failed: {:?}", a["error"]);
            }
        }
        assert!(std::time::Instant::now() < deadline, "account {qq} did not reach {want}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The single account entry for `qq` from a `wait_account_state` result.
pub fn account_entry<'a>(detail: &'a Value, qq: &str) -> &'a Value {
    detail["accounts"]
        .as_array()
        .expect("accounts array")
        .iter()
        .find(|a| a["qq"] == qq)
        .unwrap_or_else(|| panic!("account {qq} missing from {detail}"))
}
