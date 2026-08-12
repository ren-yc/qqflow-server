//! SQLCipher round-trip integration test (no real QQ data involved).
//!
//! Scenario (models the real world):
//!   * a "writer" connection simulates the QQ client on `raw.db`
//!     (SQLCipher with QQ's PRAGMA parameters, WAL journal mode)
//!   * `real.db` simulates QQ's file on disk: raw.db + a 1024-byte
//!     custom header, plus a verbatim `real.db-wal`
//!   * the mirror copies real.db (header stripped) + WAL, and the reader
//!     opens it via `decrypt::open_decrypted`
//!
//! Phases: (A) new row written only to the WAL must be visible through the
//! mirror (proves the real-time polling path); (B) after a checkpoint the
//! source main file changes and the mirror must rebuild and still see all
//! rows (proves checkpoint detection).

use qqflow_server::db::decrypt;
use qqflow_server::db::mirror::Mirror;
use qqflow_server::db::scan::{DbInfo, CUSTOM_HEADER_LEN};
use rusqlite::{params, Connection};

const KEY: &str = "0123456789abcdef";

fn pragma_suite() -> &'static str {
    "PRAGMA cipher_page_size = 4096;\n\
     PRAGMA key = '0123456789abcdef';\n\
     PRAGMA kdf_iter = 4000;\n\
     PRAGMA cipher_hmac_algorithm = HMAC_SHA1;\n\
     PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;\n\
     PRAGMA cipher = 'aes-256-cbc';\n"
}

fn make_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \"40093\" TEXT, \"40800\" BLOB);\
         CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);",
    )
    .unwrap();
}

fn insert_group(conn: &Connection, rowid: i64, group: &str, seq: i64, uid: &str, nick: &str, text: &str) {
    conn.execute(
        "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5)",
        params![group, seq, uid, nick, text.as_bytes()],
    )
    .unwrap();
    assert_eq!(rowid, conn.last_insert_rowid());
}

/// Rebuild `real.db` from the writer's raw.db, prepending the fake header,
/// and refresh the WAL copy. Returns whether the main file changed.
/// Refresh the simulated on-disk WAL copy (QQ's WAL file is live; in this
/// test "real.db-wal" is a snapshot that must be re-copied after writer ops).
fn refresh_wal(raw: &std::path::Path, wal: &std::path::Path) {
    let _ = std::fs::copy(raw.with_extension("db-wal"), wal);
}

fn refresh_real(raw: &std::path::Path, real: &std::path::Path, wal: &std::path::Path, always: bool) -> bool {
    let meta = std::fs::metadata(real).ok();
    let changed = always
        || meta.map(|m| m.len()).unwrap_or(0)
            != std::fs::metadata(raw).unwrap().len() + CUSTOM_HEADER_LEN;
    if changed {
        let mut bytes = std::fs::read(raw).unwrap();
        let mut header = vec![0u8; CUSTOM_HEADER_LEN as usize];
        header[0..8].copy_from_slice(b"QQNTDB!1"); // recognizable fake header
        let mut all = header;
        all.append(&mut bytes);
        std::fs::write(real, all).unwrap();
    }
    refresh_wal(raw, wal);
    changed
}

fn count_rows(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM group_msg_table", [], |r| r.get(0)).unwrap()
}

#[test]
fn roundtrip_wal_polling_and_checkpoint() {
    let dir = std::env::temp_dir().join(format!("qqflow_rt_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // ---- writer (simulated QQ client) -------------------------------------
    let raw = dir.join("raw.db");
    let writer = Connection::open(&raw).unwrap();
    writer.execute_batch(pragma_suite()).unwrap();
    writer.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
    make_schema(&writer);
    insert_group(&writer, 1, "10001", 0x6771A6B50001, "u_a", "张三", "你好");

    // ---- real files on disk -----------------------------------------------
    let real = dir.join("real.db");
    let real_wal = dir.join("real.db-wal");
    refresh_real(&raw, &real, &real_wal, true); // initial snapshot

    // Phase A: writer adds a row that lands in the WAL only.
    insert_group(&writer, 2, "10001", 0x6771A6B60002, "u_b", "李四", "收到");
    let wal_meta_before = std::fs::metadata(raw.with_extension("db-wal")).unwrap().len();
    assert!(wal_meta_before > 32, "WAL should contain frames");
    refresh_wal(&raw, &real_wal); // QQ's on-disk WAL now carries row 2

    // Mirror sync WITHOUT touching the main file (poll behavior).
    let info = DbInfo { qq: "test".into(), path: real.clone() };
    let mut mirror = Mirror::new(&info, &dir.join("mirror")).unwrap();
    // (initial mirror was built when real.db had only row 1)
    let rebuilt = mirror.sync().unwrap();
    assert!(!rebuilt, "WAL copy must not trigger a rebuild");

    let reader = decrypt::open_decrypted(&mirror.main_path, KEY).unwrap();
    assert_eq!(count_rows(&reader), 2, "row 2 lives in the WAL and must be visible");
    drop(reader); // close before the mirror is rebuilt underneath

    // Phase B: checkpoint merges rows into the writer's main file.
    writer.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let wal_len_after = std::fs::metadata(raw.with_extension("db-wal")).unwrap().len();
    assert!(wal_len_after <= 32, "WAL should be reset after checkpoint");
    // QQ's on-disk main file now contains the merged pages (len changed).
    let main_changed = refresh_real(&raw, &real, &real_wal, false);
    assert!(main_changed, "simulated QQ main file must grow after checkpoint");

    let rebuilt = mirror.sync().unwrap();
    assert!(rebuilt, "source main change must trigger mirror rebuild");
    let reader2 = decrypt::open_decrypted(&mirror.main_path, KEY).unwrap();
    assert_eq!(count_rows(&reader2), 2, "rows survive the checkpoint");
    drop(reader2); // close before any further mirror rewrite

    // Phase C: poller-style incremental append through the store index.
    insert_group(&writer, 3, "20002", 0x6771A6B70003, "u_c", "王五", "新消息");
    refresh_wal(&raw, &real_wal); // live WAL carries row 3
    let main_changed = refresh_real(&raw, &real, &real_wal, false);
    assert!(!main_changed, "WAL-only write must not change the main file");
    let rebuilt = mirror.sync().unwrap();
    assert!(!rebuilt, "WAL-only write must not rebuild");
    let reader3 = decrypt::open_decrypted(&mirror.main_path, KEY).unwrap();
    assert_eq!(count_rows(&reader3), 3, "WAL-only row visible through poll");

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn wrong_key_fails() {
    let dir = std::env::temp_dir().join(format!("qqflow_rt_key_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let raw = dir.join("raw.db");
    let writer = Connection::open(&raw).unwrap();
    writer.execute_batch(pragma_suite()).unwrap();
    make_schema(&writer);

    let mut bytes = std::fs::read(&raw).unwrap();
    let header = vec![0u8; 1024];
    let mut all = header.clone();
    all.append(&mut bytes);
    std::fs::write(dir.join("real.db"), all).unwrap();

    let info = DbInfo { qq: "test".into(), path: dir.join("real.db") };
    let mirror = Mirror::new(&info, &dir.join("mirror")).unwrap();
    let bad = decrypt::open_decrypted(&mirror.main_path, "wrongkey16bytes!");
    assert!(bad.is_err(), "wrong key must fail verification");
    let _ = std::fs::remove_dir_all(&dir);
}
