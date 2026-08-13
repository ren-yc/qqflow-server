//! SQLCipher round-trip integration test (no real QQ data involved).
//!
//! Scenario (models the real world):
//!   * a "writer" connection simulates the QQ client on `raw.db`
//!     (SQLCipher with QQ's PRAGMA parameters, WAL journal mode)
//!   * the "QQ files" on disk are `nt_msg.db` (raw.db + a 1024-byte custom
//!     header, rewritten IN PLACE) plus a verbatim `nt_msg.db-wal` snapshot
//!   * the reader opens `nt_msg.db` LIVE and read-only through the offset
//!     VFS (`db::live::LiveReader` / `decrypt::open_live`) — the production
//!     no-copy path — and must see rows that only live in the WAL and
//!     survive a checkpoint WITHOUT reopening.
//!
//! Phases:
//!   (a) open the PREFIXED file directly -> rows visible (VFS arbitration);
//!   (b) a row appended to the WAL only is visible to the STILL-OPEN reader
//!       (live WAL tail reads);
//!   (c) after a checkpoint the source main file grows and the WAL resets —
//!       the same reader still sees all rows (transparent checkpoint);
//!   (e) a cold reopen sees the final state;
//!   (d) a wrong key fails with the 密钥 error.

use qqflow_server::db::live::LiveReader;
use rusqlite::{params, Connection};

mod common;

const KEY: &str = "0123456789abcdef";

fn count_rows(conn: &Connection) -> i64 {
    conn.query_row("SELECT count(*) FROM group_msg_table", [], |r| r.get(0)).unwrap()
}

#[test]
fn live_read_wal_and_checkpoint() {
    let dir = std::env::temp_dir().join(format!("qqflow_live_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();

    // ---- writer (simulated QQ client), kept alive -------------------------
    let (writer, raw) = common::open_fake_writer(&dir);
    let ts: i64 = 1782864000;
    let insert = |conn: &Connection, rowid: i64, seq: i64, nick: &str, text: &str| {
        conn.execute(
            "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5)",
            params!["10001", seq, "u_a", nick, text.as_bytes()],
        )
        .unwrap();
        assert_eq!(rowid, conn.last_insert_rowid());
    };
    insert(&writer, 1, (ts << 32) | 1, "张三", "你好");

    // ---- the "QQ files" on disk: prefixed main + WAL snapshot ------------
    let real = common::materialize_source(&dir);

    // Phase (a): open the PREFIXED file directly, read-only, via the VFS.
    let mut reader = LiveReader::new(real.clone(), KEY.into());
    reader.open().unwrap();
    assert_eq!(
        count_rows(reader.acquire().unwrap()),
        1,
        "prefixed source readable through the offset VFS"
    );

    // Phase (b): writer appends a row that lands in the WAL only.
    insert(&writer, 2, (ts << 32) | 2, "李四", "收到");
    let wal_len = std::fs::metadata(raw.with_extension("db-wal")).unwrap().len();
    assert!(wal_len > 32, "WAL should contain frames");
    common::materialize_source(&dir); // QQ's on-disk WAL now carries row 2

    assert_eq!(
        count_rows(reader.acquire().unwrap()),
        2,
        "still-open reader must see WAL-only rows"
    );

    // Phase (c): checkpoint merges rows into the writer's main file and
    // resets the WAL; the source files are rewritten in place (same inode).
    writer.execute_batch("PRAGMA wal_checkpoint(TRUNCATE);").unwrap();
    let wal_len_after = std::fs::metadata(raw.with_extension("db-wal")).unwrap().len();
    assert!(wal_len_after <= 32, "WAL should be reset after checkpoint");
    let main_len_before = std::fs::metadata(&real).unwrap().len();
    common::materialize_source(&dir); // main grew + WAL reset
    assert!(
        std::fs::metadata(&real).unwrap().len() > main_len_before,
        "source main must grow after checkpoint"
    );

    assert_eq!(
        count_rows(reader.acquire().unwrap()),
        2,
        "same reader survives the checkpoint"
    );

    // Phase (e): cold reopen sees the final state.
    reader.force_reopen();
    assert_eq!(count_rows(reader.acquire().unwrap()), 2, "cold reopen works");
    drop(reader);
    drop(writer);

    let _ = std::fs::remove_dir_all(&dir);
}

#[test]
fn live_read_wrong_key_fails() {
    let dir = std::env::temp_dir().join(format!("qqflow_live_key_{}", std::process::id()));
    std::fs::create_dir_all(&dir).unwrap();
    let (writer, _raw) = common::open_fake_writer(&dir);
    common::seed_dataset(&writer, 0);
    let real = common::materialize_source(&dir);

    let mut reader = LiveReader::new(real, "wrongkey16bytes!".into());
    let err = reader.open().unwrap_err();
    assert!(format!("{err:#}").contains("密钥"), "got: {err:#}");
    drop(writer);

    let _ = std::fs::remove_dir_all(&dir);
}
