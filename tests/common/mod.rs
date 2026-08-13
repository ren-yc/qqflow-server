//! Shared fake-DB fixtures for integration tests.
//!
//! A fake QQ-style source database: SQLCipher (QQ's exact PRAGMA suite) +
//! 1024-byte custom header + WAL journal mode, with a known dataset
//! (6 group rows incl. recall/system/media/miniapp + 2 c2c rows) plus an
//! optional number of extra plain group rows. Never touches real QQ data.

use std::path::Path;

use qqflow_server::db::scan::CUSTOM_HEADER_LEN;
use rusqlite::Connection;

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
    conn.execute_batch(
        "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \"40093\" TEXT, \"40800\" BLOB);\
         CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);",
    )
    .unwrap();
}

/// Write a fresh fake source DB into `nt_db_dir` (created if needed) and
/// return the path to `nt_msg.db`. The DB is regenerated from scratch on
/// every call, so re-running with a different `extra` simulates QQ writing
/// new messages (file size changes -> watcher/checkpoint detection).
pub fn write_fake_source(nt_db_dir: &Path, extra: u32) -> std::path::PathBuf {
    std::fs::create_dir_all(nt_db_dir).unwrap();
    let raw = nt_db_dir.join("raw.db");
    let _ = std::fs::remove_file(&raw);
    {
        let conn = Connection::open(&raw).unwrap();
        conn.execute_batch(&pragma_suite(FAKE_KEY)).unwrap();
        conn.execute_batch("PRAGMA journal_mode = WAL;").unwrap();
        make_schema(&conn);

        // Groups: normal text, recall, system (群名修改), large media blob, miniapp JSON.
        let g = |rowid: i64, group: &str, seq: i64, uid: &str, nick: &str, blob: &[u8]| {
            conn.execute(
                "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params![group, seq, uid, nick, blob],
            )
            .unwrap();
            assert_eq!(rowid, conn.last_insert_rowid());
        };
        // seq = (ts << 32) | seqno (real QQ layout), ts ≈ 2026-07-01 (unix 1782864000)
        let ts: i64 = 1782864000;
        g(1, "10001", (ts << 32) | 1, "u_a", "张三", "你好，欢迎加入".as_bytes());
        g(2, "10001", (ts << 32) | 2, "u_b", "李四", "收到".as_bytes());
        g(3, "10001", (ts << 32) | 3, "u_a", "张三", "李四撤回了一条消息\n你猜猜撤回了什么".as_bytes());
        g(4, "10001", (ts << 32) | 4, "u_b", "李四", "群主已将群名修改为「测试群」".as_bytes());
        let mut media = vec![0u8; 70_000];
        media[5000..5008].copy_from_slice(b".jpg.exe");
        g(5, "10001", (ts << 32) | 5, "u_c", "王五", &media);
        g(6, "20002", (ts << 32) | 1, "u_a", "张三",
            "{\"appID\":\"x\",\"prompt\":\"分享一个链接\",\"desc\":\"有趣内容\",\"title\":\"标题\"}".as_bytes());
        conn.execute(
            "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["u_12345", (ts << 32) | 1, "王五", "在吗？".as_bytes()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["u_12345", (ts << 32) | 2, "王五", "明天见".as_bytes()],
        )
        .unwrap();

        for i in 0..extra {
            let n = 7 + i as i64;
            conn.execute(
                "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5)",
                rusqlite::params!["10001", (ts << 32) | n, "u_a", "张三", format!("事件驱动新增-{n}").as_bytes()],
            )
            .unwrap();
        }
    } // writer dropped -> WAL auto-checkpointed

    let main = nt_db_dir.join("nt_msg.db");
    let mut bytes = std::fs::read(&raw).unwrap();
    let mut all = vec![0u8; CUSTOM_HEADER_LEN as usize];
    all[0..8].copy_from_slice(b"QQNTDB!1");
    all.append(&mut bytes);
    std::fs::write(&main, all).unwrap();
    let _ = std::fs::remove_file(&raw);
    // Copy the WAL if the writer left one behind.
    let raw_wal = nt_db_dir.join("raw.db-wal");
    if raw_wal.exists() {
        std::fs::copy(&raw_wal, nt_db_dir.join("nt_msg.db-wal")).unwrap();
        let _ = std::fs::remove_file(&raw_wal);
    }
    main
}
