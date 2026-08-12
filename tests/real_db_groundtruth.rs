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
//! Everything goes through the SAME pipeline the server uses (Mirror +
//! decrypt::open_decrypted), so "opens at all" doubles as the arbitration
//! experiment for the PRAGMA-order question vs the reference implementation.

use std::collections::BTreeSet;
use std::path::Path;

use qqflow_server::db::decrypt;
use qqflow_server::db::mirror::Mirror;
use qqflow_server::db::scan;
use rusqlite::Connection;

/// Fabricated account number (random, not a real QQ).
const FAKE_QQ: &str = "335663881";
const FAKE_KEY: &str = "0123456789abcdef";

/// Fake DB location (also used by the behavioral-repro runbook).
pub fn fake_db_path() -> std::path::PathBuf {
    std::env::temp_dir()
        .join("qqflow_fake")
        .join(FAKE_QQ)
        .join("nt_qq")
        .join("nt_db")
        .join("nt_msg.db")
}

/// Both fake-db tests rebuild the same fixture directory — serialize them
/// (integration tests run in parallel threads of one process).
static FAKE_DB_LOCK: std::sync::Mutex<()> = std::sync::Mutex::new(());

fn pragma_suite(key: &str) -> String {
    format!(
        "PRAGMA cipher_page_size = 4096;\n\
         PRAGMA key = '{key}';\n\
         PRAGMA kdf_iter = 4000;\n\
         PRAGMA cipher_hmac_algorithm = HMAC_SHA1;\n\
         PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;\n\
         PRAGMA cipher = 'aes-256-cbc';\n"
    )
}

fn make_schema(conn: &Connection) {
    conn.execute_batch(
        "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \"40093\" TEXT, \"40800\" BLOB);\
         CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);",
    )
    .unwrap();
}

/// Build a fake QQ-style database (SQLCipher + 1024B header) at
/// `fake_db_path()`. Regenerates from scratch on every run so the
/// behavioral-repro server always sees a known dataset.
fn build_fake_db() -> std::path::PathBuf {
    let path = fake_db_path();
    let fake_root = std::env::temp_dir().join("qqflow_fake");
    let _ = std::fs::remove_dir_all(&fake_root);
    std::fs::create_dir_all(path.parent().unwrap()).unwrap();

    let raw = std::env::temp_dir().join("qqflow_fake_raw.db");
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
        g(1, "10001", (ts << 32) |1, "u_a", "张三", "你好，欢迎加入".as_bytes());
        g(2, "10001", (ts << 32) |2, "u_b", "李四", "收到".as_bytes());
        g(3, "10001", (ts << 32) |3, "u_a", "张三", "李四撤回了一条消息\n你猜猜撤回了什么".as_bytes());
        g(4, "10001", (ts << 32) |4, "u_b", "李四", "群主已将群名修改为「测试群」".as_bytes());
        let mut media = vec![0u8; 70_000];
        media[5000..5008].copy_from_slice(b".jpg.exe");
        g(5, "10001", (ts << 32) |5, "u_c", "王五", &media);
        g(6, "20002", (ts << 32) |1, "u_a", "张三",
            "{\"appID\":\"x\",\"prompt\":\"分享一个链接\",\"desc\":\"有趣内容\",\"title\":\"标题\"}".as_bytes());
        conn.execute(
            "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["u_12345", (ts << 32) |1, "王五", "在吗？".as_bytes()],
        )
        .unwrap();
        conn.execute(
            "INSERT INTO c2c_msg_table VALUES (?1, ?2, ?3, ?4)",
            rusqlite::params!["u_12345", (ts << 32) |2, "王五", "明天见".as_bytes()],
        )
        .unwrap();
    }

    // Prepend the 1024-byte custom header (QQ's nt_msg.db layout).
    let mut bytes = std::fs::read(&raw).unwrap();
    let mut all = vec![0u8; scan::CUSTOM_HEADER_LEN as usize];
    all[0..8].copy_from_slice(b"QQNTDB!1");
    all.append(&mut bytes);
    std::fs::write(&path, all).unwrap();
    let _ = std::fs::remove_file(&raw);

    // Verify the server's own pipeline can read it back (mirror + decrypt).
    path
}

/// Verify the fake DB is readable through the server's pipeline (mirror +
/// decrypt + direct row counts). The behavioral-repro runbook depends on it.
#[test]
fn fake_db_for_behavioral_repro() {
    let _guard = FAKE_DB_LOCK.lock().unwrap();
    let path = build_fake_db();
    let info = scan::DbInfo { qq: FAKE_QQ.into(), path: path.clone() };
    let mirror_dir = std::env::temp_dir().join("qqflow_fake_mirror");
    let mirror = Mirror::new(&info, &mirror_dir).unwrap();
    let conn = decrypt::open_decrypted(&mirror.main_path, FAKE_KEY).unwrap();
    let n: i64 = conn
        .query_row("SELECT count(*) FROM group_msg_table", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 6, "fake group rows readable through the real pipeline");
    let n: i64 = conn
        .query_row("SELECT count(*) FROM c2c_msg_table", [], |r| r.get(0))
        .unwrap();
    assert_eq!(n, 2, "fake c2c rows readable through the real pipeline");
    let _ = std::fs::remove_dir_all(&mirror_dir);

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
    let info = scan::DbInfo { qq: FAKE_QQ.into(), path };
    let mirror_dir = std::env::temp_dir().join("qqflow_fake_mirror");
    let mirror = Mirror::new(&info, &mirror_dir).unwrap();
    let conn = decrypt::open_decrypted(&mirror.main_path, FAKE_KEY).unwrap();
    let store = qqflow_server::store::index::build_index(&conn).unwrap();
    let _ = std::fs::remove_dir_all(&mirror_dir);

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
        let t0 = std::time::Instant::now();
        let mirror_dir = std::env::temp_dir().join("qqflow_gt_mirror").join(&info.qq);
        let mirror = Mirror::new(info, &mirror_dir).unwrap();
        let conn = decrypt::open_decrypted(&mirror.main_path, &key)
            .expect("real DB must open with the PRAGMA suite (arbitrates PRAGMA order)");

        for (table, id_col) in [("group_msg_table", "40021"), ("c2c_msg_table", "40020")] {
            let (cnt, max_rowid): (i64, i64) = conn
                .query_row(&format!("SELECT count(*), max(rowid) FROM {table}"), [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            let (min_ts, max_ts): (i64, i64) = conn
                .query_row(&format!("SELECT min(\"40001\">>16), max(\"40001\">>16) FROM {table}"), [], |r| {
                    Ok((r.get(0)?, r.get(1)?))
                })
                .unwrap();
            let out_of_order: i64 = conn
                .query_row(
                    &format!(
                        "SELECT count(*) FROM (SELECT \"40001\">>16 AS t, LAG(\"40001\">>16) OVER (ORDER BY rowid) AS p FROM {table}) WHERE p IS NOT NULL AND t < p"
                    ),
                    [],
                    |r| r.get(0),
                )
                .unwrap();
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
                 tsRange(seq>>16)=[{min_ts},{max_ts}] outOfOrder={out_of_order} blobLen=[{b_min},{b_max}] >64KB={big}"
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

        drop(conn);
        let _ = std::fs::remove_dir_all(&mirror_dir);
        println!("[GT] qq {} done: mirror+open+queries {:.1}s", info.qq, t0.elapsed().as_secs_f64());
    }
}
