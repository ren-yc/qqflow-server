//! Full-table scan that builds the in-memory index.
//!
//! QQ NT stores messages in two tables with numeric binary column names:
//!   group_msg_table: "40021" (group id), "40001" (seq), "40020" (sender uid),
//!                    "40093" (nickname), "40800" (message blob)
//!   c2c_msg_table:   "40020" (peer uid), "40001" (seq), "40093" (nickname),
//!                    "40800" (message blob)
//!
//! Spec-derived optional columns (QQDecrypt/nt_msg_db_util analysis) are
//! probed per table and appended when present — QQ versions may lack them
//! and the index degrades to the legacy behavior:
//!   "40013" message direction (0 other / 1,2 self / 3 system) -> is_send
//!   "40050" unix send time (seconds) -> authoritative ts (fallback seq>>32)
//!   "40090" sender group card (group table) -> display nick preference

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::parser::types::{seq_to_time, ChatType, MessageRecord, MsgType};
use crate::parser::{self};

use super::{conv_key, MediaEntry, Store};

const GROUP_TABLE: &str = "group_msg_table";
const C2C_TABLE: &str = "c2c_msg_table";

/// Spec-derived optional columns actually present in one table
/// (value-driven: absent columns degrade, never fail the scan).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
struct TableCols {
    has_dir: bool,  // "40013" message direction
    has_time: bool, // "40050" unix send time
    has_card: bool, // "40090" sender group card
}

/// Probe which spec-derived columns the table has (metadata-only query).
fn probe_cols(conn: &Connection, table: &str) -> TableCols {
    let mut cols = TableCols::default();
    let Ok(mut stmt) = conn.prepare(&format!("PRAGMA table_info(\"{table}\")")) else {
        return cols;
    };
    let Ok(rows) = stmt.query_map([], |r| r.get::<_, String>(1)) else {
        return cols;
    };
    for name in rows.flatten() {
        match name.as_str() {
            "40013" => cols.has_dir = true,
            "40050" => cols.has_time = true,
            "40090" => cols.has_card = true,
            _ => {}
        }
    }
    cols
}

/// Base (always-present) columns per table.
fn base_cols(chat_type: ChatType) -> &'static str {
    match chat_type {
        ChatType::Group => "\"40021\", \"40001\", \"40020\", \"40093\", \"40800\"",
        ChatType::C2c => "\"40020\", \"40001\", \"40093\", \"40800\"",
    }
}

/// Base column count per table (rowid included).
fn base_len(chat_type: ChatType) -> usize {
    match chat_type {
        ChatType::Group => 6, // rowid + 5
        ChatType::C2c => 5,   // rowid + 4
    }
}

/// Column list for a scan/read query: base columns, then the probed
/// optional columns in a fixed order (40013, 40050, 40090) so row indices
/// stay stable.
fn cols_sql(chat_type: ChatType, cols: TableCols) -> String {
    let mut sql = base_cols(chat_type).to_string();
    if cols.has_dir {
        sql.push_str(", \"40013\"");
    }
    if cols.has_time {
        sql.push_str(", \"40050\"");
    }
    if cols.has_card {
        sql.push_str(", \"40090\"");
    }
    sql
}

/// One decoded message row (all columns, optional ones as Options).
struct RowData {
    rowid: i64,
    talker: String,
    seq: i64,
    uid: String,
    nick: String,
    blob: Vec<u8>,
    dir: Option<i64>,
    time: Option<i64>,
    card: Option<String>,
}

/// Map a query row to `RowData`, reading the probed optional columns at
/// their fixed indices (SQL NULL-safe via Option).
fn map_row(
    chat_type: ChatType,
    cols: TableCols,
    row: &rusqlite::Row<'_>,
) -> rusqlite::Result<RowData> {
    // Column mapping differs per table (the c2c query has one column fewer
    // and no separate sender column — the peer is the sender):
    //   group: rowid, "40021"(talker), "40001"(seq), "40020"(sender), "40093"(nick), "40800"(blob)
    //   c2c:   rowid, "40020"(peer=talker=sender), "40001"(seq), "40093"(nick), "40800"(blob)
    let (rowid, talker, seq, uid, nick, blob) = match chat_type {
        ChatType::Group => {
            let rowid: i64 = row.get(0)?;
            let talker: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let uid: String = row.get(3)?;
            let nick: String = row.get(4)?;
            let blob: Vec<u8> = row.get(5)?;
            (rowid, talker, seq, uid, nick, blob)
        }
        ChatType::C2c => {
            let rowid: i64 = row.get(0)?;
            let talker: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let nick: String = row.get(3)?;
            let blob: Vec<u8> = row.get(4)?;
            (rowid, talker.clone(), seq, talker, nick, blob)
        }
    };
    let base = base_len(chat_type);
    let dir = if cols.has_dir { row.get::<_, Option<i64>>(base)? } else { None };
    let time = if cols.has_time { row.get::<_, Option<i64>>(base + usize::from(cols.has_dir))? } else { None };
    let card = if cols.has_card { row.get::<_, Option<String>>(base + usize::from(cols.has_dir) + usize::from(cols.has_time))? } else { None };
    Ok(RowData { rowid, talker, seq, uid, nick, blob, dir, time, card })
}

/// Decode a row into a `MessageRecord`: ts prefers "40050" (spec-authoritative
/// unix send time) with per-row fallback to `seq >> 32`; the group card
/// "40090" wins over the "40093" nickname when non-empty.
fn row_to_record(chat_type: ChatType, d: RowData) -> MessageRecord {
    let from_nick = if chat_type == ChatType::Group {
        d.card.filter(|c| !c.is_empty()).unwrap_or(d.nick.clone())
    } else {
        d.nick.clone()
    };
    MessageRecord {
        rowid: d.rowid,
        seq: d.seq,
        ts: d.time.filter(|t| *t > 0).unwrap_or_else(|| seq_to_time(d.seq)),
        chat_type,
        talker: d.talker,
        from_uid: d.uid,
        from_nick,
        direction: d.dir,
        parsed: parser::extract_message(&d.blob),
    }
}

/// Guess the group display name from "修改群名为X" system messages.
fn guess_group_name(text: &str, current: &str) -> String {
    for marker in ["修改群名为", "已将群名修改为"] {
        if let Some(idx) = text.find(marker) {
            let name = text[idx + marker.len()..].trim();
            let name = name.trim_matches(|c: char| {
                c.is_ascii_whitespace()
                    || c == '"'
                    || c == '\''
                    || c == '，'
                    || c == '。'
                    || c == '「'
                    || c == '」'
                    || c == '（'
                    || c == '）'
                    || c == '('
                    || c == ')'
            });
            if !name.is_empty() && name.chars().count() <= 64 {
                return name.to_string();
            }
        }
    }
    current.to_string()
}

/// Apply one parsed record: conversation create/lookup, group-name guess,
/// uid -> nickname map update, media entry registration, message push
/// (sets `dirty`).
fn apply_record(store: &mut Store, rec: MessageRecord) {
    let key = conv_key(rec.chat_type, &rec.talker);
    let conv = store.convs.entry(key).or_insert_with(|| {
        let name = if rec.chat_type == ChatType::Group {
            rec.talker.clone() // placeholder until a rename message appears
        } else {
            rec.from_nick.clone()
        };
        super::Conversation {
            chat_type: rec.chat_type,
            talker: rec.talker.clone(),
            name,
            msgs: Vec::new(),
            dirty: false,
        }
    });
    // Update group name from rename system messages.
    if rec.chat_type == ChatType::Group && rec.parsed.msg_type == MsgType::System {
        let new_name = guess_group_name(&rec.parsed.content, &conv.name);
        if new_name != conv.name && new_name != rec.talker {
            conv.name = new_name;
        }
    }
    if !rec.from_nick.is_empty() {
        store.uid_names.insert(rec.from_uid.clone(), rec.from_nick.clone());
    }
    // Register fetchable media (first-wins): key = md5 hex or uuid, only
    // when a local cache path exists.
    if let Some(m) = &rec.parsed.media
        && let Some(key) = m.key()
        && let Some(local_path) = m.local_path.as_deref().filter(|p| !p.is_empty())
    {
        store.media.entry(key).or_insert(MediaEntry {
            local_path: local_path.to_string(),
            file_name: m.file_name.clone(),
        });
    }
    conv.msgs.push(rec);
    conv.dirty = true;
}

fn scan_table(
    conn: &Connection,
    table: &str,
    cols: TableCols,
    chat_type: ChatType,
    store: &mut Store,
) -> Result<i64> {
    let sql = format!("SELECT rowid, {} FROM {table}", cols_sql(chat_type, cols));
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare scan {table}"))?;
    let rows = stmt.query_map([], |row| map_row(chat_type, cols, row))?;
    let mut watermark = 0i64;
    for r in rows {
        let d = match r {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("scan {table}: row skipped: {e}");
                continue;
            }
        };
        watermark = watermark.max(d.rowid);
        apply_record(store, row_to_record(chat_type, d));
    }
    Ok(watermark)
}

/// Full scan of both message tables; returns the new store.
pub fn build_index(conn: &Connection) -> Result<Store> {
    let mut store = Store::default();
    let g_cols = probe_cols(conn, GROUP_TABLE);
    let c_cols = probe_cols(conn, C2C_TABLE);
    store.watermark_group = scan_table(conn, GROUP_TABLE, g_cols, ChatType::Group, &mut store)
        .context("scan group_msg_table")?;
    store.watermark_c2c = scan_table(conn, C2C_TABLE, c_cols, ChatType::C2c, &mut store)
        .context("scan c2c_msg_table")?;
    // Final lazy sort is applied on first query; force now so queries are clean.
    for conv in store.convs.values_mut() {
        conv.ensure_sorted();
    }
    Ok(store)
}

/// Poll-time read: rows with rowid > watermark for one table, parsed into
/// records. Pure read — the store is not touched, so when the companion
/// table's read fails there is nothing to roll back and a retry simply
/// re-reads the same rows (no duplicates).
pub fn read_new(
    conn: &Connection,
    chat_type: ChatType,
    watermark: i64,
) -> Result<(i64, Vec<MessageRecord>)> {
    // The column probe runs per call: metadata-only and cheap, and it
    // picks up QQ upgrades while the service runs.
    let (table, cols) = match chat_type {
        ChatType::Group => (GROUP_TABLE, probe_cols(conn, GROUP_TABLE)),
        ChatType::C2c => (C2C_TABLE, probe_cols(conn, C2C_TABLE)),
    };
    let sql = format!("SELECT rowid, {} FROM {table} WHERE rowid > ?1", cols_sql(chat_type, cols));
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare read {table}"))?;
    let rows = stmt.query_map([watermark], |row| map_row(chat_type, cols, row))?;
    let mut new_wm = watermark;
    let mut records = Vec::new();
    for r in rows {
        let d = match r {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("read {table}: row skipped: {e}");
                continue;
            }
        };
        new_wm = new_wm.max(d.rowid);
        records.push(row_to_record(chat_type, d));
    }
    Ok((new_wm, records))
}

/// Apply records read by `read_new` to the store (single write-lock critical
/// section; callers write the watermarks in the same section).
pub fn apply_records(store: &mut Store, records: &[MessageRecord]) {
    for rec in records {
        apply_record(store, rec.clone());
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    /// In-memory tables with (or without) the spec-derived optional columns.
    fn make_table(with_optional: bool) -> Connection {
        let conn = Connection::open_in_memory().unwrap();
        if with_optional {
            conn.execute_batch(
                "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \
                 \"40093\" TEXT, \"40800\" BLOB, \"40013\" INTEGER, \"40050\" INTEGER, \"40090\" TEXT);\
                 CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);\
                 INSERT INTO group_msg_table VALUES ('10001', 100, 'u_a', '张三', CAST('你好' AS BLOB), 1, 1234567890, '群名片');",
            )
            .unwrap();
        } else {
            conn.execute_batch(
                "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40001\" INTEGER, \"40020\" TEXT, \
                 \"40093\" TEXT, \"40800\" BLOB);\
                 CREATE TABLE c2c_msg_table (\"40020\" TEXT, \"40001\" INTEGER, \"40093\" TEXT, \"40800\" BLOB);\
                 INSERT INTO group_msg_table VALUES ('10001', 100, 'u_a', '张三', CAST('你好' AS BLOB));",
            )
            .unwrap();
        }
        conn
    }

    #[test]
    fn probe_flags_reflect_columns() {
        let conn = make_table(true);
        assert_eq!(
            probe_cols(&conn, "group_msg_table"),
            TableCols { has_dir: true, has_time: true, has_card: true }
        );
        let conn = make_table(false);
        assert_eq!(probe_cols(&conn, "group_msg_table"), TableCols::default());
    }

    #[test]
    fn full_columns_produce_direction_ts_and_card() {
        let conn = make_table(true);
        let store = build_index(&conn).unwrap();
        let conv = store.conversation(ChatType::Group, "10001").unwrap();
        assert_eq!(conv.msgs.len(), 1);
        let m = &conv.msgs[0];
        assert_eq!(m.direction, Some(1), "40013 read");
        assert_eq!(m.ts, 1234567890, "40050 authoritative");
        assert_eq!(m.from_nick, "群名片", "40090 card wins over 40093");
        // seq = 100 -> seq>>32 = 0: the 40050 value must be used, not 0.
        assert_ne!(m.ts, seq_to_time(m.seq));
    }

    #[test]
    fn missing_columns_degrade() {
        let conn = make_table(false);
        let store = build_index(&conn).unwrap();
        let conv = store.conversation(ChatType::Group, "10001").unwrap();
        let m = &conv.msgs[0];
        assert_eq!(m.direction, None, "no 40013 -> None");
        assert_eq!(m.ts, seq_to_time(m.seq), "no 40050 -> seq>>32");
        assert_eq!(m.from_nick, "张三", "no 40090 -> 40093");
    }

    #[test]
    fn read_new_respects_columns_like_scan() {
        let conn = make_table(true);
        let (wm, records) = read_new(&conn, ChatType::Group, 0).unwrap();
        assert_eq!(wm, 1);
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].direction, Some(1));
        assert_eq!(records[0].ts, 1234567890);
        assert_eq!(records[0].from_nick, "群名片");
    }
}
