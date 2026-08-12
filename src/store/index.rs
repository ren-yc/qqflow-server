//! Full-table scan that builds the in-memory index.
//!
//! QQ NT stores messages in two tables with numeric binary column names:
//!   group_msg_table: "40021" (group id), "40001" (seq), "40020" (sender uid),
//!                    "40093" (nickname), "40800" (message blob)
//!   c2c_msg_table:   "40020" (peer uid), "40001" (seq), "40093" (nickname),
//!                    "40800" (message blob)

use anyhow::{Context, Result};
use rusqlite::Connection;

use crate::parser::types::{seq_to_time, ChatType, MessageRecord};
use crate::parser::{self};

use super::{conv_key, Store};

const GROUP_TABLE: &str = "group_msg_table";
const C2C_TABLE: &str = "c2c_msg_table";
const GROUP_COLS: &str = "\"40021\", \"40001\", \"40020\", \"40093\", \"40800\"";
const C2C_COLS: &str = "\"40020\", \"40001\", \"40093\", \"40800\"";

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

#[allow(clippy::too_many_arguments)] // one call site per scanned row
fn apply_row(
    store: &mut Store,
    chat_type: ChatType,
    rowid: i64,
    talker: &str,
    from_uid: &str,
    from_nick: &str,
    seq: i64,
    blob: &[u8],
) {
    let parsed = parser::extract_text(blob);
    let ts = seq_to_time(seq);
    let rec = MessageRecord {
        rowid,
        seq,
        ts,
        chat_type,
        talker: talker.to_string(),
        from_uid: from_uid.to_string(),
        from_nick: from_nick.to_string(),
        parsed,
    };

    let key = conv_key(chat_type, talker);
    let conv = store.convs.entry(key).or_insert_with(|| {
        let name = if chat_type == ChatType::Group {
            talker.to_string() // placeholder until a rename message appears
        } else {
            from_nick.to_string()
        };
        super::Conversation {
            chat_type,
            talker: talker.to_string(),
            name,
            msgs: Vec::new(),
            dirty: false,
        }
    });
    // Update group name from rename system messages.
    if chat_type == ChatType::Group && rec.parsed.msg_type == crate::parser::types::MsgType::System {
        let new_name = guess_group_name(&rec.parsed.content, &conv.name);
        if new_name != conv.name && new_name != talker {
            conv.name = new_name;
        }
    }
    if !from_nick.is_empty() {
        store.uid_names.insert(from_uid.to_string(), from_nick.to_string());
    }
    conv.msgs.push(rec);
    conv.dirty = true;
}

fn scan_table(
    conn: &Connection,
    table: &str,
    cols: &str,
    chat_type: ChatType,
    store: &mut Store,
) -> Result<i64> {
    let sql = format!("SELECT rowid, {cols} FROM {table}");
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare scan {table}"))?;
    // Column mapping differs per table (the c2c query has one column fewer
    // and no separate sender column — the peer is the sender):
    //   group: rowid, "40021"(talker), "40001"(seq), "40020"(sender), "40093"(nick), "40800"(blob)
    //   c2c:   rowid, "40020"(peer=talker=sender), "40001"(seq), "40093"(nick), "40800"(blob)
    let rows = stmt.query_map([], |row| match chat_type {
        ChatType::Group => {
            let rowid: i64 = row.get(0)?;
            let a: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let uid: String = row.get(3)?;
            let nick: String = row.get(4)?;
            let blob: Vec<u8> = row.get(5)?;
            Ok((rowid, a, seq, uid, nick, blob))
        }
        ChatType::C2c => {
            let rowid: i64 = row.get(0)?;
            let a: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let nick: String = row.get(3)?;
            let blob: Vec<u8> = row.get(4)?;
            Ok((rowid, a.clone(), seq, a, nick, blob))
        }
    })?;
    let mut watermark = 0i64;
    for r in rows {
        let (rowid, a, seq, uid, nick, blob) = match r {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("scan {table}: row skipped: {e}");
                continue;
            }
        };
        apply_row(store, chat_type, rowid, &a, &uid, &nick, seq, &blob);
        watermark = watermark.max(rowid);
    }
    Ok(watermark)
}

/// Full scan of both message tables; returns the new store.
pub fn build_index(conn: &Connection) -> Result<Store> {
    let mut store = Store::default();
    store.watermark_group = scan_table(conn, GROUP_TABLE, GROUP_COLS, ChatType::Group, &mut store)
        .context("scan group_msg_table")?;
    store.watermark_c2c = scan_table(conn, C2C_TABLE, C2C_COLS, ChatType::C2c, &mut store)
        .context("scan c2c_msg_table")?;
    // Final lazy sort is applied on first query; force now so queries are clean.
    for conv in store.convs.values_mut() {
        conv.ensure_sorted();
    }
    Ok(store)
}

/// Poll-time incremental append: rows with rowid > watermark for one table.
/// Returns the new watermark and the appended records (for SSE events).
pub fn append_new(
    conn: &Connection,
    chat_type: ChatType,
    store: &mut Store,
    watermark: i64,
) -> Result<(i64, Vec<MessageRecord>)> {
    let (table, cols) = match chat_type {
        ChatType::Group => (GROUP_TABLE, GROUP_COLS),
        ChatType::C2c => (C2C_TABLE, C2C_COLS),
    };
    let sql = format!("SELECT rowid, {cols} FROM {table} WHERE rowid > ?1");
    let mut stmt = conn
        .prepare(&sql)
        .with_context(|| format!("prepare append {table}"))?;
    // Same per-table column mapping as `scan_table`; see its comment.
    let rows = stmt.query_map([watermark], |row| match chat_type {
        ChatType::Group => {
            let rowid: i64 = row.get(0)?;
            let a: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let uid: String = row.get(3)?;
            let nick: String = row.get(4)?;
            let blob: Vec<u8> = row.get(5)?;
            Ok((rowid, a, seq, uid, nick, blob))
        }
        ChatType::C2c => {
            let rowid: i64 = row.get(0)?;
            let a: String = row.get(1)?;
            let seq: i64 = row.get(2)?;
            let nick: String = row.get(3)?;
            let blob: Vec<u8> = row.get(4)?;
            Ok((rowid, a.clone(), seq, a, nick, blob))
        }
    })?;
    let mut new_wm = watermark;
    let mut appended = Vec::new();
    for r in rows {
        let (rowid, a, seq, uid, nick, blob) = match r {
            Ok(v) => v,
            Err(e) => {
                tracing::debug!("append {table}: row skipped: {e}");
                continue;
            }
        };
        apply_row(store, chat_type, rowid, &a, &uid, &nick, seq, &blob);
        new_wm = new_wm.max(rowid);
        appended.push(MessageRecord {
            rowid,
            seq,
            ts: seq_to_time(seq),
            chat_type,
            talker: a,
            from_uid: uid,
            from_nick: nick,
            parsed: crate::parser::extract_text(&blob),
        });
    }
    Ok((new_wm, appended))
}
