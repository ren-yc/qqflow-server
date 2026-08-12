//! UID → QQ number mapping.
//!
//! QQ NT keeps a uid mapping table (`nt_uid_mapping_table` with columns
//! "48901" = uid, "40020" = qq number). Column names and table layouts are
//! version-dependent; this is a best-effort query that degrades gracefully.

use rusqlite::Connection;

/// Return (uid, qq) pairs from known mapping tables, best-effort.
pub fn load_uid_map(conn: &Connection, limit: usize) -> Vec<(String, String)> {
    let mut out = Vec::new();
    for table in ["nt_uid_mapping_table", "uid_mapping", "buddy_mapping"] {
        let sql = format!(
            "SELECT \"48901\", \"40020\" FROM {table} LIMIT {limit}"
        );
        if let Ok(mut stmt) = conn.prepare(&sql)
            && let Ok(rows) = stmt.query_map([], |r| {
                Ok((r.get::<_, String>(0).unwrap_or_default(), r.get::<_, String>(1).unwrap_or_default()))
            }) {
                for r in rows.flatten() {
                    let (uid, qq) = r;
                    if !uid.is_empty() && !qq.is_empty() {
                        out.push((uid, qq));
                    }
                }
                if !out.is_empty() {
                    return out;
                }
            }
    }
    out
}
