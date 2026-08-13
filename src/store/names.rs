//! Best-effort loader for uid/群 name maps (备注名、QQ 号、群名) from QQ's
//! mapping sources.
//!
//! Sources are version-fragile and have no documented stable schema: the
//! uid mapping table (`nt_uid_mapping_table` inside `nt_msg.db`) maps
//! `u_...` uids to remark/nick/QQ data, and group names typically live in a
//! sibling database (`group_info.db`, same SQLCipher key, with or without
//! the 1024-byte header). Column classification is VALUE-DRIVEN — `u_`
//! ratio, all-digit ratio, CJK ratio over a sample — with column-name hints
//! as a tie-breaker, and the group-id column is picked by overlap with the
//! group ids the index already knows (the strongest version-robust signal).
//! Group tables whose names merely drift (`group_msg_table` contains
//! "group" too) are only trusted after an agreement check: their name
//! column must reproduce the rename-message-derived group names, so a
//! message table (group id + sender nick) can never poison the group-name
//! map.
//!
//! By contract `load_names` NEVER fails: any error degrades to an empty map
//! and a debug log, so schema churn leaves the server running with today's
//! message-derived names (the ground-truth probe in
//! `tests/real_db_groundtruth.rs` arbitrates the real layout).

use std::collections::{HashMap, HashSet};
use std::path::Path;

use rusqlite::Connection;

use crate::db::decrypt::open_live_mode;
use crate::store::NameMaps;

/// Sample size for column classification (first N rows).
const PROBE_LIMIT: &str = "1000";
/// Fallback sibling-file scan cap (renamed group db).
const FALLBACK_SCAN_CAP: usize = 6;

/// Candidate uid-mapping tables inside `nt_msg.db` (name drift tolerated).
const UID_TABLES: &[&str] = &[
    "nt_uid_mapping_table",
    "uid_mapping",
    "buddy_mapping",
    "nt_buddylist",
];

/// Candidate sibling files that may hold group info (name drift tolerated).
const GROUP_FILES: &[&str] = &[
    "group_info.db",
    "nt_group_info.db",
    "group_table.db",
    "nt_group_table.db",
    "troop_info.db",
    "nt_uid_mapping.db",
];

/// Candidate sibling files holding contact profiles (uid -> remark/nick).
/// Ground truth: `nt_uid_mapping_table` inside nt_msg.db only carries
/// uid -> QQ number on current versions; remark/nick live in profile_info.db.
const PROFILE_FILES: &[&str] = &["profile_info.db"];

/// Candidate profile tables inside a profile database. (Not
/// `buddy_req_list_5` — friend-request rows carry 加好友验证问题 text that a
/// CJK-ratio classifier mistakes for remarks; ground-truth-confirmed.)
const PROFILE_TABLES: &[&str] = &[
    "profile_info_v2",
    "profile_info_v6",
    "profile_info_adelie",
    "buddy_list",
];

/// Candidate group tables inside a group-info source.
const GROUP_TABLES: &[&str] = &[
    "group_list",
    "group_detail_info_ver1",
    "group_table",
    "nt_group_table",
    "nt_group_info",
    "group_info",
    "troop_info",
    "nt_troop_info",
    "troop_list",
];

/// Key sets derived from the built index — used to pick the group-id column
/// by overlap with known group numbers, and to agreement-verify group-name
/// columns against the rename-message-derived names.
#[derive(Debug, Default)]
pub struct KnownKeys {
    pub uids: HashSet<String>,
    pub group_ids: HashSet<String>,
    /// groupId -> name derived from "修改群名" system messages (only groups
    /// that were renamed; `conv.name` != group id).
    pub group_names: HashMap<String, String>,
}

impl KnownKeys {
    pub fn from_store(store: &crate::store::Store) -> Self {
        let mut uids = HashSet::new();
        let mut group_ids = HashSet::new();
        let mut group_names = HashMap::new();
        for conv in store.convs.values() {
            match conv.chat_type {
                crate::parser::types::ChatType::Group => {
                    group_ids.insert(conv.talker.clone());
                    if conv.name != conv.talker && !conv.name.is_empty() {
                        group_names.insert(conv.talker.clone(), conv.name.clone());
                    }
                }
                crate::parser::types::ChatType::C2c => {
                    uids.insert(conv.talker.clone());
                }
            }
            for m in &conv.msgs {
                if !m.from_uid.is_empty() {
                    uids.insert(m.from_uid.clone());
                }
            }
        }
        Self {
            uids,
            group_ids,
            group_names,
        }
    }
}

/// Load name maps for one account. `msg_conn` is the already-open live
/// `nt_msg.db` connection (the uid mapping table lives inside it);
/// `nt_db_dir` is its directory (sibling group-info and profile databases).
/// Best-effort by contract — never returns Err, never panics.
pub fn load_names(msg_conn: &Connection, nt_db_dir: &Path, key: &str, known: &KnownKeys) -> NameMaps {
    let mut maps = NameMaps::default();
    // Contact nick/remark/QQ: the sibling profile_info.db first (its data
    // is authoritative; per-key first-wins merge), then the uid mapping
    // table inside nt_msg.db (ground truth: it only carries uid -> QQ
    // number on current versions).
    load_uid_maps_sibling(nt_db_dir, key, known, &mut maps);
    load_uid_maps(msg_conn, &mut maps);
    // Group tables may live inside nt_msg.db itself — probe it before
    // opening any sibling file (free, already-open connection).
    if !harvest_group_db(msg_conn, known, &mut maps) {
        load_group_maps(nt_db_dir, key, known, &mut maps);
    }
    maps
}

/// (per-column) value statistics over a sample.
#[derive(Debug, Default)]
struct ColStats {
    name: String,
    /// non-NULL values examined.
    total: usize,
    /// non-empty string values.
    nonempty: usize,
    /// values starting with "u_".
    u: usize,
    /// all-ASCII-digit values, 5..=12 digits (QQ numbers / group ids).
    digit: usize,
    /// values that are mostly Han characters.
    cjk: usize,
    /// distinct non-empty values (column cardinality).
    distinct: HashSet<String>,
    /// first non-empty sample value.
    sample: String,
}

fn ratio(hits: usize, base: usize) -> f64 {
    if base == 0 {
        0.0
    } else {
        hits as f64 / base as f64
    }
}

impl ColStats {
    fn u_ratio(&self) -> f64 {
        ratio(self.u, self.nonempty)
    }
    fn digit_ratio(&self) -> f64 {
        ratio(self.digit, self.nonempty)
    }
    fn cjk_ratio(&self) -> f64 {
        ratio(self.cjk, self.nonempty)
    }
}

fn cjk_ratio(s: &str) -> f64 {
    let total = s.chars().count();
    if total == 0 {
        return 0.0;
    }
    let han = s.chars().filter(|c| ('\u{4e00}'..='\u{9fa5}').contains(c)).count();
    han as f64 / total as f64
}

/// Text/Integer cell values to string (QQ numbers may be stored as INTEGER).
fn value_str(v: rusqlite::types::ValueRef) -> Option<String> {
    match v {
        rusqlite::types::ValueRef::Text(t) => std::str::from_utf8(t).ok().map(String::from),
        rusqlite::types::ValueRef::Integer(i) => Some(i.to_string()),
        _ => None,
    }
}

fn list_tables(conn: &Connection) -> HashSet<String> {
    let mut out = HashSet::new();
    if let Ok(mut stmt) = conn.prepare("SELECT name FROM sqlite_master WHERE type='table'") {
        for n in stmt
            .query_map([], |r| r.get::<_, String>(0))
            .ok()
            .into_iter()
            .flatten()
            .filter_map(Result::ok)
        {
            out.insert(n);
        }
    }
    out
}

/// Value statistics for every column of `table` (sample of up to 1000
/// rows). None when the table is missing or unreadable.
fn probe_table(conn: &Connection, table: &str) -> Option<Vec<ColStats>> {
    let names: Vec<String> = conn
        .prepare(&format!("PRAGMA table_info(\"{table}\")"))
        .ok()?
        .query_map([], |r| r.get::<_, String>(1))
        .ok()?
        .flatten()
        .collect();
    if names.is_empty() {
        return None;
    }
    let mut cols: Vec<ColStats> = names
        .into_iter()
        .map(|name| ColStats {
            name,
            ..Default::default()
        })
        .collect();
    let mut stmt = conn
        .prepare(&format!("SELECT * FROM \"{table}\" LIMIT {PROBE_LIMIT}"))
        .ok()?;
    let ncols = cols.len();
    let rows = stmt
        .query_map([], |row| {
            let mut vals = Vec::with_capacity(ncols);
            for i in 0..ncols {
                vals.push(row.get_ref(i).ok().and_then(value_str));
            }
            Ok(vals)
        })
        .ok()?;
    for r in rows {
        let Ok(vals) = r else { continue };
        for (i, v) in vals.iter().enumerate() {
            if i >= cols.len() {
                break;
            }
            let c = &mut cols[i];
            let Some(s) = v else { continue };
            c.total += 1;
            if s.is_empty() {
                continue;
            }
            c.nonempty += 1;
            c.distinct.insert(s.clone());
            if c.sample.is_empty() {
                c.sample = s.clone();
            }
            if s.starts_with("u_") {
                c.u += 1;
            }
            if (5..=12).contains(&s.len()) && s.bytes().all(|b| b.is_ascii_digit()) {
                c.digit += 1;
            }
            if s.chars().count() >= 2 && cjk_ratio(s) >= 0.6 {
                c.cjk += 1;
            }
        }
    }
    Some(cols)
}

// ---- column classification -------------------------------------------------

/// uid key column: mostly `u_...` values.
fn classify_uid_key(cols: &[ColStats]) -> Option<usize> {
    cols.iter()
        .enumerate()
        .find(|(_, c)| c.nonempty > 0 && c.u_ratio() > 0.8)
        .map(|(i, _)| i)
}

/// uid key column by overlap with the uids the index already knows (for
/// profile tables whose key column is not `u_`-prefixed).
fn classify_uid_key_overlap(cols: &[ColStats], known: &KnownKeys) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut best_overlap = 0usize;
    for (i, c) in cols.iter().enumerate() {
        let overlap = c.distinct.iter().filter(|v| known.uids.contains(*v)).count();
        if overlap > best_overlap {
            best_overlap = overlap;
            best = Some(i);
        }
    }
    (best_overlap > 0).then_some(best).flatten()
}

/// QQ-number column: all-digit high-cardinality values; column-name hint
/// (`qq`/`uin`) first, then a high-cardinality fallback (a 0/1 "type"
/// column has low distinct).
fn classify_qq(cols: &[ColStats], exclude: &[usize]) -> Option<usize> {
    let candidates: Vec<usize> = (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].digit_ratio() > 0.9)
        .collect();
    candidates
        .iter()
        .copied()
        .find(|i| {
            let n = cols[*i].name.to_lowercase();
            n.contains("qq") || n.contains("uin")
        })
        .or_else(|| {
            candidates
                .iter()
                .copied()
                .find(|i| cols[*i].distinct.len() >= cols[*i].nonempty / 2)
        })
}

/// Remark/name column: mostly-CJK values AND a column-name hint
/// (`remark`/`note`/`alias`/`备注`) or a QQ known numeric field id
/// (`60026` 群备注, `20003` 好友备注). Hint-only by design: the most-CJK
/// fallback is a false-positive magnet (加好友验证问题、入群问题 are CJK too
/// — ground-truth-confirmed) and on current versions no readable table
/// stores remarks at all.
fn classify_remark(cols: &[ColStats], exclude: &[usize]) -> Option<usize> {
    (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].cjk_ratio() > 0.5)
        .find(|i| {
            let n = cols[*i].name.to_lowercase();
            n.contains("remark")
                || n.contains("note")
                || n.contains("alias")
                || n.contains("备注")
                || n == "60026"
                || n == "20003"
        })
}

/// Nick column (uid tables): QQ's known numeric field id `20002` (昵称)
/// first — mixed-script nicks like "Yuchen Ren" fail the CJK ratio
/// (ground-truth-confirmed) — then `nick`/`昵称`/`name` hints. No CJK
/// fallback: long CJK profile texts (AI 助手介绍等) are false-positive
/// magnets, and message-derived `uid_names` is the standing fallback.
fn classify_nick(cols: &[ColStats], exclude: &[usize]) -> Option<usize> {
    if let Some(i) = (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0)
        .find(|i| cols[*i].name == "20002")
    {
        return Some(i);
    }
    (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].cjk_ratio() > 0.5)
        .find(|i| {
            let n = cols[*i].name.to_lowercase();
            n.contains("nick") || n.contains("昵称") || n.contains("name")
        })
}

/// Group-id key column: strongest signal is overlap with the group ids the
/// index already knows; then QQ's known numeric field id `60001` (群号,
/// ground-truth-confirmed); fallback is all-digit with the highest
/// cardinality.
fn classify_group_key(cols: &[ColStats], known: &KnownKeys) -> Option<usize> {
    let mut best: Option<usize> = None;
    let mut best_overlap = 0usize;
    for (i, c) in cols.iter().enumerate() {
        if c.nonempty == 0 {
            continue;
        }
        let overlap = c.distinct.iter().filter(|v| known.group_ids.contains(*v)).count();
        if overlap > best_overlap {
            best_overlap = overlap;
            best = Some(i);
        }
    }
    if best_overlap > 0 {
        return best;
    }
    if let Some(i) = cols
        .iter()
        .enumerate()
        .find(|(_, c)| c.nonempty > 0 && c.digit_ratio() > 0.9 && c.name == "60001")
    {
        return Some(i.0);
    }
    (0..cols.len())
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].digit_ratio() > 0.9)
        .max_by(|a, b| cols[*a].distinct.len().cmp(&cols[*b].distinct.len()))
}

/// Group name column: mostly-CJK values; QQ's known numeric field id
/// `60007` (群名称, ground-truth-confirmed — tech-group names like
/// "OpenMMLab社区3群" are often < 60% Han, so the CJK-only fallback misses
/// them) first, then the `name`/`title`/`名` hint, then the most-CJK
/// non-key column.
fn classify_group_name(cols: &[ColStats], exclude: &[usize]) -> Option<usize> {
    let hinted = (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && (cols[*i].cjk_ratio() > 0.5 || cols[*i].name == "60007"))
        .find(|i| cols[*i].name == "60007");
    if let Some(i) = hinted {
        return Some(i);
    }
    let hinted = (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].cjk_ratio() > 0.5)
        .find(|i| {
            let n = cols[*i].name.to_lowercase();
            n.contains("name") || n.contains("title") || n.contains("名")
        });
    if let Some(i) = hinted {
        return Some(i);
    }
    (0..cols.len())
        .filter(|i| !exclude.contains(i))
        .filter(|i| cols[*i].nonempty > 0 && cols[*i].cjk_ratio() > 0.5)
        .max_by(|a, b| {
            cols[*a]
                .cjk_ratio()
                .partial_cmp(&cols[*b].cjk_ratio())
                .unwrap_or(std::cmp::Ordering::Equal)
        })
}

// ---- harvest ----------------------------------------------------------------

/// Fill `map` with (key-col, val-col) pairs from the whole table (mapping
/// tables are small; a full read is cheaper than any index dance). First
/// row wins per key; empty keys/values are skipped.
fn fill_map(
    conn: &Connection,
    table: &str,
    key_idx: usize,
    val_idx: Option<usize>,
    map: &mut HashMap<String, String>,
) {
    let Some(val_idx) = val_idx else { return };
    let Ok(mut stmt) = conn.prepare(&format!("SELECT * FROM \"{table}\"")) else {
        return;
    };
    let Ok(rows) = stmt.query_map([], |row| {
        let key = row.get_ref(key_idx).ok().and_then(value_str).unwrap_or_default();
        let val = row.get_ref(val_idx).ok().and_then(value_str).unwrap_or_default();
        Ok((key, val))
    }) else {
        return;
    };
    for r in rows {
        let Ok((k, v)) = r else { continue };
        if !k.is_empty() && !v.is_empty() {
            map.entry(k).or_insert(v);
        }
    }
}

/// uid -> remark / nick / qq from the sibling profile database (contact
/// profiles). All candidate tables are merged per-key (`fill_map` is
/// first-wins, so earlier tables keep priority — the profile DB is probed
/// BEFORE the nt_msg mapping table because its data is authoritative).
fn load_uid_maps_sibling(nt_db_dir: &Path, key: &str, known: &KnownKeys, maps: &mut NameMaps) {
    for name in PROFILE_FILES {
        let path = nt_db_dir.join(name);
        let Some(conn) = open_sibling(&path, key) else { continue };
        harvest_uid_profiles(&conn, known, maps);
        drop(conn);
    }
}

/// uid -> remark / nick / qq from one profile database connection.
fn harvest_uid_profiles(conn: &Connection, known: &KnownKeys, maps: &mut NameMaps) {
    let tables = list_tables(conn);
    let mut candidates: Vec<&String> = tables
        .iter()
        .filter(|t| PROFILE_TABLES.contains(&t.as_str()))
        .collect();
    for t in &tables {
        let l = t.to_lowercase();
        if !PROFILE_TABLES.contains(&t.as_str())
            && l.contains("profile")
            && !l.contains("fts")
            && !l.contains("public")
        {
            candidates.push(t);
        }
    }
    candidates.sort();
    for cand in candidates {
        let Some(cols) = probe_table(conn, cand) else { continue };
        let key_idx = classify_uid_key(&cols).or_else(|| classify_uid_key_overlap(&cols, known));
        let Some(key_idx) = key_idx else { continue };
        let exclude = vec![key_idx];
        let remark_idx = classify_remark(&cols, &exclude);
        let nick_idx = classify_nick(&cols, &exclude);
        let qq_idx = classify_qq(&cols, &exclude);
        if remark_idx.is_none() && nick_idx.is_none() && qq_idx.is_none() {
            tracing::debug!("[names] {cand}: no remark/nick/qq column identified");
            continue;
        }
        fill_map(conn, cand, key_idx, remark_idx, &mut maps.uid_remark);
        fill_map(conn, cand, key_idx, nick_idx, &mut maps.uid_nick);
        fill_map(conn, cand, key_idx, qq_idx, &mut maps.uid_qq);
        tracing::debug!(
            "[names] {cand}: uid_remark={} uid_nick={} uid_qq={}",
            maps.uid_remark.len(),
            maps.uid_nick.len(),
            maps.uid_qq.len()
        );
    }
}

/// uid -> remark / qq from the uid mapping table inside `msg_conn`.
fn load_uid_maps(conn: &Connection, maps: &mut NameMaps) {
    let tables = list_tables(conn);
    for cand in UID_TABLES {
        if !tables.contains(*cand) {
            continue;
        }
        let Some(cols) = probe_table(conn, cand) else { continue };
        let Some(key_idx) = classify_uid_key(&cols) else { continue };
        let exclude = vec![key_idx];
        let remark_idx = classify_remark(&cols, &exclude);
        let nick_idx = classify_nick(&cols, &exclude);
        let qq_idx = classify_qq(&cols, &exclude);
        if remark_idx.is_none() && nick_idx.is_none() && qq_idx.is_none() {
            tracing::debug!("[names] {cand}: no remark/nick/qq column identified, trying next");
            continue;
        }
        fill_map(conn, cand, key_idx, remark_idx, &mut maps.uid_remark);
        fill_map(conn, cand, key_idx, nick_idx, &mut maps.uid_nick);
        fill_map(conn, cand, key_idx, qq_idx, &mut maps.uid_qq);
        tracing::debug!(
            "[names] {cand}: uid_remark={} uid_nick={} uid_qq={}",
            maps.uid_remark.len(),
            maps.uid_nick.len(),
            maps.uid_qq.len()
        );
        return; // first table that yields a key column wins
    }
}

/// Fill group_name/group_remark from `conn`'s group tables. Returns true
/// when a group-id key column was found (the source is authoritative).
fn harvest_group_db(conn: &Connection, known: &KnownKeys, maps: &mut NameMaps) -> bool {
    let tables = list_tables(conn);
    // Exact matches keep GROUP_TABLES declaration order (group_list before
    // group_detail_info_ver1) — sorting alphabetically would let the
    // detail table (includes disbanded groups) shadow the live group list.
    let exact: Vec<&String> = tables
        .iter()
        .filter(|t| GROUP_TABLES.contains(&t.as_str()))
        .collect();
    // Name-drift fallback: any other table mentioning group/troop. These
    // must be agreement-verified below — a message table like
    // `group_msg_table` (group id + sender nick) matches this filter too
    // and would otherwise poison the group-name map.
    let mut drift: Vec<&String> = tables
        .iter()
        .filter(|t| !GROUP_TABLES.contains(&t.as_str()))
        .filter(|t| {
            let l = t.to_lowercase();
            l.contains("group") || l.contains("troop")
        })
        .collect();
    drift.sort();
    for (cand, is_drift) in exact
        .iter()
        .map(|t| (*t, false))
        .chain(drift.iter().map(|t| (*t, true)))
    {
        let Some(cols) = probe_table(conn, cand) else { continue };
        let Some(key_idx) = classify_group_key(&cols, known) else { continue };
        let exclude = vec![key_idx];
        let name_idx = classify_group_name(&cols, &exclude);
        let remark_idx = classify_remark(&cols, &exclude);
        if name_idx.is_none() && remark_idx.is_none() {
            continue;
        }
        // Drift-named tables are only trusted when their name column
        // actually carries the rename-message-derived group names.
        if is_drift && !name_agrees(conn, cand, key_idx, name_idx, known) {
            tracing::debug!("[names] {cand}: name column fails agreement check, skipping");
            continue;
        }
        fill_map(conn, cand, key_idx, name_idx, &mut maps.group_name);
        fill_map(conn, cand, key_idx, remark_idx, &mut maps.group_remark);
        tracing::debug!(
            "[names] {cand}: group_name={} group_remark={}",
            maps.group_name.len(),
            maps.group_remark.len()
        );
        return true;
    }
    false
}

/// True when rows of `table` correlate (group id -> name) with the
/// rename-message-derived group names — the arbitration that separates
/// group-info tables from message/member tables. Without any rename-derived
/// names to verify against, drift-named tables are skipped.
fn name_agrees(
    conn: &Connection,
    table: &str,
    key_idx: usize,
    name_idx: Option<usize>,
    known: &KnownKeys,
) -> bool {
    if known.group_names.is_empty() {
        return false;
    }
    let Some(name_idx) = name_idx else { return false };
    let Ok(mut stmt) = conn.prepare(&format!("SELECT * FROM \"{table}\" LIMIT 1000")) else {
        return false;
    };
    let Ok(rows) = stmt.query_map([], |row| {
        let key = row
            .get_ref(key_idx)
            .ok()
            .and_then(value_str)
            .unwrap_or_default();
        let val = row
            .get_ref(name_idx)
            .ok()
            .and_then(value_str)
            .unwrap_or_default();
        Ok((key, val))
    }) else {
        return false;
    };
    rows.flatten()
        .any(|(k, v)| known.group_names.get(&k).is_some_and(|exp| *exp == v))
}

/// Open a sibling database with the same key, trying the 1024-byte-header
/// layout first (QQ's client stack) then plain (headerless).
fn open_sibling(path: &Path, key: &str) -> Option<Connection> {
    match open_live_mode(path, key, true).or_else(|e| {
        tracing::debug!("[names] open {} (with offset) failed: {e:#}", path.display());
        open_live_mode(path, key, false)
    }) {
        Ok(conn) => Some(conn),
        Err(e) => {
            tracing::debug!("[names] open {} (plain) failed: {e:#}", path.display());
            None
        }
    }
}

/// Group maps from sibling databases in `nt_db_dir` (known names first,
/// then a capped scan over other `*.db` files).
fn load_group_maps(nt_db_dir: &Path, key: &str, known: &KnownKeys, maps: &mut NameMaps) {
    if !nt_db_dir.is_dir() {
        return;
    }
    for name in GROUP_FILES {
        let path = nt_db_dir.join(name);
        let Some(conn) = open_sibling(&path, key) else { continue };
        let filled = harvest_group_db(&conn, known, maps);
        drop(conn);
        if filled {
            return;
        }
    }
    let mut extra: Vec<std::path::PathBuf> = std::fs::read_dir(nt_db_dir)
        .ok()
        .into_iter()
        .flatten()
        .filter_map(|e| e.ok().map(|e| e.path()))
        .filter(|p| p.extension().is_some_and(|x| x == "db"))
        .filter(|p| {
            let n = p.file_name().and_then(|n| n.to_str()).unwrap_or("");
            !n.is_empty() && !n.starts_with("nt_msg") && n != "raw.db" && n != "group_info.db"
        })
        .take(FALLBACK_SCAN_CAP)
        .collect();
    extra.sort();
    for path in extra {
        let Some(conn) = open_sibling(&path, key) else { continue };
        let filled = harvest_group_db(&conn, known, maps);
        drop(conn);
        if filled {
            return;
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn mem_conn() -> Connection {
        Connection::open_in_memory().unwrap()
    }

    #[test]
    fn uid_maps_harvest_remark_nick_and_qq() {
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE nt_uid_mapping_table (nt_uid TEXT, remark TEXT, nickname TEXT, qq TEXT);\
             INSERT INTO nt_uid_mapping_table VALUES ('u_a', '张三备注', '张三', '10001');\
             INSERT INTO nt_uid_mapping_table VALUES ('u_b', '', '李四', '10002');",
        )
        .unwrap();
        let mut maps = NameMaps::default();
        load_uid_maps(&conn, &mut maps);
        assert_eq!(maps.uid_remark.get("u_a").map(String::as_str), Some("张三备注"));
        assert_eq!(maps.uid_remark.get("u_b"), None, "empty remark skipped");
        assert_eq!(maps.uid_nick.get("u_a").map(String::as_str), Some("张三"), "nickname col via hint");
        assert_eq!(maps.uid_nick.get("u_b").map(String::as_str), Some("李四"));
        assert_eq!(maps.uid_qq.get("u_a").map(String::as_str), Some("10001"));
    }

    #[test]
    fn mixed_script_nick_20002_classifies_without_cjk() {
        // "Yuchen Ren" / "." are ~0% Han — the 20002 field-id hint must
        // classify them without the CJK-ratio gate (ground-truth shape).
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE profile_info_v2 (\"1000\" TEXT, \"20002\" TEXT, \"1002\" TEXT);\
             INSERT INTO profile_info_v2 VALUES ('u_a', 'Yuchen Ren', '10001');\
             INSERT INTO profile_info_v2 VALUES ('u_b', '.', '10002');",
        )
        .unwrap();
        let mut maps = NameMaps::default();
        let known = KnownKeys::default();
        harvest_uid_profiles(&conn, &known, &mut maps);
        assert_eq!(maps.uid_nick.get("u_a").map(String::as_str), Some("Yuchen Ren"));
        assert_eq!(maps.uid_remark.len(), 0, "no remark column -> stays empty");
        assert_eq!(maps.uid_qq.get("u_a").map(String::as_str), Some("10001"));
    }

    #[test]
    fn missing_or_emptied_tables_yield_empty_maps_without_panic() {
        let conn = mem_conn(); // no tables at all
        let mut maps = NameMaps::default();
        load_uid_maps(&conn, &mut maps);
        assert!(maps.uid_remark.is_empty() && maps.uid_qq.is_empty());

        conn.execute_batch("CREATE TABLE nt_uid_mapping_table (a TEXT, b TEXT);")
            .unwrap();
        let mut maps = NameMaps::default();
        load_uid_maps(&conn, &mut maps); // no u_ values -> no key column
        assert!(maps.uid_remark.is_empty(), "no uid key column -> nothing harvested");
    }

    #[test]
    fn numeric_column_names_degrade_gracefully() {
        // No name hints at all: "20003" is all-digit -> qq via cardinality
        // fallback; remark/nick need hints (CJK-ratio alone is a
        // false-positive magnet), so they stay empty.
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE nt_uid_mapping_table (\"20002\" TEXT, \"20003\" TEXT, \"10002\" TEXT);\
             INSERT INTO nt_uid_mapping_table VALUES ('u_a', '10001', '张三');",
        )
        .unwrap();
        let mut maps = NameMaps::default();
        load_uid_maps(&conn, &mut maps);
        assert_eq!(maps.uid_qq.get("u_a").map(String::as_str), Some("10001"));
        assert!(maps.uid_remark.is_empty(), "no remark hint -> stays empty");
        assert!(maps.uid_nick.is_empty(), "no nick hint -> stays empty");
    }

    #[test]
    fn cjk_question_columns_are_not_remarks() {
        // Regression (ground truth): 加好友验证问题 / 入群问题 columns are
        // CJK and would pass a most-CJK fallback — they must NOT become
        // remark/group_remark entries.
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE group_detail_info_ver1 (\"60001\" TEXT, \"60007\" TEXT, \"60224\" TEXT);\
             INSERT INTO group_detail_info_ver1 VALUES ('10001', '测试群', '您从何处了解到本群？');\
             INSERT INTO group_detail_info_ver1 VALUES ('20002', '第二群', '问题1:为什么要加我？');",
        )
        .unwrap();
        let known = KnownKeys::default();
        let mut maps = NameMaps::default();
        assert!(harvest_group_db(&conn, &known, &mut maps), "key column found");
        assert_eq!(maps.group_name.get("10001").map(String::as_str), Some("测试群"));
        assert_eq!(maps.group_name.get("20002").map(String::as_str), Some("第二群"));
        assert!(maps.group_remark.is_empty(), "question columns are not remarks");
    }

    #[test]
    fn group_key_picked_by_known_overlap() {
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE group_list (id TEXT, name TEXT, remark TEXT);\
             INSERT INTO group_list VALUES ('10001', '测试群', '');\
             INSERT INTO group_list VALUES ('99999', '未知群', '');",
        )
        .unwrap();
        let mut known = KnownKeys {
            uids: HashSet::new(),
            group_ids: HashSet::new(),
            group_names: HashMap::new(),
        };
        known.group_ids.insert("10001".into());
        let mut maps = NameMaps::default();
        assert!(harvest_group_db(&conn, &known, &mut maps), "key column found");
        assert_eq!(maps.group_name.get("10001").map(String::as_str), Some("测试群"));
        assert_eq!(maps.group_name.get("99999").map(String::as_str), Some("未知群"));
        assert!(!maps.group_name.is_empty());
    }

    #[test]
    fn group_harvest_skips_sources_without_group_ids() {
        // A buddy-list style table has no overlap with known group ids and
        // its keys are u_... — must not be treated as a group source.
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE buddy_mapping (uid TEXT, remark TEXT);\
             INSERT INTO buddy_mapping VALUES ('u_a', '张三备注');",
        )
        .unwrap();
        let known = KnownKeys {
            uids: HashSet::from(["u_a".into()]),
            group_ids: HashSet::from(["10001".into()]),
            group_names: HashMap::new(),
        };
        let mut maps = NameMaps::default();
        assert!(!harvest_group_db(&conn, &known, &mut maps), "no group table here");
        assert!(maps.group_name.is_empty());
    }

    #[test]
    fn group_key_falls_back_to_digit_columns_without_overlap() {
        let conn = mem_conn();
        conn.execute_batch(
            "CREATE TABLE group_list (id TEXT, name TEXT);\
             INSERT INTO group_list VALUES ('10001', '测试群');",
        )
        .unwrap();
        let known = KnownKeys {
            uids: HashSet::new(),
            group_ids: HashSet::new(), // no overlap -> digit fallback
            group_names: HashMap::new(),
        };
        let mut maps = NameMaps::default();
        assert!(harvest_group_db(&conn, &known, &mut maps));
        assert_eq!(maps.group_name.get("10001").map(String::as_str), Some("测试群"));
    }

    #[test]
    fn drift_tables_require_name_agreement() {
        let conn = mem_conn();
        // A message table (group id + sender nick) matches the drift filter
        // and overlaps the known group ids — it must NOT become a group
        // source: the "name" column is a member nickname, not the group name.
        conn.execute_batch(
            "CREATE TABLE group_msg_table (\"40021\" TEXT, \"40093\" TEXT);\
             INSERT INTO group_msg_table VALUES ('10001', '张三');",
        )
        .unwrap();
        let known = KnownKeys {
            uids: HashSet::from(["u_a".into()]),
            group_ids: HashSet::from(["10001".into()]),
            group_names: HashMap::from([("10001".into(), "测试群".into())]),
        };
        let mut maps = NameMaps::default();
        assert!(
            !harvest_group_db(&conn, &known, &mut maps),
            "message table must not be a group source"
        );
        assert!(maps.group_name.is_empty());

        // A renamed metadata table whose names AGREE with the rename
        // messages is accepted despite the drift name.
        conn.execute_batch(
            "CREATE TABLE nt_group_info_table (\"40021\" TEXT, \"40093\" TEXT);\
             INSERT INTO nt_group_info_table VALUES ('10001', '测试群');",
        )
        .unwrap();
        let mut maps = NameMaps::default();
        assert!(harvest_group_db(&conn, &known, &mut maps), "agreement passes drift gate");
        assert_eq!(maps.group_name.get("10001").map(String::as_str), Some("测试群"));
    }
}
