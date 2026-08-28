//! Store-layer media path resolution (shared by the index build, the sync
//! apply phase, the media endpoints and the export module).
//!
//! QQ's layout is `<root>/<qq>/nt_qq/nt_db/nt_msg.db`; relative "45812"
//! local cache paths resolve against `<root>/<qq>/nt_qq/nt_data`. Both the
//! index registration and the serving endpoints must agree on one rule, so
//! it lives here instead of being re-derived at each call site.
//!
//! Filesystem-scan fallback (cache index): real-machine probing shows the
//! "45812" path survives on disk for only ~0.3% of media rows (QQ clears
//! its cache), while ~63% of the dead rows still have a file named by its
//! md5 (or file-name md5) somewhere under nt_data. `scan_cache_index` walks
//! the media directories once per registration; `fallback_candidate` turns
//! a store key back into an absolute cache path, so those rows can still
//! register a mediaId instead of being silently unservable.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use crate::parser::types::{MediaInfo, MsgType};

/// Top-level `nt_data` directories that hold message media. The walk skips
/// avatar/PhotoWall/OnlineStatus/… — real-machine probe: fallback rescues
/// only ever come from these (Pic 610 / Emoji 1717 / Video 4 / Ptt 1).
const MEDIA_SCAN_DIRS: &[&str] = &["Pic", "Ptt", "Video", "Emoji", "File"];

/// Depth and file-count caps for the cache walk (defense against
/// pathological layouts; the real cache is ~9k files / 2.2k dirs and walks
/// in ~230 ms on a debug build).
const MAX_SCAN_DEPTH: usize = 6;
const MAX_SCAN_FILES: usize = 300_000;

/// Snapshot of the account's media cache, stem-keyed. Built once per
/// registration (and refreshed on manual sync); `index::apply_record` only
/// does in-memory lookups against it, keeping the watch-tick sync pass
/// zero-file-IO.
#[derive(Debug, Default)]
pub struct CacheIndex {
    /// Lowercased file stem -> absolute paths (md5 / file-name tiers).
    pub by_stem: HashMap<String, Vec<PathBuf>>,
    /// Alphanumeric-normalized stem -> paths (uuid tier; only stems with
    /// >= 8 alphanumeric characters are indexed).
    pub by_alnum: HashMap<String, Vec<PathBuf>>,
}

fn stem_lower(name: &str) -> String {
    Path::new(name)
        .file_stem()
        .map(|s| s.to_string_lossy().to_ascii_lowercase())
        .unwrap_or_default()
}

fn alnum_lower(s: &str) -> String {
    s.chars()
        .filter(|c| c.is_ascii_alphanumeric())
        .collect::<String>()
        .to_ascii_lowercase()
}

/// 32-hex stem check (the md5-shaped names QQ uses for cache files).
pub(crate) fn is_hex32(s: &str) -> bool {
    s.len() == 32 && s.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Walk the media directories of the account's cache root and index file
/// stems. `None` when the root is missing/unreadable — callers degrade to
/// "no fallback" (the pre-fallback behavior).
pub fn scan_cache_index(root: &Path) -> Option<CacheIndex> {
    if !root.is_dir() {
        return None;
    }
    let mut ci = CacheIndex::default();
    let mut files = 0usize;
    for dir in MEDIA_SCAN_DIRS {
        walk(&root.join(dir), 0, &mut ci, &mut files);
    }
    Some(ci)
}

/// Recursive read-only walk: symlinks are skipped (no cycles, no escapes),
/// directories recurse within the depth cap, regular files register their
/// stems. A failed read_dir just stops that branch — best effort, never
/// panics.
fn walk(dir: &Path, depth: usize, ci: &mut CacheIndex, files: &mut usize) {
    if depth > MAX_SCAN_DEPTH || *files > MAX_SCAN_FILES {
        return;
    }
    let Ok(entries) = std::fs::read_dir(dir) else {
        return;
    };
    for e in entries.flatten() {
        let Ok(ft) = e.file_type() else { continue };
        if ft.is_symlink() {
            continue;
        }
        if ft.is_dir() {
            walk(&e.path(), depth + 1, ci, files);
            continue;
        }
        if !ft.is_file() {
            continue;
        }
        *files += 1;
        let name = e.file_name();
        let stem = stem_lower(&name.to_string_lossy());
        if stem.is_empty() {
            continue;
        }
        ci.by_stem.entry(stem.clone()).or_default().push(e.path());
        let al = alnum_lower(&stem);
        if al.len() >= 8 {
            ci.by_alnum.entry(al).or_default().push(e.path());
        }
    }
}

/// Preferred extensions per media kind (probe-confirmed on a real cache:
/// jpg/png/gif/jpeg/dng images, amr voices, mp4 videos). Extension-less
/// files (downloads in progress under OriTemp) are still allowed — they
/// just score zero here.
pub fn ext_family(msg_type: MsgType) -> &'static [&'static str] {
    match msg_type {
        MsgType::Image => &["jpg", "jpeg", "png", "gif", "bmp", "webp", "dng"],
        MsgType::Voice => &["amr", "silk", "ptt"],
        MsgType::Video => &["mp4", "mov", "flv", "mkv", "webm"],
        _ => &[],
    }
}

/// Best candidate among same-stem files: kind-appropriate extension first,
/// then exact 45405 size, then newest mtime, then the lexicographically
/// smallest path (deterministic). Candidates whose metadata already fails
/// are dropped — registering a vanished file would advertise a guaranteed
/// 404.
fn pick_candidate(paths: &[PathBuf], msg_type: MsgType, want_size: Option<i64>) -> Option<&PathBuf> {
    let family = ext_family(msg_type);
    let mut best: Option<(&PathBuf, (i32, i32, i64))> = None;
    for p in paths {
        let Ok(meta) = std::fs::metadata(p) else {
            continue;
        };
        let ext = p
            .extension()
            .map(|e| e.to_string_lossy().to_ascii_lowercase())
            .unwrap_or_default();
        let score = (
            i32::from(family.contains(&ext.as_str())),
            i32::from(want_size.is_some_and(|w| meta.len() as i64 == w)),
            meta.modified()
                .ok()
                .and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok())
                .map(|d| d.as_secs() as i64)
                .unwrap_or(0),
        );
        let better = match best {
            None => true,
            Some((bp, bs)) => score > bs || (score == bs && p.to_string_lossy() < bp.to_string_lossy()),
        };
        if better {
            best = Some((p, score));
        }
    }
    best.map(|(p, _)| p)
}

/// Fallback lookup for media whose own "45812" is absent or dead: a cache
/// file named after the media's identifiers. Tier order (probe-validated on
/// a real database: md5 2186 / file-name md5 146 / uuid 0 rescues):
///   1. the store key itself — md5 hex stem (lowercased), or the
///      alphanumeric-normalized uuid when the key is not md5-shaped
///      (letters required: digit-only uuids are emoji-package ids and
///      never file names);
///   2. the file-name-derived md5 when it differs from the key (QQ names
///      the stored file by the file's own md5, which can differ from 45424
///      — the registered key stays 45424, so mediaId remains stable);
///   3. a non-md5 file-name stem (last resort).
///
/// Registration always uses the original store key — the returned path is
/// what goes into the `MediaEntry`.
pub fn fallback_candidate(
    ci: &CacheIndex,
    m: &MediaInfo,
    key: &str,
    msg_type: MsgType,
) -> Option<PathBuf> {
    if is_hex32(key) {
        if let Some(p) = ci.by_stem.get(key).and_then(|v| pick_candidate(v, msg_type, m.size)) {
            return Some(p.clone());
        }
    } else {
        let al = alnum_lower(key);
        if al.len() >= 8
            && al.bytes().any(|b| b.is_ascii_alphabetic())
            && let Some(p) = ci.by_alnum.get(&al).and_then(|v| pick_candidate(v, msg_type, m.size))
        {
            return Some(p.clone());
        }
    }
    if let Some(fn_md5) = crate::parser::types::md5_from_file_name(m.file_name.as_deref())
        .filter(|f| f != key)
        && let Some(p) = ci.by_stem.get(&fn_md5).and_then(|v| pick_candidate(v, msg_type, m.size))
    {
        return Some(p.clone());
    }
    if let Some(name) = m.file_name.as_deref() {
        let stem = stem_lower(name);
        if !stem.is_empty()
            && !is_hex32(&stem)
            && let Some(p) = ci.by_stem.get(&stem).and_then(|v| pick_candidate(v, msg_type, m.size))
        {
            return Some(p.clone());
        }
    }
    None
}

/// Derive the account's `nt_data` media root from the source `nt_db`
/// directory: `<root>/<qq>/nt_qq/nt_db` -> `<root>/<qq>/nt_qq/nt_data`.
pub fn media_root_of(db_dir: &Path) -> Option<PathBuf> {
    db_dir.parent().map(|p| p.join("nt_data"))
}

/// Resolve a local cache path to an absolute filesystem path: absolute
/// "45812" paths are used as-is (they come from QQ's own DB); relative
/// paths resolve against the account's `nt_data` root, rejecting any `..`
/// component at join time. None when unresolvable or the file is missing.
pub fn resolve_local_path(local_path: &str, media_root: Option<&Path>) -> Option<PathBuf> {
    let raw = Path::new(local_path);
    let joined = if raw.is_absolute() {
        raw.to_path_buf()
    } else {
        let root = media_root?;
        if raw.components().any(|c| matches!(c, std::path::Component::ParentDir)) {
            return None;
        }
        root.join(raw)
    };
    joined.canonicalize().ok()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("qqflow_media_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    #[test]
    fn media_root_of_derives_nt_data_sibling() {
        let dir = temp_dir("root").join("335663881").join("nt_qq").join("nt_db");
        assert_eq!(
            media_root_of(&dir),
            Some(dir.parent().unwrap().join("nt_data"))
        );
    }

    #[test]
    fn resolve_relative_path_under_root_and_rejects_dotdot() {
        let root = temp_dir("resolve");
        let media_root = root.join("nt_data");
        std::fs::create_dir_all(media_root.join("Pic")).unwrap();
        let f = media_root.join("Pic").join("x.png");
        std::fs::write(&f, b"x").unwrap();
        // canonicalize may return a \\?\ verbatim prefix on Windows.
        assert_eq!(
            resolve_local_path("Pic/x.png", Some(&media_root)),
            Some(f.canonicalize().unwrap())
        );
        assert!(resolve_local_path("Pic/../secret", Some(&media_root)).is_none(), ".. rejected");
        assert!(resolve_local_path("missing.png", Some(&media_root)).is_none(), "missing file -> None");
    }

    // ---- cache-index fallback tests -------------------------------------

    fn write_file(root: &Path, rel: &str) -> PathBuf {
        let p = root.join(rel);
        std::fs::create_dir_all(p.parent().unwrap()).unwrap();
        std::fs::write(&p, b"x").unwrap();
        p
    }

    fn mk_media(md5: Option<&str>, name: Option<&str>, uuid: Option<&str>, size: Option<i64>) -> MediaInfo {
        MediaInfo::from(crate::parser::proto::MediaSegment {
            md5_hex: md5.map(String::from),
            file_name: name.map(String::from),
            uuid: uuid.map(String::from),
            size,
            ..Default::default()
        })
    }

    #[test]
    fn scan_indexes_whitelisted_dirs_only() {
        let root = temp_dir("scan");
        let nt = root.join("nt_data");
        let pic = write_file(&nt, "Pic/2026-08/Ori/aabbccddeeff00112233445566778899.jpg");
        write_file(&nt, "avatar/0123456789abcdef.png");
        write_file(&nt, "PhotoWall/x.png");
        let ci = scan_cache_index(&nt).expect("index built");
        let md5_paths = &ci.by_stem["aabbccddeeff00112233445566778899"];
        assert!(
            md5_paths.iter().any(|p| p == &pic),
            "Pic file indexed under its lowercased stem"
        );
        assert!(
            !ci.by_stem.contains_key("0123456789abcdef"),
            "avatar excluded from the media scan"
        );
        assert!(!ci.by_stem.contains_key("x"), "PhotoWall excluded");
    }

    #[test]
    fn scan_normalizes_uppercase_stems() {
        let root = temp_dir("scan2");
        let nt = root.join("nt_data");
        let f = write_file(&nt, "Pic/2026-08/Ori/AABBCCDDEEFF00112233445566778899.jpg");
        let ci = scan_cache_index(&nt).unwrap();
        assert_eq!(ci.by_stem.get("aabbccddeeff00112233445566778899").unwrap(), &vec![f]);
        assert!(
            ci.by_alnum.contains_key("aabbccddeeff00112233445566778899"),
            "alnum map mirrors the md5 stem"
        );
    }

    #[test]
    fn fallback_md5_tier_matches_uppercase_file() {
        let root = temp_dir("fb1");
        let nt = root.join("nt_data");
        let f = write_file(&nt, "Pic/2026-08/Ori/AABBCCDDEEFF00112233445566778899.jpg");
        let ci = scan_cache_index(&nt).unwrap();
        let m = mk_media(
            Some("AABBCCDDEEFF00112233445566778899"),
            Some("other.png"),
            None,
            None,
        );
        let key = m.key().unwrap().to_string();
        let p = fallback_candidate(&ci, &m, &key, MsgType::Image).expect("rescued by md5 tier");
        assert_eq!(p.file_name().unwrap(), f.file_name().unwrap());
    }

    #[test]
    fn fallback_file_name_md5_tier_when_45424_differs() {
        let root = temp_dir("fb2");
        let nt = root.join("nt_data");
        let f = write_file(&nt, "Pic/2026-08/Ori/ffffffffffffffffffffffffffffffff.png");
        let ci = scan_cache_index(&nt).unwrap();
        // 45424 carries md5 A; the cache file is named by the file's own
        // md5 B (the 45402 name) — tier 2 rescues under the key A.
        let m = mk_media(
            Some("aabbccddeeff00112233445566778899"),
            Some("FFFFFFFFFFFFFFFFFFFFFFFFFFFFFFFF.png"),
            None,
            None,
        );
        let key = m.key().unwrap().to_string();
        assert_eq!(key, "aabbccddeeff00112233445566778899", "45424 wins the key");
        let p = fallback_candidate(&ci, &m, &key, MsgType::Image).expect("file-name md5 tier");
        assert_eq!(p.file_name().unwrap(), f.file_name().unwrap());
    }

    #[test]
    fn fallback_no_match_and_uuid_letter_guard() {
        let root = temp_dir("fb3");
        let nt = root.join("nt_data");
        write_file(&nt, "Emoji/emoji-recv/2026-01/Ori/1234567890.png");
        let ci = scan_cache_index(&nt).unwrap();
        // Digit-only uuid (emoji-package id) must never match a file.
        let m = mk_media(None, Some("x.png"), Some("1234567890"), None);
        assert!(fallback_candidate(&ci, &m, m.key().unwrap(), MsgType::Image).is_none());
        // Unrelated md5: no file anywhere -> None.
        let m = mk_media(Some("deadbeefdeadbeefdeadbeefdeadbeef"), Some("photo.png"), None, None);
        assert!(fallback_candidate(&ci, &m, m.key().unwrap(), MsgType::Image).is_none());
    }

    #[test]
    fn fallback_skips_vanished_files() {
        let root = temp_dir("fb4");
        let nt = root.join("nt_data");
        let f = write_file(&nt, "Pic/2026-08/Ori/aabbccddeeff00112233445566778899.jpg");
        let ci = scan_cache_index(&nt).unwrap();
        std::fs::remove_file(&f).unwrap();
        let m = mk_media(Some("aabbccddeeff00112233445566778899"), None, None, None);
        assert!(
            fallback_candidate(&ci, &m, m.key().unwrap(), MsgType::Image).is_none(),
            "vanished file is never registered"
        );
    }

    #[test]
    fn fallback_prefers_exact_size_then_family_ext() {
        let root = temp_dir("fb5");
        let nt = root.join("nt_data");
        // Same stem twice: wrong-size jpg vs exact-size png — the 45405
        // size match must win (family-appropriate extension only breaks
        // size ties).
        let a = write_file(&nt, "Pic/2025-01/Ori/aabbccddeeff00112233445566778899.jpg");
        std::fs::write(&a, vec![0u8; 100]).unwrap();
        let b = write_file(&nt, "Pic/2026-08/Ori/aabbccddeeff00112233445566778899.png");
        std::fs::write(&b, vec![0u8; 12345]).unwrap();
        let ci = scan_cache_index(&nt).unwrap();
        let m = mk_media(Some("aabbccddeeff00112233445566778899"), None, None, Some(12345));
        let p = fallback_candidate(&ci, &m, m.key().unwrap(), MsgType::Image).expect("rescued");
        assert_eq!(p.file_name().unwrap(), "aabbccddeeff00112233445566778899.png");
    }
}
