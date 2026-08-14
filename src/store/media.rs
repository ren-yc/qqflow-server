//! Store-layer media path resolution (shared by the index build, the sync
//! apply phase, the media endpoints and the export module).
//!
//! QQ's layout is `<root>/<qq>/nt_qq/nt_db/nt_msg.db`; relative "45812"
//! local cache paths resolve against `<root>/<qq>/nt_qq/nt_data`. Both the
//! index registration and the serving endpoints must agree on one rule, so
//! it lives here instead of being re-derived at each call site.

use std::path::{Path, PathBuf};

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
        std::fs::create_dir_all(&media_root.join("Pic")).unwrap();
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
}
