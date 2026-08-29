//! Containment rules for filesystem path components built from untrusted input.
//!
//! Ported from weflow-server, which had the same class of bug in a worse place.
//! Four surfaces here turn caller- or database-supplied strings into path
//! components: the media export writer ([`crate::store::media_export`]), the
//! exported-media route ([`crate::server::handlers::media`]), the deregister
//! purge (`server::purge_exported_media`), and the local cache resolver
//! ([`crate::store::media::resolve_local_path`]). They shared no validation, so
//! each had drifted to its own subset of checks — and the export writer, the
//! only one that CREATES directories, had none at all. This module is the
//! single semantics.
//!
//! Why rejecting `.` / `..` / separators is NOT sufficient on Windows, which is
//! this service's primary platform (measured, not assumed):
//!
//! - Win32 strips **trailing dots and spaces** from a path component, so a
//!   name like `q-..` does not mean "parent" — it normalizes to a literal
//!   directory `q-`, consuming one level downward. Prefixing untrusted input
//!   therefore buys nothing; any component ending in `.` or ` ` must go.
//! - Normalization is **lexical, in user space** (`GetFullPathName`, reached
//!   through `CreateFileW`), so intermediate directories need not exist for
//!   `..` to collapse. A traversal that fails with `ENOENT` on Unix still
//!   resolves here.
//! - A component containing `:` opens an NTFS alternate data stream
//!   (`name.jpg:hidden`) or names a drive, and carries no separator, so a
//!   separator-only filter lets it through. Worse, [`Path::join`] treats it as
//!   a fresh prefix and DISCARDS the root: `root.join("a:b")` is `"a:b"`,
//!   relative to the process CWD rather than to `root`.
//!
//! The rule this module enforces, and which callers should follow: derive a
//! safe name, then *assert containment* against the canonicalized root.
//! Filtering the input alone is what let weflow's SNS export escape.

use std::path::Path;

/// Longest single path component accepted. Real names here are md5 hex or a
/// uuid plus an extension (`<32 hex>.jpg`), a QQ number, or `<id>@chatroom`,
/// all far below this; the bound keeps a hostile name from pushing the joined
/// path past the platform limit and turning containment into an IO error.
const MAX_SEGMENT: usize = 128;

/// True when `s` is safe to use as exactly one path component.
///
/// Rejects: empty, `.`, `..`, anything holding `/` `\` or `:`, control
/// characters (a NUL truncates the path at the syscall boundary), a trailing
/// dot or space (see the module docs — the Windows-specific one naive filters
/// miss), and anything past [`MAX_SEGMENT`].
pub fn safe_segment(s: &str) -> bool {
    !s.is_empty()
        && s.len() <= MAX_SEGMENT
        && s != "."
        && s != ".."
        && !s.contains('/')
        && !s.contains('\\')
        && !s.contains(':')
        && !s.contains(|c: char| c.is_control())
        && !s.ends_with('.')
        && !s.ends_with(' ')
}

/// Assert that `path` really resolves inside `root`, as the last line of
/// defense after a name was derived.
///
/// Both sides are canonicalized so symlinks, `8.3` short names and the `\\?\`
/// verbatim prefix cannot produce a false match. `path` itself usually does not
/// exist yet (it is about to be written), so the check is on its parent — which
/// is also the component a traversal has to move, making it the right anchor.
///
/// Fails closed: an unresolvable root or parent returns false rather than
/// falling back to a lexical comparison, because comparing a verbatim-prefixed
/// canonical path against a raw one silently never matches.
pub fn is_contained(root: &Path, path: &Path) -> bool {
    let Some(parent) = path.parent() else {
        return false;
    };
    let (Ok(root), Ok(parent)) = (root.canonicalize(), parent.canonicalize()) else {
        return false;
    };
    parent.starts_with(&root)
}

/// Assert that an already-canonicalized `path` is inside `root`, for the case
/// where the target exists and was resolved by the caller.
///
/// Distinct from [`is_contained`]: that one anchors on the parent because the
/// destination is about to be created; this one checks the resolved file
/// itself. Used by [`crate::store::media::resolve_local_path`], where
/// `canonicalize` has already succeeded and the question is only whether the
/// result stayed in bounds.
pub fn is_contained_resolved(root: &Path, path: &Path) -> bool {
    let Ok(root) = root.canonicalize() else {
        return false;
    };
    path.starts_with(&root)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn segment_rejects_traversal_and_windows_quirks() {
        // ordinary names this service actually produces
        assert!(safe_segment("aabbccddeeff00112233445566778899.jpg"));
        assert!(safe_segment("10001"), "a QQ number is one segment");
        assert!(safe_segment("12345678@chatroom"));
        assert!(safe_segment("中文名字.png"), "non-ascii is fine, it carries no separator");

        // classic traversal
        assert!(!safe_segment(""));
        assert!(!safe_segment("."));
        assert!(!safe_segment(".."));
        assert!(!safe_segment("../evil"));
        assert!(!safe_segment("..\\evil"));
        assert!(!safe_segment("a/b"));
        assert!(!safe_segment("a\\b"));

        // Windows-specific: trailing dot/space are stripped by Win32, so these
        // normalize to a shorter name and can consume or move a level.
        assert!(!safe_segment("q-.."), "trailing dots make this a level move");
        assert!(!safe_segment("evil."));
        assert!(!safe_segment("evil "));
        assert!(!safe_segment("..."), "resolves to the directory itself");

        // NTFS alternate data stream / drive-relative, no separator present.
        // `Path::join` also discards the root for these, see the module docs.
        assert!(!safe_segment("name.jpg:hidden"));
        assert!(!safe_segment("C:"));
        assert!(!safe_segment("::NTOSFull::D:\\x.jpg"), "QQ's own marker is not a segment");

        // control characters truncate at the syscall boundary
        assert!(!safe_segment("evil\0.jpg"));
        assert!(!safe_segment("evil\n.jpg"));

        assert!(!safe_segment(&"a".repeat(MAX_SEGMENT + 1)));
        assert!(safe_segment(&"a".repeat(MAX_SEGMENT)));
    }

    fn tmp_root(name: &str) -> std::path::PathBuf {
        let root = Path::new(env!("CARGO_MANIFEST_DIR"))
            .join("target")
            .join("test-tmp")
            .join(format!("pathsafe-{name}-{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&root);
        root
    }

    #[test]
    fn containment_holds_and_fails_closed() {
        let root = tmp_root("contained");
        std::fs::create_dir_all(root.join("exports")).unwrap();
        let exports = root.join("exports");

        let inside = exports.join("aabbccdd.jpg");
        assert!(is_contained(&exports, &inside));

        // the traversal the export writer used to allow through `talker`
        let escaped = exports.join(r"..\..\outside").join("images").join("x.jpg");
        assert!(!is_contained(&exports, &escaped));

        // unresolvable parent -> false, never a lexical fallback
        assert!(!is_contained(&exports, &exports.join("nope").join("x.jpg")));

        let _ = std::fs::remove_dir_all(&root);
    }

    #[test]
    fn resolved_containment_matches_canonical_root() {
        let root = tmp_root("resolved");
        std::fs::create_dir_all(root.join("nt_data").join("Pic")).unwrap();
        let nt_data = root.join("nt_data");
        let f = nt_data.join("Pic").join("x.png");
        std::fs::write(&f, b"x").unwrap();

        // A canonical path (verbatim-prefixed on Windows) must still match a
        // raw root — this is the comparison that silently never matches when
        // the root is not canonicalized first.
        let canon = f.canonicalize().unwrap();
        assert!(is_contained_resolved(&nt_data, &canon));

        let outside = root.join("outside.png");
        std::fs::write(&outside, b"x").unwrap();
        assert!(!is_contained_resolved(&nt_data, &outside.canonicalize().unwrap()));

        let _ = std::fs::remove_dir_all(&root);
    }
}
