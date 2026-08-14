//! WeFlow-style on-demand media export.
//!
//! `media=1` on /api/v1/messages copies the page's media files from QQ's
//! local cache into `<exportRoot>/<talker>/<images|voices|videos>/<file>`
//! and reports `mediaFileName` / `mediaUrl` / `mediaLocalPath` per message
//! (WeFlow HTTP-API.md layout). Copy, not hardlink — QQ clears its cache
//! and a hardlink would die with it.
//!
//! File names are derived from the media store key (md5 hex or uuid, plus
//! the source extension): same content -> same destination across pages, so
//! the same-size idempotency check is sound (two messages with the same key
//! are byte-identical; different content can never share a destination
//! name). QQ's original `fileName` is only kept when no key exists.
//! A missing source (cache cleared) or a disabled kind silently yields None
//! — the caller omits the fields.
//!
//! `export_page` performs real file IO and is meant to run on the blocking
//! pool (`spawn_blocking`) from the async handler, never on a tokio worker.

use std::path::{Path, PathBuf};

use crate::parser::types::MediaInfo;
use crate::store::media::resolve_local_path;

/// Which media kinds to export (WeFlow sub-switches image/voice/video/emoji).
#[derive(Debug, Clone, Copy)]
pub struct ExportOptions {
    pub image: bool,
    pub voice: bool,
    pub video: bool,
    /// QQ emoji (content type 6) carry display text only — recognized but
    /// inert in v1 (animated gif images export under `images`).
    pub emoji: bool,
}

impl Default for ExportOptions {
    fn default() -> Self {
        Self { image: true, voice: true, video: true, emoji: true }
    }
}

/// Export root + URL base for one request.
#[derive(Debug, Clone)]
pub struct ExportContext {
    pub root: PathBuf,
    pub base_url: String,
    pub talker: String,
}

/// One successfully exported media file.
#[derive(Debug, Clone)]
pub struct ExportOut {
    pub file_name: String,
    /// `{base_url}/api/v1/media/{talker}/{kind}/{file}`
    pub url: String,
    /// Absolute path under the export root.
    pub local_path: String,
}

/// Safe export file name: `<key>.<source ext>` when a store key exists
/// (md5 hex or uuid — unique per content, so two messages with different
/// bytes can never collide on one destination; QQ's original file names
/// are arbitrary and DO collide across messages), else QQ's file name when
/// it is a bare URL-safe name, else the source file name.
fn export_file_name(m: &MediaInfo, source: &Path) -> String {
    let url_safe = |s: &str| {
        !s.is_empty()
            && s.len() <= 128
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    if let Some(key) = m.key().filter(|k| url_safe(k)) {
        if let Some(ext) = source.extension().and_then(|e| e.to_str()).filter(|e| !e.is_empty() && e.len() <= 8) {
            return format!("{key}.{ext}");
        }
        return key.to_string();
    }
    if let Some(name) = m.file_name.as_deref()
        && url_safe(name)
    {
        return name.to_string();
    }
    source
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| url_safe(n))
        .unwrap_or("media")
        .to_string()
}

/// Export one media file into `<root>/<talker>/<kind_dir>/<file>`. Returns
/// None when the kind is disabled, the source file is missing (QQ cleared
/// the cache), or the copy failed. Idempotent: an existing destination with
/// the same size is left untouched (mtime preserved) — sound because the
/// destination name embeds the content key, so a same-size destination is
/// the same media, never a different file that shares a name.
pub fn export_media(
    ctx: &ExportContext,
    m: &MediaInfo,
    kind_dir: &str,
    enabled: bool,
    media_root: Option<&Path>,
) -> Option<ExportOut> {
    if !enabled {
        return None;
    }
    let source = resolve_local_path(m.local_path.as_deref()?, media_root)?;
    let file_name = export_file_name(m, &source);
    let dest_dir = ctx.root.join(&ctx.talker).join(kind_dir);
    if std::fs::create_dir_all(&dest_dir).is_err() {
        tracing::debug!("[media-export] create dir failed: {}", dest_dir.display());
        return None;
    }
    let dest = dest_dir.join(&file_name);
    // Idempotent: same-size destination is already exported.
    if let (Ok(dm), Ok(sm)) = (dest.metadata(), source.metadata())
        && dm.is_file() && dm.len() == sm.len()
    {
        return Some(out(ctx, &ctx.talker, kind_dir, &file_name, &dest));
    }
    if std::fs::copy(&source, &dest).is_err() {
        tracing::debug!("[media-export] copy failed: {} -> {}", source.display(), dest.display());
        return None;
    }
    Some(out(ctx, &ctx.talker, kind_dir, &file_name, &dest))
}

fn out(ctx: &ExportContext, talker: &str, kind: &str, file_name: &str, dest: &Path) -> ExportOut {
    ExportOut {
        file_name: file_name.to_string(),
        url: format!("{}/api/v1/media/{talker}/{kind}/{file_name}", ctx.base_url),
        local_path: dest.to_string_lossy().into_owned(),
    }
}

/// Export a whole page of messages (the `media=1` / `meiti` path): each
/// media message gets its export fields filled; returns (messages, exported
/// count). Real file IO — run on the blocking pool (`spawn_blocking`) from
/// the async handler, never directly on a tokio worker.
pub fn export_page(
    ctx: &ExportContext,
    opts: &ExportOptions,
    media_root: Option<&Path>,
    items: Vec<crate::store::query::MessageOut>,
) -> (Vec<crate::store::query::MessageOut>, usize) {
    let mut exported = 0usize;
    let messages: Vec<crate::store::query::MessageOut> = items
        .into_iter()
        .map(|mut m| {
            // The WeFlow `mediaType` string maps straight to the export
            // subdirectory + kind switch (no enum round trip needed).
            let (dir, enabled) = match m.media_type.as_deref() {
                Some("image") => ("images", opts.image),
                Some("voice") => ("voices", opts.voice),
                Some("video") => ("videos", opts.video),
                _ => return m,
            };
            if let Some(info) = m.media.as_ref()
                && let Some(out) = export_media(ctx, info, dir, enabled, media_root)
            {
                exported += 1;
                m.media_file_name = Some(out.file_name);
                m.media_url = Some(out.url);
                m.media_local_path = Some(out.local_path);
            }
            m
        })
        .collect();
    (messages, exported)
}

#[cfg(test)]
mod tests {
    use super::*;

    fn temp_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("qqflow_export_{name}_{}", std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn media(md5: &str, name: Option<&str>, path: Option<&str>) -> MediaInfo {
        // Build through the parse-time conversion so the store key is
        // computed exactly like a decoded segment.
        MediaInfo::from(crate::parser::proto::MediaSegment {
            md5_hex: Some(md5.into()),
            file_name: name.map(String::from),
            local_path: path.map(String::from),
            ..Default::default()
        })
    }

    #[test]
    fn file_name_fallback_and_safety() {
        let root = temp_dir("names");
        let safe = root.join("aabb.png");
        std::fs::write(&safe, b"x").unwrap();
        let m = media("aabbccddeeff00112233445566778899", Some("aabb.png"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            "aabbccddeeff00112233445566778899.png",
            "md5 key preferred over QQ's arbitrary file name (collision-safe)"
        );
        let m = media("aabbccddeeff00112233445566778899", Some("a/../evil.png"), None);
        assert_eq!(export_file_name(&m, &safe), "aabbccddeeff00112233445566778899.png", "separator/.. rejected -> key.ext");
        let m = media("aabbccddeeff00112233445566778899", Some("..\\evil"), None);
        assert_eq!(export_file_name(&m, &safe), "aabbccddeeff00112233445566778899.png", "backslash rejected");
        let m = media("aabbccddeeff00112233445566778899", Some("中文名字.png"), None);
        assert_eq!(export_file_name(&m, &safe), "aabbccddeeff00112233445566778899.png", "non-url-safe name rejected");
        let m = media("", Some(""), None);
        assert_eq!(export_file_name(&m, &safe), "aabb.png", "no key/name -> source file name");
        // No md5/uuid at all: QQ's bare URL-safe name is kept.
        let m = media("", Some("photo.png"), None);
        assert_eq!(export_file_name(&m, &safe), "photo.png", "raw name kept only without a key");
    }

    #[test]
    fn export_copies_and_is_idempotent() {
        let root = temp_dir("copy");
        let src_dir = root.join("src");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src = src_dir.join("aabb.png");
        std::fs::write(&src, b"fake image bytes").unwrap();
        let ctx = ExportContext {
            root: root.join("out"),
            base_url: "http://127.0.0.1:5032".into(),
            talker: "10001".into(),
        };
        let m = media("aabbccddeeff00112233445566778899", Some("aabb.png"), Some(src.to_str().unwrap()));
        let e = export_media(&ctx, &m, "images", true, None).expect("export");
        assert_eq!(e.file_name, "aabbccddeeff00112233445566778899.png");
        assert_eq!(
            e.url,
            "http://127.0.0.1:5032/api/v1/media/10001/images/aabbccddeeff00112233445566778899.png"
        );
        let dest = Path::new(&e.local_path);
        assert_eq!(std::fs::read(dest).unwrap(), b"fake image bytes");
        let mtime = dest.metadata().unwrap().modified().unwrap();
        // Second export: same key + same size -> skip, mtime untouched.
        let e2 = export_media(&ctx, &m, "images", true, None).expect("idempotent");
        assert_eq!(e2.local_path, e.local_path);
        assert_eq!(dest.metadata().unwrap().modified().unwrap(), mtime, "same-size skip preserves mtime");
        // Different content, same QQ file name: never the same destination
        // (the md5 key differs -> distinct file, no overwrite).
        let src2 = src_dir.join("b.png");
        std::fs::write(&src2, b"different bytes, same claimed name").unwrap();
        let m2 = media("ffeeddccbbaa99887766554433221100", Some("aabb.png"), Some(src2.to_str().unwrap()));
        let e3 = export_media(&ctx, &m2, "images", true, None).expect("export2");
        assert_ne!(e3.file_name, e.file_name, "same QQ name, different md5 -> different file");
        assert_eq!(e3.file_name, "ffeeddccbbaa99887766554433221100.png");
    }

    #[test]
    fn export_omits_missing_source_and_disabled_kind() {
        let root = temp_dir("omit");
        let ctx = ExportContext {
            root: root.join("out"),
            base_url: "http://127.0.0.1:5032".into(),
            talker: "10001".into(),
        };
        let m = media("aabbccddeeff00112233445566778899", Some("gone.png"), Some("C:\\SomeUser\\gone.png"));
        assert!(export_media(&ctx, &m, "images", true, None).is_none(), "missing source -> None");
        let src = root.join("x.png");
        std::fs::write(&src, b"x").unwrap();
        let m = media("aabbccddeeff00112233445566778899", Some("x.png"), Some(src.to_str().unwrap()));
        assert!(export_media(&ctx, &m, "images", false, None).is_none(), "disabled kind -> None");
    }
}
