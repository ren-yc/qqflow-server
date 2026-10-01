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
//!
//! **句柄判据**（名字能不能作为「出现即可取」的地址下发）：键派生的名字（md5 hex 或 uuid ——
//! 键按内容唯一）恒可以；回落来的 QQ 原名只有**形状是 32 位十六进制**时才当作内容摘要派生。
//! 两者的共同点是「同名即同内容」，而按名取字节是**跨会话**解析的：别的会话里可能躺着同名
//! 异内容的文件，那时下发的地址会 404。所以非摘要/非键名的那些只给 `mediaLocalPath` 与
//! 元数据，不给 `mediaUrl`。
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
    ///
    /// Deliberately never read: [`export_page`] dispatches on
    /// [`crate::parser::types::media_type_str`], which only ever yields
    /// `image` / `voice` / `video`, so no row can be classified as an emoji and
    /// there is nothing for this switch to gate. It exists because the WeFlow
    /// API accepts an `emoji` parameter and dropping it from the struct would
    /// make the handler silently diverge from the contract. Wire it up here if
    /// a later parser learns to emit an `emoji` kind.
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
    /// `/api/v1/media/{file_name}`（根相对，不带凭据）；**只在名字可作句柄时给出**。
    ///
    /// 不可作句柄时是 `None`：那种名字可能在本会话之外还有一个同名异内容的文件，下发的
    /// 地址会 404 —— 而「出现即可取」是本服务的承诺，宁可只给 `local_path`。
    pub url: Option<String>,
    /// Absolute path under the export root.
    pub local_path: String,
    /// 名字是否取自 **store 键**（md5 hex 或 uuid）。键按内容唯一 ⇒ 名字也按内容唯一。
    pub key_named: bool,
    /// 文件名是否**由内容摘要派生**（32 位十六进制 + 可选扩展名）。
    pub digest_named: bool,
    /// 这个名字能不能作为句柄下发：`key_named || digest_named`。
    ///
    /// 二者都保证「同名即同内容」；回落来的 QQ 原名不保证，只作元数据 ——
    /// 「这条有媒体、叫什么」与「字节取得到」是两件事。
    pub handle_ok: bool,
}

/// 名字是否由**内容摘要**派生：32 位十六进制（大小写都算）＋ 可选扩展名。
///
/// 判据是「按名取字节是跨会话解析的」：路由只看名字、不看会话，而别的会话里可能躺着一个
/// 同名但内容不同的文件。摘要派生的名字按内容唯一，所以可以承诺「出现即可取」；平台给的
/// 文件名（QQ 的原始名、语音的服务端序号名）不满足这一点，只作元数据。
pub fn name_is_content_digest(name: &str) -> bool {
    let stem = name.rsplit_once('.').map(|(s, _)| s).unwrap_or(name);
    stem.len() == 32 && stem.bytes().all(|b| b.is_ascii_hexdigit())
}

/// Safe export file name: `<key>.<source ext>` when a store key exists
/// (md5 hex or uuid — unique per content, so two messages with different
/// bytes can never collide on one destination; QQ's original file names
/// are arbitrary and DO collide across messages), else QQ's file name when
/// it is a bare URL-safe name, else the source file name.
/// 返回 `(名字, 是否取自 store 键)`。第二个分量是**句柄判据的输入**：键派生的名字按内容
/// 唯一，回落来的 QQ 原名不保证（见模块头）。
fn export_file_name(m: &MediaInfo, source: &Path) -> (String, bool) {
    // `.` is allowed (extensions), which is what let `..` and `...` through:
    // both are built only from accepted bytes. They are not hypothetical — a
    // dot-only value reaches here whenever QQ's `fileName` is one — and on
    // Windows a trailing dot is stripped, so `x..` normalizes to `x`. Delegate
    // the dot rules to `pathsafe::safe_segment` and keep the URL-charset rule
    // local, since these names also go into a URL path.
    let url_safe = |s: &str| {
        crate::pathsafe::safe_segment(s)
            && s.bytes()
                .all(|b| b.is_ascii_alphanumeric() || matches!(b, b'.' | b'_' | b'-'))
    };
    if let Some(key) = m.key().filter(|k| url_safe(k)) {
        if let Some(ext) = source.extension().and_then(|e| e.to_str()).filter(|e| !e.is_empty() && e.len() <= 8) {
            return (format!("{key}.{ext}"), true);
        }
        return (key.to_string(), true);
    }
    if let Some(name) = m.file_name.as_deref()
        && url_safe(name)
    {
        return (name.to_string(), false);
    }
    let fallback = source
        .file_name()
        .and_then(|n| n.to_str())
        .filter(|n| url_safe(n))
        .unwrap_or("media")
        .to_string();
    (fallback, false)
}

/// Export one media file into `<root>/<talker>/<kind_dir>/<file>`. Returns
/// None when the kind is disabled, the source file is missing (QQ cleared
/// the cache), or the copy failed. Idempotent: an existing destination with
/// the same size is left untouched (mtime preserved) — sound because the
/// destination name embeds the content key, so a same-size destination is
/// the same media, never a different file that shares a name.
///
/// Source resolution: the row's own "45812" first, then `fallback_path` —
/// the registered `store.media` entry that the cache-index fallback rescued
/// at registration (media rows without a 45812 still export, and the same
/// path serves via /api/v1/media/{id}).
pub fn export_media(
    ctx: &ExportContext,
    m: &MediaInfo,
    kind_dir: &str,
    enabled: bool,
    media_root: Option<&Path>,
    fallback_path: Option<&str>,
) -> Option<ExportOut> {
    if !enabled {
        return None;
    }
    let source = m
        .local_path
        .as_deref()
        .and_then(|p| resolve_local_path(p, media_root))
        .or_else(|| fallback_path.and_then(|p| resolve_local_path(p, media_root)))?;
    let (file_name, key_named) = export_file_name(m, &source);
    // `talker` is a caller-supplied query parameter and `file_name` is derived
    // from the database, so both are checked before either becomes a path
    // component. Today a traversal `talker` is stopped one layer up — the store
    // looks conversations up by exact key, so it yields no rows and never
    // reaches here — but that is a property of the caller, not of this writer,
    // and this is the only place in the process that CREATES directories.
    if !crate::pathsafe::safe_segment(&ctx.talker) || !crate::pathsafe::safe_segment(&file_name) {
        tracing::warn!(
            "[media-export] unsafe path component rejected: talker={:?} file={:?}",
            ctx.talker,
            file_name
        );
        return None;
    }
    let dest_dir = ctx.root.join(&ctx.talker).join(kind_dir);
    if std::fs::create_dir_all(&dest_dir).is_err() {
        tracing::debug!("[media-export] create dir failed: {}", dest_dir.display());
        return None;
    }
    let dest = dest_dir.join(&file_name);
    // Containment after the fact, not just filtering before it: the segment
    // checks above are lexical, and on Windows the filesystem gets a say
    // (short names, symlinks, junctions). `create_dir_all` has already run, so
    // the parent exists and canonicalizes.
    if !crate::pathsafe::is_contained(&ctx.root, &dest) {
        tracing::warn!("[media-export] destination escaped export root: {}", dest.display());
        return None;
    }
    // Idempotent: same-size destination is already exported.
    if let (Ok(dm), Ok(sm)) = (dest.metadata(), source.metadata())
        && dm.is_file() && dm.len() == sm.len()
    {
        return Some(out(&file_name, &dest, key_named));
    }
    if std::fs::copy(&source, &dest).is_err() {
        tracing::debug!("[media-export] copy failed: {} -> {}", source.display(), dest.display());
        return None;
    }
    Some(out(&file_name, &dest, key_named))
}

fn out(file_name: &str, dest: &Path, key_named: bool) -> ExportOut {
    let digest_named = name_is_content_digest(file_name);
    // 句柄判据：**键派生**（md5/uuid，按内容唯一）或**形状是内容摘要**（32 位十六进制）。
    // 两者都保证「同名即同内容」，所以按名取字节是确定的。回落来的 QQ 原名不保证 ——
    // 那种名字只给 local_path，不给可按名取字节的 url。
    let handle_ok = key_named || digest_named;
    ExportOut {
        file_name: file_name.to_string(),
        // **相对路径，且不带 token**：token 一旦进了响应体就会出现在客户端日志与
        // 任何转发里，而它本来是只走请求头的凭据；相对路径同时免掉了把服务基址烤进
        // 响应——反代或换端口不会下发失效地址。调用方按自己的基址拼接。
        //
        // **单段形式**：`{id}` 就是导出文件名。三段式（会话/类型/文件名）已删除 ——
        // 它要求调用方知道会话与类型，而这两个信息在导出结果里本来就有，多要一次只是让
        // 调用方多一处可以写错的地方。
        url: handle_ok.then(|| format!("/api/v1/media/{file_name}")),
        local_path: dest.to_string_lossy().into_owned(),
        key_named,
        digest_named,
        handle_ok,
    }
}

/// Export a whole page of messages (the `media=1` / `meiti` path): each
/// media message gets its export fields filled; returns (messages, exported
/// count). Real file IO — run on the blocking pool (`spawn_blocking`) from
/// the async handler, never directly on a tokio worker.
///
/// `media_entries` is the registered `store.media` snapshot: rows whose own
/// "45812" is absent (cache-index-fallback rescues) export from the
/// registered entry instead, so `media=1` and `/api/v1/media/{id}` agree on
/// one source per mediaId.
pub fn export_page(
    ctx: &ExportContext,
    opts: &ExportOptions,
    media_root: Option<&Path>,
    media_entries: &std::collections::HashMap<String, crate::store::MediaEntry>,
    items: Vec<crate::store::query::MessageOut>,
) -> (Vec<crate::store::query::MessageOut>, usize) {
    let mut exported = 0usize;
    let messages: Vec<crate::store::query::MessageOut> = items
        .into_iter()
        .map(|mut m| {
            // 消息行上的类型字符串直接映射到导出子目录与 kind 开关（不需要枚举往返）。
            let (dir, enabled) = match m.r#type.as_deref() {
                Some("image") => ("images", opts.image),
                Some("voice") => ("voices", opts.voice),
                Some("video") => ("videos", opts.video),
                _ => return m,
            };
            if let Some(info) = m.media.as_ref() {
                let fallback = info
                    .key()
                    .and_then(|k| media_entries.get(k))
                    .map(|e| e.local_path.as_str());
                if let Some(out) = export_media(ctx, info, dir, enabled, media_root, fallback) {
                    exported += 1;
                    m.media_file_name = Some(out.file_name);
                    // url 已在导出侧按句柄判据决定给不给（不可作句柄时是 None）。
                    m.media_url = out.url.clone();
                    m.media_local_path = Some(out.local_path);
                    // 「确实写出了本地文件」（上面这个分支就是它）＋「名字可作句柄」：
                    // 两个条件同时成立，这个名字才可以被 ChatLab 面回填成可取句柄。
                    m.media_export_handle_ok = out.handle_ok;
                }
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
            ("aabbccddeeff00112233445566778899.png".to_string(), true),
            "md5 key preferred over QQ's arbitrary file name (collision-safe)"
        );
        let m = media("aabbccddeeff00112233445566778899", Some("a/../evil.png"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("aabbccddeeff00112233445566778899.png".to_string(), true),
            "separator/.. rejected -> key.ext"
        );
        let m = media("aabbccddeeff00112233445566778899", Some("..\\evil"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("aabbccddeeff00112233445566778899.png".to_string(), true),
            "backslash rejected"
        );
        let m = media("aabbccddeeff00112233445566778899", Some("中文名字.png"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("aabbccddeeff00112233445566778899.png".to_string(), true),
            "non-url-safe name rejected"
        );
        let m = media("", Some(""), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("aabb.png".to_string(), false),
            "no key/name -> source file name (**不是**键派生 ⇒ 不作句柄)"
        );
        // No md5/uuid at all: QQ's bare URL-safe name is kept.
        let m = media("", Some("photo.png"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("photo.png".to_string(), false),
            "raw name kept only without a key"
        );
        // uuid 键：形状不是 32 位十六进制，但它**取自 store 键**，所以同样按内容唯一。
        let m = media("0f8fad5b-d9cb-469f-a165-70867728950e", Some("wechat.png"), None);
        assert_eq!(
            export_file_name(&m, &safe),
            ("0f8fad5b-d9cb-469f-a165-70867728950e.png".to_string(), true),
            "uuid key is a key: the name is content-unique even though it is not hex-shaped"
        );
    }

    #[test]
    fn export_copies_and_is_idempotent() {
        let root = temp_dir("copy");
        // The source lives under the account's nt_data root, as it does in
        // production: `resolve_local_path` contains its result to that root, so
        // a fixture that passes `media_root: None` no longer resolves anything.
        let src_dir = root.join("nt_data");
        std::fs::create_dir_all(&src_dir).unwrap();
        let src = src_dir.join("aabb.png");
        std::fs::write(&src, b"fake image bytes").unwrap();
        let ctx = ExportContext {
            root: root.join("out"),
            base_url: "http://127.0.0.1:5032".into(),
            talker: "10001".into(),
        };
        let m = media("aabbccddeeff00112233445566778899", Some("aabb.png"), Some(src.to_str().unwrap()));
        let e = export_media(&ctx, &m, "images", true, Some(&src_dir), None).expect("export");
        assert_eq!(e.file_name, "aabbccddeeff00112233445566778899.png");
        // 根相对路径、不含 token：调用方按自己的基址拼接。名字取自 store 键（md5）⇒ 是句柄。
        let url = e.url.as_deref().expect("md5 键派生的名字可作句柄");
        assert_eq!(url, "/api/v1/media/aabbccddeeff00112233445566778899.png");
        assert!(!url.contains("access_token"), "响应体里不得出现凭据");
        assert!(e.key_named && e.digest_named && e.handle_ok);

        // 没有键 ⇒ 名字回落成 QQ 原名：**不给 url**（同名异内容可能躺在别的会话里），
        // 只给 local_path 与元数据。
        let src_plain = src_dir.join("photo.png");
        std::fs::write(&src_plain, b"no key bytes").unwrap();
        let m_plain = media("", Some("photo.png"), Some(src_plain.to_str().unwrap()));
        let e_plain =
            export_media(&ctx, &m_plain, "images", true, Some(&src_dir), None).expect("export plain");
        assert_eq!(e_plain.file_name, "photo.png");
        assert!(
            e_plain.url.is_none(),
            "非键名、非摘要名不得下发按名取字节的地址：{e_plain:?}"
        );
        assert!(!e_plain.key_named && !e_plain.digest_named && !e_plain.handle_ok);
        assert!(e_plain.local_path.ends_with("photo.png"), "本地路径照给");

        // uuid 键：形状不是 32 位十六进制，但它**取自 store 键** ⇒ 名字同样按内容唯一。
        // 这条是「不要用形状代替来源」的反例：只看形状会把 uuid 名误判成不可作句柄。
        let src_uuid = src_dir.join("u.png");
        std::fs::write(&src_uuid, b"uuid bytes").unwrap();
        let m_uuid = media(
            "0f8fad5b-d9cb-469f-a165-70867728950e",
            Some("u.png"),
            Some(src_uuid.to_str().unwrap()),
        );
        let e_uuid =
            export_media(&ctx, &m_uuid, "images", true, Some(&src_dir), None).expect("export uuid");
        assert_eq!(e_uuid.file_name, "0f8fad5b-d9cb-469f-a165-70867728950e.png");
        assert!(
            e_uuid.key_named && !e_uuid.digest_named && e_uuid.handle_ok,
            "uuid 键派生的名字可作句柄：{e_uuid:?}"
        );
        assert!(e_uuid.url.is_some());
        let dest = Path::new(&e.local_path);
        assert_eq!(std::fs::read(dest).unwrap(), b"fake image bytes");
        let mtime = dest.metadata().unwrap().modified().unwrap();
        // Second export: same key + same size -> skip, mtime untouched.
        let e2 = export_media(&ctx, &m, "images", true, Some(&src_dir), None).expect("idempotent");
        assert_eq!(e2.local_path, e.local_path);
        assert_eq!(dest.metadata().unwrap().modified().unwrap(), mtime, "same-size skip preserves mtime");
        // 判据本身：32 位十六进制（大小写都算）＋ 可选扩展名。
        assert!(name_is_content_digest("aabbccddeeff00112233445566778899.png"));
        assert!(name_is_content_digest("AABBCCDDEEFF00112233445566778899"));
        assert!(!name_is_content_digest("photo.png"), "平台给的原名不派生自内容");
        assert!(!name_is_content_digest("voice_123.silk"));
        assert!(!name_is_content_digest("aabbccddeeff0011223344556677889.jpg"));

        // Different content, same QQ file name: never the same destination
        // (the md5 key differs -> distinct file, no overwrite).
        let src2 = src_dir.join("b.png");
        std::fs::write(&src2, b"different bytes, same claimed name").unwrap();
        let m2 = media("ffeeddccbbaa99887766554433221100", Some("aabb.png"), Some(src2.to_str().unwrap()));
        let e3 = export_media(&ctx, &m2, "images", true, Some(&src_dir), None).expect("export2");
        assert_ne!(e3.file_name, e.file_name, "same QQ name, different md5 -> different file");
        assert_eq!(e3.file_name, "ffeeddccbbaa99887766554433221100.png");
    }

    #[test]
    fn export_omits_missing_source_and_disabled_kind() {
        let root = temp_dir("omit");
        let nt_data = root.join("nt_data");
        std::fs::create_dir_all(&nt_data).unwrap();
        let ctx = ExportContext {
            root: root.join("out"),
            base_url: "http://127.0.0.1:5032".into(),
            talker: "10001".into(),
        };
        let m = media("aabbccddeeff00112233445566778899", Some("gone.png"), Some("C:\\SomeUser\\gone.png"));
        assert!(
            export_media(&ctx, &m, "images", true, Some(&nt_data), None).is_none(),
            "missing source -> None"
        );
        let src = nt_data.join("x.png");
        std::fs::write(&src, b"x").unwrap();
        let m = media("aabbccddeeff00112233445566778899", Some("x.png"), Some(src.to_str().unwrap()));
        assert!(
            export_media(&ctx, &m, "images", false, Some(&nt_data), None).is_none(),
            "disabled kind -> None"
        );

        // Fallback source: the row has no 45812, but the registered store
        // entry points at a live file — export must resolve through it.
        let fb = nt_data.join("fallback.jpg");
        std::fs::write(&fb, b"fb bytes").unwrap();
        let m = media("aabbccddeeff00112233445566778899", Some("a.jpg"), None);
        let e = export_media(&ctx, &m, "images", true, Some(&nt_data), Some(fb.to_str().unwrap()))
            .expect("fallback source exports");
        assert_eq!(std::fs::read(Path::new(&e.local_path)).unwrap(), b"fb bytes");

        // A source outside nt_data is no longer exportable even though it
        // exists — the trust-boundary change, asserted where it is observable.
        let outside = root.join("outside.jpg");
        std::fs::write(&outside, b"nope").unwrap();
        let m = media("aabbccddeeff00112233445566778899", Some("b.jpg"), None);
        assert!(
            export_media(&ctx, &m, "images", true, Some(&nt_data), Some(outside.to_str().unwrap())).is_none(),
            "source outside media_root must not export"
        );
    }

    /// A traversal `talker` must not create anything outside the export root.
    /// The store stops such a request one layer earlier (conversations are
    /// looked up by exact key, so it matches nothing), which is why this is
    /// asserted here rather than through the handler: the writer has to hold
    /// the line on its own.
    #[test]
    fn export_rejects_traversal_talker() {
        let root = temp_dir("talker");
        let nt_data = root.join("nt_data");
        std::fs::create_dir_all(&nt_data).unwrap();
        let src = nt_data.join("aabb.png");
        std::fs::write(&src, b"bytes").unwrap();
        let out_root = root.join("out");
        std::fs::create_dir_all(&out_root).unwrap();

        let m = media("aabbccddeeff00112233445566778899", Some("aabb.png"), Some(src.to_str().unwrap()));
        for talker in ["..", "../../pwned", r"..\..\pwned", "a:b", "10001.", "", "."] {
            let ctx = ExportContext {
                root: out_root.clone(),
                base_url: "http://127.0.0.1:5032".into(),
                talker: talker.to_string(),
            };
            assert!(
                export_media(&ctx, &m, "images", true, Some(&nt_data), None).is_none(),
                "talker {talker:?} must not export"
            );
        }
        // Nothing escaped: the export root holds no sibling the loop created.
        assert!(!root.join("pwned").exists(), "traversal created a directory outside the export root");
        // ...and the legitimate case still works, so the guard is not just
        // rejecting everything.
        let ctx = ExportContext {
            root: out_root.clone(),
            base_url: "http://127.0.0.1:5032".into(),
            talker: "10001".into(),
        };
        assert!(export_media(&ctx, &m, "images", true, Some(&nt_data), None).is_some());
    }
}
