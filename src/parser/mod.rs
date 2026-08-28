//! Message BLOB parsing: structured wire decode first, heuristic fallback.
//!
//! QQ NT stores each message as a protobuf-ish BLOB (column "40800").
//! `extract_message` tries the spec-confirmed wire layout first
//! (`proto::parse_msg_body` — text from 45101, exact media metadata for
//! image/voice/video), and only falls back to the heuristic parser
//! (`extract_text`) when the blob is not a recognizable structured message.
//! The heuristic path extracts readable text without a schema — a
//! deliberate trade-off inherited from the behavior of QQFlow's parser:
//!   * protobuf varint length prefixes often decode to CJK extension-block
//!     codepoints, so only common Han characters (U+4E00..U+9FA5) count
//!   * a candidate must be >= 2 chars with > 60% common characters
//!   * media is recognized by ASCII signatures (.jpg/.png/.gif/gchatpic,
//!     .amr/.silk/.ptt, shortvideo/.mp4) on blobs > 64 KB
//!   * recall/system messages are recognized by characteristic phrases
//!   * an iteration budget (n*50) bounds worst-case cost

pub mod proto;
pub mod types;

use types::{MsgType, ParsedMessage};

const MAX_TEXT_BLOB: usize = 64 * 1024;
const MIN_COMMON_RATIO: f64 = 0.6;
const MIN_TEXT_LEN: usize = 2;

/// Common Han character check: CJK Unified Ideographs only (not extensions).
fn is_common_han(c: char) -> bool {
    ('\u{4E00}'..='\u{9FA5}').contains(&c)
}

fn common_ratio(s: &str) -> f64 {
    if s.is_empty() {
        return 0.0;
    }
    let total = s.chars().count();
    let common = s.chars().filter(|&c| is_common_han(c)).count();
    common as f64 / total as f64
}

fn is_valid_chat_text(s: &str) -> bool {
    s.chars().count() >= MIN_TEXT_LEN && common_ratio(s) > MIN_COMMON_RATIO
}

/// JSON-structured message check (mini-program / share / card):
/// ≥ 50 chars with > 12% JSON syntax characters.
fn is_json_blob(text: &str) -> bool {
    let total = text.chars().count() as f64;
    if total < 50.0 {
        return false;
    }
    let json_chars = text
        .chars()
        .filter(|c| matches!(c, '{' | '}' | '"' | ':' | '[' | ']'))
        .count() as f64;
    json_chars / total > 0.12
}

/// Extract a single JSON string field value (handles \" escapes).
fn extract_json_field(text: &str, field_name: &str) -> Option<String> {
    let search = format!("\"{}\":\"", field_name);
    let pos = text.find(&search)?;
    let after_key = &text[pos + search.len()..];
    let mut result = String::new();
    let mut in_escape = false;
    for c in after_key.chars() {
        if in_escape {
            result.push(c);
            in_escape = false;
            continue;
        }
        if c == '\\' {
            in_escape = true;
            continue;
        }
        if c == '"' {
            break;
        }
        result.push(c);
    }
    let cleaned = result
        .replace("\\/", "/")
        .replace("\\n", " ")
        .replace("\\t", " ")
        .trim()
        .to_string();
    if cleaned.is_empty() {
        None
    } else {
        Some(cleaned)
    }
}

/// Try to extract human-readable content from a JSON-structured message
/// (mini-program / share / card). Returns None when not JSON-structured.
/// Behavior matches the reference implementation: prompt/desc/title/nick
/// joined with " | " ("来自: " prefix for nick), fallback "[小程序/分享]".
fn extract_json_blob(text: &str) -> Option<String> {
    if !is_json_blob(text) {
        return None;
    }
    let mut parts: Vec<String> = Vec::new();
    // prompt is most common for shares/mini-programs; strip QQ's "[小程序]" etc. prefixes.
    if let Some(p) = extract_json_field(text, "prompt") {
        // QQ prefixes share prompts with bracketed labels like "[小程序]";
        // strip only a leading bracket group, never real text.
        let cleaned = match p.find(']') {
            Some(idx) if p.starts_with('[') => p[idx + 1..].trim_start().to_string(),
            _ => p,
        };
        if !cleaned.is_empty() {
            parts.push(cleaned);
        }
    }
    if let Some(d) = extract_json_field(text, "desc")
        && !parts.iter().any(|p| p == &d)
    {
        parts.push(d);
    }
    if let Some(t) = extract_json_field(text, "title")
        && !parts.iter().any(|p| p == &t)
    {
        parts.push(t);
    }
    if let Some(n) = extract_json_field(text, "nick") {
        let nick_text = format!("来自: {n}");
        if !parts.iter().any(|p| p.contains(&nick_text)) {
            parts.push(nick_text);
        }
    }
    Some(if parts.is_empty() {
        "[小程序/分享]".to_string()
    } else {
        parts.join(" | ")
    })
}

fn media_type_from_bytes(blob: &[u8]) -> Option<MsgType> {
    let lower = blob.to_ascii_lowercase();
    let has = |pat: &[u8]| {
        lower.windows(pat.len()).any(|w| w == pat)
    };
    if has(b".jpg") || has(b".png") || has(b".gif") || has(b"gchatpic") {
        return Some(MsgType::Image);
    }
    if has(b".amr") || has(b".silk") || has(b".ptt") {
        return Some(MsgType::Voice);
    }
    if has(b"shortvideo") || has(b".mp4") {
        return Some(MsgType::Video);
    }
    None
}

/// Find the longest contiguous run of characters that passes the validity
/// check, scanning forward from a valid character.
fn scan_from(blob: &[u8], budget: &mut usize) -> Option<(String, usize)> {
    let text = String::from_utf8_lossy(blob);
    let chars: Vec<char> = text.chars().collect();
    let mut best: Option<(String, usize, usize)> = None; // (text, start, len)
    let mut i = 0;
    while i < chars.len() {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        if !is_common_han(chars[i]) {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < chars.len() {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            let c = chars[j];
            // Allow common Han, ASCII letters/digits/punct, CJK punctuation,
            // and emoji ranges; stop on rare/extended Han (protobuf artifacts).
            let ok = is_common_han(c)
                || c.is_ascii()
                || matches!(c, '\u{3000}'..='\u{303F}' | '\u{FF00}'..='\u{FFEF}' | '\u{1F000}'..='\u{1FAFF}' | '\u{2600}'..='\u{27BF}');
            if !ok {
                break;
            }
            j += 1;
        }
        let candidate: String = chars[i..j].iter().collect();
        if is_valid_chat_text(&candidate) {
            let len = j - i;
            if best.as_ref().is_none_or(|(_, _, bl)| len > *bl) {
                best = Some((candidate, i, len));
            }
        }
        i = j.max(i + 1);
    }
    best.map(|(t, _, _)| (t, 0))
}

/// ASCII fallback scan: longest run of printable ASCII (letters, digits,
/// punctuation, whitespace) with at least two alphanumerics.
fn scan_ascii(blob: &[u8], budget: &mut usize) -> Option<String> {
    let text = String::from_utf8_lossy(blob);
    let chars: Vec<char> = text.chars().collect();
    let mut best = String::new();
    let mut i = 0;
    while i < chars.len() {
        if *budget == 0 {
            break;
        }
        *budget -= 1;
        if !chars[i].is_ascii() {
            i += 1;
            continue;
        }
        let mut j = i;
        while j < chars.len() {
            if *budget == 0 {
                break;
            }
            *budget -= 1;
            let c = chars[j];
            if !(c.is_ascii_alphanumeric() || c.is_ascii_punctuation() || c.is_ascii_whitespace()) {
                break;
            }
            j += 1;
        }
        let cand: String = chars[i..j].iter().collect();
        let trimmed = cand.trim();
        if trimmed.chars().filter(|c| c.is_ascii_alphanumeric()).count() >= MIN_TEXT_LEN
            && trimmed.len() > best.len() {
                best = trimmed.to_string();
            }
        i = j.max(i + 1);
    }
    if best.is_empty() { None } else { Some(best) }
}

/// Parse one message BLOB: structured wire decode first, heuristic
/// fallback. The structured pass only wins for exact matches (a real
/// top-level 40800 field with known content types) — every other blob
/// lands in the unchanged heuristic parser.
pub fn extract_message(blob: &[u8]) -> ParsedMessage {
    extract_structured(blob).unwrap_or_else(|| extract_text(blob))
}

/// Structured 40800 decode. Returns Some only when the blob carries a real
/// top-level 40800 field and at least one known content type: 1/6 text
/// (45101 / 47602 emoji display text), 2 image / 4 voice / 5 video (exact
/// media metadata; first media segment wins — v1 multi-image limitation).
fn extract_structured(blob: &[u8]) -> Option<ParsedMessage> {
    use types::MediaInfo;
    let segments = proto::parse_msg_body(blob);
    let mut text = String::new();
    let mut media: Option<MediaInfo> = None;
    let mut media_type: Option<MsgType> = None;
    let mut saw_known = false;
    for seg in segments {
        match seg.content_type {
            Some(1) | Some(6) => {
                saw_known = true;
                if let Some(t) = seg.text.filter(|t| !t.is_empty()) {
                    if !text.is_empty() {
                        text.push('\n');
                    }
                    text.push_str(&t);
                }
            }
            Some(2) | Some(4) | Some(5) => {
                saw_known = true;
                if media.is_none()
                    && let Some(m) = seg.media
                {
                    // Move the first media segment's fields — no second
                    // copy of the metadata (v1 multi-image limitation).
                    media = Some(MediaInfo::from(m));
                    media_type = match seg.content_type {
                        Some(2) => Some(MsgType::Image),
                        Some(4) => Some(MsgType::Voice),
                        _ => Some(MsgType::Video),
                    };
                }
            }
            _ => {}
        }
    }
    if !saw_known {
        return None; // not a structured message body — heuristic fallback
    }
    let msg_type = media_type.unwrap_or(MsgType::Text);
    let content = if !text.is_empty() {
        text
    } else {
        // Known segment but nothing extractable -> heuristic fallback.
        media_type?.media_placeholder().to_string()
    };
    Some(ParsedMessage { msg_type, content, media })
}

/// Extract readable text from a message BLOB (heuristic — unchanged).
pub fn extract_text(blob: &[u8]) -> ParsedMessage {
    if blob.is_empty() {
        return ParsedMessage::simple(MsgType::Text, "[空]");
    }

    // Large blobs are media payloads.
    if blob.len() > MAX_TEXT_BLOB {
        if let Some(m) = media_type_from_bytes(blob) {
            return ParsedMessage::simple(m, m.media_placeholder());
        }
        return ParsedMessage::simple(MsgType::Other, "[未知大消息]");
    }

    // Media signatures can appear in smaller structured blobs too.
    if let Some(m) = media_type_from_bytes(blob) {
        return ParsedMessage::simple(m, m.media_placeholder());
    }

    // Recall / system detection on raw bytes (UTF-8 substrings).
    if let Ok(s) = std::str::from_utf8(blob) {
        if s.contains("你猜猜撤回了什么") {
            return ParsedMessage::simple(MsgType::Recall, s);
        }
        // "已将群名修改为/修改群名为" are the actual rename-message shapes
        // ("修改群名" alone never appears in real rename messages).
        if s.contains("拍了拍")
            || s.contains("撤回了一条")
            || s.contains("修改群名")
            || s.contains("已将群名修改为")
            || s.contains("修改群名为")
        {
            return ParsedMessage::simple(MsgType::System, s);
        }
        // Structured JSON payloads (mini-program / share / card): extract
        // prompt/desc/title/nick instead of dumping raw JSON.
        if let Some(extracted) = extract_json_blob(s.trim()) {
            return ParsedMessage::simple(MsgType::Other, extracted);
        }
    }

    // Heuristic run scan with an operation budget: Han-first, then ASCII.
    let budget = &mut (blob.len() * 50);
    if let Some((text, _)) = scan_from(blob, budget) {
        return ParsedMessage::simple(MsgType::Text, text);
    }
    if let Some(text) = scan_ascii(blob, budget) {
        return ParsedMessage::simple(MsgType::Text, text);
    }

    ParsedMessage::simple(MsgType::Other, "[无法解析]")
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::proto::tests::{bytes_field, segment as body_of, varint_field};
    use types::{direction_to_is_send, MediaInfo};

    fn text_segment(text: &str) -> Vec<u8> {
        let mut seg = Vec::new();
        varint_field(45002, 1, &mut seg);
        bytes_field(45101, text.as_bytes(), &mut seg);
        body_of(&seg)
    }
    fn image_segment() -> Vec<u8> {
        let mut seg = Vec::new();
        varint_field(45002, 2, &mut seg);
        varint_field(45003, 1, &mut seg);
        bytes_field(45503, b"R020-uuid", &mut seg);
        bytes_field(45402, b"9f2a.png", &mut seg);
        bytes_field(45424, b"9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d", &mut seg);
        varint_field(45405, 152340, &mut seg);
        varint_field(45411, 1080, &mut seg);
        varint_field(45412, 1920, &mut seg);
        bytes_field(45812, b"C:\\SomeUser\\nt_data\\Pic\\2026-08\\9f2a.png", &mut seg);
        body_of(&seg)
    }

    #[test]
    fn plain_text_unchanged_by_structured_pass() {
        // A plain UTF-8 string is not a structured body: the result must be
        // byte-identical to the heuristic parser (no regression).
        for blob in [
            "你好，很高兴认识你".as_bytes().to_vec(),
            b"hello world 12345".to_vec(),
            vec![0xE5, 0x8F, 0x91], // partial UTF-8
            b"".to_vec(),
        ] {
            let a = extract_message(&blob);
            let b = extract_text(&blob);
            assert_eq!(a.msg_type, b.msg_type, "blob {blob:?}");
            assert_eq!(a.content, b.content, "blob {blob:?}");
            assert!(a.media.is_none(), "no structured media on heuristic path");
        }
    }

    #[test]
    fn structured_text_exact() {
        let p = extract_message(&text_segment("结构化正文，含标点！"));
        assert_eq!(p.msg_type, MsgType::Text);
        assert_eq!(p.content, "结构化正文，含标点！");
        assert!(p.media.is_none());
    }

    #[test]
    fn structured_image_exact_metadata() {
        let p = extract_message(&image_segment());
        assert_eq!(p.msg_type, MsgType::Image);
        assert_eq!(p.content, "[image]");
        let m = p.media.expect("image metadata");
        assert_eq!(m.key(), Some("9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d"));
        assert_eq!(m.md5.as_deref(), Some("9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d"));
        assert_eq!(m.uuid.as_deref(), Some("R020-uuid"));
        assert_eq!(m.file_name.as_deref(), Some("9f2a.png"));
        assert_eq!(m.size, Some(152340));
        assert_eq!((m.width, m.height), (Some(1080), Some(1920)));
        assert_eq!(m.local_path.as_deref(), Some("C:\\SomeUser\\nt_data\\Pic\\2026-08\\9f2a.png"));
        assert!(m.urls.is_empty());
    }

    #[test]
    fn mixed_text_and_image_keeps_both() {
        let mut seg1 = Vec::new();
        varint_field(45002, 1, &mut seg1);
        bytes_field(45101, "看这个".as_bytes(), &mut seg1);
        let mut seg2 = Vec::new();
        varint_field(45002, 2, &mut seg2);
        varint_field(45003, 1, &mut seg2);
        bytes_field(45424, b"aabbccddeeff00112233445566778899", &mut seg2);
        let mut body = Vec::new();
        bytes_field(40800, &seg1, &mut body);
        bytes_field(40800, &seg2, &mut body);
        let p = extract_message(&body);
        assert_eq!(p.msg_type, MsgType::Image, "media type wins");
        assert_eq!(p.content, "看这个", "caption kept as content");
        assert_eq!(p.media.as_ref().and_then(MediaInfo::key), Some("aabbccddeeff00112233445566778899"));
    }

    #[test]
    fn unknown_content_type_falls_back_to_heuristic() {
        // Content type 3 (file) / 9 (redbag) / 99 (unknown) are not
        // structured evidence — the heuristic decides.
        for ct in [3u64, 9, 99] {
            let mut seg = Vec::new();
            varint_field(45002, ct, &mut seg);
            bytes_field(45101, "正文".as_bytes(), &mut seg);
            let p = extract_message(&body_of(&seg));
            assert!(p.media.is_none(), "ct={ct}");
            // Heuristic sees raw bytes; must not panic and stays text-ish.
            assert!(matches!(p.msg_type, MsgType::Text | MsgType::Other));
        }
    }

    #[test]
    fn malformed_structured_blob_falls_back() {
        // Truncated varint / group wire types / garbage must never panic.
        for blob in [vec![0xFF, 0xFF, 0xFF], vec![0x80, 0x80], b"not protobuf".to_vec()] {
            let p = extract_message(&blob);
            let h = extract_text(&blob);
            assert_eq!(p.msg_type, h.msg_type, "fallback equals heuristic for {blob:?}");
        }
    }

    #[test]
    fn text_mentioning_image_signature_stays_text() {
        // Regression guard for the heuristic false positive: a structured
        // TEXT message mentioning ".jpg" must stay text — the old
        // ASCII-signature heuristic would classify it as an image.
        let mut seg = Vec::new();
        varint_field(45002, 1, &mut seg);
        bytes_field(45101, "请查收 file.jpg 的修改意见".as_bytes(), &mut seg);
        let p = extract_message(&body_of(&seg));
        assert_eq!(p.msg_type, MsgType::Text);
        assert!(p.content.contains("file.jpg"));
    }

    #[test]
    fn direction_mapping() {
        assert_eq!(direction_to_is_send(0), 0, "other");
        assert_eq!(direction_to_is_send(1), 1, "self");
        assert_eq!(direction_to_is_send(2), 1, "self (variant)");
        assert_eq!(direction_to_is_send(3), 0, "system");
    }


    #[test]
    fn empty_blob() {
        let p = extract_text(b"");
        assert_eq!(p.content, "[空]");
    }

    #[test]
    fn plain_text() {
        let p = extract_text("你好，很高兴认识你".as_bytes());
        assert_eq!(p.msg_type, MsgType::Text);
        assert!(p.content.contains("你好"));
    }

    #[test]
    fn ascii_text() {
        let p = extract_text("hello world 12345".as_bytes());
        assert_eq!(p.msg_type, MsgType::Text);
        assert!(p.content.contains("hello"));
    }

    #[test]
    fn recall_detection() {
        let p = extract_text("xxx撤回了一条消息\n你猜猜撤回了什么".as_bytes());
        assert_eq!(p.msg_type, MsgType::Recall);
    }

    #[test]
    fn media_detection() {
        let mut blob = vec![b'a'; 70_000];
        blob[5000..5008].copy_from_slice(b".jpg.exe");
        let p = extract_text(&blob);
        assert_eq!(p.msg_type, MsgType::Image);
    }

    #[test]
    fn short_garbage_is_other() {
        // A one-char or invalid blob must not produce nonsense text.
        let p = extract_text(&[0xE5, 0x8F, 0x91]); // partial UTF-8
        assert!(matches!(p.msg_type, MsgType::Other | MsgType::Text));
    }

    #[test]
    fn miniapp_json_fields_extracted() {
        let blob = r#"{"appID":"x","prompt":"分享一个链接","desc":"有趣内容","title":"标题"}"#.as_bytes();
        let p = extract_text(blob);
        assert_eq!(p.msg_type, MsgType::Other);
        assert!(p.content.contains("分享一个链接"), "got: {}", p.content);
        assert!(p.content.contains("有趣内容"), "got: {}", p.content);
        assert!(p.content.contains("标题"), "got: {}", p.content);
    }

    #[test]
    fn miniapp_bracket_label_stripped_not_text() {
        // "[小程序]xxx" prefix is stripped, but a plain "分享..." prompt text
        // must NOT be eaten by the label stripping.
        let blob = r#"{"appID":"x","prompt":"[小程序]分享一个链接","desc":"有趣内容"}"#.as_bytes();
        let p = extract_text(blob);
        assert_eq!(p.msg_type, MsgType::Other);
        assert!(p.content.contains("分享一个链接"), "got: {}", p.content);
        assert!(!p.content.contains('['), "got: {}", p.content);
    }

    #[test]
    fn miniapp_json_fallback_label() {
        // JSON-structured but without extractable fields -> generic label.
        let blob = r#"{"type":1,"code":2,"data":{},"x":true,"y":null,"z":12345}"#.as_bytes();
        let p = extract_text(blob);
        assert_eq!(p.msg_type, MsgType::Other);
        assert!(p.content.contains("小程序"), "got: {}", p.content);
    }

    #[test]
    fn rename_message_is_system() {
        // Real QQ rename messages say "已将群名修改为", which must classify
        // as System so the index can extract the new group name.
        let p = extract_text("群主已将群名修改为「测试群」".as_bytes());
        assert_eq!(p.msg_type, MsgType::System);
    }

    #[test]
    fn plain_ascii_is_not_miniapp() {
        // Long plain ASCII has ~0% JSON-syntax chars: must stay text.
        let p = extract_text(b"aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa");
        assert_eq!(p.msg_type, MsgType::Text);
    }
}
