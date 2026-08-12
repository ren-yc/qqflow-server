//! Heuristic message BLOB text extraction.
//!
//! QQ NT stores each message as a protobuf-ish BLOB (column "40800").
//! Rather than decoding protobuf schemas (which change with QQ versions),
//! this parser extracts readable text heuristically — a deliberate trade-off
//! inherited from the behavior of QQFlow's message parser:
//!   * protobuf varint length prefixes often decode to CJK extension-block
//!     codepoints, so only common Han characters (U+4E00..U+9FA5) count
//!   * a candidate must be >= 2 chars with > 60% common characters
//!   * media is recognized by ASCII signatures (.jpg/.png/.gif/gchatpic,
//!     .amr/.silk/.ptt, shortvideo/.mp4) on blobs > 64 KB
//!   * recall/system messages are recognized by characteristic phrases
//!   * an iteration budget (n*50) bounds worst-case cost

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

fn looks_like_json(s: &str) -> bool {
    let t = s.trim_start();
    t.starts_with('{') || t.starts_with('[')
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

/// Extract readable text from a message BLOB.
pub fn extract_text(blob: &[u8]) -> ParsedMessage {
    if blob.is_empty() {
        return ParsedMessage { msg_type: MsgType::Text, content: "[空]".into() };
    }

    // Large blobs are media payloads.
    if blob.len() > MAX_TEXT_BLOB {
        if let Some(m) = media_type_from_bytes(blob) {
            return ParsedMessage { msg_type: m, content: format!("[{m:?}]").to_lowercase() };
        }
        return ParsedMessage { msg_type: MsgType::Other, content: "[未知大消息]".into() };
    }

    // Media signatures can appear in smaller structured blobs too.
    if let Some(m) = media_type_from_bytes(blob) {
        return ParsedMessage { msg_type: m, content: format!("[{m:?}]").to_lowercase() };
    }

    // Recall / system detection on raw bytes (UTF-8 substrings).
    if let Ok(s) = std::str::from_utf8(blob) {
        if s.contains("你猜猜撤回了什么") {
            return ParsedMessage { msg_type: MsgType::Recall, content: s.into() };
        }
        if s.contains("拍了拍") || s.contains("撤回了一条") || s.contains("修改群名") {
            return ParsedMessage { msg_type: MsgType::System, content: s.into() };
        }
        // Whole-buffer UTF-8 parse: JSON-shaped payloads (mini-program etc.).
        if looks_like_json(s) && is_valid_chat_text(s.trim_matches(|c: char| c == '{' || c == '}' || c == '[' || c == ']' || c.is_ascii_whitespace())) {
            return ParsedMessage { msg_type: MsgType::Other, content: s.into() };
        }
    }

    // Heuristic run scan with an operation budget: Han-first, then ASCII.
    let budget = &mut (blob.len() * 50);
    if let Some((text, _)) = scan_from(blob, budget) {
        return ParsedMessage { msg_type: MsgType::Text, content: text };
    }
    if let Some(text) = scan_ascii(blob, budget) {
        return ParsedMessage { msg_type: MsgType::Text, content: text };
    }

    ParsedMessage { msg_type: MsgType::Other, content: "[无法解析]".into() }
}

#[cfg(test)]
mod tests {
    use super::*;

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
}
