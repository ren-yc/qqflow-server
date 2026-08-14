//! Minimal protobuf wire-format reader for the "40800" message body.
//!
//! QQ NT stores each message as a protobuf-ish BLOB; the upstream field
//! analysis (QQBackup/nt_msg_db_util `db_docs/40800.md`) pins the layout:
//!
//! ```proto
//! message MsgBody { repeated MsgContent content = 40800; }
//! // MsgContent: 45001 seg id, 45002 content type, 45003 media subtype,
//! // 45101 text body, 47602 emoji display text,
//! // media: 45503 uuid, 45402 file name, 45406 raw 16B md5, 45424 md5 hex,
//! // 45405 size, 45411/45412 width/height, 45419 ext,
//! // 45802/45803/45804 thumb/preview/original CDN urls, 45812 local path
//! ```
//!
//! Deliberately schema-light: a flat two-level scan that skips unknown
//! fields by wire type and never panics — truncated or malformed payloads
//! yield partial results and callers fall back to the heuristic parser.

/// Top-level message-body field: repeated MsgContent.
pub const FIELD_MSG_CONTENT: u64 = 40800;

const FIELD_SEG_ID: u64 = 45001;
const FIELD_CONTENT_TYPE: u64 = 45002;
const FIELD_MEDIA_SUBTYPE: u64 = 45003;
const FIELD_TEXT: u64 = 45101;
const FIELD_EMOJI_ID: u64 = 47601;
const FIELD_EMOJI_TEXT: u64 = 47602;

const FIELD_UUID: u64 = 45503;
const FIELD_FILE_NAME: u64 = 45402;
const FIELD_RAW_MD5: u64 = 45406;
const FIELD_MD5_HEX: u64 = 45424;
const FIELD_SIZE: u64 = 45405;
const FIELD_WIDTH: u64 = 45411;
const FIELD_HEIGHT: u64 = 45412;
const FIELD_URL_THUMB: u64 = 45802;
const FIELD_URL_PREVIEW: u64 = 45803;
const FIELD_URL_ORIGINAL: u64 = 45804;
const FIELD_LOCAL_PATH: u64 = 45812;
const FIELD_CDN_HOST: u64 = 45816;

/// Cursor over a wire-format buffer. All reads are bounds-checked; a
/// malformed or truncated payload yields `None` instead of panicking.
struct Cursor<'a> {
    buf: &'a [u8],
    pos: usize,
}

impl<'a> Cursor<'a> {
    fn new(buf: &'a [u8]) -> Self {
        Self { buf, pos: 0 }
    }

    /// LSB-first base-128 varint (at most 10 bytes).
    fn read_varint(&mut self) -> Option<u64> {
        let mut v: u64 = 0;
        for i in 0..10 {
            let b = *self.buf.get(self.pos)?;
            self.pos += 1;
            v |= ((b & 0x7f) as u64) << (7 * i);
            if b & 0x80 == 0 {
                return Some(v);
            }
        }
        None // 10th byte still continued -> malformed
    }

    /// Read a tag: (field_number, wire_type).
    fn read_tag(&mut self) -> Option<(u64, u64)> {
        let tag = self.read_varint()?;
        Some((tag >> 3, tag & 0x7))
    }

    fn read_bytes(&mut self, len: usize) -> Option<&'a [u8]> {
        let end = self.pos.checked_add(len)?;
        let out = self.buf.get(self.pos..end)?;
        self.pos = end;
        Some(out)
    }

    /// Length-delimited value: varint length prefix + payload.
    fn read_len_bytes(&mut self) -> Option<&'a [u8]> {
        let len = self.read_varint()?;
        self.read_bytes(len as usize)
    }

    /// Skip one value of the given wire type. Wire types 3/4 (groups) are
    /// not supported — `None` makes the caller stop decoding (fallback).
    fn skip(&mut self, wire: u64) -> Option<()> {
        match wire {
            0 => self.read_varint().map(|_| ()),
            1 => self.read_bytes(8).map(|_| ()),
            2 => self.read_len_bytes().map(|_| ()),
            5 => self.read_bytes(4).map(|_| ()),
            _ => None,
        }
    }
}

/// Media metadata extracted from one media segment (content type 2/4/5).
#[derive(Debug, Clone, Default)]
pub struct MediaSegment {
    /// File UUID (45503).
    pub uuid: Option<String>,
    /// File name (45402) — often "md5.ext".
    pub file_name: Option<String>,
    /// 16-byte raw MD5 (45406); kept as bytes, never hex-encoded.
    pub raw_md5: Option<Vec<u8>>,
    /// Image MD5 hex string (45424).
    pub md5_hex: Option<String>,
    /// File size in bytes (45405).
    pub size: Option<i64>,
    /// Image width (45411).
    pub width: Option<i32>,
    /// Image height (45412).
    pub height: Option<i32>,
    /// Local cache path (45812).
    pub local_path: Option<String>,
    /// CDN URLs in field order (45802 thumb / 45803 preview / 45804 original).
    pub urls: Vec<String>,
}

impl MediaSegment {
    /// True when the segment actually carried image/voice/video fields.
    fn has_media(&self) -> bool {
        self.uuid.is_some()
            || self.file_name.is_some()
            || self.raw_md5.is_some()
            || self.md5_hex.is_some()
            || self.size.is_some()
            || self.local_path.is_some()
            || !self.urls.is_empty()
    }
}

/// One decoded MsgContent segment.
#[derive(Debug, Clone, Default)]
pub struct ParsedSegment {
    /// Segment-level ID (45001) — never the table PK "40001".
    pub seg_id: Option<u64>,
    /// Content type (45002): 1 Text / 2 Image / 3 File / 4 Ptt / 5 Video /
    /// 6 QQFace / 7 Reply / 8 GrayTip / 9 RedBag / 10 Ark / 11 MarketFace /
    /// 14 Markdown / 16 LegacyForward / 17 MarkdownButton / 21 LiveRecord.
    pub content_type: Option<u64>,
    /// Media subtype (45003): 1 image / 2 voice / 7 video / 11 file.
    pub media_subtype: Option<u64>,
    /// Text body (45101, UTF-8) with emoji display text (47602) as fallback.
    pub text: Option<String>,
    /// Media metadata when the segment carried media fields.
    pub media: Option<MediaSegment>,
}

fn str_from_bytes(bytes: &[u8]) -> Option<String> {
    String::from_utf8(bytes.to_vec()).ok()
}

/// Decode one MsgContent segment (payload already stripped of its 40800
/// wrapper). Unknown fields are skipped by wire type; a truncated segment
/// keeps whatever decoded so far.
fn decode_segment(bytes: &[u8]) -> ParsedSegment {
    let mut cursor = Cursor::new(bytes);
    let mut out = ParsedSegment::default();
    let mut media = MediaSegment::default();
    let mut text_raw: Option<&[u8]> = None;
    let mut emoji_text: Option<&[u8]> = None;
    while let Some((field, wire)) = cursor.read_tag() {
        match (field, wire) {
            (FIELD_SEG_ID, 0) => out.seg_id = cursor.read_varint(),
            (FIELD_CONTENT_TYPE, 0) => out.content_type = cursor.read_varint(),
            (FIELD_MEDIA_SUBTYPE, 0) => out.media_subtype = cursor.read_varint(),
            (FIELD_TEXT, 2) => text_raw = cursor.read_len_bytes(),
            (FIELD_EMOJI_ID, 0) => {
                let _ = cursor.read_varint();
            }
            (FIELD_EMOJI_TEXT, 2) => emoji_text = cursor.read_len_bytes(),
            (FIELD_UUID, 2) => media.uuid = cursor.read_len_bytes().and_then(str_from_bytes),
            (FIELD_FILE_NAME, 2) => media.file_name = cursor.read_len_bytes().and_then(str_from_bytes),
            (FIELD_RAW_MD5, 2) => media.raw_md5 = cursor.read_len_bytes().map(|b| b.to_vec()),
            (FIELD_MD5_HEX, 2) => media.md5_hex = cursor.read_len_bytes().and_then(str_from_bytes),
            (FIELD_SIZE, 0) => media.size = cursor.read_varint().map(|v| v as i64),
            (FIELD_WIDTH, 0) => media.width = cursor.read_varint().map(|v| v as i32),
            (FIELD_HEIGHT, 0) => media.height = cursor.read_varint().map(|v| v as i32),
            (FIELD_LOCAL_PATH, 2) => media.local_path = cursor.read_len_bytes().and_then(str_from_bytes),
            (FIELD_URL_THUMB | FIELD_URL_PREVIEW | FIELD_URL_ORIGINAL | FIELD_CDN_HOST, 2) => {
                if let Some(u) = cursor.read_len_bytes().and_then(str_from_bytes).filter(|s| !s.is_empty()) {
                    media.urls.push(u);
                }
            }
            _ => {
                if cursor.skip(wire).is_none() {
                    break; // malformed (truncated / unsupported wire type)
                }
            }
        }
    }
    out.text = text_raw
        .and_then(str_from_bytes)
        .filter(|s| !s.is_empty())
        .or_else(|| emoji_text.and_then(str_from_bytes).filter(|s| !s.is_empty()));
    if media.has_media() {
        out.media = Some(media);
    }
    out
}

/// Parse the top-level MsgBody: repeated MsgContent segments on field 40800.
/// The legacy varint-encoded variant (wire 1) is skipped. Malformed input
/// yields whatever decoded cleanly — an empty result means "not a
/// structured message body" and callers fall back to the heuristic parser.
pub fn parse_msg_body(blob: &[u8]) -> Vec<ParsedSegment> {
    let mut out = Vec::new();
    let mut cursor = Cursor::new(blob);
    while let Some((field, wire)) = cursor.read_tag() {
        if field == FIELD_MSG_CONTENT && wire == 2 {
            match cursor.read_len_bytes() {
                Some(seg) => out.push(decode_segment(seg)),
                None => break,
            }
        } else if cursor.skip(wire).is_none() {
            break;
        }
    }
    out
}

#[cfg(test)]
mod tests {
    use super::*;

    // Test-side protobuf encoder (mirrors tests/common, kept local so the
    // wire reader is self-testing).
    fn enc_varint(mut v: u64, out: &mut Vec<u8>) {
        loop {
            let b = (v & 0x7f) as u8;
            v >>= 7;
            if v == 0 {
                out.push(b);
                break;
            }
            out.push(b | 0x80);
        }
    }
    fn enc_field(field: u64, wire: u64, payload: &[u8], out: &mut Vec<u8>) {
        enc_varint((field << 3) | wire, out);
        out.extend_from_slice(payload);
    }
    fn varint_field(field: u64, v: u64, out: &mut Vec<u8>) {
        let mut payload = Vec::new();
        enc_varint(v, &mut payload);
        enc_field(field, 0, &payload, out);
    }
    fn bytes_field(field: u64, bytes: &[u8], out: &mut Vec<u8>) {
        let mut payload = Vec::new();
        enc_varint(bytes.len() as u64, &mut payload);
        payload.extend_from_slice(bytes);
        enc_field(field, 2, &payload, out);
    }
    fn segment(fields: &[u8]) -> Vec<u8> {
        let mut body = Vec::new();
        bytes_field(FIELD_MSG_CONTENT, fields, &mut body);
        body
    }

    #[test]
    fn empty_and_garbage_inputs() {
        assert!(parse_msg_body(b"").is_empty());
        assert!(parse_msg_body(&[0xFF, 0xFF, 0xFF]).is_empty(), "over-long varint");
        assert!(parse_msg_body(b"garbage not protobuf").is_empty());
    }

    #[test]
    fn decodes_text_segment() {
        let mut seg = Vec::new();
        varint_field(FIELD_SEG_ID, 7, &mut seg);
        varint_field(FIELD_CONTENT_TYPE, 1, &mut seg);
        bytes_field(FIELD_TEXT, "你好，世界".as_bytes(), &mut seg);
        let parsed = parse_msg_body(&segment(&seg));
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].seg_id, Some(7));
        assert_eq!(parsed[0].content_type, Some(1));
        assert_eq!(parsed[0].text.as_deref(), Some("你好，世界"));
    }

    #[test]
    fn decodes_image_segment_with_all_fields() {
        let mut seg = Vec::new();
        varint_field(FIELD_CONTENT_TYPE, 2, &mut seg);
        varint_field(FIELD_MEDIA_SUBTYPE, 1, &mut seg);
        bytes_field(FIELD_UUID, b"R020-uuid", &mut seg);
        bytes_field(FIELD_FILE_NAME, b"aabb.png", &mut seg);
        bytes_field(FIELD_RAW_MD5, &[0xaa; 16], &mut seg);
        bytes_field(FIELD_MD5_HEX, b"aabbccddeeff00112233445566778899", &mut seg);
        varint_field(FIELD_SIZE, 12345, &mut seg);
        varint_field(FIELD_WIDTH, 640, &mut seg);
        varint_field(FIELD_HEIGHT, 480, &mut seg);
        bytes_field(FIELD_LOCAL_PATH, b"C:\\nt_data\\Pic\\2026-08\\x.png", &mut seg);
        bytes_field(FIELD_URL_THUMB, b"https://cdn.example/thumb", &mut seg);
        let parsed = parse_msg_body(&segment(&seg));
        let m = parsed[0].media.as_ref().expect("media segment");
        assert_eq!(m.uuid.as_deref(), Some("R020-uuid"));
        assert_eq!(m.file_name.as_deref(), Some("aabb.png"));
        assert_eq!(m.raw_md5.as_deref(), Some(&[0xaa; 16][..]));
        assert_eq!(m.md5_hex.as_deref(), Some("aabbccddeeff00112233445566778899"));
        assert_eq!(m.size, Some(12345));
        assert_eq!((m.width, m.height), (Some(640), Some(480)));
        assert_eq!(m.local_path.as_deref(), Some("C:\\nt_data\\Pic\\2026-08\\x.png"));
        assert_eq!(m.urls, vec!["https://cdn.example/thumb"]);
        assert_eq!(parsed[0].content_type, Some(2));
    }

    #[test]
    fn truncated_tail_keeps_prior_fields() {
        let mut seg = Vec::new();
        varint_field(FIELD_CONTENT_TYPE, 2, &mut seg);
        varint_field(FIELD_SEG_ID, 9, &mut seg);
        bytes_field(FIELD_MD5_HEX, b"ab", &mut seg); // tag + len byte + 2 payload
        // Patch the md5 length prefix to claim 32 bytes while only 2 exist —
        // the segment tail overruns but earlier fields must survive.
        let len_pos = seg.len() - 3; // one byte before the "ab" payload
        seg[len_pos] = 32;
        let parsed = parse_msg_body(&segment(&seg));
        assert!(!parsed.is_empty());
        assert_eq!(parsed[0].seg_id, Some(9), "prior fields survive truncation");
        assert!(parsed[0].media.is_none(), "corrupt tail yields no media");
    }

    #[test]
    fn wire_type_3_or_4_bails_to_partial() {
        // A group-open tag (wire 3) must not panic; decode stops there.
        let mut body = Vec::new();
        enc_field(FIELD_MSG_CONTENT, 3, &[], &mut body); // group start
        assert!(parse_msg_body(&body).is_empty());
    }

    #[test]
    fn unknown_fields_are_skipped() {
        let mut seg = Vec::new();
        varint_field(FIELD_CONTENT_TYPE, 1, &mut seg);
        varint_field(999999, 7, &mut seg); // unknown varint
        bytes_field(888888, b"unknown", &mut seg); // unknown length-delimited
        varint_field(777777, 0, &mut seg); // unknown fixed32
        varint_field(FIELD_SEG_ID, 1, &mut seg);
        bytes_field(FIELD_TEXT, b"after unknowns", &mut seg);
        let parsed = parse_msg_body(&segment(&seg));
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].text.as_deref(), Some("after unknowns"));
    }

    #[test]
    fn multi_segment_preserves_order() {
        let mut seg1 = Vec::new();
        varint_field(FIELD_CONTENT_TYPE, 1, &mut seg1);
        bytes_field(FIELD_TEXT, b"first", &mut seg1);
        let mut seg2 = Vec::new();
        varint_field(FIELD_CONTENT_TYPE, 1, &mut seg2);
        bytes_field(FIELD_TEXT, b"second", &mut seg2);
        let mut body = Vec::new();
        bytes_field(FIELD_MSG_CONTENT, &seg1, &mut body);
        bytes_field(FIELD_MSG_CONTENT, &seg2, &mut body);
        let parsed = parse_msg_body(&body);
        assert_eq!(parsed.len(), 2);
        assert_eq!(parsed[0].text.as_deref(), Some("first"));
        assert_eq!(parsed[1].text.as_deref(), Some("second"));
    }
}
