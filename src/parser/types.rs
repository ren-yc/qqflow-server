//! Parsed message types.

use serde::Serialize;

/// Message category recognized by the parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MsgType {
    Text,
    Image,
    Voice,
    Video,
    Recall,
    System,
    Other,
}

impl MsgType {
    /// Numeric `localType` used by WeFlow-style clients.
    pub fn code(self) -> i64 {
        match self {
            MsgType::Text => 0,
            MsgType::Image => 3,
            MsgType::Voice => 4,
            MsgType::Video => 5,
            MsgType::Recall => 6,
            MsgType::System => 7,
            MsgType::Other => 1,
        }
    }

    /// WeFlow `mediaType` string for media kinds ("image"/"voice"/"video").
    pub fn media_type_str(self) -> Option<&'static str> {
        match self {
            MsgType::Image => Some("image"),
            MsgType::Voice => Some("voice"),
            MsgType::Video => Some("video"),
            _ => None,
        }
    }

    /// Display placeholder for media messages ("[image]"/"[voice]"/"[video]")
    /// — one spelling shared by the structured and heuristic parser paths.
    pub fn media_placeholder(self) -> &'static str {
        match self {
            MsgType::Image => "[image]",
            MsgType::Voice => "[voice]",
            MsgType::Video => "[video]",
            _ => "[media]",
        }
    }
}

/// Media metadata parsed from a structured message segment (image/voice/
/// video) — field ids per the upstream 40800 analysis (45424 md5 hex,
/// 45405 size, 45411/45412 dims, 45503 uuid, 45812 local cache path, CDN
/// urls 45802/45803/45804). All optional: absent fields stay absent.
#[derive(Debug, Clone, Default, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MediaInfo {
    /// File UUID (45503).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    /// Image MD5 hex string (45424) — the media store lookup key.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    /// File name (45402), often "md5.ext".
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    /// File size in bytes (45405).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    /// Image width (45411).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,
    /// Image height (45412).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,
    /// Local cache path (45812) — served by /api/v1/media/{id}.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub local_path: Option<String>,
    /// CDN URLs (45802 thumb / 45803 preview / 45804 original).
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
    /// Store lookup key — computed once at parse time (see [`MediaInfo::key`]).
    #[serde(skip)]
    pub key: Option<String>,
}

impl MediaInfo {
    /// Store lookup key: md5 hex (45424, or extracted from the "MD5.ext"
    /// file-name shape — real QQ images are named after their uppercase
    /// MD5 while 45424 is often empty), else uuid; None when nothing is
    /// present. Computed once at parse time — every query/export path
    /// reuses the cached value instead of re-deriving it per row.
    ///
    /// md5 keys are normalized to lowercase: 45424 can carry uppercase hex
    /// while the file-name-derived fallback lowercases, and the store
    /// registration / mediaId / export file names must agree on one
    /// spelling for the same image (case mismatches would register the
    /// same media under two keys).
    pub fn key(&self) -> Option<&str> {
        self.key.as_deref()
    }

    fn compute_key(&self) -> Option<String> {
        self.md5
            .clone()
            .filter(|s| !s.is_empty())
            .map(|s| s.to_ascii_lowercase())
            .or_else(|| md5_from_file_name(self.file_name.as_deref()))
            .or_else(|| self.uuid.clone().filter(|s| !s.is_empty()))
    }
}

/// QQ names image files "<UPPERCASE_MD5>.ext" — a 32-hex stem is a usable
/// md5 key when the 45424 field is empty (ground-truth confirmed).
fn md5_from_file_name(name: Option<&str>) -> Option<String> {
    let stem = name?.rsplit_once('.')?.0.trim();
    if stem.len() == 32 && stem.bytes().all(|b| b.is_ascii_hexdigit()) {
        Some(stem.to_ascii_lowercase())
    } else {
        None
    }
}

impl From<crate::parser::proto::MediaSegment> for MediaInfo {
    fn from(seg: crate::parser::proto::MediaSegment) -> Self {
        let mut info = Self {
            uuid: seg.uuid,
            md5: seg.md5_hex,
            file_name: seg.file_name,
            size: seg.size,
            width: seg.width,
            height: seg.height,
            local_path: seg.local_path,
            urls: seg.urls,
            key: None,
        };
        info.key = info.compute_key();
        info
    }
}

/// Result of parsing one message BLOB.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedMessage {
    pub msg_type: MsgType,
    pub content: String,
    /// Structured media metadata (image/voice/video); None for text/system
    /// messages and for blobs the structured parser could not decode.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaInfo>,
}

impl ParsedMessage {
    /// Build a parse result without structured media metadata (heuristic
    /// path and constructors in tests).
    pub fn simple(msg_type: MsgType, content: impl Into<String>) -> Self {
        Self { msg_type, content: content.into(), media: None }
    }
}

/// A single chat record (row from group_msg_table / c2c_msg_table).
#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub rowid: i64,
    /// Column "40001": message seq (high 32 bits carry the unix timestamp).
    pub seq: i64,
    pub ts: i64,
    pub chat_type: ChatType,
    /// Group: group id ("40021"); c2c: peer uid ("40020").
    pub talker: String,
    /// Sender uid ("40020" in group table; c2c peer for c2c table).
    pub from_uid: String,
    /// Sender nickname ("40093" — the global, context-free name; group
    /// cards ("40090") live in `card` and only display inside their group).
    pub from_nick: String,
    /// Sender group card ("40090", group table only) — display scope is the
    /// conversation it appeared in (`Store.group_cards`), never the global
    /// name maps. None for c2c rows and versions without the column.
    pub card: Option<String>,
    /// Raw "40013" direction (0 other / 1,2 self / 3 system); None when the
    /// column is absent in this QQ version (is_send degrades to 0).
    pub direction: Option<i64>,
    pub parsed: ParsedMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChatType {
    #[default]
    Group,
    C2c,
}

impl ChatType {
    /// WeFlow session `type`: 2 = group, 1 = private.
    pub fn weflow_code(self) -> i64 {
        match self {
            ChatType::Group => 2,
            ChatType::C2c => 1,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            ChatType::Group => "group",
            ChatType::C2c => "private",
        }
    }
}

/// QQ NT stores the message time in the high 32 bits of the "40001" seq
/// value (verified against a real database: seq = (ts << 32) | low32).
pub fn seq_to_time(seq: i64) -> i64 {
    seq >> 32
}

/// Map the raw "40013" direction to WeFlow `is_send` (1 = sent by me):
/// 0 (other) -> 0, 1/2 (self) -> 1, 3 (system) and any unknown bitmask
/// value -> 0 (never claim a message is self-sent on unverified semantics).
pub fn direction_to_is_send(d: i64) -> i64 {
    match d {
        1 | 2 => 1,
        _ => 0,
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn media_key_prefers_md5_then_file_name_md5_then_uuid() {
        // Built through the parse-time conversion (From<MediaSegment>) — the
        // cached key is computed exactly like a decoded segment.
        let mk = |md5: Option<String>, name: Option<&str>, uuid: Option<&str>| {
            MediaInfo::from(crate::parser::proto::MediaSegment {
                md5_hex: md5,
                file_name: name.map(String::from),
                uuid: uuid.map(String::from),
                ..Default::default()
            })
        };
        let m = mk(Some("aabbccddeeff00112233445566778899".into()), Some("OTHER.png"), None);
        assert_eq!(m.key(), Some("aabbccddeeff00112233445566778899"));
        // Uppercase 45424 (real QQ data) must normalize to the same key as
        // the file-name-derived fallback — one image, one key.
        let m = mk(Some("41675A034F01EEDEAEC4D93CBFBB4A06".into()), Some("41675A034F01EEDEAEC4D93CBFBB4A06.png"), None);
        assert_eq!(
            m.key(),
            Some("41675a034f01eedeaec4d93cbfbb4a06"),
            "45424 md5 lowercased"
        );
        // Empty 45424 + "MD5.ext" name (ground-truth shape) -> derived key.
        let m = mk(Some(String::new()), Some("41675A034F01EEDEAEC4D93CBFBB4A06.png"), None);
        assert_eq!(m.key(), Some("41675a034f01eedeaec4d93cbfbb4a06"), "file-name md5 fallback, lowercased");
        let m = mk(None, Some("not-a-md5.png"), Some("R020"));
        assert_eq!(m.key(), Some("R020"));
        assert_eq!(MediaInfo::default().key(), None);
    }
}
