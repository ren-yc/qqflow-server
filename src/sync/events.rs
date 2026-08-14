//! SSE event payloads — field names match the WeFlow push contract:
//! event / sessionId / sessionType / rawid / avatarUrl / sourceName /
//! groupName / content / timestamp. `sync` carries two extra fields
//! (lastRowidGroup/lastRowidC2c) as a qqflow-server extension; unknown
//! events are ignored by WeFlow-style clients.

use serde::Serialize;

use crate::parser::types::{ChatType, MediaInfo};

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Event {
    pub event: String,
    pub session_id: String,
    pub session_type: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub group_name: Option<String>,
    pub rawid: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub avatar_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub source_name: Option<String>,
    pub content: String,
    pub timestamp: i64,
    /// Structured media metadata for image/voice/video messages.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaInfo>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rowid_group: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub last_rowid_c2c: Option<i64>,
}

impl Event {
    #[allow(clippy::too_many_arguments)] // one constructor per event kind
    pub fn message_new(
        chat_type: ChatType,
        session_id: String,
        group_name: Option<String>,
        rawid: i64,
        source_name: Option<String>,
        content: String,
        timestamp: i64,
        media: Option<MediaInfo>,
    ) -> Self {
        Self {
            event: "message.new".into(),
            session_id,
            session_type: chat_type.as_str().into(),
            group_name,
            rawid: rawid.to_string(),
            avatar_url: None,
            source_name,
            content,
            timestamp,
            media,
            last_rowid_group: None,
            last_rowid_c2c: None,
        }
    }

    /// Revoke events never carry media (a recall record can only come from
    /// the heuristic phrase path, which produces no structured media).
    pub fn message_revoke(
        chat_type: ChatType,
        session_id: String,
        group_name: Option<String>,
        rawid: i64,
        source_name: Option<String>,
        content: String,
        timestamp: i64,
    ) -> Self {
        Self {
            event: "message.revoke".into(),
            session_id,
            session_type: chat_type.as_str().into(),
            group_name,
            rawid: rawid.to_string(),
            avatar_url: None,
            source_name,
            content,
            timestamp,
            media: None,
            last_rowid_group: None,
            last_rowid_c2c: None,
        }
    }

    /// `sync` baseline event sent on SSE connection open.
    pub fn sync(watermark_group: i64, watermark_c2c: i64, ts: i64) -> Self {
        Self {
            event: "sync".into(),
            session_id: String::new(),
            session_type: String::new(),
            group_name: None,
            rawid: String::new(),
            avatar_url: None,
            source_name: None,
            content: String::new(),
            timestamp: ts,
            media: None,
            last_rowid_group: Some(watermark_group),
            last_rowid_c2c: Some(watermark_c2c),
        }
    }
}
