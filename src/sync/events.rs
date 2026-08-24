//! SSE event payloads — field names match the WeFlow push contract:
//! event / sessionId / sessionType / rawid / avatarUrl / sourceName /
//! groupName / content / timestamp. qqflow-server extensions: the `sync`
//! baseline event (lastRowidGroup/lastRowidC2c) and the `media` object
//! (image/voice/video metadata) + `mediaId` on `message.new`. Media paths
//! are deliberately never pushed: the QQ cache path is machine-local and
//! mostly stale, so clients fetch bytes via `GET /api/v1/media/{id}` using
//! the advertised `mediaId` (present only when servable — same rule as the
//! REST `messages.mediaId`). Unknown events are ignored by WeFlow-style
//! clients.

use serde::Serialize;

use crate::parser::types::{ChatType, MediaInfo};

/// Media metadata serialization view pushed over SSE. Deliberately excludes
/// the raw QQ cache path (`MediaInfo.local_path`, field "45812"): it is
/// machine-local, mostly stale on real devices (QQ clears its cache;
/// ground-truth disk presence ~0.3%) and would leak host layout to
/// downstream clients. Fetching goes through `GET /api/v1/media/{id}` using
/// the sibling `mediaId`, which appears only when the store registered a
/// live path (see `store::query::fetchable_media_id` — the same rule as the
/// REST `messages.mediaId`).
#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct PushMedia {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub uuid: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub size: Option<i64>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub width: Option<i32>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub height: Option<i32>,
    #[serde(skip_serializing_if = "Vec::is_empty")]
    pub urls: Vec<String>,
}

impl From<&MediaInfo> for PushMedia {
    fn from(m: &MediaInfo) -> Self {
        Self {
            uuid: m.uuid.clone(),
            md5: m.md5.clone(),
            file_name: m.file_name.clone(),
            size: m.size,
            width: m.width,
            height: m.height,
            urls: m.urls.clone(),
        }
    }
}

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
    /// Structured media metadata for image/voice/video messages — a
    /// serialization view WITHOUT the raw QQ cache path (see [`PushMedia`]).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<PushMedia>,
    /// Fetchable media key (md5 hex or uuid) — present only when the store
    /// has a registered live local path, so `GET /api/v1/media/{id}` serves
    /// bytes (same promise as the REST `messages.mediaId`; the raw key
    /// never rides inside `media`, only this filtered field does).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
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
        media_id: Option<String>,
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
            media: media.as_ref().map(PushMedia::from),
            media_id,
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
            media_id: None,
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
            media_id: None,
            last_rowid_group: Some(watermark_group),
            last_rowid_c2c: Some(watermark_c2c),
        }
    }
}
