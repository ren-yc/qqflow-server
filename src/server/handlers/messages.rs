//! GET|POST /api/v1/messages — query messages of one session.
//!
//! WeFlow contract: `talker` required; `limit` (1..=10000, default 100),
//! `offset`, `start`/`end` (YYYYMMDD or unix seconds), `keyword`,
//! `chatlab`/`format` output switch. The `media` param is accepted for
//! WeFlow compatibility (media rides on every message via `MessageOut.media`
//! and bytes are served by /api/v1/media/{id}); the envelope reports
//! `media.enabled=true` with the page's media count.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::parser::types::MsgType;
use crate::store::media_export::{self, ExportContext, ExportOptions};
use crate::store::query::{query_messages, MessageOut, MessageQuery};
use crate::store::AppState;

use super::{authorized, merge_body, parse_time_bound};
use crate::server::error::ApiError;

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub talker: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    pub start: Option<String>,
    pub end: Option<String>,
    pub keyword: Option<String>,
    #[serde(default)]
    pub chatlab: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    /// WeFlow media export switch (alias: meiti) — `1/true` exports this
    /// page's media and fills mediaFileName/mediaUrl/mediaLocalPath.
    #[serde(default)]
    pub media: Option<String>,
    #[serde(default)]
    pub meiti: Option<String>,
    /// Per-kind export sub-switches (default true; "0"/"false" disables).
    #[serde(default)]
    pub image: Option<String>,
    #[serde(default)]
    pub tupian: Option<String>,
    #[serde(default)]
    pub voice: Option<String>,
    #[serde(default)]
    pub vioce: Option<String>,
    #[serde(default)]
    pub video: Option<String>,
    /// Recognized but inert in v1: QQ emoji carry display text, no files.
    #[serde(default)]
    pub emoji: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
}

fn truthy(v: Option<&str>) -> bool {
    matches!(v, Some("1" | "true"))
}

fn is_falsey(v: Option<&str>) -> bool {
    matches!(v, Some("0" | "false"))
}

fn kind_of(m: &MessageOut) -> Option<MsgType> {
    match m.media_type.as_deref() {
        Some("image") => Some(MsgType::Image),
        Some("voice") => Some(MsgType::Voice),
        Some("video") => Some(MsgType::Video),
        _ => None,
    }
}

fn default_limit() -> usize {
    100
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let talker = params.talker.as_deref().ok_or_else(|| ApiError::bad_request("缺少必填参数 talker"))?;
    let limit = params.limit.clamp(1, 10000);
    let q = MessageQuery {
        talker,
        limit,
        offset: params.offset,
        start: params.start.as_deref().and_then(|s| parse_time_bound(s, false)),
        end: params.end.as_deref().and_then(|s| parse_time_bound(s, true)),
        keyword: params.keyword.as_deref(),
    };
    let (items, has_more) = {
        let store = state.store.read();
        query_messages(&store, &q)
    };

    let chatlab = params.chatlab.as_deref() == Some("1")
        || params.chatlab.as_deref() == Some("true")
        || params.format.as_deref() == Some("chatlab");
    let media_on = truthy(params.media.as_deref()) || truthy(params.meiti.as_deref());

    let body = if chatlab {
        // ChatLab output carries no export envelope (WeFlow parity).
        chatlab_envelope(&state, talker, &items)
    } else if media_on {
        // WeFlow-shaped export: copy this page's media into the export
        // root, fill per-message mediaFileName/mediaUrl/mediaLocalPath.
        export_envelope(&state, talker, has_more, &params, items)
    } else {
        // No media param: capability envelope unchanged (compat) — media
        // metadata still rides on every message.
        let media_count = items.iter().filter(|m| m.media.is_some()).count();
        json!({
            "success": true,
            "talker": talker,
            "count": items.len(),
            "hasMore": has_more,
            "media": { "enabled": true, "exportPath": "", "count": media_count },
            "messages": items,
        })
    };
    Ok(Json(body))
}

/// WeFlow-shaped media export envelope (`media=1` / `meiti`): exports the
/// page's media files into `<exportRoot>/<talker>/<kind>/<file>` and fills
/// the per-message export fields; `count` = successfully exported messages.
/// Missing sources (QQ cleared the cache) are skipped gracefully.
fn export_envelope(state: &AppState, talker: &str, has_more: bool, params: &Params, items: Vec<MessageOut>) -> Value {
    let opts = ExportOptions {
        image: !is_falsey(params.image.as_deref()) && !is_falsey(params.tupian.as_deref()),
        voice: !is_falsey(params.voice.as_deref()) && !is_falsey(params.vioce.as_deref()),
        video: !is_falsey(params.video.as_deref()),
        emoji: !is_falsey(params.emoji.as_deref()),
    };
    let media_root = state.store.read().media_root.clone();
    let ctx = ExportContext {
        root: state.export_root.as_ref().clone(),
        base_url: state.base_url.as_str().to_string(),
        talker: talker.to_string(),
    };
    let mut exported = 0usize;
    let messages: Vec<MessageOut> = items
        .into_iter()
        .map(|mut m| {
            if let (Some(kind), Some(info)) = (kind_of(&m), m.media.clone())
                && let Some(out) = media_export::export_media(&ctx, &info, kind, &opts, media_root.as_deref())
            {
                exported += 1;
                m.media_file_name = Some(out.file_name);
                m.media_url = Some(out.url);
                m.media_local_path = Some(out.local_path);
            }
            m
        })
        .collect();
    json!({
        "success": true,
        "talker": talker,
        "count": messages.len(),
        "hasMore": has_more,
        "media": { "enabled": true, "exportPath": ctx.root.to_string_lossy(), "count": exported },
        "messages": messages,
    })
}

/// ChatLab-style envelope for /api/v1/messages (meta + members + messages).
fn chatlab_envelope(state: &AppState, talker: &str, items: &[crate::store::query::MessageOut]) -> Value {
    let store = state.store.read();
    // find_conversation falls back to the other chat type, so an all-digit
    // c2c peer uid resolves to its real conversation (and real meta.type).
    let conv = store.find_conversation(talker);
    let chat_type = conv
        .map(|c| c.chat_type)
        .unwrap_or_else(|| crate::store::query::classify_talker(talker).0);
    let name = conv
        .map(|c| store.display_name(c.chat_type, &c.talker))
        .unwrap_or_else(|| talker.to_string());
    // members: uid -> name seen in this session (remark preferred)
    let members: Vec<Value> = items
        .iter()
        .filter_map(|m| {
            let uid = &m.sender_username;
            let nick = store.display_uid(uid);
            if uid.is_empty() { None } else {
                Some(json!({
                    "platformId": uid,
                    "accountName": nick,
                    "groupNickname": nick,
                    "avatar": "",
                }))
            }
        })
        .collect();
    let messages: Vec<Value> = items
        .iter()
        .rev() // chatlab is chronological
        .map(|m| json!({
            "sender": m.sender_username,
            "accountName": store.display_uid(&m.sender_username),
            "timestamp": m.create_time,
            "type": m.local_type,
            "content": m.content,
            "platformMessageId": m.server_id,
        }))
        .collect();
    json!({
        "success": true,
        "chatlab": {
            "version": "0.0.2",
            "exportedAt": chrono::Utc::now().timestamp(),
            "generator": "qqflow-server",
        },
        "meta": {
            "name": name,
            "platform": "qq",
            "type": chat_type.as_str(),
            "groupId": talker,
        },
        "members": members,
        "messages": messages,
    })
}
