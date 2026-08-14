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

use crate::store::media_export::{self, ExportContext, ExportOptions};
use crate::store::query::{query_messages, MessageOut, MessageQuery};
use crate::store::AppState;

use super::{authorized, merge_body, parse_time_bound, FlexBool};
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
    /// ChatLab output switch — bool (POST body) or "1"/"true" (query).
    #[serde(default)]
    pub chatlab: FlexBool,
    #[serde(default)]
    pub format: Option<String>,
    /// WeFlow media export switch (alias `meiti`) — true exports this
    /// page's media and fills mediaFileName/mediaUrl/mediaLocalPath.
    /// Accepted as JSON bool or "1"/"true" (see `FlexBool`).
    #[serde(default, alias = "meiti")]
    pub media: FlexBool,
    /// Per-kind export sub-switches (default on; false / "0" disables;
    /// `tupian`/`vioce` are the WeFlow spellings).
    #[serde(default, alias = "tupian")]
    pub image: FlexBool,
    #[serde(default, alias = "vioce")]
    pub voice: FlexBool,
    #[serde(default)]
    pub video: FlexBool,
    /// Recognized but inert in v1: QQ emoji carry display text, no files.
    #[serde(default)]
    pub emoji: FlexBool,
    #[serde(default)]
    pub access_token: Option<String>,
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

    let chatlab = params.chatlab.is_true() || params.format.as_deref() == Some("chatlab");
    let media_on = params.media.is_true();

    let body = if chatlab {
        // ChatLab output carries no export envelope (WeFlow parity).
        chatlab_envelope(&state, talker, &items)
    } else if media_on {
        // WeFlow-shaped export: copy this page's media into the export
        // root, fill per-message mediaFileName/mediaUrl/mediaLocalPath.
        export_envelope(&state, talker, has_more, &params, items).await?
    } else {
        // No media param: capability envelope unchanged (compat) — media
        // metadata still rides on every message.
        let media_count = items.iter().filter(|m| m.media.is_some()).count();
        envelope(
            talker,
            items.len(),
            has_more,
            json!({ "enabled": true, "exportPath": "", "count": media_count }),
            json!(items),
        )
    };
    Ok(Json(body))
}

/// WeFlow message-envelope shape — one shared builder for the media=1 and
/// compat paths so the contract field set cannot drift between them.
fn envelope(talker: &str, count: usize, has_more: bool, media: Value, messages: Value) -> Value {
    json!({
        "success": true,
        "talker": talker,
        "count": count,
        "hasMore": has_more,
        "media": media,
        "messages": messages,
    })
}

/// WeFlow-shaped media export envelope (`media=1` / `meiti`): exports the
/// page's media files into `<exportRoot>/<talker>/<kind>/<file>` and fills
/// the per-message export fields; `count` = successfully exported messages.
/// Missing sources (QQ cleared the cache) are skipped gracefully.
///
/// The copy loop runs on the blocking pool — export is real file IO and
/// must never stall the tokio workers (concurrent exports would starve
/// every other request, including SSE keep-alives).
async fn export_envelope(
    state: &AppState,
    talker: &str,
    has_more: bool,
    params: &Params,
    items: Vec<MessageOut>,
) -> Result<Value, ApiError> {
    let opts = ExportOptions {
        image: !params.image.is_false(),
        voice: !params.voice.is_false(),
        video: !params.video.is_false(),
        emoji: !params.emoji.is_false(),
    };
    let (media_root, media_entries) = {
        let store = state.store.read();
        // `media_entries` = registered store.media snapshot: rows without a
        // "45812" (cache-index-fallback rescues) export from their
        // registered entry, so media=1 and /api/v1/media/{id} agree on one
        // source per mediaId.
        (store.media_root.clone(), store.media.clone())
    };
    let ctx = ExportContext {
        root: state.export_root.as_ref().clone(),
        base_url: state.base_url.as_str().to_string(),
        talker: talker.to_string(),
    };
    let export_path = ctx.root.to_string_lossy().into_owned();
    let (messages, exported) = tokio::task::spawn_blocking(move || {
        media_export::export_page(&ctx, &opts, media_root.as_deref(), &media_entries, items)
    })
    .await
    .map_err(|e| ApiError::internal(format!("媒体导出任务异常: {e}")))?;
    Ok(envelope(
        talker,
        messages.len(),
        has_more,
        json!({ "enabled": true, "exportPath": export_path, "count": exported }),
        json!(messages),
    ))
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
    // members: uid -> name seen in this session (remark preferred; in a
    // group the sender's per-conversation card (40090) wins — it never
    // leaks into c2c or the global contact lists).
    let members: Vec<Value> = items
        .iter()
        .filter_map(|m| {
            let uid = &m.sender_username;
            let nick = store.display_sender(chat_type, talker, uid);
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
            "accountName": store.display_sender(chat_type, talker, &m.sender_username),
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
