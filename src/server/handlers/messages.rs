//! GET|POST /api/v1/messages — query messages of one session.
//!
//! WeFlow contract: `talker` required; `limit` (1..=10000, default 100),
//! `offset`, `start`/`end` (YYYYMMDD or unix seconds), `keyword`,
//! `chatlab`/`format` output switch. Media params are accepted but v1
//! returns `media.enabled=false` (QQ media is stored separately).

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::store::query::{query_messages, MessageQuery};
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
    // accepted for WeFlow compatibility; ignored in v1
    #[serde(default)]
    pub media: Option<String>,
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

    let chatlab = params.chatlab.as_deref() == Some("1")
        || params.chatlab.as_deref() == Some("true")
        || params.format.as_deref() == Some("chatlab");

    let body = if chatlab {
        chatlab_envelope(&state, talker, &items)
    } else {
        json!({
            "success": true,
            "talker": talker,
            "count": items.len(),
            "hasMore": has_more,
            "media": { "enabled": false, "exportPath": "", "count": 0 },
            "messages": items,
        })
    };
    Ok(Json(body))
}

/// ChatLab-style envelope for /api/v1/messages (meta + members + messages).
fn chatlab_envelope(state: &AppState, talker: &str, items: &[crate::store::query::MessageOut]) -> Value {
    let store = state.store.read();
    let (chat_type, _) = crate::store::query::classify_talker(talker);
    let name = store
        .conversation(chat_type, talker)
        .map(|c| c.name.clone())
        .unwrap_or_else(|| talker.to_string());
    // members: uid -> name seen in this session
    let members: Vec<Value> = items
        .iter()
        .filter_map(|m| {
            let uid = &m.sender_username;
            let nick = store.uid_names.get(uid).cloned().unwrap_or_default();
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
            "accountName": store.uid_names.get(&m.sender_username).cloned().unwrap_or_default(),
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
