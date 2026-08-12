//! GET /api/v1/sessions/:id/messages — ChatLab Pull protocol:
//! incremental sync with a `sync` pagination block.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::parser::types::ChatType;
use crate::server::error::ApiError;
use crate::store::query::classify_talker;
use crate::store::AppState;

use super::{authorized, parse_time_bound};

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    pub since: Option<String>,
    pub end: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub access_token: Option<String>,
}

fn default_limit() -> usize {
    5000
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<Params>,
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }

    let limit = params.limit.clamp(1, 5000);
    let since = params.since.as_deref().and_then(|s| parse_time_bound(s, false));
    let end = params.end.as_deref().and_then(|s| parse_time_bound(s, true));
    let watermark = end.unwrap_or_else(|| chrono::Utc::now().timestamp());

    let (chat_type, talker) = classify_talker(&id);
    let store = state.store.read();
    let Some(conv) = store.conversation(chat_type, talker) else {
        return Err(ApiError::not_found(format!("会话不存在: {id}")));
    };

    // Chronological messages within [since, end].
    let mut idx: Vec<usize> = conv.msgs.iter().enumerate().map(|(i, _)| i).collect();
    idx.sort_by_key(|&i| (conv.msgs[i].ts, conv.msgs[i].rowid));
    let filtered: Vec<usize> = idx
        .into_iter()
        .filter(|&i| {
            let m = &conv.msgs[i];
            since.is_none_or(|s| m.ts >= s) && end.is_none_or(|e| m.ts <= e)
        })
        .collect();

    let total = filtered.len();
    let page: Vec<usize> = filtered.iter().copied().skip(params.offset).take(limit).collect();
    let has_more = params.offset + page.len() < total;

    let messages: Vec<Value> = page
        .iter()
        .map(|&i| {
            let m = &conv.msgs[i];
            json!({
                "sender": m.from_uid,
                "accountName": store.uid_names.get(&m.from_uid).cloned().unwrap_or_default(),
                "timestamp": m.ts,
                "type": m.parsed.msg_type.code(),
                "content": m.parsed.content,
                "platformMessageId": m.seq.to_string(),
            })
        })
        .collect();

    let members: Vec<Value> = {
        let mut seen: Vec<String> = Vec::new();
        let mut out = Vec::new();
        for m in &conv.msgs {
            if !seen.contains(&m.from_uid) && !m.from_uid.is_empty() {
                seen.push(m.from_uid.clone());
                let nick = store.uid_names.get(&m.from_uid).cloned().unwrap_or_default();
                out.push(json!({
                    "platformId": m.from_uid,
                    "accountName": nick,
                    "groupNickname": if chat_type == ChatType::Group { nick.clone() } else { String::new() },
                    "avatar": "",
                }));
            }
        }
        out
    };

    let next_since = page.last().map(|&i| conv.msgs[i].ts).unwrap_or(since.unwrap_or(0));
    Ok(Json(json!({
        "chatlab": {
            "version": "0.0.2",
            "exportedAt": chrono::Utc::now().timestamp(),
            "generator": "qqflow-server",
        },
        "meta": {
            "name": conv.name,
            "platform": "qq",
            "type": chat_type.as_str(),
            "groupId": talker,
        },
        "members": members,
        "messages": messages,
        "sync": {
            "hasMore": has_more,
            "nextSince": if has_more { next_since } else { watermark },
            "nextOffset": if has_more { params.offset + page.len() } else { 0 },
            "watermark": watermark,
        }
    })))
}
