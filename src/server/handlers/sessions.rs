//! GET|POST /api/v1/sessions — session list, newest last-message first.
//! `format=chatlab` returns the ChatLab Pull session shape.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::server::error::ApiError;
use crate::store::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub keyword: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// `page.nextCursor` 的回传入参；解析不了就退回 `offset`。
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
    pub format: Option<String>,
    #[serde(default, alias = "token")]
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
    let limit = params.limit.clamp(1, 10000);
    // `cursor` 是 `page.nextCursor` 的回传入参；解析不了就退回 `offset`，
    // 与其它参数一样「坏值退化为默认而不是报错」。
    let offset = params
        .cursor
        .as_deref()
        .and_then(|c| c.parse::<usize>().ok())
        .unwrap_or(params.offset);
    let chatlab = params.format.as_deref() == Some("chatlab");

    let store = state.store.read();
    if chatlab {
        // ChatLab 把**没有 page 块**的响应读作「这就是完整一页」，所以截断必须显式
        // 告知，否则第 limit 条之后的会话会被静默丢掉。总数与列表共用同一个谓词。
        let total = crate::store::query::count_sessions(&store, params.keyword.as_deref());
        let sessions: Vec<Value> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, offset)
            .into_iter()
            .map(|s| {
                json!({
                    "id": s.username,
                    "name": s.display_name,
                    "platform": "qq",
                    "type": if s.r#type == 2 { "group" } else { "private" },
                    "messageCount": 0,
                    "lastMessageAt": s.last_timestamp,
                })
            })
            .collect();
        let next_offset = offset + sessions.len();
        let has_more = next_offset < total;
        Ok(Json(json!({
            "sessions": sessions,
            "count": sessions.len(),
            "page": {
                "hasMore": has_more,
                "nextCursor": if has_more { Some(next_offset.to_string()) } else { None },
            },
        })))
    } else {
        let sessions: Vec<Value> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, offset)
            .into_iter()
            .map(|s| {
                json!({
                    "username": s.username,
                    "displayName": s.display_name,
                    "type": s.r#type,
                    "lastTimestamp": s.last_timestamp,
                    "unreadCount": s.unread_count,
                })
            })
            .collect();
        let count = sessions.len();
        Ok(Json(json!({ "success": true, "count": count, "sessions": sessions })))
    }
}
