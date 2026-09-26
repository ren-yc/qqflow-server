//! GET|POST /api/v1/sessions — session list, newest last-message first.
//! `format=chatlab` returns the ChatLab Pull session shape.

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};


use crate::server::dto::{Page, SessionChatlab, SessionNative, SessionsChatlab, SessionsNative};
use crate::server::error::{ApiError, EnvelopeQuery};
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
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
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
        let sessions: Vec<SessionChatlab> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, offset)
            .into_iter()
            .map(|s| {
                SessionChatlab {
                    id: s.username.clone(),
                    last_message_at: s.last_timestamp,
                    // 本仓库不维护每会话条数，恒为 0（**键要留着**：下游按它排序）。
                    message_count: 0,
                    name: s.display_name.clone(),
                    platform: "qq".to_string(),
                    r#type: if s.r#type == 2 { "group" } else { "private" }.to_string(),
                }
            })
            .collect();
        let next_offset = offset + sessions.len();
        let has_more = next_offset < total;
        let body = SessionsChatlab {
            count: sessions.len(),
            page: Page {
                has_more,
                next_cursor: has_more.then(|| next_offset.to_string()),
            },
            sessions,
        };
        Ok(axum::response::IntoResponse::into_response(Json(body)))
    } else {
        let sessions: Vec<SessionNative> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, offset)
            .into_iter()
            .map(|s| SessionNative {
                display_name: s.display_name.clone(),
                last_timestamp: s.last_timestamp,
                r#type: s.r#type,
                unread_count: s.unread_count,
                username: s.username.clone(),
            })
            .collect();
        let count = sessions.len();
        let body = SessionsNative { count, sessions, success: true };
        Ok(axum::response::IntoResponse::into_response(Json(body)))
    }
}
