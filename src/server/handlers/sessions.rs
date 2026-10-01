//! GET /api/v1/sessions — 会话列表（**只输出原生形状**）。
//!
//! ChatLab 形状走 `/chatlab/sessions`（见 `chatlab_sessions`）。两个面共用本函数的其余部分
//! （鉴权、筛选、稳定排序、切片），但**分页参数不共用**：老面只认 `offset`，ChatLab 形状认
//! `cursor`（同时接受 `offset`）。一刀切会让「换个面就该换参数」这件事静默失效。

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};


use crate::server::dto::{Page, SessionChatlab, SessionNative, SessionsChatlab, SessionsNative};
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub keyword: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// `page.nextCursor` 的回传入参。**只有 ChatLab 形状解析它** —— 老面继续接受它会让
    /// 「换个面就该换参数」静默失效（调用方以为自己在翻页，其实一直拿第一页）。
    #[serde(default)]
    pub cursor: Option<String>,
    #[serde(default)]
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
    respond(&state, &headers, params, body, false).await
}

/// `handler` 的本体，带一个「本面就是 ChatLab 形状」的开关。
///
/// `/chatlab/sessions` 用它并传 `true`：那个面**天生就是** ChatLab 形状，调用方不必知道还有
/// 另一种。两条路的其余部分（鉴权、就绪门控、筛选、分页、信封）**逐字一致** —— 抄一份就会漂移。
pub(crate) async fn respond(
    state: &Arc<AppState>,
    headers: &HeaderMap,
    params: Params,
    body: axum::body::Bytes,
    force_chatlab: bool,
) -> Result<axum::response::Response, ApiError> {
    let params = merge_body(params, &body).await?;
    if !authorized(state, headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let limit = params.limit.clamp(1, 10000);
    // 分页模型按面分开：ChatLab 形状认 `cursor`（`page.nextCursor` 的回传入参，解析不了就退回
    // `offset`，与其它参数一样「坏值退化为默认而不是报错」）；老面**只认 offset**。
    let offset = if force_chatlab {
        params
            .cursor
            .as_deref()
            .and_then(|c| c.parse::<usize>().ok())
            .unwrap_or(params.offset)
    } else {
        params.offset
    };

    let store = state.store.read();
    if force_chatlab {
        // ChatLab 把**没有 page 块**的响应读作「这就是完整一页」，所以截断必须显式
        // 告知，否则第 limit 条之后的会话会被静默丢掉。总数与列表共用同一个谓词。
        let total = crate::store::query::count_sessions(&store, params.keyword.as_deref());
        let sessions: Vec<SessionChatlab> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, offset)
            .into_iter()
            .map(|s| {
                SessionChatlab {
                    id: s.username.clone(),
                    last_message_at: s.last_timestamp,
                    // 真值（索引里的条数）。恒 0 的占位会让下游按它排序时拿到一列无意义的数字。
                    message_count: s.message_count as i64,
                    name: s.display_name.clone(),
                    platform: "qq".to_string(),
                    // 群名册里的人数（私聊没有名册 ⇒ 这个键不出现）。
                    member_count: store.chatroom_roster.get(&s.username).map(|r| r.len()),
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
