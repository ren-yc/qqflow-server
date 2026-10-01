//! `GET /chatlab/sessions` —— Pull 形状的**发现面**。
//!
//! 规范把 `baseUrl` 定义为 `/chatlab`，这条是其中的会话发现入口。与 `/api/v1/sessions`
//! **共用同一份实现**（`sessions::respond`，带「本面就是 ChatLab 形状」的开关），差别只在
//! **形状与分页参数**：老面只输出原生形状、只认 `offset`；新面**天生就是** ChatLab 形状，
//! 并接受 `cursor`（`page.nextCursor` 的回传入参）。
//!
//! 响应形状（规范）：
//!
//! ```json
//! { "sessions": [ { "id", "name", "platform", "type", "messageCount", "memberCount?", "lastMessageAt" } ],
//!   "page": { "hasMore": true, "nextCursor": "…" } }
//! ```
//!
//! `page` 是**可选增强**：规范说客户端在响应里**未发现** `page` 时按「单次全量结果」处理。
//! 这里总是给 `page` —— 那比「靠条数猜有没有截断」明确，而契约套件里有一条断言正是查这个。

use std::sync::Arc;

use axum::extract::State;
use axum::http::HeaderMap;

use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<super::sessions::Params>,
    body: axum::body::Bytes,
) -> Result<axum::response::Response, ApiError> {
    // `force_chatlab = true`：这个面**天生就是** ChatLab 形状，调用方不必知道还有另一种。
    super::sessions::respond(&state, &headers, params, body, true).await
}
