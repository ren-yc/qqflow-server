//! Unified error envelope: {"success": false, "code": <http>, "message": ..., "reason"?}
//!
//! `reason` 只在**客户端可据以自动处置**的冲突上出现，是机器字段：客户端按它决定
//! 「重新从头拉」之类的动作，不要去匹配中文 `message`。当前只有游标失效
//! （`cursor_reset_required`）用它。

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
    /// 机器可读的失效原因，见 [`crate::server::cursor::CursorError`]。
    pub reason: Option<&'static str>,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: msg.into(), reason: None }
    }
    pub fn unauthorized() -> Self {
        Self { status: StatusCode::UNAUTHORIZED, message: "未授权：请携带有效 token".into(), reason: None }
    }
    pub fn not_ready() -> Self {
        Self { status: StatusCode::SERVICE_UNAVAILABLE, message: "服务正在建立索引，请稍后重试".into(), reason: None }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into(), reason: None }
    }
    pub fn conflict(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::CONFLICT, message: msg.into(), reason: None }
    }
    /// 冲突 + 机器可读原因：客户端据此决定重拉，而不是重试。
    pub fn conflict_with_reason(reason: &'static str, msg: impl Into<String>) -> Self {
        Self { status: StatusCode::CONFLICT, message: msg.into(), reason: Some(reason) }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into(), reason: None }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = match self.reason {
            Some(reason) => json!({
                "success": false,
                "code": self.status.as_u16(),
                "message": self.message,
                "reason": reason,
            }),
            None => json!({
                "success": false,
                "code": self.status.as_u16(),
                "message": self.message,
            }),
        };
        (self.status, Json(body)).into_response()
    }
}

impl From<anyhow::Error> for ApiError {
    fn from(e: anyhow::Error) -> Self {
        ApiError::internal(format!("{e:#}"))
    }
}

/// `Query` whose rejection becomes the unified envelope.
///
/// axum's own rejection is a **plain-text 400 with an empty body**, so a client
/// that always parses `{success,code,message}` cannot tell "you sent a bad
/// parameter" from "the transport returned something unparseable" — and the
/// first is the one it can actually fix.
///
/// Wrapping the extractor keeps that promise in one place instead of asking
/// every handler to remember it.
pub struct EnvelopeQuery<T>(pub T);

impl<S, T> axum::extract::FromRequestParts<S> for EnvelopeQuery<T>
where
    T: serde::de::DeserializeOwned + Send,
    S: Send + Sync,
{
    type Rejection = ApiError;

    async fn from_request_parts(
        parts: &mut axum::http::request::Parts,
        state: &S,
    ) -> Result<Self, Self::Rejection> {
        axum::extract::Query::<T>::from_request_parts(parts, state)
            .await
            .map(|axum::extract::Query(v)| EnvelopeQuery(v))
            .map_err(|e| ApiError::bad_request(format!("参数解析失败：{e}")))
    }
}
