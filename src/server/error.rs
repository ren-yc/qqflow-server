//! Unified error envelope: {"success": false, "code": <http>, "message": ...}

use axum::http::StatusCode;
use axum::response::{IntoResponse, Response};
use axum::Json;
use serde_json::json;

#[derive(Debug, Clone)]
pub struct ApiError {
    pub status: StatusCode,
    pub message: String,
}

impl ApiError {
    pub fn bad_request(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::BAD_REQUEST, message: msg.into() }
    }
    pub fn unauthorized() -> Self {
        Self { status: StatusCode::UNAUTHORIZED, message: "未授权：请携带有效 token".into() }
    }
    pub fn not_ready() -> Self {
        Self { status: StatusCode::SERVICE_UNAVAILABLE, message: "服务正在建立索引，请稍后重试".into() }
    }
    pub fn not_found(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::NOT_FOUND, message: msg.into() }
    }
    pub fn internal(msg: impl Into<String>) -> Self {
        Self { status: StatusCode::INTERNAL_SERVER_ERROR, message: msg.into() }
    }
}

impl IntoResponse for ApiError {
    fn into_response(self) -> Response {
        let body = json!({
            "success": false,
            "code": self.status.as_u16(),
            "message": self.message,
        });
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
