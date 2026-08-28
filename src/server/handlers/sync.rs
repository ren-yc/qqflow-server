//! GET|POST /api/v1/sync — manual sync.
//!
//! Immediately runs a full sync pass on every account (incremental append
//! against the live connection, bypassing the change-detection poll loop)
//! and returns the newly appended messages, newest first. Use this at
//! client initialization or for a manual refresh to pull the most recent
//! rows.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::server::error::ApiError;
use crate::store::query::MessageOut;
use crate::store::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    #[serde(default = "default_limit")]
    pub limit: usize,
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

    // The sync does blocking DB work (SQLCipher open on reconnect, query) —
    // run it off the async runtime.
    let engine = state.sync.clone();
    let records = tokio::task::spawn_blocking(move || engine.sync_all())
        .await
        .map_err(|e| ApiError::internal(format!("sync task panicked: {e}")))?;

    let synced = records.len();
    let messages: Vec<MessageOut> = records.into_iter().take(limit).collect();

    Ok(Json(json!({
        "success": true,
        "count": messages.len(),
        "synced": synced,
        "hasMore": false,
        "messages": messages,
    })))
}
