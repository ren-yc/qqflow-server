//! GET|POST /api/v1/sync — manual sync.
//!
//! Immediately runs a full sync pass on every account (incremental append
//! against the live connection, bypassing the change-detection poll loop)
//! and reports how many rows it appended. Use this at client initialization
//! or for a manual refresh; read the rows themselves back through
//! `/api/v1/messages`, or receive them on the SSE stream.
//!
//! The response is counts-only (`newMessages` / `revokeMessages`), matching
//! weflow-server. WeFlow (安装版) has no `/api/v1/sync` at all, so there is
//! no baseline shape to align to — and returning the rows here would be a
//! second, differently-shaped copy of the messages face for no gain.

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
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
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
    // The sync does blocking DB work (SQLCipher open on reconnect, query) —
    // run it off the async runtime.
    let engine = state.sync.clone();
    let (new_count, revoke_count) = tokio::task::spawn_blocking(move || engine.sync_all())
        .await
        .map_err(|e| ApiError::internal(format!("sync task panicked: {e}")))?;

    Ok(Json(json!({
        "success": true,
        "newMessages": new_count,
        "revokeMessages": revoke_count,
    })))
}
