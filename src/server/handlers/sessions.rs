//! GET|POST /api/v1/sessions — session list, newest last-message first.
//! `format=chatlab` returns the ChatLab Pull session shape.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::server::error::ApiError;
use crate::store::AppState;

use super::authorized;

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    pub keyword: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default)]
    pub format: Option<String>,
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
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let limit = params.limit.clamp(1, 10000);
    let chatlab = params.format.as_deref() == Some("chatlab");

    let store = state.store.read();
    if chatlab {
        let sessions: Vec<Value> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, params.offset)
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
        Ok(Json(json!({ "sessions": sessions })))
    } else {
        let sessions: Vec<Value> = crate::store::query::query_sessions(&store, params.keyword.as_deref(), limit, params.offset)
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
