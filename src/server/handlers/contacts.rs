//! GET|POST /api/v1/contacts — people who appeared in chat records.
//! v1 derives contacts from the uid->nickname map (no separate contact DB).

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
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct ContactOut {
    pub username: String,
    pub display_name: String,
    pub nickname: String,
    pub remark: String,
    /// WeFlow's alias slot: for QQ this carries the contact's QQ number
    /// (from the uid mapping table / profile, when the version exposes it;
    /// empty otherwise) — the old qqflow `qq` field migrated here.
    pub alias: String,
    pub avatar_url: String,
    pub r#type: String,
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
    let store = state.store.read();
    let (contacts, total) = crate::store::query::query_contacts(
        &store,
        params.keyword.as_deref(),
        limit,
        params.offset,
    );
    let count = contacts.len();
    // `total` / `hasMore` let clients page deterministically instead of
    // inferring the end from "page shorter than limit" — which silently breaks
    // if the server-side default limit ever changes.
    let has_more = params.offset.saturating_add(count) < total;
    let body = json!({
        "success": true,
        "count": count,
        "total": total,
        "hasMore": has_more,
        "contacts": contacts,
    });
    Ok(Json(body))
}
