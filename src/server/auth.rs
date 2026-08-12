//! Access-token verification, WeFlow-compatible:
//!   1. Authorization: Bearer <token>
//!   2. ?access_token=<token>          (recommended for SSE)
//!   3. JSON body {"access_token": ...} (POST only)

use axum::http::HeaderMap;
use serde_json::Value;

use crate::config::constant_time_eq;
use crate::store::AppState;

pub fn from_headers(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|s| s.trim().to_string())
}

pub fn from_query(query: Option<&str>) -> Option<String> {
    let q = query?;
    let pairs: Vec<(&str, &str)> = q.split('&').filter_map(|kv| {
        let (k, v) = kv.split_once('=')?;
        Some((k, v))
    }).collect();
    pairs.iter().find(|(k, _)| *k == "access_token").map(|(_, v)| v.to_string())
}

pub fn from_body(body: Option<&Value>) -> Option<String> {
    body?.get("access_token")?.as_str().map(|s| s.to_string())
}

/// Verify the token against any of the three transports.
pub fn verify(state: &AppState, headers: &HeaderMap, query: Option<&str>, body: Option<&Value>) -> bool {
    let token = state.token.as_str();
    let candidates = [
        from_headers(headers),
        from_query(query),
        from_body(body),
    ];
    candidates.iter().flatten().any(|c| constant_time_eq(c, token))
}
