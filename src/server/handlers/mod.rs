//! Endpoint handlers (WeFlow-compatible paths and shapes).

pub mod accounts;
pub mod chatlab_pull;
pub mod contacts;
pub mod group_members;
pub mod health;
pub mod media;
pub mod messages;
pub mod push_events;
pub mod sessions;
pub mod sync;

/// Media content type by file extension (served by the media routes;
/// the map mirrors WeFlow's, plus QQ's amr/silk).
pub fn media_content_type(ext: &str) -> &'static str {
    match ext.to_ascii_lowercase().as_str() {
        "jpg" | "jpeg" => "image/jpeg",
        "png" => "image/png",
        "gif" => "image/gif",
        "webp" => "image/webp",
        "bmp" => "image/bmp",
        "wav" => "audio/wav",
        "mp4" => "video/mp4",
        "amr" => "audio/amr",
        "silk" => "audio/silk",
        "mp3" => "audio/mpeg",
        _ => "application/octet-stream",
    }
}

use axum::http::HeaderMap;

use crate::config::constant_time_eq;
use crate::server::error::ApiError;
use crate::store::AppState;

/// Parse WeFlow time bounds: "YYYYMMDD" (end expands to 23:59:59) or unix seconds.
pub fn parse_time_bound(s: &str, is_end: bool) -> Option<i64> {
    if s.len() == 8 && s.chars().all(|c| c.is_ascii_digit()) {
        let d = chrono::NaiveDate::parse_from_str(s, "%Y%m%d").ok()?;
        let dt = if is_end { d.and_hms_opt(23, 59, 59) } else { d.and_hms_opt(0, 0, 0) }?;
        return Some(dt.and_utc().timestamp());
    }
    s.parse::<i64>().ok()
}

/// Verify the token from any transport; SSE and POST use `?access_token=`
/// or JSON body, plain requests may use the Authorization header.
pub fn authorized(state: &AppState, headers: &HeaderMap, query_token: Option<&str>) -> bool {
    let header_token = crate::server::auth::from_headers(headers);
    [query_token, header_token.as_deref()]
        .iter()
        .flatten()
        .any(|t| constant_time_eq(t, state.token.as_str()))
}

/// Merge query params with a POST JSON body (WeFlow contract: POST
/// parameters live in the JSON body). Body fields win when present and
/// non-null; an empty or non-JSON body leaves the query params as-is.
/// This also makes the body-carried `access_token` work (D-4).
///
/// The handler extracts the raw body as `Bytes` (not `Option<Bytes>` —
/// axum's `Option<T>` only implements `FromRequestParts`, so it cannot be
/// the body-consuming last extractor); an empty body arrives as `&[]`.
pub async fn merge_body<T>(params: T, body: &[u8]) -> Result<T, ApiError>
where
    T: serde::de::DeserializeOwned + serde::Serialize,
{
    if body.is_empty() {
        return Ok(params);
    }
    let Ok(v) = serde_json::from_slice::<serde_json::Value>(body) else {
        tracing::debug!("POST body 不是有效 JSON，已忽略");
        return Ok(params);
    };
    let Some(obj) = v.as_object() else {
        return Ok(params);
    };
    let mut merged = serde_json::to_value(&params)
        .map_err(|e| ApiError::bad_request(format!("参数序列化失败: {e}")))?;
    if let Some(m) = merged.as_object_mut() {
        for (k, val) in obj {
            if !val.is_null() {
                m.insert(k.clone(), val.clone());
            }
        }
    }
    serde_json::from_value(merged).map_err(|e| ApiError::bad_request(format!("body 参数无效: {e}")))
}
