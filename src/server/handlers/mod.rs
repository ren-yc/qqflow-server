//! Endpoint handlers (WeFlow-compatible paths and shapes).

pub mod chatlab_pull;
pub mod contacts;
pub mod group_members;
pub mod health;
pub mod messages;
pub mod push_events;
pub mod sessions;

use axum::http::HeaderMap;

use crate::config::constant_time_eq;
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
