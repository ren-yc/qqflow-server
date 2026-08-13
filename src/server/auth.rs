//! Access-token verification, WeFlow-compatible:
//!   1. Authorization: Bearer <token>            (extracted here)
//!   2. ?access_token=<token>                    (recommended for SSE)
//!   3. JSON body {"access_token": ...} (POST only)
//!
//! Transports 2 and 3 arrive through the merged request params
//! (`handlers::merge_body`) and are checked by `handlers::authorized`;
//! this module only extracts the header form.

use axum::http::HeaderMap;

pub fn from_headers(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|s| s.trim().to_string())
}
