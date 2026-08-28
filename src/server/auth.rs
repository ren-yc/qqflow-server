//! Access-token verification, WeFlow-compatible.
//!
//! Five accepted transports (weflow-server parity):
//!   1. `Authorization: Bearer <token>`  (extracted here)
//!   2. `X-Api-Key: <token>`             (extracted here)
//!   3. `?access_token=<token>`          (recommended for SSE)
//!   4. `?token=<token>`                 (alias of 3)
//!   5. the same two keys inside a POST JSON body
//!
//! Transports 3-5 arrive through the merged request params
//! (`handlers::merge_body`, with `#[serde(alias = "token")]` on each handler's
//! `access_token` field) and are checked by `handlers::authorized`; this
//! module only extracts the header forms.

use axum::http::{HeaderMap, HeaderName};

/// Token from either accepted header, in check order.
pub fn from_headers(headers: &HeaderMap) -> Option<String> {
    bearer(headers).or_else(|| api_key(headers))
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    v.strip_prefix("Bearer ").map(|s| s.trim().to_string())
}

fn api_key(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(HeaderName::from_static("x-api-key"))?.to_str().ok()?;
    let v = v.trim();
    (!v.is_empty()).then(|| v.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;

    fn hdrs(pairs: &[(&'static str, &str)]) -> HeaderMap {
        let mut h = HeaderMap::new();
        for (k, v) in pairs {
            h.insert(HeaderName::from_static(k), v.parse().unwrap());
        }
        h
    }

    #[test]
    fn bearer_header_is_extracted() {
        assert_eq!(
            from_headers(&hdrs(&[("authorization", "Bearer abc123")])).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn x_api_key_header_is_extracted() {
        assert_eq!(
            from_headers(&hdrs(&[("x-api-key", "abc123")])).as_deref(),
            Some("abc123")
        );
    }

    #[test]
    fn bearer_wins_when_both_headers_are_present() {
        let h = hdrs(&[("authorization", "Bearer from-bearer"), ("x-api-key", "from-key")]);
        assert_eq!(from_headers(&h).as_deref(), Some("from-bearer"));
    }

    #[test]
    fn non_bearer_authorization_falls_through_to_x_api_key() {
        // A Basic-auth header must not shadow a valid X-Api-Key.
        let h = hdrs(&[("authorization", "Basic dXNlcjpwdw=="), ("x-api-key", "abc123")]);
        assert_eq!(from_headers(&h).as_deref(), Some("abc123"));
    }

    #[test]
    fn missing_and_empty_headers_yield_none() {
        assert_eq!(from_headers(&HeaderMap::new()), None);
        assert_eq!(from_headers(&hdrs(&[("x-api-key", "   ")])), None);
        assert_eq!(from_headers(&hdrs(&[("authorization", "Bearer")])), None);
    }
}
