//! Access-token verification, WeFlow-compatible.
//!
//! **Two** accepted transports:
//!   1. `Authorization: Bearer <token>`  (extracted here)
//!   2. `?access_token=<token>`          (arrives through the merged request params)
//!
//! 为什么砍到两条：每多一条通道就多一处凭据会被复制到的地方（请求体、代理日志、客户端抓包），
//! 而它们鉴权的是同一个东西。`X-Api-Key` 与本服务的命名无关；`?token=` 是同一通道的第二种
//! 拼写 —— 两种拼写意味着下游有一半会写错而另一半不会。凭据键也不再从 POST body 合并进来
//! （见 `handlers::merge_body`）：body 连「这是谁的凭据」都区分不出来。

use axum::http::HeaderMap;

/// Token from the accepted header transport.
pub fn from_headers(headers: &HeaderMap) -> Option<String> {
    bearer(headers)
}

fn bearer(headers: &HeaderMap) -> Option<String> {
    let v = headers.get(axum::http::header::AUTHORIZATION)?.to_str().ok()?;
    let token = v.strip_prefix("Bearer ")?.trim();
    // 空 token 不是凭据：`Bearer `（只剩空白）与 `Bearer`（没有空格）都提取不出东西。
    (!token.is_empty()).then(|| token.to_string())
}

#[cfg(test)]
mod tests {
    use super::*;
    use axum::http::HeaderName;

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
    fn removed_header_transports_are_not_accepted() {
        // `X-Api-Key` 曾是一条通道。删通道时如果只删实现、留下断言，测试会红；反过来
        // （只删断言、留下实现）不会有任何东西红 —— 所以这条负向断言必须留下。
        assert_eq!(
            from_headers(&hdrs(&[("x-api-key", "abc123")])),
            None,
            "X-Api-Key 通道已删除"
        );
        // 非 Bearer 的 Authorization 也不再回落到别的头。
        assert_eq!(
            from_headers(&hdrs(&[
                ("authorization", "Basic dXNlcjpwdw=="),
                ("x-api-key", "abc123")
            ])),
            None,
            "只认 Bearer 前缀，其余写法一律不提取"
        );
    }

    #[test]
    fn missing_and_empty_headers_yield_none() {
        assert_eq!(from_headers(&HeaderMap::new()), None);
        assert_eq!(from_headers(&hdrs(&[("authorization", "Bearer")])), None);
        assert_eq!(from_headers(&hdrs(&[("authorization", "Bearer    ")])), None);
    }
}
