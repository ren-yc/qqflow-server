//! 夹具的**转发层** + 仅测试用的 HTTP 助手。
//!
//! 造库器真身在 `qqflow_server::testing`（随 `testing` feature 编译）：需要它的有两个
//! 调用方，而它们互相看不见对方的代码 —— 集成测试是独立 crate（链库），**根包二进制
//! 同样是独立 crate**，批量导出的夹具生成入口（CLI 的 `--rows`）够不着只在 `tests/`
//! 下存在的模块。移到库里之后，两边用的是同一份造库器，夹具不会各自漂移。
//!
//! 留在这里的是 axum `oneshot` 那几个助手：它们要 `axum`（只在 `server` feature 下
//! 存在）与 `tower`（dev-dependency），搬进库只会为「没有生产调用方」的东西撑大依赖树。

// 每个集成测试二进制都会编译自己的一份这个模块，因此「本测试用不到的助手」在这里
// 是常态而不是坏味道 —— 抑制它才让 `-D warnings` 保持可用（与库内那份同口径）。
#![allow(dead_code)]
#![allow(unused_imports)]

use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use serde_json::Value;
use tower::ServiceExt;

pub use qqflow_server::testing::*;

// ---- HTTP layer helpers (axum oneshot) ---------------------------------

/// GET through `app` with optional extra headers (e.g. Bearer auth);
/// returns (status, json).
pub async fn get_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("GET");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// POST a JSON body through `app` with optional extra headers; returns
/// (status, json).
pub async fn post_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("POST");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app
        .oneshot(
            builder
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// DELETE through `app` with optional extra headers; returns (status, json).
/// No body — the deregistration route takes its parameters from the path and
/// the query string (its POST alias is what carries a JSON body).
pub async fn delete_json(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    let mut builder = Request::builder().uri(uri).method("DELETE");
    for (k, v) in headers {
        builder = builder.header(*k, *v);
    }
    let resp = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 8 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// Poll `GET /api/v1/accounts` until account `qq` reports state `want`;
/// returns the whole detail JSON. Panics when the account hits `error`
/// (with its reason) or the deadline passes.
///
/// Per-account state lives behind the token: `/health` reports only a
/// coarse `account` phase and never names an account.
pub async fn wait_account_state(
    app: &axum::Router,
    token: &str,
    qq: &str,
    want: &str,
    timeout: Duration,
) -> Value {
    let deadline = std::time::Instant::now() + timeout;
    loop {
        let (status, v) =
            get_json(app.clone(), "/api/v1/accounts", &[("authorization", &format!("Bearer {token}"))])
                .await;
        assert_eq!(status, StatusCode::OK, "account detail: {v}");
        for a in v["accounts"].as_array().expect("accounts array") {
            if a["qq"] != qq {
                continue;
            }
            if a["state"] == want {
                return v;
            }
            if a["state"] == "error" {
                panic!("account {qq} failed: {:?}", a["error"]);
            }
        }
        assert!(std::time::Instant::now() < deadline, "account {qq} did not reach {want}");
        tokio::time::sleep(Duration::from_millis(100)).await;
    }
}

/// The single account entry for `qq` from a `wait_account_state` result.
pub fn account_entry<'a>(detail: &'a Value, qq: &str) -> &'a Value {
    detail["accounts"]
        .as_array()
        .expect("accounts array")
        .iter()
        .find(|a| a["qq"] == qq)
        .unwrap_or_else(|| panic!("account {qq} missing from {detail}"))
}

