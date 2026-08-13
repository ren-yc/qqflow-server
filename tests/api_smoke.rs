//! HTTP layer smoke tests using tower::ServiceExt::oneshot
//! (no network, no real database).

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::RwLock;
use qqflow_server::parser::types::{seq_to_time, ChatType, MessageRecord, MsgType, ParsedMessage};
use qqflow_server::sync::Event;
use qqflow_server::server::build_router;
use qqflow_server::store::{conv_key, AppState, Conversation, Store};
use qqflow_server::store::query::{query_messages, MessageQuery};
use serde_json::{json, Value};
use tower::ServiceExt;

fn state_with(store: Store, ready: bool) -> Arc<AppState> {
    let (tx, _) = tokio::sync::broadcast::channel(1024);
    Arc::new(AppState {
        store: Arc::new(RwLock::new(store)),
        events: tx,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(ready)),
        token: Arc::new("test-token-123456".into()),
        sync: Arc::new(qqflow_server::sync::SyncEngine::new()),
        init: Arc::new(qqflow_server::server::AccountRegistry {
            accounts_db: parking_lot::Mutex::new(Vec::new()),
            key_store: parking_lot::Mutex::new(qqflow_server::keystore::KeyStore::default()),
            mirror_root: std::env::temp_dir().join("qqflow_smoke_mirror"),
            watch_cfg: qqflow_server::sync::watch::WatchConfig {
                debounce: std::time::Duration::from_millis(350),
                fallback: None,
            },
            shutdown: tokio::sync::watch::channel(false).1,
        }),
    })
}

fn test_state() -> Arc<AppState> {
    let mut store = Store::default();
    // group 10001 with two messages
    let conv = Conversation {
        chat_type: ChatType::Group,
        talker: "10001".into(),
        name: "项目群".into(),
        msgs: vec![
            MessageRecord {
                rowid: 1,
                seq: 0x6771A6B50001,
                ts: seq_to_time(0x6771A6B50001),
                chat_type: ChatType::Group,
                talker: "10001".into(),
                from_uid: "u_a".into(),
                from_nick: "张三".into(),
                parsed: ParsedMessage { msg_type: MsgType::Text, content: "你好".into() },
            },
            MessageRecord {
                rowid: 2,
                seq: 0x6771A6B60002,
                ts: seq_to_time(0x6771A6B60002),
                chat_type: ChatType::Group,
                talker: "10001".into(),
                from_uid: "u_b".into(),
                from_nick: "李四".into(),
                parsed: ParsedMessage { msg_type: MsgType::Image, content: "[image]".into() },
            },
        ],
        dirty: false,
    };
    store.convs.insert(conv_key(ChatType::Group, "10001"), conv);
    store.watermark_group = 2;
    store.uid_names.insert("u_a".into(), "张三".into());
    store.uid_names.insert("u_b".into(), "李四".into());
    state_with(store, true)
}

/// Send a GET through `app` and return (status, json).
async fn call(app: axum::Router, uri: &str) -> (StatusCode, Value) {
    let resp = app
        .oneshot(Request::builder().uri(uri).method("GET").body(Body::empty()).unwrap())
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

async fn get(uri: &str, token: bool) -> (StatusCode, Value) {
    let state = test_state();
    let app = build_router(state);
    let mut builder = Request::builder().uri(uri).method("GET");
    if token {
        builder = builder.header("Authorization", "Bearer test-token-123456");
    }
    let resp = app.oneshot(builder.body(Body::empty()).unwrap()).await.unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

/// POST a JSON body through `app`.
async fn post_json(app: axum::Router, uri: &str, body: Value) -> (StatusCode, Value) {
    let resp = app
        .oneshot(
            Request::builder()
                .uri(uri)
                .method("POST")
                .header("content-type", "application/json")
                .body(Body::from(body.to_string()))
                .unwrap(),
        )
        .await
        .unwrap();
    let status = resp.status();
    let bytes = axum::body::to_bytes(resp.into_body(), 1 << 20).await.unwrap();
    let json: Value = serde_json::from_slice(&bytes).unwrap_or(Value::Null);
    (status, json)
}

#[tokio::test]
async fn health_no_auth() {
    let (s, v) = get("/health", false).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "ok");
}

#[tokio::test]
async fn auth_required() {
    let (s, v) = get("/api/v1/sessions", false).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["success"], false);
}

#[tokio::test]
async fn sync_no_auth() {
    let (s, _) = get("/api/v1/sync", false).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn sync_empty_engine_shape() {
    // Empty SyncEngine (no accounts registered): sync succeeds with 0 rows.
    let (s, v) = get("/api/v1/sync?access_token=test-token-123456&limit=5", true).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["synced"], 0);
    assert_eq!(v["count"], 0);
    assert!(v["messages"].is_array());
}

#[tokio::test]
async fn sessions_with_token() {
    let (s, v) = get("/api/v1/sessions?access_token=test-token-123456", false).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["count"], 1);
    assert_eq!(v["sessions"][0]["username"], "10001");
    assert_eq!(v["sessions"][0]["displayName"], "项目群");
    assert_eq!(v["sessions"][0]["type"], 2);
}

const SEQ2: i64 = 0x6771A6B60002;

#[tokio::test]
async fn messages_pagination_and_filter() {
    let (s, v) = get(
        "/api/v1/messages?talker=10001&limit=1&offset=0&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    assert_eq!(v["hasMore"], true);
    // newest first → rowid 2
    assert_eq!(v["messages"][0]["localId"], 2);
    assert_eq!(v["messages"][0]["serverId"], SEQ2.to_string());
    assert_eq!(v["messages"][0]["createTime"], seq_to_time(SEQ2));
}

#[tokio::test]
async fn messages_media_disabled_v1() {
    let (s, v) = get(
        "/api/v1/messages?talker=10001&media=1&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["enabled"], false);
}

#[tokio::test]
async fn contacts_and_group_members() {
    let (s, v) = get("/api/v1/contacts?access_token=test-token-123456", false).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["count"], 2);

    let (s, v) = get(
        "/api/v1/group-members?chatroomId=10001&includeMessageCounts=1&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["count"], 2);
    assert_eq!(v["members"][0]["wxid"], "u_a");
    assert_eq!(v["members"][0]["messageCount"], 1);
}

#[tokio::test]
async fn chatlab_pull_sync_block() {
    let (s, v) = get(
        "/api/v1/sessions/10001/messages?access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["meta"]["platform"], "qq");
    assert_eq!(v["messages"].as_array().unwrap().len(), 2);
    assert!(v["sync"]["watermark"].as_i64().unwrap() > 0);
}

#[tokio::test]
async fn sse_streams_sync_event() {
    let state = test_state();
    let app = build_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/push/messages?access_token=test-token-123456")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
    assert_eq!(
        resp.headers().get("content-type").unwrap().to_str().unwrap(),
        "text/event-stream"
    );
}

#[tokio::test]
async fn sse_requires_auth() {
    let state = test_state();
    let app = build_router(state);
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/push/messages")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED);
}

#[test]
fn time_bound_parsing() {
    assert_eq!(
        qqflow_server::server::handlers::parse_time_bound("20260801", false),
        Some(1785542400) // 2026-08-01 00:00:00 UTC
    );
    assert_eq!(
        qqflow_server::server::handlers::parse_time_bound("20260801", true),
        Some(1785628799) // 23:59:59 same day
    );
    assert_eq!(
        qqflow_server::server::handlers::parse_time_bound("1782835200", false),
        Some(1782835200)
    );
    assert_eq!(qqflow_server::server::handlers::parse_time_bound("garbage", false), None);
}

#[test]
fn query_messages_keyword() {
    let state = test_state();
    let store = state.store.read();
    let q = MessageQuery {
        talker: "10001",
        limit: 10,
        offset: 0,
        start: None,
        end: None,
        keyword: Some("你好"),
    };
    let (items, _) = query_messages(&store, &q);
    assert_eq!(items.len(), 1);
    assert_eq!(items[0].content, "你好");
}

#[test]
fn seq_time_extraction() {
    // Real QQ layout (verified against a real database): seq = (time << 32) | low32.
    let seq = (0x6771A6B5i64 << 32) | 1;
    assert_eq!(seq_to_time(seq), 0x6771A6B5);
}

#[test]
fn event_json_shape() {
    let ev = Event::message_new(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        42,
        Some("张三".into()),
        "你好".into(),
        1782835200,
    );
    let v: Value = serde_json::to_value(&ev).unwrap();
    assert_eq!(v["event"], "message.new");
    assert_eq!(v["sessionId"], "10001");
    assert_eq!(v["sessionType"], "group");
    assert_eq!(v["rawid"], "42");
    assert_eq!(v["groupName"], "项目群");
    assert!(v.get("lastRowidGroup").is_none(), "new events must not carry sync fields");
}

#[test]
fn wrong_talker_returns_empty() {
    let state = test_state();
    let store = state.store.read();
    let q = MessageQuery {
        talker: "nonexistent",
        limit: 10,
        offset: 0,
        start: None,
        end: None,
        keyword: None,
    };
    let (items, has_more) = query_messages(&store, &q);
    assert!(items.is_empty());
    assert!(!has_more);
}

/// Conversation whose first 5 messages share one second and 2 land later —
/// regression fixture for chatlab-pull boundary-second pagination.
fn ts_boundary_state() -> Arc<AppState> {
    let mut store = Store::default();
    let base: i64 = 0x6771A6B5;
    let mk = |rowid: i64, ts: i64, content: &str| MessageRecord {
        rowid,
        seq: (ts << 32) | rowid,
        ts,
        chat_type: ChatType::Group,
        talker: "10001".into(),
        from_uid: "u_a".into(),
        from_nick: "张三".into(),
        parsed: ParsedMessage { msg_type: MsgType::Text, content: content.into() },
    };
    let conv = Conversation {
        chat_type: ChatType::Group,
        talker: "10001".into(),
        name: "项目群".into(),
        msgs: vec![
            mk(1, base, "m1"),
            mk(2, base, "m2"),
            mk(3, base, "m3"),
            mk(4, base, "m4"),
            mk(5, base, "m5"),
            mk(6, base + 1, "m6"),
            mk(7, base + 1, "m7"),
        ],
        dirty: false,
    };
    store.convs.insert(conv_key(ChatType::Group, "10001"), conv);
    state_with(store, true)
}

#[tokio::test]
async fn chatlab_pull_boundary_second_pages_cleanly() {
    let app = build_router(ts_boundary_state());
    let (s1, v1) = call(
        app.clone(),
        "/api/v1/sessions/10001/messages?limit=3&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s1, StatusCode::OK);
    // limit=3, but the page completes the whole second: 5 boundary messages.
    let msgs1 = v1["messages"].as_array().unwrap();
    assert_eq!(msgs1.len(), 5, "page completes the whole second");
    assert_eq!(v1["sync"]["hasMore"], true);
    let next_since = v1["sync"]["nextSince"].as_i64().unwrap();
    assert_eq!(next_since, 0x6771A6B5);

    // Resume with nextSince (exclusive): remaining rows, no overlap, done.
    let (s2, v2) = call(
        app,
        &format!("/api/v1/sessions/10001/messages?since={next_since}&access_token=test-token-123456"),
    )
    .await;
    assert_eq!(s2, StatusCode::OK);
    let msgs2 = v2["messages"].as_array().unwrap();
    assert_eq!(msgs2.len(), 2, "second page carries the remaining rows");
    assert_eq!(v2["sync"]["hasMore"], false);

    let ids = |msgs: &Vec<Value>| -> std::collections::BTreeSet<String> {
        msgs.iter()
            .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
            .collect()
    };
    assert!(ids(msgs1).is_disjoint(&ids(msgs2)), "pages must not overlap");
}

#[tokio::test]
async fn chatlab_pull_huge_offset_does_not_panic() {
    let app = build_router(ts_boundary_state());
    let (s, v) = call(
        app,
        "/api/v1/sessions/10001/messages?offset=18446744073709551615&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["messages"].as_array().unwrap().len(), 0);
    assert_eq!(v["sync"]["hasMore"], false);
    assert_eq!(v["sync"]["nextOffset"], 0);
}

#[tokio::test]
async fn all_digit_c2c_talker_resolves_via_fallback() {
    let mut store = Store::default();
    let seq = (0x6771A6B5i64 << 32) | 1;
    let conv = Conversation {
        chat_type: ChatType::C2c,
        talker: "12345".into(),
        name: "数字UID好友".into(),
        msgs: vec![MessageRecord {
            rowid: 1,
            seq,
            ts: seq_to_time(seq),
            chat_type: ChatType::C2c,
            talker: "12345".into(),
            from_uid: "12345".into(),
            from_nick: "数字UID好友".into(),
            parsed: ParsedMessage { msg_type: MsgType::Text, content: "在吗".into() },
        }],
        dirty: false,
    };
    store.convs.insert(conv_key(ChatType::C2c, "12345"), conv);
    let app = build_router(state_with(store, true));

    let (s, v) = call(app.clone(), "/api/v1/messages?talker=12345&access_token=test-token-123456").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["messages"].as_array().unwrap().len(), 1);
    assert_eq!(v["messages"][0]["content"], "在吗");

    let (s, v) = call(app, "/api/v1/sessions/12345/messages?access_token=test-token-123456").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["meta"]["type"], "private");
    assert_eq!(v["meta"]["groupId"], "12345");
}

#[tokio::test]
async fn sse_connects_before_ready() {
    // SSE stays open during indexing (no ready gate); the initial sync
    // event carries the current (0,0) watermarks and the build-completion
    // broadcast re-baselines the client.
    let app = build_router(state_with(Store::default(), false));
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/push/messages?access_token=test-token-123456")
                .method("GET")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::OK);
}

#[tokio::test]
async fn accounts_requires_auth() {
    let app = build_router(test_state());
    let (s, v) = post_json(
        app,
        "/api/v1/accounts",
        json!({"qq": "10001", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["success"], false);
    assert_eq!(v["code"], 401);
}

#[tokio::test]
async fn accounts_validation_and_idempotency() {
    let state = test_state();
    // Seed a ready account entry so already_ready can be exercised.
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: "ready".into(),
        message_count: 2,
        error: None,
    });
    // A scanned-style entry for 10002, so key validation is reachable
    // (resolve succeeds without a db_path).
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10002".into(),
        path: std::env::temp_dir().join("qqflow_smoke_10002.db"),
    });
    let app = build_router(state);
    let tok = "test-token-123456";

    // Malformed key -> invalid_key (not an HTTP error).
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "10002", "key": "short"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "invalid_key");

    // Ready account -> idempotent no-op.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "10001", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "already_ready");

    // Unknown qq without a db_path -> unknown_qq.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "999", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "unknown_qq");

    // Unresolvable db_path -> invalid_db_path.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "999", "key": "0123456789abcdef", "db_path": "Z:\\nonexistent"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "invalid_db_path");

    // Missing qq / key -> 400 envelope.
    let (s, v) = post_json(app, "/api/v1/accounts", json!({"access_token": tok})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 400);
}
