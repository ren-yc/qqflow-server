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
use serde_json::Value;
use tower::ServiceExt;

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

    let (tx, _) = tokio::sync::broadcast::channel(1024);
    Arc::new(AppState {
        store: Arc::new(RwLock::new(store)),
        events: tx,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(true)),
        token: Arc::new("test-token-123456".into()),
        sync: Arc::new(qqflow_server::sync::SyncEngine::new()),
    })
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
