//! HTTP layer smoke tests using tower::ServiceExt::oneshot
//! (no network, no real database).

use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::RwLock;
use qqflow_server::parser::types::{seq_to_time, ChatType, MediaInfo, MessageRecord, MsgType, ParsedMessage};
use qqflow_server::sync::Event;
use qqflow_server::server::{build_router, AccountStatus};
use qqflow_server::store::{conv_key, AppState, Conversation, Store};
use qqflow_server::store::query::{query_messages, MessageQuery};
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;

/// Unique per-call suffix — api_smoke tests run concurrently in one process
/// and would otherwise share (and race on) one temp media file / export root.
fn unique_suffix() -> u64 {
    static COUNTER: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
    COUNTER.fetch_add(1, std::sync::atomic::Ordering::SeqCst)
}

fn state_with(store: Store, ready: bool) -> Arc<AppState> {
    let (tx, _) = tokio::sync::broadcast::channel(1024);
    Arc::new(AppState {
        store: Arc::new(RwLock::new(store)),
        events: tx,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(ready)),
        token: Arc::new("test-token-123456".into()),
        sync: Arc::new(qqflow_server::sync::SyncEngine::new()),
        init: qqflow_server::server::AccountRegistry::new(
            Vec::new(),
            qqflow_server::sync::watch::WatchConfig::default(),
            tokio::sync::watch::channel(false).1,
        ),
        export_root: Arc::new(
            std::env::temp_dir().join(format!("qqflow_smoke_export_{}", unique_suffix())),
        ),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
    })
}

fn test_state() -> Arc<AppState> {
    let mut store = Store::default();
    // A real temp file backing the image message's localPath, so the media
    // endpoint test can serve actual bytes.
    let media_file = std::env::temp_dir().join(format!(
        "qqflow_api_smoke_{}_{}.jpg",
        std::process::id(),
        unique_suffix()
    ));
    std::fs::write(&media_file, b"\xFF\xD8 fake jpeg bytes \xFF\xD9").unwrap();
    let media_local = media_file.to_string_lossy().into_owned();
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
                card: None,
                direction: Some(1),
                parsed: ParsedMessage {
                    msg_type: MsgType::Text,
                    content: "你好".into(),
                    media: None,
                },
            },
            MessageRecord {
                rowid: 2,
                seq: 0x6771A6B60002,
                ts: seq_to_time(0x6771A6B60002),
                chat_type: ChatType::Group,
                talker: "10001".into(),
                from_uid: "u_b".into(),
                from_nick: "李四".into(),
                card: None,
                direction: Some(0),
                parsed: ParsedMessage {
                    msg_type: MsgType::Image,
                    content: "[image]".into(),
                    media: Some(MediaInfo {
                        uuid: Some("R020-test".into()),
                        md5: Some("aabbccddeeff00112233445566778899".into()),
                        file_name: Some("aabb.png".into()),
                        size: Some(1234),
                        width: Some(640),
                        height: Some(480),
                        local_path: Some(media_local.clone()),
                        urls: vec![],
                    }),
                },
            },
        ],
        dirty: false,
    };
    store.convs.insert(conv_key(ChatType::Group, "10001"), conv);
    store.watermark_group = 2;
    store.uid_names.insert("u_a".into(), "张三".into());
    store.uid_names.insert("u_b".into(), "李四".into());
    // Register the image's local path exactly like the index would — mediaId
    // is only promised for keys the store can actually serve.
    store.media.insert(
        "aabbccddeeff00112233445566778899".into(),
        qqflow_server::store::MediaEntry {
            local_path: media_local,
            file_name: Some("aabb.png".into()),
        },
    );
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
    common::post_json(app, uri, &[], body).await
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
async fn messages_media_enabled_v1() {
    let (s, v) = get(
        "/api/v1/messages?talker=10001&media=1&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["enabled"], true);
    // media=1 triggers the WeFlow-shaped export: exportPath = real root,
    // count = exported messages, per-message mediaFileName/Url/LocalPath.
    let export_path = v["media"]["exportPath"].as_str().unwrap();
    assert!(!export_path.is_empty(), "media=1 exports into a real directory");
    assert_eq!(v["media"]["count"], 1);
    let m0 = &v["messages"][0];
    // Export names are key-derived (<md5>.<source ext>): unique per content.
    let exported_name = "aabbccddeeff00112233445566778899.jpg";
    assert_eq!(m0["mediaFileName"], exported_name);
    assert_eq!(
        m0["mediaUrl"],
        format!("http://127.0.0.1:5032/api/v1/media/10001/images/{exported_name}")
    );
    assert!(
        m0["mediaLocalPath"].as_str().unwrap().starts_with(export_path),
        "mediaLocalPath under exportPath"
    );
    // The exported file exists on disk with the exact bytes.
    let exported = std::fs::read(m0["mediaLocalPath"].as_str().unwrap()).unwrap();
    assert_eq!(exported, b"\xFF\xD8 fake jpeg bytes \xFF\xD9");
    // Structured metadata + mediaId direct-serve survive alongside export.
    assert_eq!(m0["media"]["md5"], "aabbccddeeff00112233445566778899");
    assert_eq!(m0["media"]["uuid"], "R020-test");
    assert_eq!(m0["mediaId"], "aabbccddeeff00112233445566778899");
    // The no-media request keeps the compat envelope — assert its BODY, not
    // just its status (the media=1 response above must not leak into it).
    let (s, v2) = get(
        "/api/v1/messages?talker=10001&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v2["media"]["enabled"], true);
    assert_eq!(v2["media"]["exportPath"], "", "no media=1 -> empty exportPath");
    assert_eq!(v2["media"]["count"], 1, "media metadata still counted");
    assert!(v2["messages"][0]["mediaFileName"].is_null(), "no export fields without media=1");
    assert!(v2["messages"][1]["media"].is_null(), "text message has no media");
    // is_send: self-sent (40013=1) vs other (40013=0) — from the no-media
    // response so both responses' shapes are verified.
    assert_eq!(v2["messages"][0]["isSend"], 0, "rowid 2 is others' image");
    assert_eq!(v2["messages"][1]["isSend"], 1, "rowid 1 is self-sent");
}

#[tokio::test]
async fn messages_without_media_param_keeps_compat_envelope() {
    // No media param: capability envelope unchanged (exportPath ""), media
    // metadata still rides on messages.
    let (s, v) = get(
        "/api/v1/messages?talker=10001&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["enabled"], true);
    assert_eq!(v["media"]["exportPath"], "");
    assert_eq!(v["media"]["count"], 1);
    assert!(v["messages"][0]["mediaFileName"].is_null(), "no export fields without media=1");
}

#[tokio::test]
async fn messages_media_alias_and_kind_switches() {
    // meiti alias triggers the export like media=1.
    let (s, v) = get(
        "/api/v1/messages?talker=10001&meiti=1&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["count"], 1, "meiti alias exports");
    // tupian=0 disables image export: count 0, no per-message fields.
    let (s, v) = get(
        "/api/v1/messages?talker=10001&media=1&tupian=0&access_token=test-token-123456",
        false,
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["media"]["count"], 0, "tupian=0 skips image export");
    assert!(v["messages"][0]["mediaFileName"].is_null());
}

#[tokio::test]
async fn media_id_omitted_when_store_has_no_entry() {
    // The media metadata rides on the message, but mediaId promises a
    // fetchable /api/v1/media/{id} — with no registered local path the key
    // must be omitted, never advertised as a guaranteed 404.
    let state = test_state();
    state.store.write().media.clear();
    let app = build_router(state);
    let (s, v) = call(app, "/api/v1/messages?talker=10001&access_token=test-token-123456").await;
    assert_eq!(s, StatusCode::OK);
    let m0 = &v["messages"][0];
    assert!(m0["media"]["md5"].is_string(), "media object still rides along");
    assert!(m0["mediaId"].is_null(), "mediaId omitted when not fetchable");
}

#[tokio::test]
async fn media_single_segment_post_serves_bytes() {
    // The {id} route is GET|POST like the three-segment route (POST with
    // the token in the body is the documented transport).
    let state = test_state();
    let app = build_router(state);
    let (s, _v) = post_json(
        app.clone(),
        "/api/v1/media/aabbccddeeff00112233445566778899",
        json!({"access_token": "test-token-123456"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "POST /api/v1/media/{{id}} must not 405");
    // Without the token the POST is rejected like any other route.
    let (s, _v) = post_json(app, "/api/v1/media/aabbccddeeff00112233445566778899", json!({})).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn post_body_booleans_for_media_params() {
    // JSON booleans in the POST body must work exactly like ?media=1 —
    // the two transports share one contract.
    let state = test_state();
    let app = build_router(state);
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/messages",
        json!({
            "talker": "10001",
            "media": true,
            "access_token": "test-token-123456",
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "media:true must not 400");
    assert_eq!(v["media"]["enabled"], true);
    assert_eq!(v["media"]["count"], 1, "JSON bool media triggers the export");
    assert!(!v["messages"][0]["mediaFileName"].is_null());

    // Per-kind switches accept JSON bools too: image:false skips images.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/messages",
        json!({
            "talker": "10001",
            "media": true,
            "image": false,
            "access_token": "test-token-123456",
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "image:false must not 400");
    assert_eq!(v["media"]["count"], 0, "image:false disables the image export");
    assert!(v["messages"][0]["mediaFileName"].is_null());

    // chatlab accepts a JSON bool as well (same contract class).
    let (s, v) = post_json(
        app,
        "/api/v1/messages",
        json!({
            "talker": "10001",
            "chatlab": true,
            "access_token": "test-token-123456",
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK, "chatlab:true must not 400");
    assert_eq!(v["chatlab"]["generator"], "qqflow-server");
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
        None,
    );
    let v: Value = serde_json::to_value(&ev).unwrap();
    assert_eq!(v["event"], "message.new");
    assert_eq!(v["sessionId"], "10001");
    assert_eq!(v["sessionType"], "group");
    assert_eq!(v["rawid"], "42");
    assert_eq!(v["groupName"], "项目群");
    assert!(v.get("lastRowidGroup").is_none(), "new events must not carry sync fields");
    assert!(v.get("media").is_none(), "no media -> key absent");
}

#[test]
fn event_json_carries_media() {
    let ev = Event::message_new(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        43,
        Some("李四".into()),
        "[image]".into(),
        1782835200,
        Some(MediaInfo {
            uuid: Some("R020-test".into()),
            md5: Some("aabbccddeeff00112233445566778899".into()),
            file_name: Some("aabb.png".into()),
            size: Some(1234),
            width: Some(640),
            height: Some(480),
            local_path: None,
            urls: vec![],
        }),
    );
    let v: Value = serde_json::to_value(&ev).unwrap();
    assert_eq!(v["media"]["md5"], "aabbccddeeff00112233445566778899");
    assert_eq!(v["media"]["uuid"], "R020-test");
    assert_eq!(v["media"]["width"], 640);
    assert!(v["media"].get("localPath").is_none(), "absent optional field skipped");
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
        card: None,
        direction: Some(0),
        parsed: ParsedMessage { msg_type: MsgType::Text, content: content.into(), media: None },
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
            card: None,
            direction: Some(0),
            parsed: ParsedMessage { msg_type: MsgType::Text, content: "在吗".into(), media: None },
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
        state: AccountStatus::Ready,
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
