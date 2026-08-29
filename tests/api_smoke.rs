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
    state_with_scanned(store, ready, Vec::new())
}

/// `state_with`, but the registry is seeded as if the startup scan had found
/// `scanned`. `AccountRegistry::new` derives its `is_scanned` set from this
/// argument only, so pushing into `accounts_db` afterwards (what most tests do)
/// yields a *client-introduced* account — which deregisters differently.
fn state_with_scanned(
    store: Store,
    ready: bool,
    scanned: Vec<qqflow_server::db::scan::DbInfo>,
) -> Arc<AppState> {
    let (tx, _) = tokio::sync::broadcast::channel(1024);
    Arc::new(AppState {
        store: Arc::new(RwLock::new(store)),
        events: tx,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(ready)),
        token: Arc::new("test-token-123456".into()),
        sync: Arc::new(qqflow_server::sync::SyncEngine::new()),
        init: qqflow_server::server::AccountRegistry::new(
            scanned,
            qqflow_server::sync::watch::WatchConfig::default(),
            tokio::sync::watch::channel(false).1,
        ),
        export_root: Arc::new(
            std::env::temp_dir().join(format!("qqflow_smoke_export_{}", unique_suffix())),
        ),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(parking_lot::Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    })
}

fn test_state() -> Arc<AppState> {
    let mut store = Store::default();
    // A real temp file backing the image message's localPath, so the media
    // endpoint test can serve actual bytes.
    //
    // It lives under a fake `nt_data` root, and the store carries that root
    // exactly as `build_index` sets it in production. `resolve_local_path`
    // contains its result to `media_root`, so a bare temp path with
    // `media_root: None` resolves to nothing and no media would export.
    let media_root = std::env::temp_dir()
        .join(format!("qqflow_api_smoke_{}_{}", std::process::id(), unique_suffix()))
        .join("nt_data");
    std::fs::create_dir_all(&media_root).unwrap();
    let media_file = media_root.join("fake_image.jpg");
    std::fs::write(&media_file, b"\xFF\xD8 fake jpeg bytes \xFF\xD9").unwrap();
    let media_local = media_file.to_string_lossy().into_owned();
    store.media_root = Some(media_root);
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
                    // Built through the parse-time conversion so the store
                    // key is computed exactly like a decoded segment.
                    media: Some(MediaInfo::from(
                        qqflow_server::parser::proto::MediaSegment {
                            uuid: Some("R020-test".into()),
                            md5_hex: Some("aabbccddeeff00112233445566778899".into()),
                            file_name: Some("aabb.png".into()),
                            size: Some(1234),
                            width: Some(640),
                            height: Some(480),
                            local_path: Some(media_local.clone()),
                            urls: vec![],
                        },
                    )),
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
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["account"], "unregistered");
}

/// `/health` is unauthenticated, so it must disclose nothing about which
/// accounts exist on this machine: no qq, no counts, no database paths, no
/// failure reasons. Regression guard for the account-enumeration leak — the
/// old shape embedded the whole `Vec<AccountState>` here, which listed every
/// QQ profile the startup scan found plus its state.
#[tokio::test]
async fn health_discloses_no_account_detail() {
    let state = state_with(Store::default(), true);
    state.accounts.write().extend([
        qqflow_server::server::AccountState {
            qq: "10001".into(),
            state: AccountStatus::Ready,
            message_count: 4242,
            error: None,
        },
        // A second, scanned-but-unregistered profile: its mere presence used
        // to be observable, which is the disclosure this test forbids.
        qqflow_server::server::AccountState {
            qq: "20002".into(),
            state: AccountStatus::AwaitingKey,
            message_count: 0,
            error: None,
        },
    ]);
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10001".into(),
        path: std::path::PathBuf::from("C:\\secret\\path\\nt_msg.db"),
    });
    let app = build_router(state);

    let (s, v) = common::get_json(app.clone(), "/health", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["account"], "ready", "the bound account's phase is disclosed");
    let keys: Vec<_> = v.as_object().unwrap().keys().cloned().collect();
    assert_eq!(keys, ["account", "status", "version"], "exactly three fields: {v}");
    let body = v.to_string();
    for leak in ["10001", "20002", "4242", "secret", "nt_msg.db", "awaiting_key"] {
        assert!(!body.contains(leak), "/health leaked {leak:?}: {body}");
    }

    // The same data IS available behind the token.
    let (s, v) = common::get_json(
        app,
        "/api/v1/accounts",
        &[("authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    let accounts = v["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 2);
    assert_eq!(accounts[0]["qq"], "10001");
    assert_eq!(accounts[0]["state"], "ready");
    assert_eq!(accounts[0]["message_count"], 4242);
    assert_eq!(accounts[0]["db_path"], "C:\\secret\\path\\nt_msg.db");
    assert_eq!(accounts[1]["state"], "awaiting_key");
    assert!(accounts[1]["db_path"].is_null(), "no registry entry -> no path");
}

/// The detail endpoint accepts the same five token transports as every other
/// authenticated route, and refuses the request without one. It is NOT
/// ready-gated: a client polls it precisely while the account is `indexing`.
#[tokio::test]
async fn accounts_detail_auth_channels() {
    let state = state_with(Store::default(), false);
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Indexing,
        message_count: 0,
        error: None,
    });
    let app = build_router(state);
    let tok = "test-token-123456";

    let (s, v) = common::get_json(app.clone(), "/api/v1/accounts", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "no token: {v}");
    assert_eq!(v["code"], 401);
    let (s, _) = common::get_json(
        app.clone(),
        "/api/v1/accounts",
        &[("authorization", "Bearer wrong-token-000000")],
    )
    .await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "wrong token");

    for (uri, headers) in [
        ("/api/v1/accounts", vec![("authorization", "Bearer test-token-123456")]),
        ("/api/v1/accounts", vec![("x-api-key", tok)]),
        ("/api/v1/accounts?access_token=test-token-123456", vec![]),
        ("/api/v1/accounts?token=test-token-123456", vec![]),
    ] {
        let (s, v) = common::get_json(app.clone(), uri, &headers).await;
        assert_eq!(s, StatusCode::OK, "transport {uri} {headers:?} rejected: {v}");
        assert_eq!(v["accounts"][0]["state"], "indexing", "not ready-gated");
    }
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

/// All five documented auth transports must be accepted (weflow-server
/// parity). `X-Api-Key` and the `token` query/body spelling were previously
/// rejected, so a client written against the shared contract got a 401 on two
/// of the five forms the docs promise.
#[tokio::test]
async fn every_auth_transport_is_accepted() {
    const TOKEN: &str = "test-token-123456";

    // 1. Authorization: Bearer  2. X-Api-Key
    for (name, value) in [("authorization", format!("Bearer {TOKEN}")), ("x-api-key", TOKEN.into())] {
        let app = build_router(test_state());
        let resp = app
            .oneshot(
                Request::builder()
                    .uri("/api/v1/sessions")
                    .method("GET")
                    .header(name, value.clone())
                    .body(Body::empty())
                    .unwrap(),
            )
            .await
            .unwrap();
        assert_eq!(resp.status(), StatusCode::OK, "header transport {name} rejected");
    }

    // 3. ?access_token=   4. ?token=
    for key in ["access_token", "token"] {
        let (s, _) = call(
            build_router(test_state()),
            &format!("/api/v1/sessions?{key}={TOKEN}"),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "query transport ?{key}= rejected");
    }

    // 5. the same two keys inside a POST JSON body
    for key in ["access_token", "token"] {
        let (s, _) = post_json(
            build_router(test_state()),
            "/api/v1/sessions",
            json!({ key: TOKEN }),
        )
        .await;
        assert_eq!(s, StatusCode::OK, "body transport {key} rejected");
    }

    // A wrong token on any transport must still be a 401.
    let (s, _) = call(build_router(test_state()), "/api/v1/sessions?token=wrong").await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "wrong ?token= must not pass");
    let app = build_router(test_state());
    let resp = app
        .oneshot(
            Request::builder()
                .uri("/api/v1/sessions")
                .method("GET")
                .header("x-api-key", "wrong")
                .body(Body::empty())
                .unwrap(),
        )
        .await
        .unwrap();
    assert_eq!(resp.status(), StatusCode::UNAUTHORIZED, "wrong X-Api-Key must not pass");
}

#[tokio::test]
async fn sync_empty_engine_shape() {
    // Empty SyncEngine (no accounts registered): sync succeeds with 0 rows.
    // Counts-only shape, identical to weflow-server. `limit` is accepted and
    // ignored (no rows are returned to limit).
    let (s, v) = get("/api/v1/sync?access_token=test-token-123456&limit=5", true).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["newMessages"], 0);
    assert_eq!(v["revokeMessages"], 0);
    assert!(v["messages"].is_null(), "sync is a trigger, not a messages face");
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

/// `test_state` plus the three name sources that compete for `senderName`,
/// all set to different values so priority is observable rather than
/// coincidental: u_a gets a group card in 10001 AND a global remark; u_b gets
/// only a remark. A c2c conversation with u_a mirrors the group so the card's
/// scope can be checked — the same uid must NOT show its card there.
fn state_with_names() -> Arc<AppState> {
    let state = test_state();
    {
        let mut store = state.store.write();
        store
            .group_cards
            .entry(conv_key(ChatType::Group, "10001"))
            .or_default()
            .insert("u_a".into(), "张三群名片".into());
        store.names.uid_remark.insert("u_a".into(), "张三备注".into());
        store.names.uid_remark.insert("u_b".into(), "李四备注".into());
        // Same sender, private chat, no card in scope.
        let c2c = Conversation {
            chat_type: ChatType::C2c,
            talker: "u_a".into(),
            name: "张三".into(),
            msgs: vec![MessageRecord {
                rowid: 3,
                seq: 0x6771A6B70003,
                ts: seq_to_time(0x6771A6B70003),
                chat_type: ChatType::C2c,
                talker: "u_a".into(),
                from_uid: "u_a".into(),
                from_nick: "张三".into(),
                card: None,
                direction: Some(0),
                parsed: ParsedMessage {
                    msg_type: MsgType::Text,
                    content: "私聊".into(),
                    media: None,
                },
            }],
            dirty: false,
        };
        store.convs.insert(conv_key(ChatType::C2c, "u_a"), c2c);
    }
    state
}

/// `senderName` rides on every message so clients never rebuild the name
/// mapping from /api/v1/contacts + /api/v1/group-members. Priority is card >
/// remark > nick, and the card is scoped to its own group.
#[tokio::test]
async fn messages_carry_resolved_sender_name() {
    let app = build_router(state_with_names());
    let (s, v) = call(
        app.clone(),
        "/api/v1/messages?talker=10001&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // newest first: rowid 2 (u_b, remark only), rowid 1 (u_a, card + remark).
    assert_eq!(v["messages"][1]["senderUsername"], "u_a");
    assert_eq!(
        v["messages"][1]["senderName"], "张三群名片",
        "in a group the card (40090) outranks the global remark"
    );
    assert_eq!(
        v["messages"][0]["senderName"], "李四备注",
        "no card -> remark (20009) outranks the message nick"
    );

    // Same uid in a private chat: the group's card must not follow it.
    let (s, v) = call(
        app,
        "/api/v1/messages?talker=u_a&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(
        v["messages"][0]["senderName"], "张三备注",
        "c2c falls back to the remark — a group card never leaks out of its group"
    );
}

/// The card is a display name, not an identity: `senderName` may change per
/// conversation but `senderUsername` is the stable key clients dedupe on.
#[tokio::test]
async fn sender_name_is_per_conversation_but_username_is_stable() {
    let state = state_with_names();
    let store = state.store.read();
    let group = query_messages(
        &store,
        &MessageQuery { talker: "10001", limit: 10, offset: 0, start: None, end: None, keyword: None },
    )
    .0;
    let private = query_messages(
        &store,
        &MessageQuery { talker: "u_a", limit: 10, offset: 0, start: None, end: None, keyword: None },
    )
    .0;
    let g = group.iter().find(|m| m.sender_username == "u_a").unwrap();
    let c = &private[0];
    assert_eq!(g.sender_username, c.sender_username, "identity is stable");
    assert_ne!(g.sender_name, c.sender_name, "display name is per conversation");
}

/// ChatLab splits the name into two fields that the native `senderName`
/// merges: `accountName` is the account's own name (remark > nick > uid) and
/// `groupNickname` is the per-conversation card (40090). WeFlow (安装版) emits
/// them as distinct values, so a card holder must NOT show the card in
/// `accountName` — that is the whole point of having two keys.
#[tokio::test]
async fn chatlab_splits_account_name_from_group_nickname() {
    let app = build_router(state_with_names());
    let (_, native) = call(
        app.clone(),
        "/api/v1/messages?talker=10001&access_token=test-token-123456",
    )
    .await;
    let (s, chatlab) = call(
        app,
        "/api/v1/messages?talker=10001&chatlab=1&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // chatlab is chronological, the native shape newest-first: both [0]/[1]
    // below are u_a, who holds a card AND a remark.
    let msg = &chatlab["messages"][0];
    assert_eq!(msg["sender"], "u_a");
    assert_eq!(msg["accountName"], "张三备注", "account name is the remark, not the card");
    assert_eq!(msg["groupNickname"], "张三群名片", "the card lands in groupNickname");
    assert_ne!(
        msg["accountName"], msg["groupNickname"],
        "the two keys must not be the same value"
    );
    // The native surface keeps the merged, card-wins meaning downstream pins.
    assert_eq!(native["messages"][1]["senderName"], "张三群名片");

    let members = chatlab["members"].as_array().unwrap();
    let member = members
        .iter()
        .find(|m| m["platformId"] == "u_a")
        .expect("u_a in members");
    assert_eq!(member["accountName"], "张三备注", "members agree with messages");
    assert_eq!(member["groupNickname"], "张三群名片");
    // u_b has a remark but no card: groupNickname is empty rather than a
    // duplicate of accountName.
    let b = members.iter().find(|m| m["platformId"] == "u_b").expect("u_b in members");
    assert_eq!(b["accountName"], "李四备注");
    assert_eq!(b["groupNickname"], "");
    // One entry per sender, not one per message.
    assert_eq!(members.len(), 2, "members are deduped: {members:?}");

    // A group card never leaks into the private chat with the same uid.
    let app = build_router(state_with_names());
    let (s, c2c) = call(
        app,
        "/api/v1/messages?talker=u_a&chatlab=1&access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(c2c["messages"][0]["accountName"], "张三备注");
    assert_eq!(c2c["messages"][0]["groupNickname"], "", "c2c has no card");
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

/// The Pull face emits canonical ChatLab 0.0.2 type codes, which are a
/// DIFFERENT code space from the native `localType` on /api/v1/messages
/// (text is 0 in both, but an image is 1 in ChatLab and 3 natively).
#[tokio::test]
async fn chatlab_type_is_the_chatlab_code_space_not_local_type() {
    let app = build_router(state_with_names());
    let (s, pull) = call(
        app.clone(),
        "/api/v1/sessions/10001/messages?access_token=test-token-123456",
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // Chronological: row 1 text, row 2 image.
    assert_eq!(pull["messages"][0]["type"], 0, "text is 0 in the ChatLab space");
    assert_eq!(pull["messages"][1]["type"], 1, "image is 1 in the ChatLab space");
    // Same rows through the chatlab branch of /api/v1/messages agree...
    let (_, envelope) = call(
        app.clone(),
        "/api/v1/messages?talker=10001&chatlab=1&access_token=test-token-123456",
    )
    .await;
    assert_eq!(envelope["messages"][0]["type"], 0);
    assert_eq!(envelope["messages"][1]["type"], 1);
    // ...while the native face keeps the platform code downstream pins: the
    // same image row is localType 3 there, not 1.
    let (_, native) = call(
        app,
        "/api/v1/messages?talker=10001&access_token=test-token-123456",
    )
    .await;
    assert_eq!(native["messages"][0]["localType"], 3, "newest first: the image row");
    assert_eq!(native["messages"][1]["localType"], 0);
}

/// WeFlow's Pull contract has no 400 semantics for pagination, so malformed
/// `limit`/`offset` fall back to the defaults instead of rejecting the call.
#[tokio::test]
async fn chatlab_pull_tolerates_malformed_pagination() {
    for q in ["limit=abc", "offset=abc", "limit=&offset=", "limit=0", "limit=-3"] {
        let (s, v) = get(
            &format!("/api/v1/sessions/10001/messages?{q}&access_token=test-token-123456"),
            false,
        )
        .await;
        assert_eq!(s, StatusCode::OK, "{q} must not 400");
        assert_eq!(v["messages"].as_array().unwrap().len(), 2, "{q} served the page");
    }
}

/// `meta.ownerId` is the bound account on both ChatLab faces (WeFlow emits its
/// own wxid there). No account is bound in these fixtures, so it is empty —
/// the point is that the key exists and both faces agree.
#[tokio::test]
async fn chatlab_meta_carries_owner_id_on_both_faces() {
    let app = build_router(state_with_names());
    let (_, pull) = call(
        app.clone(),
        "/api/v1/sessions/10001/messages?access_token=test-token-123456",
    )
    .await;
    let (_, envelope) = call(
        app,
        "/api/v1/messages?talker=10001&chatlab=1&access_token=test-token-123456",
    )
    .await;
    assert!(pull["meta"]["ownerId"].is_string());
    assert_eq!(pull["meta"]["ownerId"], envelope["meta"]["ownerId"]);
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
    assert!(v.get("mediaId").is_none(), "no media -> no mediaId");
}

#[test]
fn event_json_carries_media() {
    // The raw "45812" local path is fake but PRESENT in the parsed media —
    // it must still never appear in the pushed event: paths are
    // machine-local, mostly stale, and leak host layout downstream.
    let info = MediaInfo::from(qqflow_server::parser::proto::MediaSegment {
        uuid: Some("R020-test".into()),
        md5_hex: Some("aabbccddeeff00112233445566778899".into()),
        file_name: Some("aabb.png".into()),
        size: Some(1234),
        width: Some(640),
        height: Some(480),
        local_path: Some(r"C:\SomeUser\nt_qq\nt_data\Pic\2026-08\aabb.png".into()),
        urls: vec![],
    });
    let ev = Event::message_new(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        43,
        Some("李四".into()),
        "[image]".into(),
        1782835200,
        Some(info),
        Some("aabbccddeeff00112233445566778899".into()), // registered -> fetchable
    );
    let v: Value = serde_json::to_value(&ev).unwrap();
    assert_eq!(v["media"]["md5"], "aabbccddeeff00112233445566778899");
    assert_eq!(v["media"]["uuid"], "R020-test");
    assert_eq!(v["media"]["width"], 640);
    assert!(
        v["media"].get("localPath").is_none(),
        "the raw QQ cache path must never be pushed"
    );
    assert_eq!(v["mediaId"], "aabbccddeeff00112233445566778899");
}

#[test]
fn event_json_media_id_omitted_when_not_fetchable() {
    // The media object rides along, but without a registered live path the
    // event must not promise a servable /api/v1/media/{id} — same rule as
    // the REST mediaId filter, applied to the push channel too.
    let info = MediaInfo::from(qqflow_server::parser::proto::MediaSegment {
        md5_hex: Some("aabbccddeeff00112233445566778899".into()),
        local_path: None,
        ..Default::default()
    });
    let ev = Event::message_new(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        44,
        Some("王五".into()),
        "[image]".into(),
        1782835200,
        Some(info),
        None, // not registered -> no mediaId
    );
    let v: Value = serde_json::to_value(&ev).unwrap();
    assert!(v["media"]["md5"].is_string(), "media object still rides along");
    assert!(v.get("mediaId").is_none(), "mediaId omitted when not fetchable");
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

/// A client that echoes BOTH cursors back verbatim must still see every row.
///
/// `chatlab_pull_boundary_second_pages_cleanly` resumes with `nextSince`
/// alone, so it cannot observe this: when `nextOffset` also advanced, the
/// exclusive `since` filter already dropped the served rows and the offset
/// then skipped the same count a second time — silently losing every row in
/// between. Driving the documented cursor pair is the only shape that catches
/// it, so this test paginates at `limit=1` and asserts a full drain.
#[tokio::test]
async fn chatlab_pull_drains_when_client_echoes_both_cursors() {
    let app = build_router(ts_boundary_state());
    let mut since: Option<i64> = None;
    let mut offset: u64 = 0;
    let mut ids: Vec<String> = Vec::new();
    let mut pages = 0;

    loop {
        pages += 1;
        assert!(pages <= 10, "cursor must terminate; looped {pages} times");
        let mut url = format!(
            "/api/v1/sessions/10001/messages?limit=1&offset={offset}&access_token=test-token-123456"
        );
        if let Some(s) = since {
            url.push_str(&format!("&since={s}"));
        }
        let (status, v) = call(app.clone(), &url).await;
        assert_eq!(status, StatusCode::OK);

        for m in v["messages"].as_array().unwrap() {
            ids.push(m["platformMessageId"].as_str().unwrap().to_string());
        }
        if !v["sync"]["hasMore"].as_bool().unwrap() {
            assert_eq!(v["sync"]["nextOffset"], 0, "drained cursor resets offset");
            break;
        }
        since = Some(v["sync"]["nextSince"].as_i64().unwrap());
        offset = v["sync"]["nextOffset"].as_u64().unwrap();
    }

    // The fixture holds 7 rows: 5 sharing one second, then 2 in the next.
    assert_eq!(ids.len(), 7, "every row served exactly once, got {ids:?}");
    let unique: std::collections::BTreeSet<&String> = ids.iter().collect();
    assert_eq!(unique.len(), 7, "no duplicates across pages: {ids:?}");
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
async fn accounts_validation_rejects_bad_input() {
    let state = test_state();
    // A scanned-style entry for 10002, so key validation is reachable
    // (resolve succeeds without a db_path). Nothing is bound yet.
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10002".into(),
        path: std::env::temp_dir().join("qqflow_smoke_10002.db"),
    });
    let app = build_router(state);
    let tok = "test-token-123456";

    // Malformed key -> invalid_key (not an HTTP error). The path resolved
    // from the registry, so db_path rides along; the account never had a
    // state entry, so `status` is omitted.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "10002", "key": "short"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "invalid_key");
    assert!(v["status"].is_null(), "unknown account -> status omitted: {v}");
    assert!(
        v["db_path"].as_str().unwrap().ends_with("qqflow_smoke_10002.db"),
        "resolved db_path echoed: {v}"
    );

    // Unknown qq without a db_path -> unknown_qq, no path to echo.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "999", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "unknown_qq");
    assert!(v["status"].is_null(), "unknown account -> status omitted: {v}");
    assert!(v["db_path"].is_null(), "unresolved -> db_path omitted: {v}");

    // Unresolvable db_path -> invalid_db_path.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "999", "key": "0123456789abcdef", "db_path": "Z:\\nonexistent"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "invalid_db_path");
    assert!(v["db_path"].is_null(), "unresolved -> db_path omitted: {v}");

    // Missing qq / key -> 400 envelope.
    let (s, v) = post_json(app, "/api/v1/accounts", json!({"access_token": tok})).await;
    assert_eq!(s, StatusCode::BAD_REQUEST);
    assert_eq!(v["code"], 400);
}

/// Re-registering the SAME account is idempotent, and the reject replies
/// carry the account's unchanged state-machine value.
#[tokio::test]
async fn accounts_idempotent_for_the_bound_account() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    let app = build_router(state);
    let tok = "test-token-123456";

    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": tok, "qq": "10001", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "already_ready");
    assert_eq!(v["status"], "ready");
}

/// An account left in `error` by a failed registration keeps the binding and
/// may retry: a malformed retry key is rejected without changing the state,
/// so `status` still reports `error` ("this call was rejected AND the account
/// is still broken").
#[tokio::test]
async fn accounts_error_state_echoes_status_on_reject() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10003".into(),
        state: AccountStatus::Error,
        message_count: 0,
        error: Some("解密失败".into()),
    });
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10003".into(),
        path: std::env::temp_dir().join("qqflow_smoke_10003.db"),
    });
    let app = build_router(state);

    let (s, v) = post_json(
        app,
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10003", "key": "short"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "invalid_key");
    assert_eq!(v["status"], "error");
}

/// The store is a single global index with no account dimension, so a second
/// account must be REJECTED rather than silently overwriting the first one's
/// data. Before this, registering a second qq replaced the whole index and
/// cross-contaminated the sync watermarks (the two databases have independent
/// rowid spaces), while both accounts reported `ready`.
#[tokio::test]
async fn accounts_rejects_a_second_account() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    // Resolvable, so a conflict reply cannot be confused with unknown_qq.
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10002".into(),
        path: std::env::temp_dir().join("qqflow_smoke_10002.db"),
    });
    let app = build_router(state.clone());

    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10002", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "account_conflict");
    assert_eq!(v["qq"], "10002", "the rejected request's own account");
    assert_eq!(v["occupied_by"], "10001");
    assert_eq!(v["occupied_status"], "ready");

    // The incumbent is untouched and the loser gained no state entry.
    let accs = state.accounts.read();
    assert_eq!(accs.len(), 1, "the rejected account must not be recorded: {accs:?}");
    assert_eq!(accs[0].qq, "10001");
    assert_eq!(accs[0].state, AccountStatus::Ready);
    assert_eq!(accs[0].message_count, 2);
}

/// An account in `error` still holds the binding: a transient decrypt failure
/// must not hand the server to a different account behind the operator's back.
#[tokio::test]
async fn accounts_conflict_survives_error_state() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Error,
        message_count: 0,
        error: Some("解密失败".into()),
    });
    state.init.accounts_db.lock().push(qqflow_server::db::scan::DbInfo {
        qq: "10002".into(),
        path: std::env::temp_dir().join("qqflow_smoke_10002.db"),
    });
    let app = build_router(state);

    let (s, v) = post_json(
        app,
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10002", "key": "0123456789abcdef"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "account_conflict");
    assert_eq!(v["occupied_by"], "10001");
    assert_eq!(v["occupied_status"], "error");
}

/// Two different accounts registering concurrently: exactly one wins. The
/// occupancy decision lives inside `begin_indexing`'s write lock, so the loser
/// cannot slip past the handler's fast-path check.
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn accounts_concurrent_different_qq_one_winner() {
    let state = test_state();
    let dir = std::env::temp_dir().join(format!("qqflow_smoke_race_{}", unique_suffix()));
    std::fs::create_dir_all(&dir).unwrap();
    // Both resolvable: whichever loses must lose on occupancy, not on paths.
    // The files are not real databases, so the winner's background build
    // fails into `error` — irrelevant here, the race is decided before that.
    let mut entries = Vec::new();
    for qq in ["10001", "10002"] {
        let path = dir.join(format!("{qq}.db"));
        std::fs::write(&path, b"not a database").unwrap();
        entries.push(qqflow_server::db::scan::DbInfo { qq: qq.into(), path });
    }
    state.init.accounts_db.lock().extend(entries);
    let app = build_router(state.clone());

    let a = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10001", "key": "0123456789abcdef"}),
    );
    let b = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10002", "key": "0123456789abcdef"}),
    );
    let ((sa, va), (sb, vb)) = tokio::join!(a, b);
    assert_eq!(sa, StatusCode::OK);
    assert_eq!(sb, StatusCode::OK);

    let states = [va["state"].as_str().unwrap(), vb["state"].as_str().unwrap()];
    let accepted = states.iter().filter(|s| **s == "accepted").count();
    let conflicts = states.iter().filter(|s| **s == "account_conflict").count();
    assert_eq!(accepted, 1, "exactly one registration wins: {va} / {vb}");
    assert_eq!(conflicts, 1, "the other is rejected as a conflict: {va} / {vb}");

    // Only the winner is bound; the loser left no trace.
    let bound: Vec<_> = state
        .accounts
        .read()
        .iter()
        .filter(|a| a.state != AccountStatus::AwaitingKey)
        .map(|a| a.qq.clone())
        .collect();
    assert_eq!(bound.len(), 1, "exactly one account is bound: {bound:?}");
    let _ = std::fs::remove_dir_all(&dir);
}

/// Deregistration destroys the index, so an unauthenticated caller must not
/// reach it — on either the DELETE route or its POST alias.
#[tokio::test]
async fn deregister_requires_a_token() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    let app = build_router(state.clone());

    let (s, _) = common::delete_json(app.clone(), "/api/v1/accounts/10001", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "DELETE without a token");
    let (s, _) =
        common::delete_json(app.clone(), "/api/v1/accounts/10001?access_token=wrong", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "DELETE with a wrong token");
    let (s, _) = post_json(app.clone(), "/api/v1/accounts/10001/deregister", json!({})).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED, "the POST alias is gated too");

    // Every rejected attempt left the binding and the index alone.
    assert_eq!(
        state.accounts.read().first().map(|a| a.state),
        Some(AccountStatus::Ready),
        "still bound"
    );
    assert!(!state.store.read().convs.is_empty(), "index intact");
}

/// Nothing bound -> `not_registered`, HTTP 200. Idempotent by design: a client
/// retrying a deregistration it already completed must not have to special-case
/// an error response.
#[tokio::test]
async fn deregister_unbound_is_idempotent() {
    let app = build_router(state_with(Store::default(), false));
    let (s, v) = common::delete_json(
        app,
        "/api/v1/accounts/10001",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["state"], "not_registered");
    assert_eq!(v["qq"], "10001");
    assert_eq!(v["index_cleared"], false);
    assert_eq!(v["purged_dirs"], 0);
}

/// The path `qq` is a safety interlock, not a selector: naming the wrong
/// account reports `qq_mismatch` and leaves the incumbent completely untouched,
/// rather than deregistering whatever happens to be bound.
#[tokio::test]
async fn deregister_enforces_the_qq_interlock() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    let app = build_router(state.clone());

    let (s, v) = common::delete_json(
        app.clone(),
        "/api/v1/accounts/99999",
        &[("X-Api-Key", "test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK, "a business verdict, not an HTTP error");
    assert_eq!(v["state"], "qq_mismatch");
    assert_eq!(v["occupied_by"], "10001");
    assert_eq!(v["occupied_status"], "ready");
    assert_eq!(v["index_cleared"], false);

    assert_eq!(state.accounts.read().first().map(|a| a.state), Some(AccountStatus::Ready));
    assert!(!state.store.read().convs.is_empty(), "the incumbent's index survives");
    assert!(state.ready.load(std::sync::atomic::Ordering::SeqCst), "still ready");
}

/// The full round trip through HTTP: a ready account deregisters, and every
/// observable surface agrees the server is back at its unregistered boot state
/// — `/health` scalar, the detail endpoint, and the readiness-gated business
/// endpoints. Then a re-registration is accepted, proving the unbind is not a
/// one-way door.
#[tokio::test]
async fn deregister_returns_the_server_to_its_boot_state() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    // Client-introduced (not in `AccountRegistry::new`), so the entry and its
    // db_path should be forgotten entirely.
    let db = std::env::temp_dir().join(format!("qqflow_smoke_dereg_{}.db", unique_suffix()));
    std::fs::write(&db, b"not a database").unwrap();
    state
        .init
        .accounts_db
        .lock()
        .push(qqflow_server::db::scan::DbInfo { qq: "10001".into(), path: db.clone() });
    let app = build_router(state.clone());

    // Baseline: ready, serving, disclosing the account.
    let (s, v) = call(app.clone(), "/health").await;
    assert_eq!((s, &v["status"], &v["account"]), (StatusCode::OK, &json!("ok"), &json!("ready")));
    let (s, _) = common::get_json(
        app.clone(),
        "/api/v1/sessions",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK, "business endpoints serve while ready");

    let (s, v) = common::delete_json(
        app.clone(),
        "/api/v1/accounts/10001",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["state"], "deregistered");
    assert_eq!(v["previous_status"], "ready", "what it was when the request landed");
    assert_eq!(v["index_cleared"], true);
    assert_eq!(v["purged_media"], false, "purge_media defaults to false");
    assert_eq!(v["purged_dirs"], 0);

    // `/health` is scalar again, and readiness dropped.
    let (s, v) = call(app.clone(), "/health").await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "starting");
    assert_eq!(v["account"], "unregistered");

    // The detail endpoint forgot the client-introduced account outright.
    let (s, v) = common::get_json(
        app.clone(),
        "/api/v1/accounts",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["accounts"].as_array().map(|a| a.len()), Some(0), "no entry left: {v}");

    // Readiness-gated endpoints stop serving the dropped index.
    let (s, _) = common::get_json(
        app.clone(),
        "/api/v1/sessions",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::SERVICE_UNAVAILABLE, "the index is gone");
    assert!(state.store.read().convs.is_empty());

    // And the binding is free: a fresh registration is accepted (its
    // background build then fails on the fake db file, which is fine — the
    // point is that the occupancy check no longer rejects it).
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts",
        json!({"access_token": "test-token-123456", "qq": "10002",
               "key": "0123456789abcdef", "db_path": db.to_string_lossy()}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "accepted", "the binding was released: {v}");

    let _ = std::fs::remove_file(&db);
}

/// A scanned account is not forgotten, it reverts to `awaiting_key` and keeps
/// its `db_path`: the startup scan will find it again next boot, so reporting
/// it as gone would be a lie the client then has to un-learn.
#[tokio::test]
async fn deregister_reverts_a_scanned_account_to_awaiting_key() {
    let db = std::env::temp_dir().join(format!("qqflow_smoke_scanned_{}.db", unique_suffix()));
    let state = state_with_scanned(
        Store::default(),
        true,
        vec![qqflow_server::db::scan::DbInfo { qq: "10001".into(), path: db.clone() }],
    );
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 9,
        error: None,
    });
    let app = build_router(state.clone());

    let (s, v) = common::delete_json(
        app.clone(),
        "/api/v1/accounts/10001",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "deregistered");
    assert_eq!(v["previous_status"], "ready");

    let (s, v) = common::get_json(
        app,
        "/api/v1/accounts",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    // Panics if the entry is gone, which is the assertion: the scan result survives.
    let entry = common::account_entry(&v, "10001");
    assert_eq!(entry["state"], "awaiting_key");
    assert_eq!(entry["message_count"], 0);
    assert_eq!(
        entry["db_path"], json!(db.to_string_lossy()),
        "a scanned account keeps its path"
    );
}

/// `purge_media=1` deletes only the export layout the server itself writes.
/// The export root comes from `--media-export-dir` and may be a directory the
/// operator keeps other things in, so anything outside `<talker>/<kind>` must
/// survive — and the root itself is never removed.
#[tokio::test]
async fn deregister_purge_media_stays_inside_the_export_layout() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Ready,
        message_count: 2,
        error: None,
    });
    // `test_state`'s only conversation has talker "10001".
    let root = state.export_root.clone();
    for (dir, file) in [("10001/images", "a.jpg"), ("10001/notes", "keep.txt")] {
        std::fs::create_dir_all(root.join(dir)).unwrap();
        std::fs::write(root.join(dir).join(file), b"x").unwrap();
    }
    std::fs::write(root.join("operator-notes.txt"), b"keep me").unwrap();
    let app = build_router(state.clone());

    let (s, v) = common::delete_json(
        app,
        "/api/v1/accounts/10001?purge_media=1",
        &[("Authorization", "Bearer test-token-123456")],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "deregistered");
    assert_eq!(v["purged_media"], true, "the flag echoes the request");
    assert_eq!(v["purged_dirs"], 1, "only images existed: {v}");

    assert!(!root.join("10001/images").exists(), "exported media removed");
    assert!(root.join("10001/notes/keep.txt").exists(), "unknown subdir untouched");
    assert!(root.join("operator-notes.txt").exists(), "export root never wiped");
    assert!(root.exists(), "the root itself survives");
    let _ = std::fs::remove_dir_all(&*root);
}

/// The POST alias exists for clients and proxies that cannot issue DELETE, and
/// it must be the same handler — same verdicts, same body-carried parameters.
#[tokio::test]
async fn deregister_post_alias_matches_the_delete_route() {
    let state = test_state();
    state.accounts.write().push(qqflow_server::server::AccountState {
        qq: "10001".into(),
        state: AccountStatus::Error,
        message_count: 0,
        error: Some("解密失败".into()),
    });
    let app = build_router(state.clone());

    // An account stuck in `error` still holds the binding, so clearing it is
    // exactly what the alias has to be able to do. Token and `purge_media`
    // both arrive in the JSON body here, not the query string.
    let (s, v) = post_json(
        app.clone(),
        "/api/v1/accounts/10001/deregister",
        json!({"access_token": "test-token-123456", "purge_media": false}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "deregistered");
    assert_eq!(v["previous_status"], "error", "an error account can be cleared");
    assert!(qqflow_server::server::bound_account(&state.accounts.read()).is_none());

    // Repeating it is idempotent, same as the DELETE route.
    let (s, v) = post_json(
        app,
        "/api/v1/accounts/10001/deregister",
        json!({"access_token": "test-token-123456"}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "not_registered");
}
