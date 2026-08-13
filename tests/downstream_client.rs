//! Downstream-client simulation: GET/POST against the REAL HTTP layer with a
//! REAL QQ database (same env-var contract as `real_db_groundtruth`):
//!   QQFLOW_TEST_DB_ROOT - Tencent Files-style root (<dir>/<qq>/nt_qq/nt_db/nt_msg.db)
//!   QQFLOW_TEST_DB_KEY  - 16-byte printable ASCII SQLCipher key
//!
//! Run:
//!   powershell -File scripts\build.ps1 test --test downstream_client -- --ignored --nocapture
//!
//! Exercises the exact request shapes a WeFlow-style client sends: auth via
//! Bearer header / `?access_token=` / POST JSON body, GET+POST parameter
//! transport, error envelopes, ChatLab Pull pagination (`nextSince` /
//! `nextOffset`), group members with message counts, manual sync, and the
//! SSE content-type — all against real QQ chat data.

use std::collections::BTreeSet;
use std::path::PathBuf;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::RwLock;
use qqflow_server::db::{decrypt, mirror::Mirror, scan};
use qqflow_server::server::{build_router, AccountState};
use qqflow_server::store::{index, AppState};
use qqflow_server::sync::{AccountSync, SyncEngine};
use serde_json::{json, Value};
use tower::ServiceExt;

const TEST_TOKEN: &str = "downstream-client-test-token";

/// Build the real server pipeline (scan + mirror + decrypt + index + router)
/// for the first scanned account. Returns None when the env vars are absent.
fn build_real_app() -> Option<(axum::Router, Arc<AppState>, String, PathBuf)> {
    let root = std::env::var("QQFLOW_TEST_DB_ROOT").ok()?;
    let key = std::env::var("QQFLOW_TEST_DB_KEY").ok()?;

    let accounts = scan::scan_accounts(Some(std::path::Path::new(&root)))
        .expect("scan QQFLOW_TEST_DB_ROOT");
    assert!(!accounts.is_empty(), "no accounts under {root}");
    let info = &accounts[0]; // single-account scope
    println!("[CLIENT] account {} (db: {})", info.qq, info.path.display());

    let mirror_dir =
        std::env::temp_dir().join(format!("qqflow_downstream_mirror_{}", std::process::id()));
    let mirror = Mirror::new(info, &mirror_dir).expect("mirror real db");
    let mirror = Arc::new(parking_lot::Mutex::new(mirror));
    let conn = decrypt::open_decrypted(&mirror.lock().main_path, &key).expect("decrypt real db");
    let store = index::build_index(&conn).expect("index real db");
    let count: usize = store.convs.values().map(|c| c.msgs.len()).sum();
    println!(
        "[CLIENT] indexed {count} messages in {} conversations",
        store.convs.len()
    );

    let store = Arc::new(RwLock::new(store));
    let (tx, _rx) = tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(1024);
    // Register the real per-account sync so POST /api/v1/sync runs a genuine
    // incremental pass (mirror refresh + decrypt + read_new).
    let account = Arc::new(AccountSync::new(mirror, key, store.clone(), tx.clone()));
    let sync = Arc::new(SyncEngine::new());
    sync.register(account);
    let state = Arc::new(AppState {
        store,
        events: tx,
        accounts: Arc::new(RwLock::new(vec![AccountState {
            qq: info.qq.clone(),
            state: "ready".into(),
            message_count: count,
            error: None,
        }])),
        ready: Arc::new(AtomicBool::new(true)),
        token: Arc::new(TEST_TOKEN.into()),
        sync,
    });
    let app = build_router(state.clone());
    Some((app, state, info.qq.clone(), mirror_dir))
}

/// Downstream-client GET (optional extra headers, e.g. Bearer auth).
async fn client_get(
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

/// Downstream-client POST with a JSON body (parameters and/or token inside).
async fn client_post(
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

#[tokio::test]
#[ignore]
async fn downstream_client_real_db() {
    let Some((app, state, qq, mirror_dir)) = build_real_app() else {
        println!("[CLIENT] SKIPPED: QQFLOW_TEST_DB_ROOT / QQFLOW_TEST_DB_KEY not set");
        return;
    };
    let token = state.token.as_str();

    // ---- 1. health: no auth, ready state --------------------------------
    let (s, v) = client_get(app.clone(), "/health", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "ok");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    let accounts = v["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0]["qq"], qq);
    assert_eq!(accounts[0]["state"], "ready");
    assert!(accounts[0].get("error").is_none());
    let indexed = accounts[0]["message_count"].as_u64().unwrap() as usize;
    assert!(indexed > 0, "real db must have messages");

    // ---- 2. auth: business endpoints reject missing tokens --------------
    let (s, v) = client_get(app.clone(), "/api/v1/messages?talker=x", &[]).await;
    assert_eq!(s, StatusCode::UNAUTHORIZED);
    assert_eq!(v["success"], false);
    assert_eq!(v["code"], 401);

    // ---- 3. sessions: Bearer header, WeFlow shape -----------------------
    let (s, v) = client_get(
        app.clone(),
        "/api/v1/sessions?limit=50",
        &[("authorization", &format!("Bearer {token}"))],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    let sessions = v["sessions"].as_array().unwrap();
    assert!(!sessions.is_empty(), "real db must have conversations");
    for sess in sessions {
        assert!(sess["username"].is_string());
        assert!(sess["displayName"].is_string());
        assert!(matches!(sess["type"].as_i64(), Some(1 | 2)));
        assert!(sess["lastTimestamp"].is_number());
        assert_eq!(sess["unreadCount"], 0, "v1 unread count is always 0");
    }
    let first_talker = sessions[0]["username"].as_str().unwrap().to_string();
    let group_id = sessions
        .iter()
        .find(|s| s["type"].as_i64() == Some(2))
        .map(|s| s["username"].as_str().unwrap().to_string());
    println!(
        "[CLIENT] {} sessions, first={first_talker}, group={}",
        v["count"],
        group_id.as_deref().unwrap_or("(none)")
    );
    // Display-name resolution: groups fall back to the group id until a
    // rename system message is seen; private chats carry the peer nickname.
    for sess in sessions.iter().take(5) {
        println!(
            "[CLIENT]   session {} (type {}) -> displayName {:?}",
            sess["username"],
            sess["type"],
            sess["displayName"].as_str().unwrap_or_default()
        );
    }

    // ---- 4. messages: GET via ?access_token=, field contract ------------
    let uri = format!("/api/v1/messages?talker={first_talker}&limit=20&access_token={token}");
    let (s, v) = client_get(app.clone(), &uri, &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["talker"], first_talker);
    assert_eq!(v["media"]["enabled"], false);
    let msgs = v["messages"].as_array().unwrap();
    assert!(!msgs.is_empty(), "real conversation must have messages");
    assert_eq!(v["count"].as_u64().unwrap() as usize, msgs.len());
    for m in msgs {
        assert!(m["localId"].is_number());
        assert!(m["serverId"].is_string());
        assert!(m["localType"].is_number());
        assert!(m["createTime"].is_number());
        assert_eq!(m["isSend"], 0, "v1 direction is always 0");
        assert!(m["senderUsername"].is_string());
        assert!(m["content"].is_string());
        assert!(m["rawContent"].is_string());
        assert!(m["parsedContent"].is_string());
        // mediaType appears iff the message is image/voice/video.
        if let Some(mt) = m.get("mediaType") {
            assert!(matches!(mt.as_str(), Some("image" | "voice" | "video")));
            assert!(matches!(m["localType"].as_i64(), Some(3..=5)));
        }
    }
    // newest first
    let ts_first = msgs[0]["createTime"].as_i64().unwrap();
    let ts_last = msgs.last().unwrap()["createTime"].as_i64().unwrap();
    assert!(ts_first >= ts_last, "messages must be newest first");
    println!("[CLIENT] messages({first_talker}): {} rows, ts=[{ts_last},{ts_first}]", msgs.len());

    // ---- 5. messages: POST body transport + YYYYMMDD bounds + token in body
    let (s, v) = client_post(
        app.clone(),
        "/api/v1/messages",
        &[],
        json!({
            "access_token": token,
            "talker": first_talker,
            "limit": 5,
            "start": "20200101",
            "end": "20301231",
        }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    let posted = v["messages"].as_array().unwrap();
    assert_eq!(v["count"].as_u64().unwrap() as usize, posted.len());
    assert!(posted.len() <= 5);
    println!("[CLIENT] POST messages: {} rows (limit=5, YYYYMMDD bounds)", posted.len());

    // ---- 6. ChatLab Pull: paginate with nextSince/nextOffset, no repeats
    let mut seen: BTreeSet<String> = BTreeSet::new();
    let mut uri = format!("/api/v1/sessions/{first_talker}/messages?limit=50&access_token={token}");
    let (mut prev_since, mut prev_offset) = (i64::MIN, i64::MIN);
    for page_no in 0..3 {
        let (s, v) = client_get(app.clone(), &uri, &[]).await;
        assert_eq!(s, StatusCode::OK);
        assert_eq!(v["chatlab"]["version"], "0.0.2");
        assert_eq!(v["meta"]["platform"], "qq");
        assert_eq!(v["meta"]["groupId"], first_talker);
        assert!(v["sync"]["watermark"].as_i64().unwrap() > 0);
        let page = v["messages"].as_array().unwrap();
        let dupes = page
            .iter()
            .filter(|m| seen.contains(m["platformMessageId"].as_str().unwrap()))
            .count();
        assert_eq!(dupes, 0, "page {page_no} must not repeat messages");
        seen.extend(page.iter().map(|m| m["platformMessageId"].as_str().unwrap().to_string()));
        let has_more = v["sync"]["hasMore"].as_bool().unwrap();
        println!("[CLIENT] chatlab page {page_no}: {} msgs, hasMore={has_more}", page.len());
        if !has_more {
            break;
        }
        let since = v["sync"]["nextSince"].as_i64().unwrap();
        let offset = v["sync"]["nextOffset"].as_i64().unwrap();
        // Cursor must advance (exclusive since + completed ts groups).
        assert!(
            since > prev_since || offset > prev_offset,
            "pagination cursor must advance"
        );
        prev_since = since;
        prev_offset = offset;
        uri = format!(
            "/api/v1/sessions/{first_talker}/messages?since={since}&offset={offset}&limit=50&access_token={token}"
        );
    }
    println!("[CLIENT] chatlab pull: {} unique messages", seen.len());

    // ---- 7. ChatLab Pull 404 envelope for an unknown session ------------
    let (s, v) = client_get(
        app.clone(),
        &format!("/api/v1/sessions/nonexistent-session-999/messages?access_token={token}"),
        &[],
    )
    .await;
    assert_eq!(s, StatusCode::NOT_FOUND);
    assert_eq!(v["success"], false);
    assert_eq!(v["code"], 404);

    // ---- 8. contacts: uid -> nickname map -------------------------------
    let (s, v) = client_get(
        app.clone(),
        &format!("/api/v1/contacts?limit=10&access_token={token}"),
        &[],
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    for c in v["contacts"].as_array().unwrap() {
        assert!(c["username"].is_string());
        assert!(c["displayName"].is_string());
        assert!(c["nickname"].is_string());
        assert_eq!(c["remark"], "", "v1 remark is always empty");
        assert_eq!(c["alias"], "", "v1 alias is always empty");
        assert_eq!(c["avatarUrl"], "", "v1 avatarUrl is always empty");
        assert_eq!(c["type"], "friend");
    }
    println!("[CLIENT] contacts: {} rows", v["count"]);
    for c in v["contacts"].as_array().unwrap().iter().take(5) {
        println!(
            "[CLIENT]   uid {} -> nickname {:?}",
            c["username"],
            c["displayName"].as_str().unwrap_or_default()
        );
    }

    // ---- 9. group members: GET with counts + POST with talker alias -----
    match group_id {
        Some(gid) => {
            let (s, v) = client_get(
                app.clone(),
                &format!("/api/v1/group-members?chatroomId={gid}&includeMessageCounts=1&access_token={token}"),
                &[],
            )
            .await;
            assert_eq!(s, StatusCode::OK);
            assert_eq!(v["success"], true);
            assert_eq!(v["chatroomId"], gid);
            assert_eq!(v["fromCache"], false);
            assert!(v["updatedAt"].is_number());
            let members = v["members"].as_array().unwrap();
            assert!(!members.is_empty(), "group conversation must have members");
            for m in members {
                assert!(m["wxid"].is_string());
                assert!(m["displayName"].is_string());
                assert!(m["groupNickname"].is_string());
                assert!(m["messageCount"].is_number(), "includeMessageCounts=1");
                assert_eq!(m["isOwner"], false);
                assert_eq!(m["isFriend"], false);
            }
            println!("[CLIENT] group-members({gid}): {} rows", members.len());

            // POST transport, `talker` alias for chatroomId.
            let (s, v) = client_post(
                app.clone(),
                "/api/v1/group-members",
                &[],
                json!({ "access_token": token, "talker": gid }),
            )
            .await;
            assert_eq!(s, StatusCode::OK);
            assert_eq!(v["success"], true);
            assert_eq!(v["chatroomId"], gid);
            assert!(v["members"].as_array().unwrap().iter().all(|m| m.get("messageCount").is_none()));
        }
        None => println!("[CLIENT] no group session found, skipping group-members"),
    }

    // ---- 10. manual sync: real incremental pass over the real db --------
    let (s, v) = client_post(
        app.clone(),
        "/api/v1/sync",
        &[],
        json!({ "access_token": token, "limit": 10 }),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["success"], true);
    assert_eq!(v["hasMore"], false);
    assert!(v["synced"].as_i64().unwrap() >= 0);
    assert_eq!(v["count"].as_u64().unwrap() as usize, v["messages"].as_array().unwrap().len());
    println!("[CLIENT] manual sync: synced={} returned={}", v["synced"], v["count"]);

    // ---- 11. SSE: connect shape (content-type; body streams live) -------
    let resp = app
        .oneshot(
            Request::builder()
                .uri(format!("/api/v1/push/messages?access_token={token}"))
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
    println!("[CLIENT] SSE connect: 200 text/event-stream");

    let _ = std::fs::remove_dir_all(&mirror_dir);
    println!("[CLIENT] all downstream-client checks passed");
}
