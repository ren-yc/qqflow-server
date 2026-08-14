//! Downstream-client simulation: GET/POST against the REAL HTTP layer with a
//! REAL QQ database. No secrets are hardcoded here — the registration inputs
//! resolve in priority order:
//!
//!   1. `./qqflow-server.json` in the repo root (gitignored), flat fields:
//!      `qq` / `key` / `db_path`;
//!   2. environment variables: QQFLOW_TEST_QQ / QQFLOW_TEST_DB_KEY /
//!      QQFLOW_TEST_DB_ROOT (Tencent Files-style root or a direct
//!      nt_msg.db file).
//!
//! Run:
//!   powershell -File scripts\build.ps1 test --test downstream_client -- --ignored --nocapture
//!
//! Startup is CLIENT-DRIVEN: the app boots with zero accounts, and the test
//! registers the account via `POST /api/v1/accounts` (qq + key + db_path)
//! exactly like a downstream client would, then waits for the background
//! index build to reach `ready`. Afterwards it exercises the request shapes
//! a WeFlow-style client sends: auth via Bearer header / `?access_token=` /
//! POST JSON body, GET+POST parameter transport, error envelopes, ChatLab
//! Pull pagination (`nextSince` / `nextOffset`), group members with message
//! counts, manual sync, and the SSE content-type — all against real QQ chat
//! data.

use std::collections::BTreeSet;
use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{Request, StatusCode};
use parking_lot::RwLock;
use qqflow_server::server::{build_router, AccountRegistry};
use qqflow_server::store::AppState;
use qqflow_server::sync::SyncEngine;
use serde_json::{json, Value};
use tower::ServiceExt;

mod common;

const TEST_TOKEN: &str = "downstream-client-test-token";

/// Resolve the registration inputs `(qq, key, db_path)`. Priority: the
/// repo-root `./qqflow-server.json` (gitignored — never committed), then the
/// environment variables. Returns None when neither source provides all
/// three values.
fn resolve_inputs() -> Option<(String, String, String)> {
    // 1. repo-root config file (highest priority; flat qq/key/db_path).
    if let Ok(text) = std::fs::read_to_string("qqflow-server.json")
        && let Ok(v) = serde_json::from_str::<Value>(&text)
    {
        let qq = v["qq"].as_str().map(String::from);
        let key = v["key"].as_str().map(String::from);
        let db_path = v["db_path"].as_str().map(String::from);
        if let (Some(qq), Some(key), Some(db_path)) = (qq, key, db_path) {
            println!("[CLIENT] inputs from ./qqflow-server.json");
            return Some((qq, key, db_path));
        }
    }
    // 2. environment variables.
    let qq = std::env::var("QQFLOW_TEST_QQ").ok()?;
    let key = std::env::var("QQFLOW_TEST_DB_KEY").ok()?;
    let db_path = std::env::var("QQFLOW_TEST_DB_ROOT").ok()?;
    println!("[CLIENT] inputs from environment variables");
    Some((qq, key, db_path))
}

/// Build an EMPTY app (client-driven startup: zero accounts, not ready)
/// plus the resolved registration inputs. Returns None when no input
/// source provides qq + key + db_path.
fn build_real_app() -> Option<(axum::Router, Arc<AppState>, String, String, String)> {
    let (qq, key, root) = resolve_inputs()?;
    println!("[CLIENT] account {qq} (db_path: {root})");

    let state = Arc::new(AppState {
        store: Arc::new(RwLock::new(qqflow_server::store::Store::default())),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(1024).0,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(false)),
        token: Arc::new(TEST_TOKEN.into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(
            Vec::new(),
            qqflow_server::sync::watch::WatchConfig::default(),
            tokio::sync::watch::channel(false).1,
        ),
        export_root: Arc::new(std::env::temp_dir().join("qqflow_client_export")),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
    });
    let app = build_router(state.clone());
    Some((app, state, qq, root, key))
}

/// Poll /health until the account is ready; returns its indexed message count.
async fn wait_ready(app: &axum::Router, qq: &str) -> usize {
    let v = common::wait_account_state(app, qq, "ready", Duration::from_secs(120)).await;
    v["accounts"]
        .as_array()
        .unwrap()
        .iter()
        .find(|a| a["qq"] == qq)
        .unwrap()["message_count"]
        .as_u64()
        .unwrap() as usize
}

/// Downstream-client GET (optional extra headers, e.g. Bearer auth).
async fn client_get(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
) -> (StatusCode, Value) {
    common::get_json(app, uri, headers).await
}

/// Downstream-client POST with a JSON body (parameters and/or token inside).
async fn client_post(
    app: axum::Router,
    uri: &str,
    headers: &[(&str, &str)],
    body: Value,
) -> (StatusCode, Value) {
    common::post_json(app, uri, headers, body).await
}

#[tokio::test]
#[ignore]
async fn downstream_client_real_db() {
    let Some((app, state, qq, root, key)) = build_real_app() else {
        println!(
            "[CLIENT] SKIPPED: 无 ./qqflow-server.json 且环境变量未设置 \
             (QQFLOW_TEST_QQ / QQFLOW_TEST_DB_KEY / QQFLOW_TEST_DB_ROOT)"
        );
        return;
    };
    let token = state.token.as_str();

    // ---- 0. boot state: zero accounts, not ready ------------------------
    let (s, v) = client_get(app.clone(), "/health", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "starting");
    assert_eq!(v["version"], env!("CARGO_PKG_VERSION"));
    assert_eq!(v["accounts"].as_array().unwrap().len(), 0);

    // ---- 0.1 register the account (client-driven startup) ---------------
    let (s, v) = client_post(
        app.clone(),
        "/api/v1/accounts",
        &[],
        json!({"access_token": token, "qq": qq, "key": key, "db_path": root}),
    )
    .await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["state"], "accepted", "registration accepted: {v}");

    // ---- 1. health: ready after the background index build --------------
    let indexed = wait_ready(&app, &qq).await;
    assert!(indexed > 0, "real db must have messages");
    println!("[CLIENT] indexed {indexed} messages");
    let (s, v) = client_get(app.clone(), "/health", &[]).await;
    assert_eq!(s, StatusCode::OK);
    assert_eq!(v["status"], "ok");
    let accounts = v["accounts"].as_array().unwrap();
    assert_eq!(accounts.len(), 1);
    assert_eq!(accounts[0]["qq"], qq);
    assert_eq!(accounts[0]["state"], "ready");
    assert!(accounts[0].get("error").is_none());
    assert_eq!(accounts[0]["message_count"].as_u64().unwrap() as usize, indexed);

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
    assert_eq!(v["media"]["enabled"], true, "media capability on");
    assert_eq!(v["media"]["exportPath"], "");
    let msgs = v["messages"].as_array().unwrap();
    assert!(!msgs.is_empty(), "real conversation must have messages");
    assert_eq!(v["count"].as_u64().unwrap() as usize, msgs.len());
    for m in msgs {
        assert!(m["localId"].is_number());
        assert!(m["serverId"].is_string());
        assert!(m["localType"].is_number());
        assert!(m["createTime"].is_number());
        // Direction from 40013: 0 (other/system) or 1 (self) — never 2+.
        assert!(matches!(m["isSend"].as_i64(), Some(0 | 1)), "isSend from 40013");
        assert!(m["senderUsername"].is_string());
        assert!(m["content"].is_string());
        assert!(m["rawContent"].is_string());
        assert!(m["parsedContent"].is_string());
        // mediaType appears iff the message is image/voice/video.
        if let Some(mt) = m.get("mediaType") {
            assert!(matches!(mt.as_str(), Some("image" | "voice" | "video")));
            assert!(matches!(m["localType"].as_i64(), Some(3..=5)));
        }
        // Structured media rides on image/voice/video messages; mediaId is
        // present ONLY when the store registered a local cache path (the
        // "fetchable" promise — absent means /api/v1/media/{id} would 404).
        if m.get("media").is_some() {
            if let Some(id) = m["mediaId"].as_str() {
                assert!(!id.is_empty(), "mediaId non-empty when present");
            }
        }
        assert!(
            m["mediaId"].is_null() || !m["media"].is_null(),
            "mediaId never appears without the media object"
        );
    }
    // newest first
    let ts_first = msgs[0]["createTime"].as_i64().unwrap();
    let ts_last = msgs.last().unwrap()["createTime"].as_i64().unwrap();
    assert!(ts_first >= ts_last, "messages must be newest first");
    println!("[CLIENT] messages({first_talker}): {} rows, ts=[{ts_last},{ts_first}]", msgs.len());

    // ---- 4b. media=1 export: WeFlow-shaped fields on real media rows ----
    let uri = format!("/api/v1/messages?talker={first_talker}&limit=50&media=1&access_token={token}");
    let (s, v) = client_get(app.clone(), &uri, &[]).await;
    assert_eq!(s, StatusCode::OK);
    let export_path = v["media"]["exportPath"].as_str().unwrap();
    assert!(!export_path.is_empty(), "media=1 exports into a real directory");
    let exported = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .filter(|m| m.get("mediaFileName").is_some())
        .count();
    assert_eq!(
        v["media"]["count"].as_u64().unwrap() as usize,
        exported,
        "count matches exported rows"
    );
    for m in v["messages"].as_array().unwrap() {
        if let Some(name) = m.get("mediaFileName").and_then(|n| n.as_str()) {
            let url = m["mediaUrl"].as_str().unwrap();
            assert!(url.starts_with("http://127.0.0.1:5032/api/v1/media/"), "mediaUrl shape");
            assert!(url.ends_with(&format!("/{name}")), "mediaUrl ends with the file name");
            assert!(
                m["mediaLocalPath"].as_str().unwrap().starts_with(export_path),
                "mediaLocalPath under exportPath"
            );
        }
    }
    println!("[CLIENT] media=1 export: {exported} media rows exported (count={})", v["media"]["count"]);

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
        // remark comes from the uid mapping table — real data, value
        // unknown; only the shape is asserted (was: always empty).
        assert!(c["remark"].is_string(), "remark must be a string");
        // alias carries the QQ number (WeFlow's alias slot, migrated from
        // the old `qq` field): a string, empty only when no mapping source.
        assert!(c["alias"].is_string(), "alias must be a string");
        assert_eq!(c["avatarUrl"], "", "v1 avatarUrl is always empty");
        assert_eq!(c["type"], "friend");
    }
    println!("[CLIENT] contacts: {} rows", v["count"]);
    for c in v["contacts"].as_array().unwrap().iter().take(5) {
        println!(
            "[CLIENT]   uid {} -> name {:?} remark {:?}",
            c["username"],
            c["displayName"].as_str().unwrap_or_default(),
            c["remark"].as_str().unwrap_or_default()
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

    // NOTE: nothing to clean up — the server no longer creates a mirror.
    println!("[CLIENT] all downstream-client checks passed");
}
