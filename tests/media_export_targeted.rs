//! Targeted restoration uses encrypted source fixtures and the real router.
mod common;

use std::sync::{Arc, atomic::AtomicBool};
use parking_lot::RwLock;
use qqflow_server::{server::{self, AppState}, store::Store, sync};
use serde_json::{Value, json};
use axum::{body::Body, http::{Request, StatusCode}};
use tower::ServiceExt;

struct Fixture {
    root: std::path::PathBuf,
    writer: Option<rusqlite::Connection>,
    state: Arc<AppState>,
}

impl Fixture {
    async fn new(duplicate: bool) -> Self {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!("qqflow_target_media_{}_{}", std::process::id(),
            SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst)));
        let nt_db = root.join("nt_db");
        let (writer, _) = common::open_fake_source(&nt_db, 180);
        common::append_image_row(&writer, 7, &nt_db);
        if duplicate { common::append_image_row(&writer, 7, &nt_db); }
        writer.execute("UPDATE group_msg_table SET \"40050\"=?1 WHERE \"40001\"=?2 AND \"40020\"='u_c'",
            rusqlite::params![2051222300i64, (2051222400i64 << 32) | 7]).unwrap();
        // Same platform ID with another sender is not ambiguous.
        writer.execute("INSERT INTO group_msg_table SELECT \"40021\",\"40001\",'another',\"40093\",\"40800\",\"40013\",\"40050\",\"40090\" FROM group_msg_table WHERE \"40020\"='u_c' AND \"40001\"=?1 LIMIT 1",
            [(2051222400i64 << 32) | 7]).unwrap();
        let path = common::materialize_source(&nt_db);
        std::fs::write(common::fake_media_path(&nt_db), b"\x89PNG\r\n\x1a\nactual bytes").unwrap();
        let (shutdown, rx) = tokio::sync::watch::channel(false);
        let state = Arc::new(AppState {
            store: Arc::new(RwLock::new(Store::default())),
            bus: sync::history::EventBus::new(32), accounts: Arc::new(RwLock::new(Vec::new())),
            ready: Arc::new(AtomicBool::new(false)), token: Arc::new("target-token".into()),
            sync: Arc::new(sync::SyncEngine::new()),
            init: server::AccountRegistry::new(Vec::new(), sync::watch::WatchConfig::default(), rx),
            export_root: Arc::new(root.join("export")), base_url: Arc::new("http://localhost".into()), shutdown,
        });
        let server::BindOutcome::Bound(token) = server::begin_indexing(&state, common::FAKE_QQ) else { panic!() };
        server::init_account(&state, qqflow_server::db::scan::DbInfo { qq: common::FAKE_QQ.into(), path }, common::FAKE_KEY.into(), token).await;
        assert!(state.ready.load(std::sync::atomic::Ordering::SeqCst));
        Self { root, writer: Some(writer), state }
    }

    fn target() -> Value {
        json!({"talker":"10001", "platformMessageId": ((2051222400i64 << 32) | 7).to_string(),
            "sender":"u_c", "timestamp":2051222300i64})
    }

    async fn export(&self, target: Value) -> (StatusCode, Value) {
        common::post_json(server::build_router(self.state.clone()), "/api/v1/media/export",
            &[("authorization", "Bearer target-token")], target).await
    }

    async fn bytes(&self, id: &str) -> Vec<u8> {
        let response = server::build_router(self.state.clone()).oneshot(Request::builder()
            .uri(format!("/api/v1/media/{id}"))
            .header("authorization", "Bearer target-token").body(Body::empty()).unwrap()).await.unwrap();
        assert_eq!(response.status(), StatusCode::OK);
        axum::body::to_bytes(response.into_body(), 1 << 20).await.unwrap().to_vec()
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        server::deregister_account(&self.state, common::FAKE_QQ, false);
        drop(self.writer.take());
        let _ = std::fs::remove_dir_all(&self.root);
    }
}

#[tokio::test]
async fn targeted_export_restores_old_media_with_actual_extension_and_handle() {
    let fixture = Fixture::new(false).await;
    let app = server::build_router(fixture.state.clone());
    let (_, latest) = common::get_json(app, "/chatlab/messages?talker=10001",
        &[("authorization", "Bearer target-token")]).await;
    assert!(!latest["messages"].as_array().unwrap().iter().any(|m| {
        m["platformMessageId"] == Fixture::target()["platformMessageId"] && m["sender"] == "u_c"
    }), "target must actually be outside the latest page");
    let (status, first) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let id = first["mediaId"].as_str().unwrap();
    assert!(id.ends_with(".png"));
    assert_eq!(first["fileName"], id);
    assert_eq!(first["mediaUrl"], format!("/api/v1/media/{id}"));
    assert_eq!(fixture.bytes(id).await, b"\x89PNG\r\n\x1a\nactual bytes");
    std::fs::remove_file(fixture.root.join("export/10001/images").join(id)).unwrap();
    let (status, restored) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK);
    assert_eq!(restored, first);
    assert_eq!(fixture.bytes(id).await, b"\x89PNG\r\n\x1a\nactual bytes");
}

#[tokio::test]
async fn targeted_export_distinguishes_missing_ambiguous_and_io_failures() {
    let fixture = Fixture::new(true).await;
    assert_eq!(fixture.export(Fixture::target()).await.0, StatusCode::CONFLICT);
    let mut missing = Fixture::target(); missing["platformMessageId"] = json!("absent");
    assert_eq!(fixture.export(missing).await.0, StatusCode::NOT_FOUND);
    assert_eq!(fixture.export(json!({})).await.0, StatusCode::BAD_REQUEST);
    let plain = json!({"talker":"10001", "platformMessageId":((2051222400i64 << 32) | 1).to_string(),
        "sender":"u_a", "timestamp":2051222400i64});
    assert_eq!(fixture.export(plain).await.0, StatusCode::UNPROCESSABLE_ENTITY);

    let fixture = Fixture::new(false).await;
    std::fs::write(fixture.state.export_root.as_ref(), b"not a directory").unwrap();
    assert_eq!(fixture.export(Fixture::target()).await.0, StatusCode::INTERNAL_SERVER_ERROR);
    std::fs::remove_file(common::fake_media_path(&fixture.root.join("nt_db"))).unwrap();
    assert_eq!(fixture.export(Fixture::target()).await.0, StatusCode::NOT_FOUND);
    let app = server::build_router(fixture.state.clone());
    assert_eq!(common::post_json(app, "/api/v1/media/export", &[], Fixture::target()).await.0, StatusCode::UNAUTHORIZED);
}

#[tokio::test]
async fn targeted_export_cancels_staged_bytes_after_account_switch() {
    let fixture = Fixture::new(false).await;
    let state = fixture.state.clone();
    let error = server::handlers::media_export::restore_before_publish(
        state.clone(), serde_json::from_value(Fixture::target()).unwrap(), move || {
            server::deregister_account(&state, common::FAKE_QQ, false);
        },
    ).await.unwrap_err();
    assert_eq!(error.status, StatusCode::CONFLICT);
    assert_eq!(std::fs::read_dir(fixture.root.join("export/10001/images")).unwrap().count(), 0);
    assert_eq!(std::fs::read_dir(fixture.root.join("export/.pending")).unwrap().count(), 0);
}

#[tokio::test]
async fn targeted_export_concurrent_retries_share_complete_bytes() {
    let fixture = Fixture::new(false).await;
    let (a, b) = tokio::join!(fixture.export(Fixture::target()), fixture.export(Fixture::target()));
    assert_eq!(a.0, StatusCode::OK, "{a:?}");
    assert_eq!(b, a);
    assert_eq!(fixture.export(Fixture::target()).await, a);
    assert_eq!(fixture.bytes(a.1["mediaId"].as_str().unwrap()).await, b"\x89PNG\r\n\x1a\nactual bytes");
}

#[tokio::test]
async fn targeted_export_reuses_existing_copy_after_source_removal() {
    let fixture = Fixture::new(false).await;
    let source = common::fake_media_path(&fixture.root.join("nt_db"));
    std::fs::write(&source, b"\xff\xd8\xffjpeg bytes").unwrap();
    let app = server::build_router(fixture.state.clone());
    let (status, result) = common::get_json(app, "/chatlab/messages?talker=10001&media=1&start=2051222300&end=2051222300",
        &[("authorization", "Bearer target-token")]).await;
    assert_eq!(status, StatusCode::OK);
    let previous = result["messages"].as_array().unwrap().iter().find(|m| m["sender"] == "u_c").unwrap()["media"]["fileName"].as_str().unwrap();
    std::fs::remove_file(source).unwrap();
    let (status, exported) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "{exported}");
    assert_eq!(exported["mediaId"], previous);
    assert_eq!(fixture.bytes(previous).await, b"\xff\xd8\xffjpeg bytes");
}
