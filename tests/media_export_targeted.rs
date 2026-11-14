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
            rusqlite::params![1622505500i64, (1622505600i64 << 32) | 7]).unwrap();
        // Same platform ID with another sender is not ambiguous.
        writer.execute("INSERT INTO group_msg_table SELECT \"40021\",\"40001\",'another',\"40093\",\"40800\",\"40013\",\"40050\",\"40090\" FROM group_msg_table WHERE \"40020\"='u_c' AND \"40001\"=?1 LIMIT 1",
            [(1622505600i64 << 32) | 7]).unwrap();
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
        json!({"talker":"10001", "platformMessageId": ((1622505600i64 << 32) | 7).to_string(),
            "sender":"u_c", "timestamp":1622505500i64})
    }

    /// 空 45812 变体：结构化媒体带空本地路径 + 缓存索引里有有效 fallback。
    /// 定向导出必须跳过空候选落到 fallback（回归
    /// `targeted_export_empty_local_path_falls_back_to_cache`）。
    async fn empty_local() -> Self {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let root = std::env::temp_dir().join(format!("qqflow_target_media_empty_{}_{}",
            std::process::id(), SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst)));
        let nt_db = root.join("nt_db");
        let (writer, _) = common::open_fake_source(&nt_db, 180);
        // 与 Fixture::target() 对齐的行号 7 / sender u_c / 时间 1622505500，
        // 但 45812 是**空串**：候选循环必须跳过它而不是把 media root 目录
        // 当成「源已找到」。
        let md5 = "aabbccddeeff00112233445566778899";
        let blob = common::image_blob_empty_local(md5, "fake_image_01.jpg");
        writer.execute("INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
            rusqlite::params!["10001", (1622505600i64 << 32) | 7, "u_c", "王五", blob, 0, 1622505500i64, ""]).unwrap();
        let path = common::materialize_source(&nt_db);
        // fallback：缓存索引按文件名登记的候选（write_fake_cache_file 落的固定字节）。
        common::write_fake_cache_file(&nt_db, "Pic/2026-08/fake_image_01.jpg");
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
    let display = first["fileName"].as_str().unwrap();
    assert!(!display.is_empty() && display.ends_with(".jpg"),
        "fileName 是消息元数据里的展示名（本夹具是平台原名）：{first}");
    assert_ne!(display, id, "fileName 是展示名、mediaId 才是句柄：{first}");
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
    let plain = json!({"talker":"10001", "platformMessageId":((1622505600i64 << 32) | 1).to_string(),
        "sender":"u_a", "timestamp":1622505600i64});
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

/// 发布前的数据源身份复查：导出途中换库必须 409，且 staged 字节与 `.pending/`
/// 都不残留。
///
/// 只比世代计数不够：它只在同步路径采样时推进，而导出的阻塞任务可能持锁让轮询
/// 停摆——计数器必然不动。导出请求自采指纹、发布前再采比对，才是真正起作用的
/// 那一半（与 weflow 的同名机制对齐）。回归
/// `targeted_export_cancels_when_the_source_identity_changes_midway`。
#[tokio::test]
async fn targeted_export_cancels_when_the_source_identity_changes_midway() {
    let fixture = Fixture::new(false).await;
    let state = fixture.state.clone();
    let nt_db = fixture.root.join("nt_db");
    let error = server::handlers::media_export::restore_before_publish(
        state.clone(), serde_json::from_value(Fixture::target()).unwrap(), move || {
            // 在 staging 与 publish 之间把库文件整个换掉：写一份 salt 不同的
            // 合法 QQ 形状库。**不动** `raw.db` 与存活 writer——它们属于夹具的
            // 「运行中的 QQ」，真实换库也不会动到这个进程外的东西；被替换的
            // 只是服务端读的那份 nt_msg.db（写场景 = 删库重建 / 备份还原）。
            let db = nt_db.join("nt_msg.db");
            let mut bytes = vec![0u8; qqflow_server::db::scan::CUSTOM_HEADER_LEN as usize];
            bytes.extend_from_slice(&[0xEEu8; 16]); // 新 salt
            bytes.extend_from_slice(&[0u8; 4096]);
            std::fs::write(&db, bytes).expect("写替换库");
        },
    ).await.unwrap_err();
    assert_eq!(error.status, StatusCode::CONFLICT, "换库必须让导出失败: {:?}", error.message);
    assert_eq!(error.reason, Some("cursor_reset_required"), "409 带机器可读 reason: {:?}", error.reason);
    assert_eq!(std::fs::read_dir(fixture.root.join("export/10001/images")).unwrap().count(), 0, "不落产物");
    assert_eq!(std::fs::read_dir(fixture.root.join("export/.pending")).unwrap().count(), 0, "无残留 pending");
}

/// 定向导出的候选循环必须**跳过空串与目录候选**，让后面的 fallback 有机会。
///
/// 真库见过结构化媒体带**空串 45812** 的形态（解析器原样保留）：空串经
/// `root.join("")` 解析出 media root 目录本身，此前循环以 `source.is_some()`
/// 提前终结——后续打开目录报非 NotFound 错，端点 500，而缓存里的 fallback
/// 明明可用。回归：`targeted_export_empty_local_path_falls_back_to_cache`。
#[tokio::test]
async fn targeted_export_empty_local_path_falls_back_to_cache() {
    let fixture = Fixture::empty_local().await;
    let (status, out) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "空 45812 必须落到缓存 fallback 而不是 500: {out}");
    let id = out["mediaId"].as_str().unwrap();
    // fallback 缓存文件是 write_fake_cache_file 写的固定内容。
    assert_eq!(fixture.bytes(id).await, b"fallback cache bytes");
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
    let (status, result) = common::get_json(app, "/chatlab/messages?talker=10001&media=1&start=1622505500&end=1622505500",
        &[("authorization", "Bearer target-token")]).await;
    assert_eq!(status, StatusCode::OK);
    let previous = result["messages"].as_array().unwrap().iter().find(|m| m["sender"] == "u_c").unwrap()["media"]["fileName"].as_str().unwrap();
    std::fs::remove_file(source).unwrap();
    let (status, exported) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "{exported}");
    assert_eq!(exported["mediaId"], previous);
    assert_eq!(fixture.bytes(previous).await, b"\xff\xd8\xffjpeg bytes");
}

/// 定向导出的 digest 副本在源缓存被删后必须仍可复用。
///
/// 首次**定向**导出落盘的是 `<sha256>.<ext>`（对源字节的摘要），而复用分支此前
/// 只查 `<md5>.<ext>`（store 键名）——摘要名无法从元数据重新推导（字节没了摘要
/// 就算不出来），没有「消息身份 → 已发布句柄」的登记，第二次导出落 404。
/// 既有回归 `targeted_export_reuses_existing_copy_after_source_removal` 的首次
/// 导出走 `media=1` 路径（md5 键名），从未覆盖定向导出自己的 digest 形态。
/// 登记在发布成功时写入 store（进程内，见 `MediaEntry::published_handle`）。
#[tokio::test]
async fn targeted_export_reuses_its_own_digest_copy_after_source_removal() {
    let fixture = Fixture::new(false).await;
    // 首次定向导出：source 还在，走 digest 分支，产物名是 32 位 hex。
    let (status, first) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "{first}");
    let digest_name = first["mediaId"].as_str().unwrap().to_string();
    let (stem, ext) = digest_name.rsplit_once('.').unwrap_or((digest_name.as_str(), ""));
    // 定向导出的名字是 **SHA-256**（64 位 hex）摘要 + 检测出的扩展名；比 store 键
    // （md5 hex，32 位）长一倍，这也是它与复用分支的键名对不上的原因。
    assert_eq!(stem.len(), 64, "首次定向导出必须落 SHA-256 digest 句柄: {digest_name}");
    assert!(stem.bytes().all(|b| b.is_ascii_hexdigit()), "digest 名是 hex: {digest_name}");
    assert_eq!(ext, "png");
    assert!(fixture.bytes(&digest_name).await == b"\x89PNG\r\n\x1a\nactual bytes");

    // QQ 删掉源缓存：45812 指向的文件没了。
    std::fs::remove_file(common::fake_media_path(&fixture.root.join("nt_db"))).unwrap();

    // 再次定向导出：源没了，digest 名算不出来，必须靠登记复用已发布副本。
    let (status, again) = fixture.export(Fixture::target()).await;
    assert_eq!(status, StatusCode::OK, "删源后的定向导出必须复用 digest 副本而不是 404: {again}");
    assert_eq!(again["mediaId"], digest_name, "复用的必须是同一份 digest 副本: {again}");
    assert_eq!(fixture.bytes(&digest_name).await, b"\x89PNG\r\n\x1a\nactual bytes");
}
