//! 一致性套件的**执行入口**：造夹具 → 起真实服务 → 跑 `flow-contract` 的 runner。
//!
//! 以 `#[ignore]` ＋ `FLOW_CONTRACT_DIR` 门控：日常 `cargo test` 不受影响，CI 里是一个独立
//! 步骤。没有 `FLOW_CONTRACT_DIR` 时**跳过并通过**，这样没检出契约仓库的开发机上依然全绿。
//!
//! ## 为什么自己构造 `AppState` 而不是跑服务端二进制
//!
//! 二进制的 token 存在 OS 凭据库里；CI 上没有凭据库，它会回退成随机 token 并**只写进日志**
//! —— runner 拿不到它。测试自己构造状态就能指定 token，同时仍走**真实注册路径**
//! （零账号启动 → `POST /api/v1/accounts` → 等 ready），不手工往注册表里塞东西。
//!
//! ## 为什么 harness 控制端点放在测试里
//!
//! 三条用例要「让服务端发生一件事」（追加消息、注销账号），那是**动作**不是请求。测试自己
//! 构造 Router 再 merge 一条测试专用路由即可；产品多一个能改状态的未加鉴权端点，是给所有人
//! 开的门。

mod common;

use std::sync::atomic::AtomicBool;
use std::sync::Arc;
use std::time::Duration;

use axum::body::Body;
use axum::http::{header, Request};
use parking_lot::{Mutex, RwLock};
use serde_json::{json, Value};
use tower::ServiceExt;

use qqflow_server::server::{build_router, AccountRegistry};
use qqflow_server::store::AppState;
use qqflow_server::sync::SyncEngine;

const TOKEN: &str = "conformance-runner-token-0123456789";

/// harness 每次追加消息用一个递增序号，保证 `platformMessageId` 唯一。
///
/// `append_group_row` 的第一条参数是行号，用固定值会让**两次调用**产生同一个
/// `platformMessageId` —— `nails-platform-message-id-string` 会如实报「页内重复」。
static SEQ: std::sync::atomic::AtomicI64 = std::sync::atomic::AtomicI64::new(0);

fn contract_dir() -> Option<std::path::PathBuf> {
    let d = std::env::var("FLOW_CONTRACT_DIR").ok()?;
    let p = std::path::PathBuf::from(d);
    p.join("runner/run.py").exists().then_some(p)
}

fn app_state(export_root: std::path::PathBuf) -> Arc<AppState> {
    Arc::new(AppState {
        store: Arc::new(RwLock::new(qqflow_server::store::Store::default())),
        events: tokio::sync::broadcast::channel::<qqflow_server::sync::Event>(1024).0,
        accounts: Arc::new(RwLock::new(Vec::new())),
        ready: Arc::new(AtomicBool::new(false)),
        token: Arc::new(TOKEN.into()),
        sync: Arc::new(SyncEngine::new()),
        init: AccountRegistry::new(
            Vec::new(),
            qqflow_server::sync::watch::WatchConfig::default(),
            tokio::sync::watch::channel(false).1,
        ),
        export_root: Arc::new(export_root),
        base_url: Arc::new("http://127.0.0.1:5032".into()),
        history: Arc::new(Mutex::new(Default::default())),
        shutdown: tokio::sync::watch::channel(false).0,
    })
}

fn req(method: &str, uri: &str) -> Request<Body> {
    let mut r = Request::builder()
        .method(method)
        .uri(uri)
        .body(Body::empty())
        .unwrap();
    r.headers_mut().insert(
        header::AUTHORIZATION,
        format!("Bearer {TOKEN}").parse().unwrap(),
    );
    r
}

/// 发一次鉴权探测，返回 (传输名, 状态码)。
async fn probe(app: axum::Router, name: &str, r: Request<Body>) -> (String, u16) {
    let resp = app.oneshot(r).await.unwrap();
    (name.to_string(), resp.status().as_u16())
}

async fn json_of(app: &axum::Router, r: Request<Body>) -> Value {
    let resp = app.clone().oneshot(r).await.unwrap();
    let b = axum::body::to_bytes(resp.into_body(), 8 * 1024 * 1024).await.unwrap();
    serde_json::from_slice(&b).unwrap_or(Value::Null)
}

/// 等后台索引构建到 ready。夹具很小，慢到这个程度说明是真卡住了。
async fn wait_ready(app: &axum::Router) -> bool {
    let mut last = Value::Null;
    for _ in 0..120 {
        let v = json_of(app, req("GET", "/api/v1/accounts")).await;
        let st = v["accounts"][0]["state"].as_str().unwrap_or("");
        if st == "ready" {
            return true;
        }
        last = v;
        tokio::time::sleep(Duration::from_millis(250)).await;
    }
    // 失败时把最后一次看到的账号状态打出来 —— 否则「没到 ready」这句话无法用来定位。
    println!("[conformance] 30 秒内未 ready，最后的账号状态：{last}");
    false
}

/// 测试专用的 harness 控制端点，**不进入产品路由**。
fn harness_router(
    writer: Arc<parking_lot::Mutex<rusqlite::Connection>>,
    nt_db: std::path::PathBuf,
    state: Arc<AppState>,
) -> axum::Router {
    axum::Router::new().route(
        "/__harness",
        axum::routing::post(move |body: String| {
            let writer = writer.clone();
            let nt_db = nt_db.clone();
            let src = nt_db.join("nt_msg.db");
            let state = state.clone();
            async move {
                let v: Value = serde_json::from_str(&body).unwrap_or(Value::Null);
                // 追加一条新消息：像运行中的 QQ 那样写进源库，再 materialize，
                // 让活连接看得见（与 fs_watch_e2e 同一手法）。
                if v["action"].as_str() == Some("harness.append_message") {
                        // **前提恢复**：用例之间必须互相独立，而 `sse-deregister-replay` 会注销
                        // 账号 —— 它字母序在前，于是一部分用例在「没有账号」的环境里跑。
                        // 判据是「**没有可用账号**」而不是「表为空」：本仓库注销后条目仍在，
                        // 只是状态变成 `deregistered`（weflow 那边是直接移除 —— 两仓库语义不同，
                        // 照抄 `is_empty()` 就会在这里静默失效）。
                        let usable = state.accounts.read().iter().any(|a| {
                            a.state.is_ready() || a.state == qqflow_server::server::AccountStatus::Indexing
                        });
                        if !usable
                            && let Some(info) =
                                qqflow_server::db::scan::resolve_account(common::FAKE_QQ, &src)
                        {
                            state.init.upsert_db(info);
                            let _ = qqflow_server::server::begin_indexing(&state, common::FAKE_QQ);
                            for _ in 0..120 {
                                let ready =
                                    state.accounts.read().iter().any(|a| a.state.is_ready());
                                if ready {
                                    break;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            }
                            println!("[harness] 账号不在（前面的用例注销了它），已重新注册");
                        }
                        let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                        // **必须复用夹具那个持久写入连接**，不能自己新开一个：
                        //
                        // 源库是 SQLCipher 加密的（裸 open 报 `NotADatabase`），而 `materialize_source`
                        // 是「读 `raw.db` 主文件 + 硬链接它的 WAL」——新开连接写入会让 WAL 的 salt
                        // 变化，已打开的读端就用不了那条链接了，于是新行对活连接不可见。
                        // `open_fake_writer` 的注释里正好警告过这一点。
                        let writer = writer.clone();
                        let ins = tokio::task::spawn_blocking(move || {
                            let conn = writer.lock();
                            conn.execute(
                                "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                                rusqlite::params![
                                    "10001",
                                    // `seq` 的高 32 位是时间戳：**必须晚于夹具既有的行**，否则新行落在
                                    // 水位线之内，`poll_once` 返回 0 —— 症状又是「SSE 收不到事件」
                                    // （与 weflow 那边同一个坑：追加的时间戳要越过水位线）。
                                    ((1782864000i64 + 60) << 32) | (99 + seq),
                                    "u_a",
                                    "张三",
                                    "一致性套件新增".as_bytes(),
                                    1,
                                    1782864000i64 + 60,
                                    "张三群名片"
                                ],
                            )
                        })
                        .await;
                        if let Ok(Err(e)) = &ins {
                            println!("[harness] 追加失败：{e}");
                        }
                        // **materialize 不能不跑**：新行写进的是无头的 `raw.db`，活连接读的是
                        // `nt_msg.db` —— 不 materialize 就等于没写。第一版把这两行连同显式同步
                        // 一起删掉了，症状又是「SSE 收不到事件」。
                        common::materialize_source(&nt_db);
                        // 与 weflow 的 harness 对齐：显式同步一次，让「追加 → 可见 → 广播」
                        // 成为确定的事，而不是等 Watcher 的时序。
                        let _ = tokio::task::spawn_blocking(move || state.sync.sync_all()).await;
                }
                axum::http::StatusCode::OK
            }
        }),
    )
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "由 CI 的一致性步骤显式运行：需要 FLOW_CONTRACT_DIR"]
async fn conformance_suite_passes() {
    let Some(contract) = contract_dir() else {
        println!("[conformance] 未设置 FLOW_CONTRACT_DIR（或其中没有 runner/run.py），跳过");
        return;
    };
    let dir = std::env::temp_dir().join(format!("qqflow_conformance_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let nt_db = dir.join("nt_db");
    // `open_fake_source` 返回的第二个值是 **`nt_msg.db`**（`materialize_source` 的产物），
    // 它是 `raw.db` 前面加上 1024 字节 QQ 头的**只读形态** —— 裸 open 它做 INSERT 会报
    // `NotADatabase`。要写入必须针对无头的 `raw.db`，再 materialize 一次让活连接看见。
    // `open_fake_source` 返回 (写入连接, `nt_msg.db` 路径)。**连接要留着**：写入必须走它，
    // 自己新开一个会让 WAL 的 salt 变化，硬链接失效、新行对活连接不可见。
    let (writer, _main) = common::open_fake_source(&nt_db, 0);
    let writer = Arc::new(parking_lot::Mutex::new(writer));
    let src = nt_db.join("nt_msg.db");

    let state = app_state(dir.join("export"));
    let app = build_router(state.clone()).merge(harness_router(writer.clone(), nt_db.clone(), state.clone()));

    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let addr = listener.local_addr().unwrap();
    let client = app.clone();
    let serve = tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

    // 真实注册路径：零账号启动，客户端注册。
    // `db_path` 传**文件本身**：`resolve_account` 只接受「db 文件」或「Tencent Files 风格的
    // 根目录（<root>/<数字>/nt_qq/nt_db/nt_msg.db）」；传一个「装着 db 的目录」会被判
    // `invalid_db_path` 并**静默不进注册表**（响应仍是 success，只有 state 说了实话）。
    let body = json!({
        "qq": common::FAKE_QQ,
        "key": common::FAKE_KEY,
        "db_path": src.to_string_lossy(),
    });
    let mut r = req("POST", "/api/v1/accounts");
    r.headers_mut()
        .insert(header::CONTENT_TYPE, "application/json".parse().unwrap());
    *r.body_mut() = Body::from(body.to_string());
    let reg = json_of(&client, r).await;
    println!("[conformance] 注册响应：{reg}");
    assert_eq!(reg["success"], json!(true), "注册失败：{reg}");
    assert!(wait_ready(&client).await, "索引没能在 30 秒内到 ready");

    // 五通道鉴权探测：**凭据写法各不相同**（`authorization` 要 `Bearer ` 前缀，`x-api-key`
    // 要裸 token，其余是裸 token）。
    let mut auth_probed = serde_json::Map::new();
    let plain_get = |q: &str| {
        Request::builder()
            .method("GET")
            .uri(format!("/api/v1/sessions?{q}={TOKEN}"))
            .body(Body::empty())
            .unwrap()
    };
    let with_header = |h: &'static str, v: String| {
        let mut r = Request::builder()
            .method("GET")
            .uri("/api/v1/sessions")
            .body(Body::empty())
            .unwrap();
        r.headers_mut().insert(h, v.parse().unwrap());
        r
    };
    let probes = [
        ("bearer", with_header("authorization", format!("Bearer {TOKEN}"))),
        ("x-api-key", with_header("x-api-key", TOKEN.to_string())),
        ("access_token", plain_get("access_token")),
        ("token", plain_get("token")),
        (
            "body",
            Request::builder()
                .method("POST")
                .uri("/api/v1/sessions")
                .header(header::CONTENT_TYPE, "application/json")
                .body(Body::from(json!({ "access_token": TOKEN }).to_string()))
                .unwrap(),
        ),
    ];
    for (name, r) in probes {
        let (n, s) = probe(client.clone(), name, r).await;
        auth_probed.insert(n, json!(s));
    }

    let fx = json!({
        "contractVersion": "0.1.1",
        "platform": "qq",
        "generatedBy": "qqflow-server tests/conformance_runner.rs",
        "endpoints": {
            "accounts": "/api/v1/accounts",
            "contacts": "/api/v1/contacts",
            "harness": "/__harness",
            "health": "/health",
            "messages": "/api/v1/messages",
            "pull": "/api/v1/sessions/{id}/messages",
            "push": "/api/v1/push/messages",
            "sessions": "/api/v1/sessions",
        },
        "authProbed": auth_probed,
        "slots": {
            // 夹具里**所有行共享同一个时间戳**（模拟 QQ 的真实布局），所以群会话本身就是
            // 一个「同秒多条」的样本 —— `same_second_burst` 指同一个会话。
            "group_with_messages": { "id": "10001", "expect": { "same_second": 5 } },
            "private_with_messages": { "id": "u_12345", "expect": { "messages": 2 } },
            "same_second_burst": { "id": "10001", "expect": { "same_second": 5 } },
            "unknown": { "id": "999999999", "expect": {} },
        },
        "capabilities": {
            "mediaById": true,
            // 本仓库没有回调面（SNS 是微信侧的），如实置 false。
            "sns": false,
            "memberCount": true,
            // Pull 形状的发现面（规范里的 `GET {baseUrl}/sessions`）尚未实现 —— 它是计划里
            // 「ChatLab 适配」那一步的内容。置 false 让相关用例**跳过而不是失败**：契约的
            // 能力机制就是为这种「存在性差异」准备的。实现后翻成 true 即启用。
            "pullDiscovery": false,
            // 同上：Pull 形状的通知面（`GET {baseUrl}/push/messages`，规范要求只带元信息，
            // 不带消息体）也属那一步。
            // SSE 通知面**是存在的**且符合契约（`message.new`/`message.revoke`/`sync` 三种
            // 事件都通过套件断言）—— 先前写 false 是我的推断，不是实测。
            "pullNotification": true,
            "roles": false,
            "sse": true,
            "authProbe": true,
        },
    });
    let fx_path = dir.join("fixture.json");
    std::fs::write(&fx_path, serde_json::to_string_pretty(&fx).unwrap()).unwrap();

    // `FLOW_CONTRACT_CASE` 透传给 runner 的 `--case`：把一条用例单独拉出来查。
    // 一整套跑下来时，报错本身往往不足以定位问题出在哪条路径上。
    let case_filter: Vec<String> = std::env::var("FLOW_CONTRACT_CASE")
        .ok()
        .map(|v| {
            v.split(',')
                .map(|s| s.trim().to_string())
                .filter(|s| !s.is_empty())
                .collect()
        })
        .unwrap_or_default();
    let out = std::process::Command::new("python")
        .arg(contract.join("runner/run.py"))
        .arg("--base-url")
        .arg(format!("http://{addr}"))
        .arg("--cases")
        .arg(contract.join("cases"))
        .arg("--fixture")
        .arg(&fx_path)
        .arg("--token")
        .arg(TOKEN)
        .args(case_filter.iter().flat_map(|c| ["--case", c.as_str()]))
        .current_dir(&contract)
        .output()
        .expect("python 必须可用（提交路径本来就依赖它）");
    println!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));

    serve.abort();
    assert!(out.status.success(), "一致性套件未通过（见上面的报告）");
}
