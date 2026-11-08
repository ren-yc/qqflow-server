//! 一致性套件的**执行入口**：造夹具 → 起真实服务 → 跑 `flow-contract` 的 runner。
//!
//! 以 `#[ignore]` ＋ `FLOW_CONTRACT_DIR` 门控：日常 `cargo test` 不跑它（这是设计，不是
//! 漏洞），CI 里是一个独立步骤、用 `--ignored` 显式跑。**跑它而缺 `FLOW_CONTRACT_DIR` 时是
//! panic，不是静默通过**——判据不在 `contract_dir()`（它只返回 Option），在
//! `conformance_suite_passes` 体内的 `let Some(contract) = … else { panic! }`。之所以失败开放：
//! CI 那步带着 `--ignored` 而来，若缺环境也返回 Ok，报表会出现「一致性套件绿了」而实际
//! 一个端点都没验过。
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
use axum::response::IntoResponse;
use parking_lot::RwLock;
use serde_json::{json, Value};
use tower::ServiceExt;

use qqflow_server::server::{build_router, AccountRegistry};
use qqflow_server::server::AppState;
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
        bus: qqflow_server::sync::history::EventBus::new(1024),
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

/// `conformance.pin` 与夹具的 `contractVersion` **必须同改**。
///
/// 为什么需要它：runner 只比对**夹具**与契约仓库的 `VERSION`，另有一层 tag 校验要能读到
/// git 才生效 —— 于是「只改 pin、忘了夹具」在本地不会红（要到 CI 运行时才以 exit 2 拒绝），
/// 反过来「只改夹具、忘了 pin」更隐蔽：本地跑本地 clone 的契约目录照样通过，而 CI clone
/// 的是旧 tag。两处一起改因此必须是可执行的，而不是靠记性。
#[test]
fn pinned_contract_version_matches_the_fixture() {
    let pin = std::fs::read_to_string(concat!(env!("CARGO_MANIFEST_DIR"), "/conformance.pin"))
        .expect("conformance.pin 必须存在");
    let pin = pin.trim().trim_start_matches('v');
    assert_eq!(
        pin, CONTRACT_VERSION,
        "conformance.pin 与夹具的 contractVersion 不一致：升 pin 时两处必须同改"
    );
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
                // 游标相对追加（opaqueCursor 用例）：向 cursor 表达的入库进度
                // **之后**追加一条新消息（seq 用递增序号保证唯一），回显新行的
                // platformMessageId（QQFlow 侧即 seq 字符串）。cursor 本身编码
                // rowid——入库进度之后 = rowid 更大，同一秒或更晚都可。
                if v["action"].as_str() == Some("harness.append_message_at_cursor") {
                    let usable = state.accounts.read().iter().any(|a| {
                        a.state.is_ready()
                            || a.state == qqflow_server::server::AccountStatus::Indexing
                    });
                    if !usable
                        && let Some(info) =
                            qqflow_server::db::scan::resolve_account(common::FAKE_QQ, &src)
                    {
                        // 恢复前提走**完整注册机器**：绑定（铸造令牌）→ await
                        // init_account（直接 await 到 set_ready 完成，没有后台
                        // 任务）→ 等 ready。等待超时即失败——前提没恢复时追加
                        // 的消息没人索引，用例会在 SSE/续拉断言上以更难定位的
                        // 形态红；这里直接失败并点名根因。
                        state.init.upsert_db(info.clone());
                        let token = match qqflow_server::server::begin_indexing(&state, common::FAKE_QQ) {
                            qqflow_server::server::BindOutcome::Bound(t) => t,
                            other => panic!("harness re-register must claim the binding, got {other:?}"),
                        };
                        let key = common::FAKE_KEY.to_string();
                        qqflow_server::server::init_account(&state, info, key, token).await;
                        let mut ready = false;
                        for _ in 0..120 {
                            if state.accounts.read().iter().any(|a| a.state.is_ready()) {
                                ready = true;
                                break;
                            }
                            tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                        }
                        assert!(ready, "harness 重新注册后 30 秒内必须 ready");
                    }
                    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
                    let new_seq = ((2051222400i64 + 120) << 32) | (9000 + seq);
                    let writer = writer.clone();
                    let ins = tokio::task::spawn_blocking(move || {
                        let conn = writer.lock();
                        conn.execute(
                            "INSERT INTO group_msg_table VALUES (?1, ?2, ?3, ?4, ?5, ?6, ?7, ?8)",
                            rusqlite::params![
                                "10001",
                                new_seq,
                                "u_a",
                                "张三",
                                b"cursor-boundary append".as_slice(),
                                1,
                                2051222400i64 + 120,
                                "张三群名片"
                            ],
                        )
                    })
                    .await;
                    if let Ok(Err(e)) = &ins {
                        println!("[harness] 游标追加失败：{e}");
                    }
                    common::materialize_source(&nt_db);
                    let _ = tokio::task::spawn_blocking(move || state.sync.sync_all()).await;
                    // 回显新行的 id（用例经 save 取回作为 appended_id 基准）。
                    return axum::Json(serde_json::json!({
                        "success": true,
                        "id": new_seq.to_string()
                    }))
                    .into_response();
                }
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
                            // 恢复前提走**完整注册机器**：绑定（铸造令牌）→ await
                            // init_account（直接 await 到 set_ready 完成，没有
                            // 后台任务）→ 等 ready。等待超时即失败——前提没恢复
                            // 时追加的消息没人索引，用例会在 SSE 断言上以更难
                            // 定位的形态红；这里直接失败并点名根因。
                            state.init.upsert_db(info.clone());
                            let token = match qqflow_server::server::begin_indexing(&state, common::FAKE_QQ) {
                                qqflow_server::server::BindOutcome::Bound(t) => t,
                                other => panic!("harness re-register must claim the binding, got {other:?}"),
                            };
                            let key = common::FAKE_KEY.to_string();
                            qqflow_server::server::init_account(&state, info, key, token).await;
                            let mut ready = false;
                            for _ in 0..120 {
                                if state.accounts.read().iter().any(|a| a.state.is_ready()) {
                                    ready = true;
                                    break;
                                }
                                tokio::time::sleep(std::time::Duration::from_millis(250)).await;
                            }
                            assert!(ready, "harness 重新注册后 30 秒内必须 ready");
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
                                    ((2051222400i64 + 60) << 32) | (99 + seq),
                                    "u_a",
                                    "张三",
                                    "一致性套件新增".as_bytes(),
                                    1,
                                    2051222400i64 + 60,
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
                axum::http::StatusCode::OK.into_response()
            }
        }),
    )
}

/// 夹具声明的契约版本。**必须与 `conformance.pin` 同改** —— 见下面的
/// `pinned_contract_version_matches_the_fixture`。
const CONTRACT_VERSION: &str = "0.9.0";

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "由 CI 的一致性步骤显式运行：需要 FLOW_CONTRACT_DIR"]
async fn conformance_suite_passes() {
    // 缺目录**不是**「跳过」，是失败：这条测试唯一的存在理由就是跑那套用例，
    // 而它此前在环境缺失时静默 return —— 于是 CI 里少配一个变量就会让整套门禁
    // 变成「绿着什么都没验」。
    let Some(contract) = contract_dir() else {
        panic!(
            "[conformance] 未设置 FLOW_CONTRACT_DIR（或其中没有 runner/run.py）：             一致性套件是本仓库的门禁之一，缺环境必须失败而不是静默通过"
        );
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
    // 同族库 `group_info.db`（群名、名册、群名片、群主）必须在**注册之前**就位：
    // store 的这些映射在注册建索引时一次性读入，之后再写也不会重新加载 —— 先前它只在
    // harness 追加消息时才写（注册时表还没出生），名册与群主全程为空，
    // `memberCount` 不出现（不变量允许缺席，于是「通过」却什么都没验）、群主断言拿 0 个。
    common::write_fake_group_info(&nt_db);
    // 结构化图片行（40800 段带 45812 本地路径）：让真实索引登记媒体键
    // aabbcc…——exported_media 槽位的句柄按它取字节（mediaId 承诺用例）。
    // 不加这一行，nails-media-id-fetch-succeeds 会对一个从未登记的键拿到 404。
    let _image_md5 = {
        let conn = writer.lock();
        common::append_image_row(&conn, 7, &nt_db)
    };
    common::materialize_source(&nt_db);

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

    // 鉴权探测：契约的 `authProbed` 是「传输名 → 状态码」，由 harness 探测后填入。这不是断言，
    // 是**告知** —— 用例据此判断哪些通道可用。
    //
    // **只探两条**：凭据写法不同（`authorization` 要 `Bearer ` 前缀，查询参数是裸 token），
    // 而 `X-Api-Key`、`?token=` 与 POST body 已不是通道 —— 探它们只会得到 401，而 401 与
    // 「通道还在但没带对凭据」在探测结果里无法区分。
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
        ("access_token", plain_get("access_token")),
    ];
    for (name, r) in probes {
        let (n, s) = probe(client.clone(), name, r).await;
        auth_probed.insert(n, json!(s));
    }

    let fx = json!({
        "contractVersion": CONTRACT_VERSION,
        "platform": "qq",
        "generatedBy": "qqflow-server tests/conformance_runner.rs",
        "endpoints": {
            "accounts": "/api/v1/accounts",
            "contacts": "/api/v1/contacts",
            "harness": "/__harness",
            "group-members": "/api/v1/group-members",
            "health": "/health",
            // 媒体字节端点（mediaById 能力的落点，契约用例
            // nails-media-id-fetch-succeeds 按句柄取字节）。
            "media": "/api/v1/media/{id}",
            "messages": "/api/v1/messages",
            "messages_chatlab": "/chatlab/messages",
            "pull": "/chatlab/sessions/{id}/messages",
            "push": "/chatlab/push/messages",
            "sessions": "/chatlab/sessions",
        },
        "authProbed": auth_probed,
        "slots": {
            // 夹具里**所有行共享同一个时间戳**（模拟 QQ 的真实布局），所以群会话本身就是
            // 一个「同秒多条」的样本 —— `same_second_burst` 指同一个会话。
            "group_with_messages": { "id": "10001", "expect": { "same_second": 5 } },
            "private_with_messages": { "id": "u_12345", "expect": { "messages": 2 } },
            "same_second_burst": { "id": "10001", "expect": { "same_second": 5 } },
            "unknown": { "id": "999999999", "expect": {} },
            // harness 保证句柄此刻可取的媒体（mediaId 承诺用例的槽位）：
            // 夹具造库时写入的 fake_image_01.jpg，句柄即 mediaId 本身。
            "exported_media": { "id": "aabbccddeeff00112233445566778899", "expect": {} },
        },
        "capabilities": {
            "mediaById": true,
            // 不透明进度游标（契约 0.9.0）：sync.nextCursor + cursor 参数已实现，
            // 排空后同秒晚入库的行可经游标续拉到达。
            "opaqueCursor": true,
            // 本仓库没有回调面（SNS 是微信侧的），如实置 false。
            "sns": false,
                        // 名册来源是 `group_info.db` 的 `group_member3`（见 `store::group_meta`）。
            "memberCount": true,
            // Pull 形状的发现面（`GET /chatlab/sessions`）已实现：置 true 让相关用例实跑。
            // （此处曾写着「尚未实现、置 false 跳过」——能力翻真后注释没跟上；
            // 注释与取值矛盾比没有注释更误导，故改写。）
            "pullDiscovery": true,
            // SSE 通知面（`GET /chatlab/push/messages`，规范要求只带元信息、不带消息体）
            // 存在且符合契约：`message.new`/`message.revoke`/`sync` 三种事件都通过套件断言，
            // 置 true 实跑。先前写 false 的理由是「规范要求……」这类**读规范读出的推断**，
            // 被套件实测推翻（event_notification_shape 对现行载荷通过）——推断不该当作结论。
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
        // 有跳过即失败：case 级 skip 此前不影响退出码，「夹具少声明一个端点」
        // 会让用例静默变成不跑而 CI 全绿（见 runner 的同名开关）。
        .arg("--fail-on-skip")
        .current_dir(&contract)
        .output()
        .expect("python 必须可用（提交路径本来就依赖它）");
    println!("{}\n{}", String::from_utf8_lossy(&out.stdout), String::from_utf8_lossy(&out.stderr));

    serve.abort();
    assert!(out.status.success(), "一致性套件未通过（见上面的报告）");
}

/// 游标续拉的**行标识单调性**回归：入库序与业务时间序在乱序行上分叉时，
/// cursor 模式的过滤、排序、页推进与游标编码必须全部只用行标识。
///
/// 修复前会踩中其中一段：
///
/// - **重放**（「单页排空 + 续拉」段）：排空按时间序一页交付全部行时，页末行的 rowid 不是已交付
///   集合的最大值（回溯入库的行时间早、rowid 大，排序在最前、页末也轮不到它）。
///   旧代码编码页末行 → 回传续拉用 `rowid > 页末值` 过滤 → 该行被再次交付。
/// - **空页进度重置**（「空页游标语义」段）：空页旧代码编码 0 —— 那是「从头拉取」，带 `end`
///   过滤的续拉把全历史重放一遍。
/// - **页推进**（「同秒组续接」段）：cursor 模式按时间组扩展页边界，「已交付上界」不严格。
///
/// 健全域说明：混合链（时间模式多页 → 游标续接）在乱序行存在时**单点游标
/// 本质上不可健全**——页 1 已交付大 rowid 的回溯行，小 rowid 的未交付行永远
/// 无法经游标到达。游标链只在「采纳游标前已交付集合是 rowid 前缀」时健全，
/// 单页全量排空保证这一点（契约用例 cursor-boundary-append-reachable 正是
/// 此形态）。本测试钉住健全域内的全部判别性场景。
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[ignore = "真实索引链路回归：随一致性步骤显式运行（cargo test --test conformance_runner -- --ignored）"]
async fn cursor_continuation_is_monotonic_when_row_order_diverges_from_time_order() {
    let dir = std::env::temp_dir().join(format!("qqflow_cursor_ooo_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let nt_db = dir.join("nt_db");
    // 写入必须走夹具那个持久连接（新开连接写 WAL 会变 salt，硬链接失效）；
    // 写 raw.db 后必须 materialize，活连接才看得见（见 conformance_suite_passes 的注释）。
    // `open_fake_source` 自带 seed_dataset（6 群行 + 2 c2c 行），不重复 seed。
    let (writer, _main) = common::open_fake_source(&nt_db, 0);
    let writer = Arc::new(parking_lot::Mutex::new(writer));
    common::write_fake_group_info(&nt_db);
    // 回溯入库在**注册之前**：时间戳早于标准数据集（2051222400 → 2051222300）、
    // rowid 更大（插入顺序在后）。注册后的全量索引把它一并收进来。
    {
        let conn = writer.lock();
        common::append_group_row_backdated(&conn, 990, 990, 2051222300);
    }
    common::materialize_source(&nt_db);
    let src = nt_db.join("nt_msg.db");

    let state = app_state(dir.join("export"));
    let app = build_router(state.clone());
    let listener = tokio::net::TcpListener::bind("127.0.0.1:0").await.unwrap();
    let client = app.clone();
    tokio::spawn(async move {
        let _ = axum::serve(listener, app).await;
    });

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
    assert_eq!(reg["success"], json!(true), "注册失败：{reg}");
    assert!(wait_ready(&client).await, "索引没能在 30 秒内到 ready");

    let pull_url = format!("/chatlab/sessions/{}/messages", "10001");
    let backdated = format!("{}", (2051222300i64 << 32) | 990);

    // 段一：单页排空 + 一次续拉——无重放。
    // limit 放大到能一页装下全部 6 行（5 行常规 + 回溯行）。全部常规行共享
    // 同一时间戳、回溯行时间更早：时间序把回溯行排在最前、页末轮不到它——
    // 旧代码编码页末行，续拉把回溯行再交付一次。
    let v = json_of(&client, req("GET", &format!("{pull_url}?limit=5000"))).await;
    let first_page: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(v["sync"]["hasMore"], json!(false), "必须单页排空");
    assert!(first_page.contains(&backdated), "回溯行必须在排空页里");
    assert_eq!(first_page.len(), 6, "排空页必须交付全部 6 行");

    let c0 = v["sync"]["nextCursor"].as_str().unwrap();
    let v = json_of(
        &client,
        req("GET", &format!("{pull_url}?limit=5000&cursor={c0}")),
    )
    .await;
    let got: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert!(
        got.is_empty(),
        "排空后按游标续拉必须为空（重放判据），实际交付：{got:?}"
    );

    // 段二：空页游标语义。
    //  - 带 cursor 的空页：进度保持（游标值不变，不回退为从头拉取）；
    //  - 无 cursor 的空 time-filtered 页：游标编码「end 界内已确立的最大
    //    rowid」——不是 0（从头拉取）。周期轮询的客户端（end=now）采纳空页
    //    游标再丢掉时间过滤，必须不重放全历史。
    let v = json_of(
        &client,
        req("GET", &format!("{pull_url}?limit=5000&cursor={c0}&end={}", 2051222350)),
    )
    .await;
    let got: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert!(got.is_empty(), "end 过滤（早于回溯行）必须让本页为空");
    let c1 = v["sync"]["nextCursor"].as_str().unwrap();
    assert_eq!(c1, c0, "带 cursor 的空页必须保持进度（游标不变）");

    // 无 cursor 的空 time-filtered 页：窗口 (2051222320, 2051222350] 内
    // 无行，但「ts ≤ end 的行都已确立」——游标必须编码回溯行（ts=2051222300、
    // rowid 最大），而不是 0（从头拉取）。周期轮询的客户端（end=now）采纳
    // 空页游标再丢掉时间过滤，必须不重放全历史。
    let v = json_of(
        &client,
        req("GET", &format!("{pull_url}?limit=5000&since=2051222320&end=2051222350")),
    )
    .await;
    let got: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert!(got.is_empty(), "end 过滤必须让本页为空");
    use base64::Engine as _;
    let c_empty_raw = v["sync"]["nextCursor"].as_str().unwrap();
    let decoded = String::from_utf8(
        base64::engine::general_purpose::STANDARD
            .decode(c_empty_raw)
            .unwrap(),
    )
    .unwrap();
    let rid: i64 = decoded
        .split('.')
        .nth(2)
        .and_then(|s| s.parse().ok())
        .unwrap_or(-1);
    assert_ne!(rid, 0, "空页游标不得编码 rowid 0（从头拉取）：{decoded}");
    let c_empty = c_empty_raw.to_string();
    // 采纳空页游标续拉（不带时间过滤）：不得重放全历史。
    let v = json_of(
        &client,
        req("GET", &format!("{pull_url}?limit=5000&cursor={c_empty}")),
    )
    .await;
    let got: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert!(
        got.is_empty(),
        "空页游标（end 界内已确立进度）续拉不得重放全历史，实际交付：{got:?}"
    );

    // 段三：排空后追加 → 游标续拉可达（沿用契约用例的形态）。
    let seq = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    {
        let writer = writer.clone();
        tokio::task::spawn_blocking(move || {
            let conn = writer.lock();
            common::append_group_row(&conn, 991 + seq, "排空后追加");
        })
        .await
        .unwrap();
    }
    common::materialize_source(&nt_db);
    {
        let state = state.clone();
        let _ = tokio::task::spawn_blocking(move || state.sync.sync_all()).await;
    }
    // 基准游标是段一的排空游标 c0（全量排空线，全部行在链内）；段二的空页
    // 游标属于「end=2051222350 的时间过滤线」，拿它接无过滤链会跳过
    // rowid 更小的常规行——那不是缺陷，是线语义。
    let v = json_of(
        &client,
        req("GET", &format!("{pull_url}?limit=5000&cursor={c0}")),
    )
    .await;
    let got: Vec<String> = v["messages"]
        .as_array()
        .unwrap()
        .iter()
        .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
        .collect();
    assert_eq!(
        got,
        vec![format!("{}", (2051222400i64 << 32) | (991 + seq))],
        "排空后追加的行必须经排空游标可达（且只交付它）"
    );

    // 段四：同秒组 + 游标续接——页推进按 rowid。从段三的游标线继续（cursor
    // 模式全链），limit=2：追加的三个同秒行被切成 2+1 两页。
    let seq2 = SEQ.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
    {
        let writer = writer.clone();
        tokio::task::spawn_blocking(move || {
            let conn = writer.lock();
            common::append_group_row(&conn, 992 + seq2, "同秒追加一");
            common::append_group_row(&conn, 993 + seq2, "同秒追加二");
        })
        .await
        .unwrap();
    }
    common::materialize_source(&nt_db);
    let _ = tokio::task::spawn_blocking(move || state.sync.sync_all()).await;
    let mut cursor: Option<String> = Some(c0.to_string());
    let mut pages: Vec<Vec<String>> = Vec::new();
    loop {
        let q = match &cursor {
            Some(c) => format!("?limit=2&cursor={c}"),
            None => unreachable!("cursor 模式排空链游标必须恒在场"),
        };
        let v = json_of(&client, req("GET", &format!("{pull_url}{q}"))).await;
        let page_ids: Vec<String> = v["messages"]
            .as_array()
            .unwrap()
            .iter()
            .map(|m| m["platformMessageId"].as_str().unwrap().to_string())
            .collect();
        let more = v["sync"]["hasMore"].as_bool().unwrap();
        pages.push(page_ids);
        cursor = Some(
            v["sync"]["nextCursor"]
                .as_str()
                .expect("cursor 模式排空链上 nextCursor 必须在场")
                .to_string(),
        );
        if !more {
            break;
        }
    }
    let all: Vec<String> = pages.iter().flatten().cloned().collect();
    let mut sorted = all.clone();
    sorted.sort();
    let mut expected = [
        // 本次循环交付的是 rowid 大于排空游标的三条同秒追加行（段三一条 + 这里
        // 两条），按 rowid 序被 limit=2 切成 2+1 两页。
        (2051222400i64 << 32) | (991 + seq),
        (2051222400i64 << 32) | (992 + seq2),
        (2051222400i64 << 32) | (993 + seq2),
    ]
    .iter()
    .map(|v| v.to_string())
    .collect::<Vec<_>>();
    expected.sort();
    // 页推进判别：修复前页推进按时间组——同秒行整体一页（页数=1）；修复后
    // 按 rowid 推进，limit=2 把三条同秒行切成两页（2+1）。
    assert_eq!(
        pages.len(),
        2,
        "cursor 模式页推进必须按 rowid（limit=2 把三条同秒行切成两页），实际页数：{}",
        pages.len()
    );
    assert_eq!(sorted, expected, "游标链（limit=2）交付集合必须与追加行逐条相等");
    let mut dedup = all;
    dedup.sort();
    dedup.dedup();
    assert_eq!(dedup.len(), sorted.len(), "游标链不得重复交付（重放判据）");
}
