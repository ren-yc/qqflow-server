# qqflow-server

无头 HTTP API + SSE 服务：读取本地 QQ NT 版聊天记录（SQLCipher 解密 `nt_msg.db`）。
独立项目。接口形态参考 **WeFlow HTTP API**（契约见 campus-info-hub-py 项目 `src/sources/weflow/weflow-api.md`），仅作为功能实现参考。

## 范围（v1）

- ✅ 解密层：SQLCipher 4（kdf_iter=4000 / HMAC-SHA1 / PBKDF2-HMAC-SHA512 / AES-256-CBC），剥离 1024B 自定义头
- ✅ 数据读取：全量扫描建内存索引 + 文件系统事件驱动的增量同步（notify watch + 慢速兜底轮询；WAL 通过镜像目录同步，支持 QQ 运行中实时读取）
- ✅ 服务封装：axum HTTP + SSE，WeFlow 参考端点与字段
- ✅ 三平台：Windows / Linux / macOS（仅路径探测不同，代码已按平台门控）
- ❌ **不做密钥提取**：密钥由外部工具提供（`QQBackup/qq-win-db-key` 等），输入方式见运行节
- ❌ 媒体文件导出、SNS 接口（QQ 无此数据）：v1 不实现

## 构建

原理：`rusqlite` 的 `bundled-sqlcipher-vendored-openssl` 特性在构建时源码编译 SQLCipher + OpenSSL（openssl-src），需要 C 工具链与 perl。Windows 的 openssl 构建流程直接调用 MSVC 的 cl/link（绕过 cc crate 探测），必须注入 vcvars 环境——`build.ps1` 用 vswhere 自动定位 vcvars64.bat（`QQFLOW_VCVARS` 环境变量可覆盖），并自动检查/前置 Strawberry Perl 与 nasm（Git 的 MSYS perl 会被拒绝）。

| 平台 | 前置条件 | 构建命令 |
|---|---|---|
| Windows | Rust MSVC toolchain + Visual Studio（Desktop C++ 工作负载）+ [Strawberry Perl](https://strawberryperl.com)（含 nasm） | `powershell -File scripts\build.ps1 build` |
| Linux | Rust + `build-essential`（gcc/make；perl 系统自带） | `bash scripts/build.sh build` |
| macOS | Rust + Xcode Command Line Tools（`xcode-select --install`；perl 系统自带） | `bash scripts/build.sh build` |

wrapper 透传全部 cargo 参数（`clippy`/`test`/`build --release` 等同理）；工具链由 `rust-toolchain.toml` 锁定（rustc 1.97.1）。

## 运行

```powershell
# 1. 用独立工具提取密钥
irm https://raw.githubusercontent.com/QQBackup/qq-win-db-key/master/scripts/windows/ntqq/windows_ntqq_get_key.ps1 | iex

# 2. 启动（无配置文件；参数全部由命令行指定，均有默认值）
.\qqflow-server.exe
.\qqflow-server.exe --port 5031 --host 127.0.0.1 --log info
.\qqflow-server.exe --help
```

命令行参数：`--port`（默认 5031）/ `--host`（默认 127.0.0.1）/ `--log`（默认 info，error|warn|info|debug）/ `--watch-debounce-ms`（默认 350，文件事件防抖）/ `--watch-fallback-ms`（默认 30000，慢速兜底轮询，0 关闭；watcher 失效后的自动重连不受此开关影响，固定每 10 秒重试）。

**账号为客户端驱动**：启动后服务以空账号状态运行（`/health` 列出平台扫描发现的账号，状态 `awaiting_key`）；密钥不由配置提供，由客户端运行时注册（仅内存保存，不持久化）：

```bash
curl -X POST http://127.0.0.1:5031/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d "{\"qq\": \"<QQ号>\", \"key\": \"<16字节密钥>\", \"db_path\": \"C:\\\\Users\\\\<用户名>\\\\Documents\\\\Tencent Files\", \"access_token\": \"<token.txt内容>\"}"
```

`db_path` 可为 `nt_msg.db` 文件路径或 Tencent Files 风格目录（省略则复用扫描到的路径）；密钥错误时账号进入 `error` 状态，重新注册即可恢复。

默认 `http://127.0.0.1:5031`，token 自动生成并持久化到 `<data-dir>/token.txt`（启动日志仅打印文件路径，不打印 token 值）。

## API（与 WeFlow 契约对齐）

| 端点 | 说明 |
|---|---|
| `GET/POST /health`、`/api/v1/health` | 健康检查（免鉴权） |
| `POST /api/v1/accounts` | 注册账号：`qq` + `key` + 可选 `db_path`（客户端驱动启动） |
| `GET/POST /api/v1/messages` | `talker` 必填；`limit/offset/start/end/keyword/chatlab/format` |
| `GET/POST /api/v1/sessions` | 会话列表（`format=chatlab` 输出 ChatLab 形态） |
| `GET /api/v1/sessions/{id}/messages` | ChatLab Pull 增量同步（`since/end/limit/offset` + `sync` 块） |
| `GET/POST /api/v1/contacts` | 联系人（消息中出现过的 UID→昵称） |
| `GET/POST /api/v1/group-members` | 群成员（`chatroomId`，`includeMessageCounts`） |
| `GET/POST /api/v1/push/messages` | SSE：`sync`（基线，含水位线）→ `message.new` / `message.revoke` |
| `GET/POST /api/v1/sync` | 手动同步（镜像刷新 + 增量读取，返回新增消息） |

鉴权三方式：`Authorization: Bearer <token>` / `?access_token=`（SSE 推荐）/ POST JSON Body。

```bash
curl -H "Authorization: Bearer <token>" "http://127.0.0.1:5031/api/v1/sessions"
curl -N "http://127.0.0.1:5031/api/v1/push/messages?access_token=<token>"
```

## 测试

```powershell
powershell -File scripts\build.ps1 test          # Windows
bash scripts/build.sh test                        # Linux/macOS
```

- `tests/sqlcipher_roundtrip.rs`：自建 SQLCipher 测试库（QQ 同参数 + 1024B 头 + WAL）验证
  解密 round-trip、WAL 实时路径、checkpoint 重建 —— **不触碰真实 QQ 数据**
- `tests/api_smoke.rs`：HTTP 层（tower oneshot，无网络）契约测试
- `tests/fs_watch_e2e.rs`：文件系统事件 → 同步 → SSE 广播的端到端测试（假库）
- `tests/real_db_groundtruth.rs`：真实 QQ 库 ground-truth 查询（默认 `#[ignore]`，需
  `QQFLOW_TEST_DB_ROOT` / `QQFLOW_TEST_DB_KEY` 环境变量）
- `tests/downstream_client.rs`：模拟下游客户端的 GET/POST 请求（三种鉴权、错误信封、
  ChatLab Pull 翻页、群成员、手动同步、SSE），走真实管线读真实 QQ 库（默认 `#[ignore]`；
  输入优先读项目根 `qqflow-server.json` 的 `qq`/`key`/`db_path`，缺失时回退
  `QQFLOW_TEST_QQ` / `QQFLOW_TEST_DB_KEY` / `QQFLOW_TEST_DB_ROOT` 环境变量）

## 免责声明

仅供个人学习、研究与本地数据备份。API 仅监听 127.0.0.1；密钥经 HTTP 传入且仅内存保存
（不落盘），鉴权依赖本地 token.txt，均非安全机制；QQ 升级可能导致列名/消息格式解析退化
（启发式解析天然容错）。参考实现（yfgug/QQFlow）无 LICENSE，本仓库代码均按行为规格重写，
未逐字复制。
