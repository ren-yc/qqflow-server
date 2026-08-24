# qqflow-server

无头 HTTP API + SSE 服务：读取本地 QQ NT 版聊天记录（SQLCipher 解密 `nt_msg.db`）。
独立实现，接口形态参考 **WeFlow HTTP API**。

## 范围

- ✅ 解密层：SQLCipher 4（kdf_iter=4000 / HMAC-SHA1 / PBKDF2-HMAC-SHA512 / AES-256-CBC），剥离 1024B 自定义头；直读实时源库（QQ 运行中可读，无镜像）
- ✅ 数据读取：全量扫描建内存索引 + 文件系统事件驱动的增量同步（notify watch + 慢速兜底轮询）
- ✅ 服务封装：axum HTTP + SSE，WeFlow 参考端点与字段
- ✅ 媒体：`GET /api/v1/media/{id}` 从 QQ 本地缓存直服；`media=1` 按需导出（`--media-export-dir`）经路径式路由提供
- ✅ 三平台：Windows / Linux / macOS
- ❌ **不做密钥提取**：密钥由外部工具提供（`QQBackup/qq-win-db-key` 等），运行期注册（见下）

## 构建

| 平台 | 前置条件 | 构建命令 |
|---|---|---|
| Windows | Rust MSVC toolchain + Visual Studio（Desktop C++ 工作负载）+ [Strawberry Perl](https://strawberryperl.com) | `powershell -File scripts\build.ps1 build` |
| Linux | Rust + `build-essential`（gcc/make；perl 系统自带） | `bash scripts/build.sh build` |
| macOS | Rust + Xcode Command Line Tools（`xcode-select --install`；perl 系统自带） | `bash scripts/build.sh build` |

构建需源码编译 SQLCipher + OpenSSL，故要求 C 工具链与 perl；wrapper 会自动定位 MSVC 环境与 Perl/nasm（Windows 专属），并透传全部 cargo 参数（`test`/`clippy`/`build --release` 等同理）。工具链由 `rust-toolchain.toml` 锁定。

## 运行

```powershell
# 1. 用独立工具提取密钥
irm https://raw.githubusercontent.com/QQBackup/qq-win-db-key/master/scripts/windows/ntqq/windows_ntqq_get_key.ps1 | iex

# 2. 启动（无配置文件；参数全部由命令行指定，均有默认值）
.\qqflow-server.exe
.\qqflow-server.exe --port 5032 --host 127.0.0.1 --log info
.\qqflow-server.exe --help
```

命令行参数：`--port`（默认 5032）/ `--host`（默认 127.0.0.1）/ `--log`（默认 info，error|warn|info|debug）/ `--watch-debounce-ms`（默认 350，文件事件防抖）/ `--watch-fallback-ms`（默认 30000，慢速兜底轮询，0 关闭；watcher 失效后的自动重连不受此开关影响，固定每 10 秒重试）/ `--media-export-dir`（`media=1` 的媒体导出根目录，默认 `<data-dir>/api-media`）/ `--base-url`（`mediaUrl` 链接的 base URL，默认 `http://<host>:<port>`；绑定 `0.0.0.0`/`::` 时自动回退 `127.0.0.1`，局域网客户端请显式指定）。

**账号为客户端驱动**：启动后服务以空账号状态运行（`/health` 列出平台扫描发现的账号，状态 `awaiting_key`）；密钥不由配置提供，由客户端运行时注册（仅内存保存，不持久化）：

```bash
curl -X POST http://127.0.0.1:5032/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d "{\"qq\": \"<QQ号>\", \"key\": \"<16字节密钥>\", \"db_path\": \"C:\\\\Users\\\\<用户名>\\\\Documents\\\\Tencent Files\", \"access_token\": \"<token.txt内容>\"}"
```

`db_path` 可为 `nt_msg.db` 文件路径或 Tencent Files 风格目录（省略则复用扫描到的路径）；密钥错误时账号进入 `error` 状态，重新注册即可恢复。

默认 `http://127.0.0.1:5032`，token 自动生成并持久化到 `<data-dir>/token.txt`（启动日志仅打印文件路径，不打印 token 值）。完整接口文档见 `docs/qqflow-server-api.md`。

## API（与 WeFlow 契约对齐）

| 端点 | 说明 |
|---|---|
| `GET/POST /health`、`/api/v1/health` | 健康检查（免鉴权） |
| `POST /api/v1/accounts` | 注册账号：`qq` + `key` + 可选 `db_path`（客户端驱动启动） |
| `GET/POST /api/v1/messages` | `talker` 必填；`limit/offset/start/end/keyword/chatlab/format`；`media`/`meiti` 触发媒体导出，`image`/`tupian`/`voice`/`vioce`/`video`/`emoji` 子开关 |
| `GET/POST /api/v1/sessions` | 会话列表（`format=chatlab` 输出 ChatLab 形态） |
| `GET /api/v1/sessions/{id}/messages` | ChatLab Pull 增量同步（`since/end/limit/offset` + `sync` 块） |
| `GET/POST /api/v1/contacts` | 联系人（消息中出现过的 UID ∪ 档案/映射 UID；`alias` 承载 QQ 号） |
| `GET/POST /api/v1/group-members` | 群成员（`chatroomId`，`includeMessageCounts`） |
| `GET/POST /api/v1/media/{id}`、`/api/v1/media/{talker}/{mediaType}/{file}` | 媒体直服（本地缓存）/ 导出文件服务 |
| `GET/POST /api/v1/push/messages` | SSE：`sync`（基线，含水位线）→ `message.new` / `message.revoke`；媒体消息携带 `media` 元数据（**无本地路径**）与可直取的 `mediaId` |
| `GET/POST /api/v1/sync` | 手动同步（增量读取 + 名称映射刷新，返回新增消息） |

鉴权三方式：`Authorization: Bearer <token>` / `?access_token=`（SSE 推荐）/ POST JSON Body。

```bash
curl -H "Authorization: Bearer <token>" "http://127.0.0.1:5032/api/v1/sessions"
curl -N "http://127.0.0.1:5032/api/v1/push/messages?access_token=<token>"
```

## 测试

```powershell
powershell -File scripts\build.ps1 test          # Windows
bash scripts/build.sh test                        # Linux/macOS
```

真库验证（ground-truth 探针与下游客户端模拟）默认跳过，需真实 QQ 密钥与库路径的环境变量开启，见 `tests/real_db_groundtruth.rs` 与 `tests/downstream_client.rs`。

## 鸣谢

本项目借鉴了以下项目的部分功能特性。

[hicccc77/WeFlow](https://github.com/hicccc77/WeFlow)

[yfgug/QQFlow](https://github.com/yfgug/QQFlow)

## 免责声明

仅供个人学习、研究与本地数据备份。API 仅监听 127.0.0.1；密钥经 HTTP 传入且仅内存保存
（不落盘），鉴权依赖本地 token.txt，均非安全机制；QQ 升级可能导致列名/消息格式解析退化
（结构化解析优先、启发式兜底，天然容错）。参考实现（yfgug/QQFlow）无 LICENSE，本仓库代码均按行为规格重写，未逐字复制。
