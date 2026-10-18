# qqflow-server

无头 HTTP API + SSE 服务：读取本地 QQ NT 版聊天记录（SQLCipher 解密 `nt_msg.db`）。
独立实现，接口形态参考 **WeFlow HTTP API**。

## 获取

| 方式 | 入口 | 说明 |
|---|---|---|
| Release 二进制 | GitHub Releases 的最新 tag（三平台归档 ＋ `SHA256SUMS`） | 免工具链；校验后再运行 |
| `cargo install` | `cargo install --locked qqflow-server` | 从 crates.io 编译 |
| 源码构建 | `powershell -File scripts\build.ps1 build --release`（Windows）／`bash scripts/build.sh build --release` | 前置工具链见 [AGENTS.md](AGENTS.md) 的「命令」 |

## 起服务

```powershell
qqflow-server            # 默认 127.0.0.1:5032
qqflow-server --port 5032 --host 127.0.0.1 --log info
```

- token 首次启动生成并存入系统凭据库（Windows 凭据管理器 / macOS 钥匙串 / Linux Secret Service）；
  `qqflow-server token` 打印。CLI 的 token 来源链是 `QQFLOW_TOKEN` → 本机凭据库。凭据库不可用时
  token 只在启动日志打印一次（会话级），详见 quickstart 配方五。
- 数据目录：Windows `%LOCALAPPDATA%\qqflow-server`。
- 停服务：前台 `Ctrl-C`。

## 注册账号（第一次查询之前）

服务是**零账号启动**的：`/health` 免鉴权、可先看阶段，而业务端点在账号 `ready` 之前一律返回
503。启动时它会扫描本机 QQ 目录，但**不会自己去读密钥**——库密钥（16 字节 ASCII）要你用独立
工具提取（如 QQBackup / qq-win-db-key，本仓库不做密钥提取）。注册就一次 POST：

```bash
curl -X POST -H "Authorization: Bearer $QQFLOW_TOKEN" -H "Content-Type: application/json" \
  -d '{"qq":"<QQ 号>","key":"<16 字节库密钥>","db_path":"<nt_msg.db 或其所在目录>"}' \
  http://127.0.0.1:5032/api/v1/accounts
```

受理返回 `state: "accepted"`，后台开始建索引；轮询到 `ready` 即可查询：

```bash
curl -H "Authorization: Bearer $QQFLOW_TOKEN" http://127.0.0.1:5032/api/v1/accounts
```

`db_path` 允许松散（给库文件或其上级目录都可），字段全谱与重启恢复见
[quickstart 配方五](docs/quickstart.md#配方五重启后恢复账号)与 api 文档的注册端点小节。

## 第一个请求

```bash
curl http://127.0.0.1:5032/health
curl -H "Authorization: Bearer $QQFLOW_TOKEN" \
  "http://127.0.0.1:5032/api/v1/sessions?limit=10"
```

`/health` 免鉴权且只回标量阶段；账号身份、消息数与库路径在需鉴权的 `GET /api/v1/accounts`。

## 范围

- ✅ 解密层：SQLCipher 4（kdf_iter=4000 / HMAC-SHA1 / PBKDF2-HMAC-SHA512 / AES-256-CBC），
  剥离 1024B 自定义头；直读实时源库（QQ 运行中可读，无镜像）。
- ✅ 数据读取：全量扫描建内存索引 + 文件系统事件驱动的增量同步（notify watch + 慢速兜底轮询）。
- ✅ 服务封装：axum HTTP + SSE，WeFlow 参考端点与字段。
- ✅ 媒体：`GET /api/v1/media/{id}` 从 QQ 本地缓存直服，未命中再按导出文件名在导出根下解析；
  `media=1` 按需导出。
- ✅ 三平台：Windows / Linux / macOS。
- ❌ 不做密钥提取：密钥由外部工具提供，运行期注册（见上）。
- ✅ 任务配方：[quickstart](docs/quickstart.md)（接 ChatLab、只要增量、导出含媒体、MCP、重启恢复）。

## 重启后恢复账号

密钥只存进程内存；服务重启后账号回到未注册（业务端点 503）。用
[`scripts/examples/register-accounts.ps1`](scripts/examples/register-accounts.ps1)（或
[`.sh`](scripts/examples/register-accounts.sh)）从你自备的本地配置读 key 后重新注册；完整配方
见 [docs/quickstart.md](docs/quickstart.md#配方五重启后恢复账号)。脚本只读配置、不代存密钥。

## 去哪看细节

| 想知道 | 看 |
|---|---|
| 这个版本改了什么（逐版行为变化与迁移） | [CHANGELOG.md](CHANGELOG.md) |
| 按任务怎么做（ChatLab/增量/导出/MCP/恢复） | [docs/quickstart.md](docs/quickstart.md) |
| HTTP 端点、CLI、退出码、错误形状 | [docs/qqflow-server-api.md](docs/qqflow-server-api.md) |
| 模块职责、feature 矩阵、嵌入与测试纪律 | [docs/architecture.md](docs/architecture.md) |
| agent 客户端接入（stdio MCP） | [docs/mcp.md](docs/mcp.md) |
| 发版流程与凭据/审批闸门 | [docs/release-runbook.md](docs/release-runbook.md) |
| 贡献者命令、钩子、提交规范、作为库嵌入 | [AGENTS.md](AGENTS.md) 与 [examples/embed.rs](examples/embed.rs) |
| 类型化 SDK | `clients/rust`、`clients/python`；`clients/ts` **未发布 npm**，从源码取 |

## 鸣谢

本项目借鉴了以下项目的部分功能特性（均为行为规格层面的参考，代码独立编写）：

- [hicccc77/WeFlow](https://github.com/hicccc77/WeFlow)
- [yfgug/QQFlow](https://github.com/yfgug/QQFlow)

## 免责声明

仅供个人学习、研究与本地数据备份。API 仅监听 127.0.0.1；密钥经 HTTP 传入且仅内存保存
（不落盘）；鉴权 token 存 OS 凭据库（本地回环场景，非防泄密机制）；QQ 升级可能导致列名/消息格式解析退化
（结构化解析优先、启发式兜底，天然容错）。参考实现（yfgug/QQFlow）无 LICENSE，本仓库代码均按行为规格重写，未逐字复制。
