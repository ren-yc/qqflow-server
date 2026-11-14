# 架构

本文件是 qqflow-server 的架构事实源：模块职责、数据流与跨模块陷阱。逐字段说明在 [`qqflow-server-api.md`](qqflow-server-api.md)——**接口文档说有哪些字段，本文件说为什么是这样**。

## 目录

- [定位与不变量](#定位与不变量)
- [总览与数据流](#总览与数据流)
- [核心模块](#核心模块)
- [库面与 feature](#库面与-feature)
- [服务层](#服务层)
- [工程与工具链](#工程与工具链)
- [测试与夹具](#测试与夹具)
- [设计要点与陷阱](#设计要点与陷阱)

## 定位与不变量

QQ NT 消息库的只读 HTTP / SSE 服务。下面五条是**不变量**——破坏任何一条都是设计层面的变更，不是实现细节。

1. **只读**：从不写用户的数据库。连接以只读方式打开并带 `query_only`，写入会在 SQLite 层被拒绝，而不是「我们小心不去写」。
2. **内存索引**：会话、联系人、群名片等全部解析进内存，不落任何中间库。代价是重启要重建索引；收益是没有第二份数据副本、也不必处理索引与源库的不一致。
3. **单账号**：一个进程绑定一个账号。注册之前不提供查询——这不是策略，是数据前提：密钥与账号身份共同决定能打开哪些库。
4. **密钥不落盘**：数据库密钥由外部提供，只存在于进程内存；本服务不做密钥提取。
5. **无中间件**：没有数据库、没有消息队列，全部状态都在进程内。

## 总览与数据流

```text
QQ 数据目录（nt_db 等）
  ├─ *.db        会话 / 消息 / 联系人
  └─ 库文件前 1024 字节是 QQ 自有的明文头
        │  ① 自定义 VFS：在文件层虚拟跳过明文头（db/vfs.rs）
        ▼
   只读连接（db/live.rs，经 db/decrypt.rs 校验密钥）
        │  ② 值驱动探测：先看有哪些表与列，再决定读什么（db/scan.rs）
        ▼
   解析（parser/mod.rs；proto.rs 管容器格式，types.rs 管类型空间）
        │  ③ 建索引（store/index.rs 的 build_all_live）
        ▼
   内存索引 Store（store/mod.rs）
        ├─ ④ 查询：handlers/* 读 Store → 信封 → JSON
        └─ ⑤ 增量：sync/mod.rs 轮询 + sync/watch.rs 指纹
                │  新消息 / 撤回 → 事件（sync/events.rs）
                ▼
           SSE 总线（handlers/push_events.rs）→ 订阅者
```

三条容易看错的路径：

- **`db/vfs.rs` 不是「解密」**：它只把明文头偏移藏起来，让上层当普通库打开；真正的密钥校验在 `db/decrypt.rs`。
- **`sync/events.rs` 是事件类型的唯一出处**：SSE 载荷与总线都用它，改形状要同时看两端。
- **`sync/watch.rs` 只决定「要不要再轮询」**：真正的增量来自数据库水位线，指纹只挡住无谓的轮询。

## 核心模块

按目录与顶层文件列出（单文件细节见各文件头部的 `//!` 注释）。

| 模块 | 职责 | 对外接口 |
|---|---|---|
| `main.rs` / `lib.rs` | 进程入口与库入口（承诺面 `api` ＋ 实现面） | `api` |
| `config.rs` / `logging.rs` | 命令行与数据目录、token 凭据库；日志初始化 | `Config` / `init_*` |
| `cli.rs` / `export.rs` | 子命令分流与退出码；批量导出（不碰网络，取数按页回调） | `dispatch` / `export::run` |
| `mcp.rs` | MCP 工具面：stdio 只读工具、字段投影与 ≈32 KB 截断预算 | `run` / 工具集 |
| `pathsafe.rs` | 纯守卫：路径分量与导出根目录的边界检查 | `slugify` / 校验函数 |
| `db/vfs.rs` | 自定义 VFS：虚拟跳过 1024 字节明文头偏移 | 注册函数 |
| `db/decrypt.rs` / `db/live.rs` | 打开解密库并校验密钥；活库只读连接 | `open_live` / `open_decrypted` |
| `db/scan.rs` | 值驱动探测：库 → 表 → 列，账号发现分三平台分支 | `scan_accounts` |
| `parser/{mod,proto,types}.rs` | 解析入口；平台容器格式；类型码映射与归一 | `parse_message` / `chatlab_type` |
| `store/mod.rs` / `store/index.rs` | 内存索引的数据结构、共享状态与建索引 | `Store` / `build_all_live` |
| `store/{names,query}.rs` | 显示名解析（备注 / 昵称 / 群名片优先级）；查询原语 | `query_*` |
| `store/{media,media_export}.rs` | 媒体路径登记；媒体导出与导出 URL 构造 | `contains_key` / `export_*` |
| `sync/{mod,events,watch}.rs` | 增量轮询与水位线；事件类型；文件指纹轮询 | `AccountSync` / `Event` |
| `server/mod.rs` ＋ `server/handlers/*` | 路由装配、鉴权、SSE 总线；共享件与各端点实现 | `build_router` / `parse_limit` |

```text
src/
├─ config.rs / logging.rs      # 命令行与数据目录、日志
├─ keystore/                   # 密钥形状校验；仅内存
├─ db/                         # 偏移 VFS / 解密与密钥校验 / 值驱动探测
├─ parser/                     # 平台容器格式与类型空间（proto / types）
├─ store/                      # 内存索引（会话/联系人/消息/水位）＋查询
├─ sync/                       # 增量同步引擎（poll + 事件）＋ watch
└─ server/                     # axum：鉴权、账号注册、端点、SSE、媒体
tests/                         # 假库夹具、HTTP 契约、e2e、真库探针（#[ignore]）
clients/                       # 类型化 SDK（rust / python / ts；ts 未发布）
```

## 库面与 feature

这个 crate 既是服务、也是库，两条路径共用同一份实现。

### 承诺面只有一处：`api`

其余模块在默认构建下是 `pub(crate)`——外部不可达，边界由编译器强制。理由是 `store::Store` 的字段是 `pub`（内部模块要写它），直接放出去等于把每一个字段都变成对外契约，而它们本来是内部布局。

| 面 | 内容 | 稳定性 |
|---|---|---|
| `api` | 只读索引、同步句柄、事件类型、密钥类型、数据本身 | 有 semver 承诺；`#![deny(missing_docs)]` 只作用在这里 |
| 其余模块 | 解析、存储、同步、服务层的实现 | 随时可变；仅 `--features testing` 下对集成测试可见 |

「只有 `api`」是空 feature 树下的口径：打开 `cli`／`server` 后根上还会多出入口函数（`run_cli` 等）。它们是二进制的入口；签名里的 `config::Config` 在默认构建下也不是公开可命名的类型。

嵌入契约只约束空 feature 树，所以上面这一点不影响它；但读到这句话的人应当知道：公开面在开 feature 之后确实会变大，不要以为编译器只放出了 `api`。

`testing` 只改可见性、不改功能：开它则实现面转 `pub`，同时它也是给嵌入者的造库工具面。嵌入者从 `api` 入手，`examples/embed.rs` 是它的活文档——不起 HTTP，直接把一个账号读出来。

同一 feature 开放 `QQFLOW_TOKEN_SERVICE`，集成测试可把 API token 的凭据库服务名指到专用空服务；默认构建把它编译掉，用户进程永远读内置服务名。

### feature 矩阵

| feature | 默认 | 内容 | 关掉的影响 |
|---|---|---|---|
| `server` | 是 | HTTP/SSE：axum ＋ tokio 运行时 ＋ OpenAPI 描述 | 没有服务，只剩库 |
| `cli` | 是 | 命令行子命令面：`clap` ＋ 复用 `qqflow-client`（隐含 `server`） | 只剩「旗标」这一条老路 |
| `mcp` | 是 | MCP 工具面：`rmcp` ＋ `schemars`，stdio 暴露只读工具 | 没有 `mcp` 子命令 |
| `sync` | 否 | watcher 与水位线增量：tokio ＋ notify | 没有增量同步；`api::Sync` 随之消失 |
| `testing` | 否 | 把实现面转成 `pub` | 集成测试够不着实现面 |

依赖面的实际约束（可测，不是口号）：`--no-default-features` 的依赖树里不含 axum、tokio、clap、SDK、rmcp 与 schemars。核心面（解析、存储）因此不得依赖可选面，这条边界由 CI 的一条检查守着。

### `clients/`：类型化 SDK（workspace 成员）

三个成员，与 src 的模块边界不同——它们消费 HTTP 面，不属于 crate 本体。

- **`clients/rust`（`qqflow-client`）**：类型与操作客户端 ＋ 手写行为层（就绪轮询 / 游标排空 / SSE 重连 / 媒体重试 / 检索）。`generated/` 只许生成器改（`clients/regen`，CI 断言重生成无 diff）。
- **`clients/regen`（`qqflow-regen`）**：生成工具。取 `server::openapi::document()`，做确定性规范化（3.1 → 3.0）后交给生成器；`--dump-spec` 同时供 Python 侧取规范化 spec。
- **`clients/python`（`qqflow-sdk`）**：模型由 openapi-generator 从同一份规范化 spec 生成（`scripts/regen.py`）；行为层是 `httpx.AsyncClient` 异步实现，与 Rust 侧逐方法同构。
  - `ensure_ready` 把注册应答的 200 拒绝态（`account_conflict` / `invalid_key` / `invalid_db_path` / `unknown_qq`）映射成 `StatusError` 快速失败。
  - `watch` 单连接连续产出多帧：字节级 LF 分帧、1 MiB 缓冲上限、单帧解码失败跳过不杀流、EOF 冲刷未终结残行、退避仅在干净结束时复位。
  - `MessageEvent` 携带 `event` 字段（载荷缺省时回落帧头事件名），`message.new` 与 `message.revoke` 可区分——这两个载荷不在描述 schema 里，由行为层解码。
- **`clients/ts`**：TypeScript 示例客户端（仅示例，不发布 npm）。演示级子集；配套 `stub-server.mjs` 可无真实服务跑 smoke。不进 CI 产物矩阵。

分层的理由：描述文档只声明「形状」，不声明「翻页到什么时候停、断线后从哪续」——后者是行为，生成不出来。SDK 不反依赖服务端 crate：只依赖 `reqwest`，对 API 的耦合全部在「描述 → 生成」这一步。

与 weflow 的水位语义不同：本仓水位是 SQLite 行号（weflow 是三元组），`generation` 变化后的补拉因此是「整会话重排空」。

## 服务层

服务层是「HTTP 请求 → 路由 → 处理器 → 存储」的全部实现面。

### 路由、方法与鉴权

路由与方法的唯一事实源是 `server::routes::ROUTES`（`build_router` 由它构建）。方法口径是「读端点只有 GET，动作端点保留 GET+POST」：

- **免鉴权**：`/health`、`/api/v1/health`（GET+POST）、`/openapi.json`（GET）。
- **账号**：`/api/v1/accounts`（GET+POST）、`/api/v1/accounts/{qq}`（DELETE）。
- **原生读面**：`messages`、`sessions`、`sessions/{id}/messages`、`contacts`、`group-members`、`media/{id}`、`push/messages`（均 GET，POST 一律 405）。
- **ChatLab 面**：`/chatlab/sessions`、`/chatlab/messages`、`/chatlab/sessions/{id}/messages`、`/chatlab/push/messages`（均 GET）。
- **动作**：`/api/v1/sync`（触发一次增量对账，不返回消息体；GET+POST）。

读端点砍掉 POST 的理由：两个方法完全等价，多一个只多一份「不一致」的可能；`/api/v1/sync` 是动作不是读，两个方法都留。

`/chatlab/*` 四条与老面共用同一份实现与同一条事件总线，差别在形状与参数而不是数据：ChatLab 会话面认 `cursor`（老面认 `offset`）、消息面 `talker` 必填且升序、信封不带 `success`、`media=1` 真正执行导出。

鉴权只有两条通道：`Authorization: Bearer <token>` 与 `?access_token=<token>`（常时比较）。`X-Api-Key`、`?token=` 与 POST JSON body 都不是通道——每多一条就多一处凭据会被复制到的地方。

### 响应形状只有一个事实源：`server/dto.rs`

每个端点的响应都由 `dto.rs` 里的 struct 定义，不再用 `json!` 字面量拼。键名写错从「运行时才知道」变成「编译不过」，而「哪些键在什么条件下出现」从「只存在于代码路径里」变成类型。

两条踩过才知道的纪律：

- **字段按字母序声明**。`json!` 走 `serde_json::Map`（默认 BTreeMap），历史响应的键本来就是字母序；而 struct 按声明序输出。不按字母序声明，「换 DTO」会顺带改动每个响应的键序。
- **`null` 与「省略」是两件事**。要 `null` 就写 `Option<T>` 且不加 `skip_serializing_if`；要省略才加。客户端常靠「键在不在」判断，顺手统一风格会让这个判据失效。

多形状端点各建 struct，不堆可选字段：`sessions` / `messages` / `accounts` 的响应形状由参数或状态决定，硬塞进一个「所有字段都可选」的类型会让它看起来合法而实际没有任何取值组合是对的。

`sessions` 的 `type` 在原生面是数字、在 ChatLab 面是字符串，因此是两个 struct。`messages` 与 `contacts` 面则复用核心层类型：`MessageOut` 与 `ContactOut`。

与 weflow 的 DTO 不共用，正是因为键级差异：`group-members` 的 `messageCount` 在本仓是条件键（不带 `includeMessageCounts=1` 时整个键不出现），weflow 恒输出该键；原生消息的媒体形状也不同。

照抄另一个仓库的 DTO 会改坏契约。

复用的理由：再写一份平行 struct 只会与它们漂移。代价是 `utoipa` 的 derive 渗进了 `parser` / `store`，这是有意的取舍；库边界下次划定时应换成服务层的 newtype。

共享状态 `AppState` 目前住在 `store/mod.rs`，却持有事件总线的发送端与服务侧的类型。新增字段别让它继续长——这是已知的层次问题，不是范式。

### 响应防线有三层，各管一件事

- **golden 快照**：管「你改了」——整个响应（含状态码与键序）逐字节比对；在 `tests/golden/*.json` ＋ `tests/api_smoke.rs` 的 `mod golden`。
- **schema 校验**：管「改成什么是合法的」——引用能否解析、operationId 唯一、多形状确实用 `oneOf`；在 `tests/openapi.rs`。
- **契约套件**：管「两个仓库是否一致」——同一份用例跑两边；在 `tests/conformance_runner.rs`（`#[ignore]`，CI 独立步骤）。

契约套件在 CI 里是独立一步：克隆 `conformance.pin` 所指的 tag，再驱动执行入口。另有一步只跑 `nails-*`（五条数据不变量；`FLOW_CONTRACT_CASE` 是子串过滤，改名别改漏）。

第五条 `no_mediaPath_in_pull` 防的是拉取面复活 `mediaPath`。停用某一步时要在注释里写清原因——一个不会红的门禁等于没有门禁。

快照那份有两个设计点：**易变值掩码值而不是删键**（删键会把形状一起丢掉），以及**时钟哨兵**——快照里出现接近「现在」的时间戳即失败，否则快照天天漂移，下一个人会习惯性地点「更新快照」。

哨兵不假设时间单位（秒与毫秒各比一次）——第一版只比秒级，于是毫秒级的 `updatedAt` 漏了过去。

### `/openapi.json` 由类型生成，且免鉴权

描述由 `#[derive(ToSchema)]` 从 DTO 生成，因此改 DTO 就改了它，不需要手工同步两处；副作用是 DTO 上的文档注释直接成了接口描述。

免鉴权是刻意的：它描述的是形状，不含账号、路径或密钥，而且正是给尚未拿到 token 的接入方看的。

`paths` 部分是按 OpenAPI 规范形状拼 JSON 再反序列化的——它是文档数据，不是契约响应；拼错了在加载时就会失败，而不是等到有人打开文档。

### SSE 总线

`GET /api/v1/push/messages` 是长连接：订阅 `AppState.bus`（`sync::history::EventBus`，把重放历史与 broadcast 通道绑成一件），迟到者靠重放缓冲补齐。

`/chatlab/push/messages` 挂在同一条总线上，只换了序列化器，且只发元信息、不发正文。拉取面 `GET /chatlab/sessions/{id}/messages` 与 `/api/v1/sessions/{id}/messages` 是同一 handler、同一形状。

- **历史由生产者单点写入**：`AccountSync` 与服务层都经 `EventBus::publish` 先写重放历史、再广播带 id 的载荷，订阅端只读快照。回归：`publish_records_history_without_subscribers`。
- **载体是类型不是 `json!`**：`sync::events::Event` 是带 `skip_serializing_if` 的 struct，`PushMedia` 在类型层面就没有 `aes_key`——密钥不会因为某次改动「忘了过滤」而泄露。
- **推送载荷没有快照护栏**（快照的模型是一次请求一次响应），所以它的键集由 `sse_payload_keys_are_pinned` 单独钉住；时间与路径类易变字段由掩码与哨兵处理，不在这里重复。

## 工程与工具链

构建、测试、clippy 都走一条包装脚本；工具链版本与构建 profile 钉死，钩子与 CI 门禁在这里落地。

### 一条命令：`scripts/build.ps1`（或 `.sh`）

构建、测试、clippy 都必须走包装脚本。原因不是偏好：rusqlite 捆绑 SQLCipher + vendored OpenSSL，后者的 perl `Configure` 会直接调 `cl.exe` / `link.exe`，绕过 cc crate 的自动 MSVC 探测。

没有 `vcvars64.bat` 注入的 `INCLUDE`/`LIB`/`PATH` 就编译不过。脚本同时把 Strawberry Perl 放到 `PATH` 前面：Git 自带的 MSYS perl 会把 Windows 路径写坏。工具链版本钉在 `rust-toolchain.toml`。

### 构建 profile 的两个刻意取舍

`[profile.dev]` 里有两项非默认设置，目的都是让开发机的构建产物不再无限膨胀：

- **`incremental = false`**：增量编译只对「同一个编译单元被反复小改」有收益。本仓的 `clippy` 与 `test` 走两套不同的 rustc 调用，各自的产物与增量目录互不复用；而增量目录里的条目绝大多数来自本仓自己的测试目标，并随依赖图变动持续累积，Cargo 不会回收。关掉它意味着每次改动都要完整重编本仓 crate——这是有意接受的代价。
- **`debug = "line-tables-only"`**：依赖侧的调试信息已由 `[profile.dev.package."*"]` 关掉，剩下的全部来自本仓 crate。降到行号表仍保留 backtrace 的文件名与行号。

`[profile.dev.package."*"] debug = 0` 是上面两条成立的前提，不要删：它把非成员单元的调试信息钉成关闭，使 Cargo 对宿主单元（构建脚本、proc-macro）的调试信息推断根本不参与。

一旦删掉，构建脚本所在目录的哈希随之改变，会触发 vendored OpenSSL 的完整重建。

不要用 `CARGO_INCREMENTAL` 在这两个值之间来回切——该环境变量优先级高于清单，`1` 会静默压过 `incremental = false`。

### 日志：装在哪、写去哪、说什么话

`src/logging.rs` 的模块头是这三条口径的事实源，这里只记架构层面的取舍。

- **subscriber 的安装位置**：CLI 分流在执行任何子命令之前装 warn 级 subscriber——没装 subscriber 时 `tracing::warn!` 整体是 no-op，子命令里那些「会话级失败原因」的日志会静默消失。
- serve 分支不在这里装：`dispatch` 在构造 clap 之前就把 `serve` 短路进老解析器，级别与格式仍由 `Config.log` 决定。
- **输出去向统一 stderr**（0.10.0 破坏性变更）：子命令的 stdout 是命令输出本身（`--json` 形状），日志混进去会毁掉管道消费；serve 日志合并后 `2>` 一处收齐。回归见 `log_json_format_emits_parseable_lines` 与 `serve_logs_go_to_stderr`。
- **子命令日志不带 ANSI 颜色码**：`tracing-subscriber` 不做 TTY 检测，重定向时照样写转义序列；靠 `NO_COLOR` 不行——开发机设了它、CI 没设，判据就变成「本地绿 CI 红」。回归见 `subcommand_failures_are_logged_on_stderr`。
- **`--log-format json`** 逐行一个 JSON 对象，只多一个 `tracing-serde`；**target 必须开着**——模块多，关掉后日志行没有「谁说的」。
- **语言口径与脱敏红线**：见 `src/logging.rs` 模块头（用户句中文、模块级诊断英文；日志禁入完整密钥／完整 token／消息正文，例外只有 serve 路径那两条「不打印就拿不到」的 token 行）。

### 提交钩子

`scripts/install-hooks.*` 装两个钩子：`pre-commit`（隐私扫描 + 编号引用扫描 + 文档折点与结构，累积退出码，任一项失败即拒绝提交）与 `commit-msg`（把提交信息交给同一个扫描器）。

bash 与 Python 是提交路径的硬依赖：缺失时钩子报错并阻止提交，而不是跳过——失败开放的检查等于没有检查。

### CI 的门

Linux 与 Windows 双平台跑 `clippy --all-targets -D warnings` ＋ 全量测试。除此之外还有几道与「接口化」直接相关的门：

- 公共段哈希比对（比对所 pin tag 的内容，不是本仓副本）：三个仓库的协作规则悄悄分叉。
- 编号引用扫描（`--tree` 与 `--ref`）：注释里出现仓库外材料的编号，读者无从还原上下文。
- 隐私形态层扫描（`PRIVACY_SHAPE_ONLY=1`，只跑 64-hex 层）：直推 master 且本机没装钩子时，密钥/token 形状的值漏进跟踪文件。
- `--no-default-features` 编译 `examples/embed.rs`：承诺面被实现面「借用」，或依赖树里混进 axum/tokio。
- 一致性套件：契约与实现分叉。

Python 只用于钩子与套件执行器，纯标准库——CI 与开发机都不需要装第三方包。

### 已登记的两类构建告警（预期内，处置＝维持现状）

门禁是 `-D warnings`，所以这里登记的是不会让门禁变红、但直接 `cargo build` 时会出现的两类噪声。两者都被诊断过，结论是无功能影响。

1. **默认 feature 组合下的 `dead_code`**：一批内部模块里的辅助函数（如 `db/decrypt.rs` 的 `open_decrypted`，镜像路径打开）在默认构建里看似未用——它们只被 `#[cfg(test)]` 单测调用。
   - CI 的 `clippy --all-targets` 由包装脚本补了 `--features testing`，所以看不到。判据是两条路径都能编译，不是「日志里没有 warning」。
2. **链接期 `LNK4099`（找不到 `ossl_static.pdb`）**：vendored OpenSSL 的静态库引用了自己的调试信息，而发布物里没有那份 PDB。只影响调试信息，不影响代码。不消掉的理由：加 `/ignore:4099` 会一并隐藏将来真实的 LNK4099。

## 测试与夹具

测试分三类，各有各的诚实边界；夹具只能造库，不能造索引。

### 三类测试，三种诚实

- **单元测试**（`src/**` 内嵌）：纯函数与解析逻辑，不碰库文件。
- **集成测试**（`tests/`）：用 tower 的 `oneshot` 直接打 router，不起网络；夹具造库。
- **真库探针**（`real_db_*` / `probe_*`）：`#[ignore]` ＋ 环境变量门控，CI 不跑，只打印统计、不打印任何真实标识或正文。群主列的判据就是这么定下来的。

### 夹具只能造库，不能造索引

夹具造库器住在 `src/testing/mod.rs`（随 `testing` feature 编译，默认构建里不存在），`tests/common/mod.rs` 只是它的转发层。为什么在库里：集成测试与根包二进制是两个互相看不见对方代码的独立 crate。

批量导出的夹具生成入口（CLI 的 `--rows`）也够不着只在 `tests/` 下存在的模块。

夹具建一个假的 QQ 数据目录（含 `nt_msg.db`、同族库 `group_info.db`、`profile_info.db`，以及带 1024 字节明文头的只读形态）。纪律是硬的：**禁止手工注入 store 字段**——那会让断言在一个真实运行时不存在的状态上通过。

同族库还有个坑：**写入必须发生在注册之前**，否则索引已经建完，名册与群主在整轮测试里都是空的。同一 `testing` feature 还开放 `QQFLOW_TOKEN_SERVICE`，避免并行用例互踩真实凭据库。

### 快照与键序

`tests/golden/*.json` 钉住状态码 ＋ 键序 ＋ 掩码后的内容。键序单列是因为 `serde_json` 的 `Value` 往返会按 BTreeMap 排序，只存 body 会把原始顺序抹掉。快照文件缺失即失败（缺了不再静默重建）：缺失与漂移是两种失败，但都必须看得见。

### 路由与接口描述的对等

路由的唯一事实源是 `server::routes::ROUTES`（`build_router` 由它构建，因此不存在「没进表的真实路由」）。

`/openapi.json` 的端点表与它的对等由 `documented_routes_match_the_openapi_table` 强制：集合必须等于「路由 − 豁免」（本仓豁免为空），且未声明的方法必须 405。这一条是补出来的：此前两份清单各自手写、无人比对，仍漏了两条真实操作。

### 一致性套件

`tests/conformance_runner.rs` 起真服务、造夹具，跑契约仓库（版本记在 `conformance.pin`）里的 36 条用例（`v0.8.0`：auth 1／cursor 13／discovery 3／envelope 6／lifecycle 8／nails 5）。

v0.6.1 至 v0.6.6 都是纯文档 tag，v0.7.0 新增跨仓文档模板；v0.8.0 移除该模板层、用例集未增删。换 pin 时这里要跟着改，它没有机器判据。

两条硬规矩：带 `--fail-on-skip`（有用例被跳过即失败），缺 `FLOW_CONTRACT_DIR` 即失败（不是跳过）。

夹具里的 `contractVersion` 与 pin 由 `pinned_contract_version_matches_the_fixture` 钉在一起。

### 文档锚点

`tests/docs_anchors.rs` 把「文档同步义务」变成可执行：本文件里的每个同文档锚点链接都必须命中真实标题，目录必须覆盖全部顶层章节。删掉目录项、改标题忘了改链接，都会红。

## 设计要点与陷阱

每一条都是不知道就会踩的那类。写新代码前扫一眼这里。对外承诺的字段形状另有落点：三种「没有值」的表示与规范偏离登记在 [`qqflow-server-api.md`](qqflow-server-api.md) 的「与标准 / 安装版的已知差异」「字段无值时怎么表示」两节。

那里的每个键都被 golden 快照或 `tests/sse_shape.rs` 的键集断言钉住。

### 索引构建与密钥

- **建索引进度用 `Arc` 原子计数，不碰 `Store` 的锁**：进度挂在账号状态上（`Arc<IndexProgress>`），建索引路径逐行/逐表更新原子量，HTTP 侧无锁读快照。分母只数消息表（`group_msg_table` / `c2c_msg_table`）；读失败的表不推进 `tables_done`，结束时少一格是「确实有一条表没读成」的诚实表达。
- **库密钥只有一把**（`api::open(db_path, key)`，没有 weflow 那种独立图片键）。密钥格式正确但内容过期时，注册受理后的后台解密失败：账号进入 `error`，`error` 字段与 `warn!` 日志都带同一句恢复提示（重新注册并核对 `db_path` 与库密钥）。
- **`message_resource.db` 的 HMAC 失配会在同步路径上周期性重试**，错误不一定上浮成一次显式失败；表现是资源/媒体长期缺件，而不是进程报错。遇到这种症状先按「密钥已轮换、参数过期或密钥缺失」复验，不要只查解析器。
- **明文头偏移要用自定义 VFS，`cipher_plaintext_header_size` 做不到**：SQLCipher 的该参数要求密钥派生的基准页一起变，与实际布局不匹配；因此在文件层虚拟跳过偏移。改这段前先读 `db/vfs.rs` 的头部注释。
- **账号发现分三个平台分支，只有 Windows 分支有独立实现**，其余走通配路径。改动发现逻辑时三个分支都要看——只测当前平台会漏掉另外两支。

### 类型码与字段

- **`isSend` 的归一不是简单的「1/2 → 1」**：平台的 direction 列有四种取值；列缺失或取值未知时落 `0`（即「别人发的」）。在列缺失的老版本库上，这会把自发消息判成对方消息。映射函数与边界用例在 `parser/types.rs`。
- **群名片按会话隔离**：群名片存在消息表的专用列里，按会话读取，不跨群共享。把它当「联系人的一个属性」缓存，会在多群场景下显示错名字。
- **回复关系的 `(会话, 40003)` 不是唯一键**：回复存在表列 `40850`（被回复消息的会话内序号），实现要在同一会话里找 `40003` 等于它的那一行、输出那一行的 `40001`。
- 重复不是罕见形态：大库抽样里重复组以千计，所以只在恰好一个候选（同一 `40003` 且不晚于本条的时间）时输出，否则省略键——命中多行时客户端会把引用挂到错误的消息上，而它分辨不出来。
- 上游文档说 `40003` 「可跨群复用」，漏了「群内也复用」。本地库形态优先于文档，按真实库核对过；量化数字留在源码注释里。
- **`mediaId` 只在本机确有可取路径时才下发**：「字段出现即可取字节」是承诺，不是尽力而为——登记表里没有该 id 时字段被置空。这条由所有产出消息的路径统一应用，避免一个端点给了承诺、另一个不认。
- **取字节只有一条路：`GET /api/v1/media/{id}`**。`id` 先按 store 键（md5 hex / uuid，索引登记过的本地缓存路径）解析，未命中再按导出文件名在导出根下扫四个类型目录；同名多命中时内容一致才服务（不一致 404）。

### 增量同步

- 账号绑定、索引安装、同步句柄与 watcher 登记、ready 提交和注销共用生命周期锁；慢速扫描留在锁外。锁序为生命周期锁 → 注册表、账号或 Store → 事件历史，不反向获取。
- watcher 绑定具体句柄，拒绝时显式取消；旧任务仅能回滚自身。回归：`stale_cleanup_and_watcher_cannot_remove_new_registration`。
- 注销必须等最终提交结束，新注册必须等注销清理结束；消息发布与 Store 更新同锁，避免旧事件落在 reset 之后。回归：`lifecycle_commit_excludes_deregister_through_watcher_and_ready`。
- 排队中的旧初始化即使在新账号就绪后才执行，也不能替换索引、事件基线或 ready 状态。回归：`queued_initialization_cannot_replace_a_new_account_index`。

- **水位是 SQLite 行号**（weflow 是三元组）：`generation` 变化后的补拉语义因此是「整会话重排空」，而不是按行续传。事件类型的唯一出处是 `sync/events.rs`，SSE 载荷与总线共用。
- **`sync/watch.rs` 只决定何时再轮询**，不解析内容；真正的增量来自水位线。改 watch 的判据时不要顺手把它变成内容感知——那是另一条路径的职责。

### 导出与媒体

- 定向导出以会话、消息 ID、发送者及时间唯一定位，耗时工作在阻塞池执行，在不对外服务的临时目录写完后，持生命周期锁复查注册身份，再发布文件。实际字节摘要形成可取句柄，非唯一平台文件名不能成为全局地址。
- 发布前还要复查**数据源身份**：请求开始时采一次指纹（库路径 + 创建时间 + 偏移 1024 起的 salt），发布前再采一次。
  - 不一致返回 409 加 `reason: "cursor_reset_required"`。只比世代计数不够：它只在同步路径采样时推进，导出的阻塞任务可能持锁让轮询停摆。
  - 起始采样失败时退回世代比较（尽力检测）；起始采到、发布前采不到，按「无法证明」处理，不发布。回归：`targeted_export_cancels_when_the_source_identity_changes_midway`。
- 这条复查是**尽力检测**，盲区必须说清：同一库文件的备份还原可能连指纹都不变（salt 与创建时间都可保持不变）。要闭合它得把文件身份做到卷级，本仓不做。

### 拉取面的进度游标

游标是**不透明的身份绑定串**：客户端原样回传，失效时响亮失败，绝不静默退回时间模式。

- 载荷是本进程 nonce、账号摘要、注册代次、数据源世代、会话摘要与最后交付的 rowid。
  - 会话摘要绑**解析后**的 `(chat_type, talker)` 而不是裸 id：`find_conversation` 的类型回退让两个裸 id 落到同一个会话，它们必须编出同一条线。回归 `chatlab_pull_cursor_v2_reset_semantics`。
- 失效分两档：语法坏或版本未知是 400；v1 旧格式、跨进程、跨账号、跨会话、注册代次或数据源世代不符是 409 加 `reason: "cursor_reset_required"`。
  - 判据只看身份不看内容：QQ 的追加写入与 WAL checkpoint 都不改变身份线索，放进判据等于让游标每次新消息都失效。回归 `every_identity_mismatch_is_a_reset_not_a_silent_restart`。
- 数据源世代由同步侧每轮采样推进（`db/source_identity.rs`），基线在建索引完成后立一次——没有基线时首个新消息触发的采样会把「建索引时的文件」当首次 sighting，换库窗口被吞掉。回归 `replacing_the_database_changes_the_fingerprint`。
- 账号切换时清理本次临时文件，不发布旧任务结果。已有可取副本优先复用；新产物按实际字节摘要命名。
- 定向导出发布成功时把 digest 句柄登记进 store 的媒体登记表（进程内，不落盘）。
  - 源缓存被 QQ 清掉后 digest 名无法从元数据重新推导（它是对字节的摘要），没有登记就找不到已发布的副本。
  - 登记在源不可解析时按文件存在性复用；进程重启后丢失属既定边界。回归：`targeted_export_reuses_its_own_digest_copy_after_source_removal`。
- SDK 在取字节 404 后只触发一次定向导出，并使用实际返回句柄；CLI 对缺失句柄逐消息恢复，以消息 ID、发送者和时间关联回填。回归：`media_bytes_exports_then_retries_once_after_404`、`targeted_media_restores_each_missing_message_once`。

- **子集导出的文件名是按本轮输入算的，撞名要按归属拒绝**：非续跑轮的去重集合从空开始，本轮某个会话可以算出与一个未在本轮的会话已交付产物同名的文件名，`.part` 收尾 rename 直接把它盖掉。
- 而新清单没有那个条目：交付物被换掉、下一轮也无从自愈。判据必须是归属（既有产物登记在别的会话名下，或没被任何清单认领），不是「本轮有没有写过这个路径」——后者会把同会话有意重导一并拒掉。
- 处置是会话级：记入 `skipped`（起手前就拒，不发请求、不留 `.part`），`skipped` 非空则 CLI 以 1 退出。
- 拒绝轮必须把属主那一行带进本轮新清单，否则属主下次重导时归属变成「无主」，被同一个拒绝逻辑挡住。刻意不把上一轮清单的名字播种进 `taken`：那会让撞名会话改拿 `-2`，留下两份内容不同的同名产物，比一次响亮拒绝更糟。
- 这套防护是降低覆盖事故的概率，不是消除：续跑轮把上一轮的名字先播种进 `taken`，别的会话的产物名压根算不出来（只会拿到 `-2` 后缀），那条路径本就不需要拒绝。清单丢失／被截断时「本轮无完成记录 ⇒ 重写」是既有且被需要的自愈语义。
- 回归位置：`refusing_a_collision_keeps_the_owner_exportable`、`subset_export_refuses_to_clobber_another_sessions_artifact`、`intentional_rerun_of_same_session_overwrites_itself`。
- 另两条：`unowned_leftover_file_is_not_clobbered`、`resume_with_intact_index_avoids_the_collision_entirely`。
- **媒体句柄只对「内容唯一」的名字下发**：只有 store 键或内容摘要派生的名字才被当作句柄——按名取字节要扫导出目录，同名即同内容时结果才确定。服务端只在下发句柄的那几处 stat 自己的会话目录，扫描只发生在取字节时。

### 分页与免鉴权面

- **没有 `page` 块的响应会被读成「完整一页」**：会话列表默认只给 100 条，而 ChatLab 的约定是「没有 `page` 块就表示这就是全部」。两者相乘的结果是「第 101 个会话凭空消失」，不报错也无提示。所以 ChatLab 形状必须带 `page.hasMore` 与 `page.nextCursor`，加新端点同理。
- **`/health` 免鉴权，信息量被设计约束过**：未鉴权方可以访问它，因此账号状态只有「未注册 / 建索引中 / 就绪 / 出错」四档。否则未鉴权方就能数出本机配置了几个账号。加状态时请保持这个约束。
- **ChatLab 推送帧一律不带 `platformMessageId`**：本仓取不到该值 ⇒ 整键省略，与 weflow 的字段语义是有意不同，不是遗漏。
