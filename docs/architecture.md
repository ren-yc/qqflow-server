# 架构

本文件是 qqflow-server 的**架构事实源**：模块职责、数据流与跨模块陷阱。
接口的逐字段说明在 [`qqflow-server-api.md`](qqflow-server-api.md)。两者分工是
**「接口文档说有哪些字段，本文件说为什么是这样」**。

## 目录

- [定位与不变量](#定位与不变量)
- [总览与数据流](#总览与数据流)
- [核心模块](#核心模块)
- [库面与 feature](#库面与-feature)
- [服务层](#服务层)
- [工程与工具链](#工程与工具链)
- [测试与夹具](#测试与夹具)
- [设计要点与陷阱](#设计要点与陷阱)

> 完整骨架共九节，现为**七节**：「数据源与解析」「同步机制」「配置」三节随后续
> 改动增量补（回写规则见 `AGENTS.md` 的「文档同步义务」）。

## 定位与不变量

QQ NT 消息库的**只读** HTTP / SSE 服务。下面五条是**不变量**——破坏其中任何一条
都是设计层面的变更，不是实现细节：

1. **只读**：从不写用户的数据库。连接以只读方式打开并带 `query_only`，写入会在 SQLite
   层被拒绝，而不是「我们小心不去写」。
2. **内存索引**：会话、联系人、群名片等全部解析进内存，**不落任何中间库**。
   代价是重启要重建索引；收益是没有第二份数据副本、也不必处理索引与源库的不一致。
3. **单账号**：一个进程绑定一个账号，注册之前不提供查询——密钥与账号身份共同决定
   能打开哪些库。
4. **密钥不落盘**：数据库密钥由外部提供，只存在于进程内存；本服务**不做密钥提取**。
5. **无中间件**：没有数据库、没有消息队列，全部状态都在进程内。

## 总览与数据流

```text
QQ 数据目录（nt_db 等）
  ├─ *.db        会话 / 消息 / 联系人
  └─ 库文件带 QQ 自有的明文头
        │
        │  ① 自定义 VFS：把前 1024 字节的明文头「虚拟地」跳过（db/vfs.rs）
        ▼
   只读连接（db/live.rs）——对上层而言就是一个普通 SQLite 库
        │
        │  ② 值驱动探测 + 密钥校验（db/scan.rs / db/decrypt.rs）
        ▼
   解析（parser/mod.rs，拆为 proto.rs 的容器格式与 types.rs 的类型空间）
        │
        │  ③ 建索引（store/index.rs）
        ▼
   内存索引 Store（store/mod.rs）
        │
        ├─ ④ 查询：handlers/* 读 Store → 信封 → JSON
        │
        └─ ⑤ 增量：sync/mod.rs 轮询 + sync/watch.rs 指纹
                │  新消息 / 撤回 → sync/events.rs 的事件类型
                ▼
           SSE 总线（handlers/push_events.rs）→ 订阅者
```

两条**容易看错**的路径：

- **`db/vfs.rs` 不是「解密」**：它只负责把明文头偏移「藏起来」，让上层把它当普通库；
  真正的密钥校验在 `db/decrypt.rs`。
- **`sync/events.rs` 是事件类型的唯一出处**：SSE 载荷与总线都用它，改形状要同时看两端。

## 核心模块

按**目录 + 顶层文件**列出（单文件细节见各文件头部的 `//!` 注释）。

| 模块 | 职责 | 对外接口 | 依赖方向 |
|---|---|---|---|
| `main.rs` | 进程入口：解析配置、装日志、起服务 | — | → `server` / `config` |
| `lib.rs` | 库入口：承诺面 `api` ＋ 实现面（默认 `pub(crate)`，见「库面与 feature」） | `api` | — |
| `config.rs` | 命令行与数据目录解析（含平台差异）、token 凭据库读写 | `Config` | ← 无 |
| `logging.rs` | 日志初始化 | `init()` | ← 无 |
| `cli.rs` | **命令行子命令面**（`cli` feature）：子命令分流、退出码、HTTP 查询复用 SDK | `dispatch` / `Entry` | → `config` / SDK |
| `export.rs` | **批量导出**（`cli` feature）：ChatLab Format 落盘，**不碰网络**（取数按页回调） | `run` / `Options` | ← `pathsafe` / `server::chatlab` |
| `mcp.rs` | **MCP 工具面**（`mcp` feature）：stdio 上的只读工具、字段投影与 ≈32 KB 截断预算 | `run` / 工具集 | → SDK |
| `pathsafe.rs` | **纯守卫**：路径分量与导出根目录的边界检查 | `slugify` / 校验函数 | ← 无（被 `store`、`server` 调用） |
| `db/vfs.rs` | **自定义 VFS**：虚拟跳过明文头偏移 | 注册函数 | ← 无 |
| `db/decrypt.rs` | 打开解密库、校验密钥 | `open_live` / `open_decrypted` | → `vfs` |
| `db/live.rs` | 活库只读连接 | 连接池 | → `decrypt` |
| `db/scan.rs` | **值驱动探测**：库 → 表 → 列，含账号发现（三平台分支） | `scan_accounts` 等 | → `live` |
| `parser/mod.rs` | 记录解析入口，组装 `proto` 与 `types` | `parse_message` | ← 无 |
| `parser/proto.rs` | 平台容器的二进制/字段格式解析 | 内部 | ← 无 |
| `parser/types.rs` | **类型空间**：平台类型码 ↔ ChatLab 类型的映射与归一 | `chatlab_type` / `direction_to_is_send` | ← 无 |
| `store/mod.rs` | 内存索引的数据结构与共享状态 | `Store` / `AppState` | → 无外部 IO |
| `store/index.rs` | 从库里**建**索引；含群名片列读取 | `build_all_live` | → `db` / `parser` |
| `store/names.rs` | 显示名解析（备注 / 昵称 / 群名片优先级） | 内部 | ← 无 |
| `store/query.rs` | 索引上的查询原语（被 handler 调用，含**跨两层的调用点**） | `query_*` | → `store` |
| `store/media.rs` | 媒体路径登记（「这个 id 在本机可取字节吗」） | `contains_key` 等 | ← 无 |
| `store/media_export.rs` | 媒体导出与导出 URL 构造 | `export_*` | → `pathsafe` |
| `sync/mod.rs` | 增量轮询、水位线、事件构造 | `AccountSync` | → `store` |
| `sync/events.rs` | **事件类型的唯一出处**（SSE 载荷与总线共用） | `Event` | ← 无 |
| `sync/watch.rs` | 文件指纹轮询（**决定何时再轮询**，不解析内容） | `WatchConfig` | → 文件系统 |
| `server/mod.rs` | 路由装配、鉴权、SSE 总线 | `serve_with_shutdown` | → 全部 |
| `server/handlers/mod.rs` | **handler 之间的共享件**：参数解析、信封合并 | `merge_body` / `parse_limit` | — |
| `server/handlers/*` | 各端点的实现（account / session / message / contact / media / push） | — | → `store` / `sync` |

## 库面与 feature

这个 crate **既是服务、也是库**，两条路径共用同一份实现。

### 两条使用路径

| 用途 | 怎么用 |
|---|---|
| 起服务 | `cargo run`（或 `cargo install <包名>`）—— 默认 feature 就是它 |
| 当库用 | `default-features = false`，再按需开 feature。**必须显式关掉默认 feature**，否则会连带拉进 axum、tokio、clap、SDK、rmcp 与 schemars |

嵌入者从 **`api`** 入手，它始终可用（不随任何 feature 开关）—— 「读自己的聊天记录」是最小可用面。
`examples/embed.rs` 是它的活文档：**不起 HTTP**，直接把一个账号读出来。

### 承诺面只有一处：`api`

其余模块在默认构建下是 **`pub(crate)`** —— 外部不可达，**边界由编译器强制**。这样做的理由：
`store::Store` 的字段是 `pub`（内部模块要写它），直接放出去等于把**每一个字段**都变成对外契约，
而它们本来是内部布局。

| 面 | 内容 | 稳定性 |
|---|---|---|
| `api` | 只读索引、同步句柄、事件类型、密钥类型、数据本身 | 有 semver 承诺；`#![deny(missing_docs)]` 只作用在这里 |
| 其余模块 | 解析、存储、同步、服务层的实现 | 随时可变；仅 `--features testing` 下对集成测试可见 |

严格地说，**「只有 `api`」是空 feature 树下的口径**：打开 `cli`／`server` 之后，根上还会多出**入口函数**
（`run_cli`）—— 它们是**二进制的入口**，不是给嵌入者的
承诺面，签名里带的 `config::Config` 在默认构建下也不是公开可命名的类型。嵌入契约只约束**空 feature 树**，
所以这一点不影响它；写在这里是为了让读到这句话的人知道：公开面在开 feature 之后**确实会变大**，
而不是以为编译器只放出了 `api`。

`testing` 只改**可见性**，不改功能：开它则实现面转 `pub`（集成测试在独立 crate 里，只能看见
`pub`）。它同时是**给嵌入者的造库工具**面 —— 写自己的测试时同样要造库、造密钥。

### feature 矩阵

| feature | 内容 | 关掉的影响 |
|---|---|---|
| `server`（默认）| HTTP/SSE：axum ＋ tokio 运行时 ＋ OpenAPI 描述 | 没有服务，只剩库 |
| `cli`（默认）| 命令行子命令面：clap ＋ `qqflow-client` SDK（进而 reqwest／tokio）| 只剩「旗标」式老入口，没有子命令 |
| `mcp`（默认）| MCP 工具面：`rmcp`（server／macros／transport-io）＋ `schemars`，stdio 上暴露只读工具 | 没有 `mcp` 子命令 |
| `sync` | watcher 与水位线增量：tokio ＋ notify | 没有增量同步；`api::Sync` 随之消失 |
| `testing` | 把实现面转成 `pub`（见上） | 集成测试够不着实现面 |

**依赖面的实际约束**（可测，不是口号）：`--no-default-features` 的依赖树里**不含 axum、tokio、clap、SDK、rmcp 与 schemars**。
核心面（解析、存储）因此不得依赖可选面 —— 这条边界由 CI 上的一条检查守着。

### `clients/`：类型化 SDK（workspace 成员）

两个成员，与 src 的模块边界不同 —— 它们**消费** HTTP 面，不属于 crate 本体：

| 成员 | 内容 | 维护方式 |
|---|---|---|
| `clients/rust`（`qqflow-client`） | 类型与操作客户端 ＋ 手写行为层（就绪轮询 / 游标排空 / SSE 重连 / 媒体重试 / 检索） | `generated/` 只许生成器改（`clients/regen`，CI 断言重生成无 diff）；行为层手写并测 |
| `clients/regen`（`qqflow-regen`） | 生成工具：取 `server::openapi::document()`，做确定性规范化（3.1 → 3.0）后交给生成器；`--dump-spec` 同时供 Python 侧取规范化 spec | 改规范化规则 = 改语义，需评审 |
| `clients/python`（`qqflow-sdk`） | Python 版：模型由 openapi-generator 从同一份规范化 spec 生成（`scripts/regen.py`）；行为层 `httpx.AsyncClient` 异步实现，与 Rust 侧逐方法同构：`wait_ready`（wait-only 就绪轮询，不做任何注册动作）、`ensure_ready`（注册应答里 200 拒绝态 `account_conflict`/`invalid_key`/`invalid_db_path`/`unknown_qq` 映射为 `StatusError` 快速失败）、`watch`（单连接连续产出多帧；字节级 LF 分帧防 U+0085/U+2028/U+2029 拆断 JSON 正文；1 MiB 缓冲上限，同时约束单个完整帧与未成帧累计——超限帧不交付、结束本次流退避重连；单帧解码失败跳过；EOF 冲刷残行；退避仅在干净结束时复位）；`MessageEvent` 携带 `event` 字段（载荷缺省时回落帧头事件名），new 与 revoke 可区分 | 同上：生成物入库 + no-diff 门禁（`--check` 比对整棵生成树摘要）；行为层手写并测 |
| `clients/ts` | TypeScript 示例客户端（仅示例，不发布 npm）：`wait_ready`/`ensure_ready`/`watch` 的**演示级子集**（分帧与退避形状与 Rust/Python 对齐，不承诺逐语义一致、不承诺兼容性）+ 可执行 smoke（真断言：只接受业务拒绝或受理超时两种结局；配套入库的 `stub-server.mjs` 可无真实服务跑门禁） | 不进 CI 产物矩阵；`tsc --noEmit` + smoke 手动跑 |

分层的理由：描述文档只声明「形状」，不声明「翻页到什么时候停、断线后从哪续」——后者是行为，
生成不出来；而类型若靠手写，必然与描述静默分叉。所以形状交给生成器（入库 + no-diff 门禁），
行为交给手写层（对 mock 夹具测试）。与 weflow 侧的**语义差异**都在客户端自有类型里：老面 SSE 的
`message.new`/`message.revoke` 载荷不在描述 schema 里，由行为层的 `MessageEvent` 解码；水位是
SQLite 行号（weflow 是三元组），`generation` 变化后的补拉语义因此是「整会话重排空」。

## 服务层

### 响应形状只有一个事实源：`server/dto.rs`

每个端点的响应都由 `dto.rs` 里的 struct 定义，不再用 `json!` 字面量拼。三条纪律都写在
该模块的头部注释里，这里只说**为什么**值得多这一层：键名写错从「运行时才知道」变成
「编译不过」，而「哪些键在什么条件下出现」从「只存在于代码路径里」变成类型。

其中两条是踩过才知道的：

- **字段按字母序声明**。`json!` 走 `serde_json::Map`（默认 BTreeMap），所以历史响应的键
  **本来就是字母序**；而 struct 按**声明序**输出。不按字母序声明，「换 DTO」会顺带改动每个
  响应的键序 —— 语义上无害，但会让 review 淹没在无意义的 diff 里。
- **`null` 与「省略」是两件事**。要 `null` 就写 `Option<T>` 且**不加** `skip_serializing_if`；
  要省略才加。客户端常靠「键在不在」判断（媒体导出与否、有没有引用），顺手统一风格会让
  这个判据失效。

多形状端点**各建 struct**，不堆可选字段：`sessions` / `messages` / `accounts` 的响应形状由
参数或状态决定，硬塞进一个「所有字段都可选」的类型会让它**看起来**合法而实际没有任何取值
组合是对的。同名键类型不同时更是如此 —— `sessions` 的 `type` 在原生面是数字、在 ChatLab 面
是字符串。

### 响应防线有三层，各管一件事

| 层 | 管什么 | 在哪 |
|---|---|---|
| **golden 快照** | 「**你改了**」：整个响应（含**状态码**与**键序**）逐字节比对 | `tests/golden/*.json` ＋ `tests/api_smoke.rs` 的 `mod golden` |
| **schema 校验** | 「**改成什么是合法的**」：引用能否解析、operationId 唯一、多形状确实用 `oneOf` | `tests/openapi.rs` |
| **契约套件** | 「**两个仓库是否一致**」：同一份用例跑两边 | `tests/conformance_runner.rs`（`#[ignore]`，CI 独立步骤） |

契约套件在 CI 里是**独立一步**（不是靠 `cargo test` 顺带跑的）：克隆 `conformance.pin` 所指的
tag，再驱动上面的执行入口。另有一步只跑 `nails-*`（四条数据不变量）。**停用某一步时要在
注释里写清原因** —— 一个不会红的门禁等于没有门禁。


快照那份有两个设计点值得知道：**易变值掩码值而不是删键**（删键会把形状一起丢掉），以及
**时钟哨兵** —— 快照里出现接近「现在」的时间戳即失败。后者的理由是：否则快照天天漂移，
下一个人会习惯性地点「更新快照」，护栏名存实亡。哨兵**不假设时间单位**（秒与毫秒各比一次）
—— 第一版只比秒级，于是毫秒级的 `updatedAt` 从它底下漏了过去。

### `/openapi.json` 由类型生成，且**免鉴权**

描述由 `#[derive(ToSchema)]` 从 DTO 生成，因此改 DTO 就改了它，不需要手工同步两处；副作用
是 DTO 上的文档注释**直接成了接口描述**。

免鉴权是刻意的：它描述的是形状，不含账号、路径或密钥，而且正是给**尚未拿到 token 的接入方**
看的。

`paths` 部分是按 OpenAPI 规范形状拼 JSON 再反序列化的 —— 它是**文档数据**，不是契约响应；
用 builder 逐层构造只会让那段代码变成对其 builder API 的考古。拼错了在加载时就会失败，
而不是等到有人打开文档。

### SSE 总线

`GET /api/v1/push/messages` 是长连接：订阅 `AppState.bus`（`sync::history::EventBus`，把重放
历史与 broadcast 通道绑成一件），迟到者靠重放缓冲补齐。四点必须知道：

- **历史由生产者单点写入**：`AccountSync` 与服务层都经 `EventBus::publish` 先写重放历史、再广播
  带 id 的载荷，订阅端只读快照、不再各自 append。否则坏在两处：零订阅者时广播没人接、那段事件
  根本不进历史，之后带旧 `Last-Event-ID` 重连的客户端漏收却无从得知；而 N 个在线订阅者各写一份
  会让事件 id 随连接数跳号、1000 条窗口被重复条目稀释。回归：
  `publish_records_history_without_subscribers`。
- **载体是类型不是 `json!`**：`sync::events::Event` 是带 `skip_serializing_if` 的 struct，
  `PushMedia` 在**类型层面就没有** `aes_key` —— 密钥不会因为某次改动「忘了过滤」而泄露。
- **推送载荷没有快照护栏**（快照的模型是一次请求一次响应），所以它的键集由
  `sse_payload_keys_are_pinned` 单独钉住。
- 那些「时间」「路径」类的易变字段在快照里由掩码与哨兵处理，不在这里重复。

### 本仓库特有：两处「复用既有类型」与一处「跨仓库差异」

- **复用了核心层的类型**。`messages` 面的消息项直接用 `store::query::MessageOut`，`contacts`
  用 `handlers::contacts::ContactOut` —— 它们本来就是类型化定义，条件键也已用
  `skip_serializing_if` 表达。再写一份平行 struct 只会与它们漂移。代价是 `utoipa` 的 derive
  渗进了 `parser`/`store`：这是**有意的取舍**（替代方案是复制同样的结构体），库边界划定时
  应换成服务层的 newtype。
- **SSE 载荷本来就是类型**。`sync::events::Event` 从一开始就是带 `skip_serializing_if` 的
  struct，`PushMedia` 在**类型层面就没有** `aes_key` —— 密钥不会因为某次改动「忘了过滤」而
  泄露。它的键集由 `sse_shape.rs` 的三条断言钉住（SSE 是流式接口，快照覆盖不到）。

**跨仓库差异**（两个仓库的 DTO **不共用**，正是因为这些键级差异）：`group-members` 的
`messageCount` 在本仓库是**条件键**（不带 `includeMessageCounts=1` 时**整个键不出现**），
而 weflow 恒输出该键（值 0）；原生消息的媒体形状不同（`mediaId` ＋
`{fileName,height,localPath,md5,size,uuid,width}`，而消息行上的类型键两边都是 `type`）。
照抄另一个仓库的 DTO 会**改坏契约**。

**取字节只有一条路**：`GET /api/v1/media/{id}`。`id` 先按 store 键（md5 hex / uuid，索引登记过
的本地缓存路径）解析，未命中再按**导出文件名**在导出根下扫四个类型目录；同名多命中时**内容一致
才服务**（不一致 404），且只有「按内容唯一」的名字才被当作句柄下发（store 键 **或** 内容摘要派生；
见 §3.1）。服务端只在下发句柄的那几处 stat 自己的会话目录，扫描只发生在取字节时。

### ChatLab 面（四条路由）

规范把 `baseUrl` 定义为 `/chatlab`，那四条与 `/api/v1/*` **共用同一份实现与同一条事件总线**，
差别是**形状与参数**而不是数据：

| 路由 | 作用 | 与老面的差别 |
|---|---|---|
| `GET /chatlab/sessions` | 会话发现面 | 只输出 ChatLab 形状；认 `cursor`（老面只认 `offset`） |
| `GET /chatlab/messages` | 消息面（原「混合面」的新家） | `talker` 必填；信封**不带 `success`**、`count` 是本页条数、消息**升序**、翻页走 `page`；`media=1` 真正执行导出 |
| `GET /chatlab/sessions/{id}/messages` | 拉取面（Pull 协议） | 与 `/api/v1/sessions/{id}/messages` 同一 handler、同一形状 |
| `GET /chatlab/push/messages` | 通知面（SSE） | 只发元信息、不发正文；**本仓所有帧都不带 `platformMessageId`**（取不到 ⇒ 整键省略，与 weflow 有意不同） |

**读端点只有 GET**（老面的 `messages`/`sessions`/`contacts`/`group-members`/`media/{id}`/
`push/messages` 的 POST 都是 405）；`/api/v1/sync` 与 `/health`、`/api/v1/accounts` 仍接受两个
方法 —— 动作端点与读端点是两回事。

## 工程与工具链

### 一条命令：`scripts/build.ps1`（或 `.sh`）

构建、测试、clippy **都必须走包装脚本**。原因不是偏好：rusqlite 捆绑 SQLCipher + vendored
OpenSSL，后者的 perl `Configure` 会直接调 `cl.exe` / `link.exe`，**绕过 cc crate 的自动 MSVC 探测**
——没有 `vcvars64.bat` 注入的 `INCLUDE`/`LIB`/`PATH` 就编译不过。脚本同时把 **Strawberry Perl**
放到 `PATH` 前面：Git 自带的 MSYS perl 会把 Windows 路径写坏。工具链版本钉在 `rust-toolchain.toml`。

### 提交钩子

`scripts/install-hooks.*` 装两个钩子：`pre-commit`（隐私扫描 + 编号引用扫描，**累积退出码**，
任一项失败即拒绝提交）与 `commit-msg`（把提交信息交给同一个扫描器）。**bash 与 Python 是提交
路径的硬依赖**：缺失时钩子报错并阻止提交，而不是跳过——失败开放的检查等于没有检查。

### CI 的门

Linux 与 Windows 双平台跑 `clippy --all-targets -D warnings` ＋ 全量测试。除此之外还有几道
与「接口化」直接相关的门，它们各自防一种特定的漂移：

| 门 | 防的是什么 |
|---|---|
| 公共段哈希比对（比对**所 pin tag** 的内容，不是本仓副本） | 三个仓库的协作规则悄悄分叉 |
| 编号引用扫描（`--tree` 与 `--ref`） | 注释里出现仓库外材料的编号，读者无从还原上下文 |
| `--no-default-features` 编译 `examples/embed.rs` | 承诺面被实现面「借用」，或依赖树里混进 axum/tokio |
| 一致性套件（见下节） | 契约与实现分叉 |

Python 只用于钩子与套件执行器，**纯标准库**——CI 与开发机都不需要装第三方包。

### 已登记的两类构建告警（预期内，处置＝维持现状）

门禁是 `-D warnings`，所以这里登记的是**不会**让门禁变红、但直接 `cargo build` 时会出现在日志里的两类
噪声。两者都被诊断过，结论是**无功能影响**；一并写下「为什么维持现状」，避免下次有人把它当成缺陷重查一遍。

1. **默认 feature 组合下的 `dead_code`**：一批**内部**模块里的辅助函数（如 `db/wcdb.rs` 的 `decrypt_page`、
   `db/open.rs` 的 `open_snapshot`／`quick_check`）在默认构建里看似未用 —— 它们只被 `testing` 夹具或
   `#[cfg(test)]` 单测调用。CI 的 `clippy --all-targets` 由包装脚本补了 `--features testing`，所以看不到；
   直接用 `cargo build` 会打印。**判据是两条路径都能编译**（默认 feature 与 `--no-default-features`，后者由
   `examples/embed.rs` 那道门盯着），**不是**「日志里没有 warning」。
2. **链接期 `LNK4099`（找不到 `ossl_static.pdb`）**：vendored OpenSSL 的静态库引用了自己的调试信息，而
   发布物里没有那份 PDB。**只影响调试信息，不影响代码**。
   （**为什么不消掉**：给 Windows 链接器加 `/ignore:4099` 能一并静音，但也会**隐藏将来真实的 LNK4099**；
   让 vendored OpenSSL 不带调试信息实测不可靠。两害相权，选择保留噪声。）

## 测试与夹具

### 三类测试，三种诚实

- **单元测试**（`src/**` 内嵌）：纯函数与解析逻辑，不碰库文件；
- **集成测试**（`tests/`）：用 tower 的 `oneshot` 直接打 router，**不起网络**；夹具造库；
- **真库探针**（`real_db_*` / `probe_*`）：`#[ignore]` ＋ 环境变量门控，**CI 不跑**，只打印统计、
  不打印任何真实标识或正文。上游文档与本地库形态不一致时，只有它能给出正确答案
  （群主列的判据就是这么定下来的）。

### 夹具只能**造库**，不能造索引

夹具造库器住在 `src/testing/mod.rs`（随 `testing` feature 编译，默认构建里不存在），
`tests/common/mod.rs` 只是它的转发层 ＋ 仅测试用的 axum `oneshot` 助手（后者要 `axum` 与
`tower`，留在测试侧才不撑大库的依赖树）。**为什么造库部分在库里**：需要它的有两个调用方，
而它们互相看不见对方的代码——集成测试是独立 crate（链库），**根包二进制同样是独立 crate**，
批量导出的夹具生成入口（CLI 的 `--rows`）够不着只在 `tests/` 下存在的模块。它负责建一个假的
QQ 数据目录（含 `nt_msg.db`、同族库 `group_info.db`、
`profile_info.db`，以及带 1024 字节 QQ 头的只读形态）。纪律是硬的：**禁止手工注入 store 字段**
——那会让断言在一个真实运行时不存在的状态上通过。同族库还有个坑：**写入必须发生在注册之前**，
否则索引已经建完，名册与群主在整轮测试里都是空的。

### 快照与键序

`tests/golden/*.json` 钉住**状态码 ＋ 键序 ＋ 掩码后的内容**。键序单列是因为 `serde_json` 的
`Value` 往返会按 BTreeMap 排序，只存 body 会把原始顺序抹掉，「逐字节」就名不副实。
**快照文件缺失即失败**（缺了不再静默重建）：缺失与漂移是两种失败，但都必须看得见。

### 路由与接口描述的对等

路由的唯一事实源是 `server::routes::ROUTES`（`build_router` 由它构建，因此不存在「没进表的
真实路由」）；`/openapi.json` 的端点表与它的对等由 `documented_routes_match_the_openapi_table`
强制：集合必须等于「路由 − 豁免」（本仓豁免为空），且**未声明的方法必须 405**（405 同时证明
这条路径确实注册了）。这一条是补出来的：此前两份清单各自手写、无人比对，「收口」之后仍漏了
两条真实操作。

### 一致性套件

`tests/conformance_runner.rs` 起真服务、造夹具，跑契约仓库（版本记在 `conformance.pin`）里的
34 条用例。两条硬规矩：**带 `--fail-on-skip`**（有用例被跳过即失败，避免「夹具少声明一个端点」
让用例静默变成不跑），**缺 `FLOW_CONTRACT_DIR` 即失败**（不是跳过）。夹具里的
`contractVersion` 与 pin 由 `pinned_contract_version_matches_the_fixture` 钉在一起。

### 文档锚点

`tests/docs_anchors.rs` 把「文档同步义务」变成可执行：本文件里的每个同文档锚点链接都必须
命中真实标题，目录必须覆盖全部顶层章节。删掉目录项、改标题忘了改链接，都会红。

## 设计要点与陷阱

每一条都是**不知道就会踩**的那类。写新代码前扫一眼这里。

**对外承诺的字段形状另有落点**：三种「没有值」的表示（省略键 / `null` / 空串）、规范与安装版的
已知偏离、以及本仓与 weflow 的允许差异，都登记在 `docs/qqflow-server-api.md` 的
「与标准 / 安装版的已知差异」与「字段无值时怎么表示」两节。改 DTO 之前先读那两节 ——
那里的每个键都被 golden 快照或 `tests/sse_shape.rs` 的键集断言钉住。

### 明文头偏移：`cipher_plaintext_header_size` 做不到这件事

库文件前 1024 字节是明文头，常规做法是用 SQLCipher 的 `cipher_plaintext_header_size`
参数——但它要求密钥派生的基准页也一起变，与这里的实际布局不匹配。因此改用**自定义 VFS**
在文件层虚拟跳过这段偏移。改这一段前先读 `db/vfs.rs` 的头部注释。

### `isSend` 的归一**不是**简单的「1/2 → 1」

平台的原始 direction 列有四种取值；**列缺失或取值未知时落 `0`（即「别人发的」）**。
把「不是自己发的」等同于「别人发的」，在列缺失的老版本库上会把自发消息判成对方消息。
映射函数与它的边界用例在 `parser/types.rs`。

### 子集导出的文件名是按本轮输入算的：撞名要按**归属**拒绝，而不是按「本轮写过没有」

`export --session` 非续跑轮的文件名去重集合从空开始，编号按本轮输入顺序重算。于是本轮某个会话可以算出
与一个**未在本轮**的会话已交付产物同名的文件名，`.part` 收尾 rename 直接把它盖掉；而新一轮 `index.json`
没有那个会话的条目 ⇒ 交付物被换掉、清单不再提它、下一轮也无从自愈。

拒绝的判据必须是**归属**（既有产物登记在别的会话名下，或压根没被任何一轮清单认领），不是「本轮有没有写过
这个路径」：后者会把**同会话有意重导**一并拒掉——那覆盖的是自己的旧产物，语义正确，拒了就等于「产物一旦
生成再也改不了，除非手工删文件」。刻意也**不**把上一轮清单的名字播种进 `taken`：那会让撞名会话改拿 `-2`，
留下两份内容不同的同名产物，对使用者比一次响亮拒绝更糟。

处置是会话级：记入 `skipped`（起手前就拒，不发请求、不留 `.part`），`skipped` 非空 ⇒ CLI 以 1 退出
（沿用既有退出码口径）。

**拒绝轮必须把属主那一行带进本轮新清单**，否则这套机制自己会造出死锁：本轮收尾的 `write_index` 只写
本轮交付的条目 ⇒ 属主的产物从清单上消失 ⇒ 属主下次重导时，同名文件算出的归属变成「无主」，被同一个
拒绝逻辑挡住，唯一出路只剩手工删文件。回归位置：`refusing_a_collision_keeps_the_owner_exportable`
（撤掉这一步实测变红）。其余回归：`subset_export_refuses_to_clobber_another_sessions_artifact`、
`intentional_rerun_of_same_session_overwrites_itself`（钉住那半边豁免）、`unowned_leftover_file_is_not_clobbered`。

**它的成立条件是「清单可读」，不是「任何时刻都能认出归属」**：非续跑轮不去重播种，所以撞名算得出、也拒
得了；续跑轮把上一轮的名字先播种进 `taken`，别的会话的产物名**压根算不出来**（只会拿到 `-2` 后缀），
于是那条路径本就不需要拒绝（实测钉在 `resume_with_intact_index_avoids_the_collision_entirely`）。清单
丢失／被截断时，「本轮无完成记录 ⇒ 重写」是既有且**被需要**的自愈语义，此时同名文件的「自己的／别人的」
无法区分（要区分得往产物里加归属头，那方案早被否决）。结论：这套防护是**降低**覆盖事故的概率，不是
消除；把它当万能保险会误判。

### 群名片按会话隔离：同一个人在不同群里是不同的名片

群名片存在消息表的专用列里，**按会话读取**，不跨群共享。把它当「联系人的一个属性」缓存，
会在多群场景下显示错名字。

### `mediaId` 只在**本机确有可取路径**时才下发

「字段出现即可取字节」是承诺，不是尽力而为：登记表里没有该 id 时，字段被**置空**而不是
给一个取不到的 id。这条由**所有**产出消息的路径统一应用，避免一个端点给了承诺、另一个不认。

### `AppState` 住在 `store` 层，但它持有服务侧的东西

共享状态目前放在 `store/mod.rs`，却持有事件总线的发送端与服务侧的类型。
新增字段时注意**别让它继续长**——这是已知的层次问题，不是范式。

### 三个平台分支：只有 Windows 分支有独立实现

账号发现按平台分三支，其中 Windows 分支有独立的扫描实现，其余走通配路径。
改动发现逻辑时，**三个分支都要看**——只测当前平台会漏掉另外两支。
### 没有 `page` 块的响应会被读成「完整一页」

会话列表默认只给 100 条，而 ChatLab 的约定是：**响应里没有 `page` 块，就表示「这就是全部」**。
两者相乘的结果是「第 101 个会话凭空消失」——不报错、也没有任何提示。

所以 ChatLab 形状**必须**带 `page.hasMore` 与 `page.nextCursor`。加新端点时同理：
**分页信息不是可选项**，缺了它，截断就从「有损」变成了「静默丢数据」。
### `40850` 指向的 `40003` **在一个会话内也不唯一**

回复关系存在表列 `40850`（被回复消息的**会话内序号**）：要在同一会话里找 `40003` 等于它的那一行，
输出**那一行的 `40001`**（即 `platformMessageId`）。

但 `(会话, 40003)` **不是唯一键**——实测某真实库 31,820 行群消息里重复了 **1371 组**。
所以**不能查到就填**：命中多行时客户端会把引用挂到**错误的消息**上，而它**分辨不出来**。
实现只在恰好一个候选（同一 `40003` 且不晚于本条的时间）时输出，否则**省略该键**。

顺带一条方法论：上游字段文档说 `40003` 是「群内消息序号…可跨群复用」，**漏了「群内也复用」**。
**本地库形态优先于文档**——判据要按实测写，并在注释里留下实测数字。
