# 更新日志

本文件记录 qqflow-server 的版本变更，自 v0.5.0 起维护。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。


## [未发布]

### 变更

- **`clients/rust`（`qqflow-client`）与 `clients/python`（`qqflow-sdk`）的 `media_bytes` 增加 `talker` 参数（破坏性）**：
  404 重试用的导出门需要**会话 id**，而此前用的是 `message.account_name`——那是发信人显示名，
  私聊里它是对面昵称、群聊里是发信人昵称，与会话 id 只在「显示名恰好没被改过」时相同，真实数据几乎必然对不上。**迁移方式**：
  `media_bytes(&message)` 改为 `media_bytes(&message, talker)`（Rust）/ `media_bytes(message, talker)`（Python），
  `talker` 用发起导出时传给 ChatLab 面的同一个会话 id。`media_bytes_by_id(id)` 不受影响。
  空 `talker` 现在两语言都**本地拒绝**（Rust 报 `UnexpectedBody`，Python 报 `ShapeError`）——
  此前 Rust 会把空串发给导出端点，把「参数无效」伪装成「句柄不可导出」。回归位置：
  `tests/behavior.rs::media_bytes_exports_then_retries_once_after_404`（夹具昵称 ≠ 会话 id，
  mock 拒绝错误 talker）与 `tests/cli_e2e.rs::with_media_skips_an_unfetchable_handle_without_failing`。
  消息无媒体句柄时的错误也从「合成 404 + 句子塞 url 字段」改为 `UnexpectedBody`/`ShapeError`——
  依赖旧错误形态分类的调用方需要调整。

- **MCP `get_messages` 响应新增 `sinceResolved`**：`since` 接受相对串（`7d`/`24h`），续拉发生在
  下一轮对话——重发相对串会把窗口悄悄前移。响应回给本轮解析出的**排他**绝对下界，续拉传
  `nextOffset` + `sinceResolved`；`since` 未提供时为 `null`。
- **CLI `contacts` 子命令分页化**：带 `--limit`/`--offset`，输出从裸数组改为
  `{total, hasMore, contacts}`（此前固定取第一页且不报总数）。
- **CLI 用法错误口径统一**：显式空 `--talker`、带空白的 `--since`、嵌入分支的同类输入，
  一律以用法错误退出（此前部分形态静默返回空结果退出 0）；`--since` 解析返回 trim 后的值。
- **TS 客户端 `watch` 的干净 EOF 改为重连**：服务端优雅关停/空闲重启不再静默终止跟随
  （与 weflow 同构）。
- **发布流水线新增质量门**：release 前在同 tag 重跑 clippy、全量测试与契约 nails，
  构建依赖该门——测试红着打 tag 会被拒绝。

### 新增

- **两个 SDK 各补五项公共面（Rust 与 Python 同名同义）**：`sync_now()`（手动触发一次增量同步）、`pull_page(talker, since, offset, limit)`（**单页** Pull 入口，`drain_session` 改为复用它 ⇒ 游标装配从两处回到一处）、`chatlab_messages(...)`（ChatLab 形状的消息面，此前该面只被内部当触发导出用、没有公共入口）、`group_members(chatroom_id, include_message_counts)`、`list_all_sessions` 的关键词与页大小。
  回归位置：`sync_now_posts_with_the_bearer_token_and_decodes_counters`、`pull_page_decodes_the_sync_block_and_sends_the_cursors`、`pull_page_omits_defaulted_cursors_instead_of_sending_zero`、`chatlab_messages_decodes_the_chatlab_envelope_and_paging`、`group_members_decodes_roster_page_and_sends_chatroom_param`、`list_all_sessions_pages_and_collapses_cross_page_duplicates`。
  行为变化：`drain_session` 的请求序列不变；Python 侧原先从第二页起发 `offset=0`，现与 Rust 一致地省略（服务端默认值即 0，取到的页相同）。
  **Rust 侧的 `list_all_sessions` 此前只有页大小、没有关键词**——调用方只能取回全量再本地过滤，而该面**以空页为终止条件**，过滤会缩短某一页、排在后面的命中项永远读不到。现已修平。
  **本仓的一处有意差异**：`group_members` 的 `messageCount` 是**条件键**（不带 counts 时整个键不出现，而不是 0），与 weflow 的「恒出现、值为 0」不同，由 `golden/group-members.json` 与 `golden/group-members-no-counts.json` 两形态钉住。

- **CLI 子命令面（`cli` feature，已进 `default`）**：`serve`／`token`／`sessions`／`messages`／`search`／`contacts`／`sync`／`export`。**裸跑仍等于 `serve`**；退出码 `0/1/2` 的用法错误口径由 `tests/cli.rs` 逐例钉住。两处与 weflow 的能力差异：`contacts` 只走 HTTP（本仓嵌入面没有联系人读面）；嵌入形态的关键词只对解析后正文判命中（本仓不保存原始 XML）。
  **迁移方式**：`run_cli()` 由 `async fn` 改为同步分流入口（`main.rs` 随之不再自建运行时）。
- **`export` 子命令**：ChatLab Format 批量落盘，带 `--format json|jsonl`、`--session` 多选、`--resume` 续跑、`--limit`／`--since`／`--end`。`--with-media` 先经消息面触发服务端导出、再取字节落盘到 `<out>/media/`，**导出物里的 `media.fileName` 只保留确实落盘的句柄**。回归位置：`with_media_keeps_only_handles_whose_bytes_are_on_disk`、`message_line_omits_media_without_a_handle`、`export_with_media_lands_bytes_and_keeps_the_handle`。导出物不写任何 URL；每行写盘前做令牌子串检查，命中即整轮中止并删除半成品。**会话类型无法从 talker 反推**（QQ 的群号与 uid 都是数字串），故 `SessionTarget` 带 `chat_type`，meta 的 `type` 只来自发现面的类型码。
- **MCP 工具面（`mcp` feature，已进 `default`）**：`mcp` 子命令在 stdio 上暴露 8 个**只读**查询工具（`list_sessions`／`get_messages`／`get_messages_raw`／`search_messages`／`get_contacts`／`get_media`／`group_members`／`sync_now`）。单次输出约 32 KB 字符预算，超预算少给条数并置 `truncated`；「数据离机」提示写进 instructions、每个工具的 description、README 顶部与新增的 `docs/mcp.md`。依赖 `rmcp`／`schemars` 均为可选 ⇒ `--no-default-features` 的零 tokio 嵌入契约不受影响。
- **造库器移入库内（`testing` feature）**：批量导出的夹具要能真实生成会话库供 `tests/` 复用。
- **`Config` 支持显式传入 API token**（默认 `None` ⇒ 生产行为不变；给 `Some` 时服务端直接用该值、不碰系统凭据库）。动机是 CI 的 Linux runner 上系统凭据库缺失/未就绪时，服务端按设计降级成会话级 token 继续起服务，而测试的就绪探针在同样环境里持续读到「无 token」并死等——**红在与凭据无关的关停断言上**。现在关停测试自铸随机 token 经 `Config` 注入，就绪探针用同一 token 鉴权。

### 修复

- **MCP：预算截断时 `hasMore` 必须为真**。`get_messages`／`search_messages`／`get_contacts` 此前透出**页面自身的** `hasMore`，于是 `truncated: true` 与 `hasMore: false` 会同时出现——按 `hasMore` 判停的调用方会**静默停在不完整结果上**。现改为 `has_more || truncated`。回归位置：`mcp_truncation_reports_has_more_so_the_caller_does_not_stop`。
- **MCP：`search_messages` 的续拉游标此前无处回传**。响应给出 ChatLab 的 `nextCursor`，但该工具的参数里既无 cursor 也无 offset ⇒ **第 2 页永远取不到**。现改为 `offset` 入参 ＋ 响应给 `nextOffset`，并**移除**那个回传不了的 `nextCursor`；`get_contacts`／`get_messages` 在预算截断时同步补 `nextOffset`。回归位置：`mcp_search_pagination_actually_advances`。
- **CLI：非法 `--since` 现在以用法错误退 2**。此前 clap 放行、手工解析再 anyhow 上抛退 1，而 `--limit abc` 走 value_parser 退 2——同一种「用法写错」两种退出码。回归位置：`invalid_since_is_a_usage_error_exit_2`。
- **CLI：取媒体字节只有 404 才算「句柄不可取」**。此前任何错误都被当成不可取而跳过，瞬时 5xx／网络错会**静默少下载媒体而整体仍退 0**。现只有 `Status { status: 404, .. }` 跳过、其余上抛。回归位置：`with_media_fails_loudly_when_bytes_fetch_errors`。
- **`export --resume` 要真是续跑**：① `begin` 建了文件后首行检查失败不回收 ⇒ 留下的空文件让 `--resume` 永久跳过该会话；② `--resume` 只看 `exists()` ⇒ 截断／空文件被当成已完成（现加 `file_is_complete`）；③ 续跑沿用上一轮的文件名（编号按输入列表顺序算，列表一变就漂移出第二份产物）；④ 被跳过的会话不再进新清单（否则 `index.json` 会把上一轮条目整个抹掉）；⑤ jsonl 收尾的 `flush().ok()` 吞错 ⇒ 磁盘满时留下截断产物却以成功收场。
- **秘密检查覆盖转义与编码形态**：此前只比原文，秘密以 JSON 转义形或百分号编码形落盘时会逃逸。现同时比原文、转义形、编码形（`secret_check_covers_escaped_and_percent_encoded_forms`）。
- **pathsafe：挡掉 Win32 保留设备名**。`safe_segment` 与 `slugify` 都不挡 `CON`／`NUL`／`COM1`…（大小写不敏感、带扩展名也算）。作末分量时 Win32 在触碰文件系统之前就把名字解析成设备：写 `NUL` **静默丢弃字节**、开 `COM1` 可能阻塞。现 `safe_segment` 拒绝、`slugify` 加前导下划线。`slugify` 是本仓随 `export` 一并移植的折叠函数（此前只有校验、没有折叠）。
- **两个 SDK 的错误族与参数校验归一**：① Python 侧 httpx 异常族此前不被包装 ⇒ `except ClientError` 漏掉网络故障，现新增 `TransportError(ClientError)`；② `_decode` 只把 `>= 400` 当错 ⇒ 3xx 变成 `ShapeError`，现按「非 2xx 即错」；③ 时间界校验 `str.isdigit()` 放行全角数字 ⇒ 漏到服务端吃 400，现改 ASCII-only；④ `group_members` 不校验空 `chatroomId` ⇒ 空名册被读成「这个群没有成员」，现 fail-fast；⑤ Rust 的 `MessageQuery::params()` 把错误 URL 写死为 `/api/v1/messages`，经 `chatlab_messages` 调用时报错指向另一个端点，现由调用方传端点；⑥ `pull_page` 与两处 media 路径直拼 id ⇒ 含 `#`／`?` 时打到别的路径，两侧都加路径段百分号编码。回归位置：`encode_path_segment_escapes_delimiters_but_keeps_real_id_shapes`、`empty_talker_error_names_the_endpoint_that_was_actually_called`、`group_members_rejects_an_empty_chatroom_without_a_request`。
- **SSE 重放历史改由生产者单点写入**：此前每个订阅端各自编号，会产生发布编号与投递倒序；并堵住订阅与基线发布留下的三个并发缺口（订阅/快照缝隙、发布编号与投递倒序、基线发布越过归零基线）。**索引安装的取消重验移进写锁临界区**，杜绝「已注销的构建复活」。

### 变更（对门禁与工具链，不对接口）

- **包装脚本不再把调用方首参注入第二遍**：`build.ps1` 此前在 `$args[0]` 恰为子命令时才补 `--features testing`，`build.ps1 --locked test` 这类写法会漏注入（报错是一堆「模块是私有的」），而 `--features=x`／`-F testing` 形式会被重复注入。
- **`graceful_shutdown` 测试的等待谓词升级为三段式**（端口可连 → token 可读 → 用该 token 打通一次鉴权），并把两种失败分开报错、各给 remedy。纯测试侧，不改产品行为。
- **验收测试补强区分力**：改查值而非查键名、`--with-media` 断言句柄集合与 `media/` 文件集合相等、404 与 5xx 互为对照；桩 pull 面的 `meta.type` 刻意写成与发现面矛盾的值 ⇒ 群类型断言从此只能靠**推导**通过。
- **`docs/architecture.md` 新增「已登记的两类构建告警（预期内，处置＝维持现状）」**：默认 feature 组合下的 `dead_code`（CI 门禁带 `--features testing` 所以看不到），以及链接期 `LNK4099`（vendored OpenSSL 缺 `ossl_static.pdb`，只影响调试信息）。
- **顶层 Python 包补 `py.typed`**：生成层内部有该标记、顶层手写包没有 ⇒ 消费方的类型注解全部静默失效。


## [0.8.0] - 2026-10-04

### 变更

- **`clients/rust`（`qqflow-client`）与 `clients/python`（`qqflow-sdk`）删除 `search`（破坏性）**：
  同一端点上它已被 `list_messages` 完全覆盖——后者的参数是前者的超集（多了
  `limit`/`offset`/`media`），且两仓此前都没有任何测试盯着 `search`。**迁移方式**：
  `search(talker, keyword, start, end)` 改写为
  `list_messages(talker, keyword=…, start=…, end=…)`，语义不变（`end` 覆盖整天）。
  回归位置：`list_messages_pages_by_offset_and_exposes_native_fields`。
- **`ensure_ready` 对 200 拒绝态立即失败（行为变化）**：注册端点用 HTTP 200 表达业务拒绝
  （本仓的 `account_conflict` / `invalid_key` / `invalid_db_path` / `unknown_qq`）。此前只有
  Python 侧分类，Rust 侧不看响应体，会把确定性的拒绝一路轮询到超时并报「not-registered」——
  把根因（绑定被占、密钥被拒、库路径无效）伪装成「还没就绪」。现在两仓同规：Rust 返回
  `ClientError::Refused { state, .. }`，Python 抛 `StatusError(200, …state=…)`。
  **迁移方式**：对错误做穷尽匹配的调用方补一条 `Refused` 分支并读 `state`；原有的
  `NotReady` 分支保留，它仍覆盖真正未就绪而超时的情形。回归位置：
  `ensure_ready_fails_fast_on_a_refusal_state`。
- **`ensure_ready` 改为 `register` + `wait_ready` 的组合**：同一端点只有一处实现，
  注册契约变更不会只落在半个 SDK 里。行为不变。
- **时间界校验放宽为「`YYYYMMDD` 或 unix 秒」**：服务端两种都收，此前客户端只放行
  8 位日期，把合法的 unix 秒上界挡在本地（`end` 为裸日期时覆盖整天，这条不变）。

### 新增

- **两个 SDK 的公共面扩展（七项，Rust 与 Python 同名同义）**：`health()`（`/health`，
  **免鉴权且不发送凭据**）、`accounts()`（账号明细——`error` 与 `messageCount` 只在这个面）、
  `register(body)`（**非阻塞**注册，原始 `state`/`status` 作为值返回的 `RegisterOutcome`）、
  `wait_ready()`（Rust 侧补齐，wait-only，与 Python 对称）、`list_messages(MessageQuery)`
  （原生消息面：`start`/`end`/`limit`/`offset`/`media`，带 `rawContent`/`isSend`/`localType`）、
  `contacts(ContactsQuery)`、`media_bytes_by_id(id)`（按单段句柄取字节，不触发导出）。
  这些正是「就绪门控 / 轮询 / 媒体」三类消费者此前只能自己拼 HTTP 的部分。
- **`clients/rust` 的 `list_all_sessions` 补测试**：它以「空页」为终止条件（该面没有
  `hasMore`），此前没有任何断言盯着——停止条件写错会静默截断会话列表。回归位置：
  `list_all_sessions_pages_and_collapses_cross_page_duplicates`。
- **`docs/qqflow-server-api.md` 的「类型化客户端（SDK）」一节改为公共面清单**：逐方法列出
  打哪个面与语义要点，并点出两处容易读错的地方——`list_all_sessions` 是取尽而
  `list_messages`/`contacts` 只取一页；时间界收 `YYYYMMDD` **或** unix 秒且 `end` 的裸日期
  覆盖整天。
- **`clients/python`（`qqflow-sdk`）`watch()` 的缓冲上限语义**：1 MiB 上限现在同时
  约束「单个完整帧」与「未成帧累计」——超限帧（无论格式是否合法）一律**不交付**，
  本次流结束并退避重连（原先超限的完整帧会先被交付）。这是行为变化；SDK 未发布、
  无迁移动作。回归位置：`clients/python/tests/test_behavior.py` 的
  `test_sdk_watch_rejects_oversized_single_frame_whole_and_split`（对照
  `test_sdk_watch_delivers_frame_just_under_the_cap`）。
- **`clients/python`（`qqflow-sdk`）`watch()` 的重连退避**：退避只在「干净结束」
  （EOF、无超限、无传输错误）时复位到 0.5s，否则按 0.5→1→2→4… 升级到 30s 上限。
  原先每建一次连接即复位：畸形但能建连的流永远 0.5s 一轮（实测 3 秒 6 连），退避
  形同虚设。回归位置：`test_sdk_watch_backoff_escalates_on_persistent_overflow`（对照
  `test_sdk_watch_backoff_returns_to_floor_after_clean_stream_end`）。
- **`clients/python`（`qqflow-sdk`）SSE 一帧多条 `data:` 行按规范以 LF 拼接**成单一
  载荷（原先逐行覆盖、只保留末行）。当前服务端每帧恰一行 `data:`，对现有形状逐字节
  中性；回归位置：`test_sdk_watch_joins_multiple_data_lines_per_frame`（三行边界对照
  `test_sdk_watch_joins_three_data_lines_boundary`）。
- **`clients/python`（`qqflow-sdk`）`watch` 撤销从未生效的 `poll_interval` 形参
  （破坏性）**：该形参在 0.7.0 引入后从未被函数体读取（Rust 侧亦无对应参数），
  现已从签名移除。**迁移方式**：调用方删掉该实参即可，行为不变。SDK 未发布，
  已核实在役调用点为零。
- **`clients/ts` 示例**：POST 显式携带 `Content-Type: application/json`（服务端 axum
  Json 提取器对非 JSON 内容类型回 415，示例此前必然踩中）；`smoke.ts` 第 3 步改为
  真断言——只接受「业务拒绝（StatusError 带 state）」或「受理后就绪等待超时
  （NotReadyError）」两种合法结局，其它结局（415/5xx/网络错误/未知异常）非零退出；
  新增可入库的 `stub-server.mjs`（无真实服务即可门禁：`STUB_STATUS=500` 必须让
  smoke 变红）；`watch` 补上 `Last-Event-ID` 传递与字节上限（`id:`/`event:`/`data:`
  切分与空行成帧已有），与 weflow 版同构（本仓版为回调、weflow 版为异步生成器），
  README 写明两版等价。
### 新增

- **`clients/python` ruff 门禁**：`clients/python/pyproject.toml` 落显式的
  `[tool.ruff]` 与 `[tool.ruff.lint]` 表（规则集写死在配置里、不依赖默认集；
  生成层的排除只写在配置文件中——CLI 传相对排除按 cwd 解析、传 `--config` 会改变
  配置内相对路径的基准，两种写法都会让排除静默失效并把生成层整套扫进去）；
  CI `check.yml` 增加对 `src` 与 `tests` 的 ruff 步骤，并附「摘掉排除后命中必须
  涨到千量级」的区分力自检（本地实测 2149 条）；`ruff` 以精确版本钉进 dev extras
  （默认规则集随版本漂移，钉死才可复现）。手写层首批告警清零（pyupgrade 系、
  import 排序与 `RUF022` 修复后逐处人工过 diff；`watch` EOF 支路的裸 `except
  Exception` 收窄为与主循环相同的 `(ShapeError, ValidationError, AttributeError)`）。
- **`clients/python/src/qqflow_sdk/LICENSE`**：仓库根许可证复制进包树，随构建产物
  分发（消费方打包时由各自的 package-data 声明决定是否入 wheel）。
- **`clients/python/scripts/regen.py --check` 改为对生成树做摘要比对**（此前只比
  `spec.json`）：重生成到临时目录、与已提交生成树逐文件比摘要，模型层或空白层面的
  漂移不再可能「spec 没变就算绿」。生成器版本钉在 npx 调用里（wrapper 2.41.0 →
  openapi-generator 7.25.0），不再依赖可漂移的 latest。


- **`clients/rust`（`qqflow-client`）**：类型化 Rust SDK（workspace 成员，随根包版本 0.7.0）。
  类型与操作客户端由 `/openapi.json` 描述生成（生成物入库，CI 断言「重生成无 diff」）；
  行为面手写六件套：`ensure_ready`（注册 + 就绪轮询）、`drain_session`（Pull 游标原样回传排空）、
  `list_all_sessions`、`watch`（SSE 重连带 `Last-Event-ID`、心跳过滤、`generation` 变化上报；
  老面 `message.new`/`message.revoke` 载荷由客户端自有类型解码）、`media_bytes`（404 后按
  「先 `media=1` 导出再取」重试一次）、`search`（`YYYYMMDD` 客户端校验）。本轮**不发布** crates.io。
- **`clients/regen`（`qqflow-regen`）**：生成工具（`cargo run -p qqflow-regen`，`--check` 供 CI 用）。
  规范化（3.1 → 3.0）与 weflow 侧同构：可空 `type` 数组转 `nullable`、`Option` 的 `oneOf` null 臂丢弃；
  `--dump-spec` 把规范化后的描述交给 Python 侧作生成输入（golden 快照的占位掩码会把易变值的
  真实类型烧成 string）。
- **`clients/python`（`qqflow-sdk`）**：类型化 Python SDK（workspace 外的独立包，随根包版本 0.7.0）。
  模型由 openapi-generator 从同一份规范化 spec 生成（生成物与 spec 一并入库，CI 断言「重生成无 diff」）；
  行为面 `httpx.AsyncClient` 异步手写，与 Rust 侧逐方法同构：`ensure_ready`（注册 + 就绪轮询；
  200 拒绝态 `account_conflict`/`invalid_key`/`invalid_db_path`/`unknown_qq` 现映射为 `StatusError` 快速失败）、
  `wait_ready`（wait-only 就绪轮询，不做任何注册动作）、`drain_session`（Pull 游标原样回传排空）、
  `list_all_sessions`、`watch`（SSE **单连接连续产出多帧**——原先每帧断开重连；字节级 LF 分帧，
  `aiter_lines` 的 splitlines 语义会把含 U+0085/U+2028/U+2029 的 JSON 正文拆断；1 MiB 未消费缓冲上限；
  单帧解码失败跳过不杀流；EOF 把未终结残行并入末帧冲刷；重连带 `Last-Event-ID`、`generation` 变化上报
  ——水位是 per-table 行号，跳变后的补拉是**整会话重排空**；`message.new`/`message.revoke` 解码为
  `MessageEvent`，该模型现携带 `event` 字段（载荷缺省时回落帧头事件名），new 与 revoke 不再不可区分；
  老面 `message.new`/`message.revoke` 由客户端自有 `MessageEvent` 模型解码）、`media_bytes`（404 后按
  「先 `media=1` 导出再取」重试一次）、`search`（`YYYYMMDD` 客户端校验）。本轮**不发布** PyPI。

### 修复

- **`/openapi.json` 为路径模板参数补 `parameters` 声明**：四条带占位符的操作此前没有参数声明，
  违反 OpenAPI 规范；golden 快照只记录输出、不校验合法性，因此一直无声。守卫断言见
  `tests/openapi.rs`。
## [0.7.0] - 2026-10-02

接口面的形状收敛：老面只做原生/富数据面，ChatLab 形状搬到 `/chatlab/*`；媒体只留一条按名取字节的
路由（`{id}` 增加导出根回落）；鉴权通道减到两条。**破坏性变更较多**，逐条迁移见文末「迁移」。

### 新增

- **`GET /chatlab/messages`** —— ChatLab 形状的消息面，也是原「混合面」
  （`/api/v1/messages?chatlab=1`）的新家：参数 `talker`（必填）/ `limit` / `offset` / `cursor` /
  `start` / `end` / `media` / `keyword`；信封
  `{talker,count,page,chatlab,meta,members,messages}`，**不带 `success`**，`count` 是**本页条数**，
  消息**升序**，`page{hasMore,nextCursor}` 报告截断。`media=1` **真正执行导出** —— 旧的混合面在
  收集导出任务**之前**就 return 了，所以 `media=1` 在 ChatLab 形状上从未导出过；「先触发导出、
  再取字节」这条两步走在那个面上并不成立。
- **消息的媒体元数据**：拉取面与消息面每条消息新增 `media{type,fileName,md5}`（无媒体省略整键；
  `md5` 取不到时省略该键）。它是**元数据**：只有 `media=1` 且该条**确实写出了本地文件**、
  且文件名由内容键派生时，`fileName` 才是可取句柄。
- **`GET /api/v1/media/{id}` 的导出根回落**：`id` 先按 store 键（md5 hex / uuid 的本地缓存路径）
  解析，未命中再按**导出文件名**在 `<exportPath>/<talker>/<images|voices|videos|emojis>/<file>`
  下解析。三段式路由因此不再需要。
- `sync` 帧新增 `generation`（**恒出现**）：客户端据此区分「注销后新账号刚开始」与「自己漏收了」。
  （该帧同时从手拼 `json!` 改为**类型化 DTO**，三个类型已登记进 `/openapi.json`。）

### 变更（破坏性）

- **老面不再输出 ChatLab 形状**：`/api/v1/messages` 与 `/api/v1/sessions` 上的 `chatlab=1` /
  `format=chatlab` 开关已删除，两个面只输出原生形状。`/api/v1/sessions` 也**不再识别 `cursor`**
  （只认 `offset`）；`/chatlab/sessions` 继续认 `cursor`。两条路**必须拆参数** —— 一刀切会让
  「换个面就该换参数」静默失效（调用方以为在翻页，实际一直拿第一页）。
- **读端点只有 GET**：`messages` / `sessions` / `contacts` / `group-members` / `media/{id}` /
  `push/messages` 的 POST 变 405。`/api/v1/sync` 与 `/health`、`/api/v1/accounts` 仍接受两个方法。
- **鉴权只剩两条通道**：`Authorization: Bearer` 与 `?access_token=`。`X-Api-Key`、`?token=`
  与 POST body 里的凭据键都已删除；`merge_body` 现在**跳过** `access_token`/`token` 两个键
  （否则「body 不能鉴权」会被一次合并静默绕过）。
- **媒体字节只留 `GET /api/v1/media/{id}`**：三段式 `/api/v1/media/{talker}/{media_Type}/{file}`
  已删除；`mediaUrl` 改指 `/api/v1/media/{导出文件名}`（生产点在 `store/media_export.rs`）。
- **`mediaType` → `type`**（只消息行的扁平键；SSE 的载荷没有这个键，不受影响）。
- **`group-members` 的成员集合改为名册 ∪ 发言人**：从未发过言的成员也会出现（`messageCount` 为 0），
  `count` 会变大；名册有而消息无时**返回成员而不是 404**。同时删 `talker` 与 `withCounts` 别名、
  `forceRefresh` 占位参数（同步统一走 `/api/v1/sync`）。
- **`replyToMessageId` 在三个面统一为「无引用则省略该键」**（本仓此前只有拉取面有该键，
  消息面没有；现在三个面同规）。
- **原生面 `media.exportPath` 未导出时省略**（此前是空串 —— 空串会被读成「有路径、只是空的」）。
- 删除参数别名：`meiti` / `tupian` / `vioce`；删除 `POST /api/v1/accounts/{qq}/deregister` 别名。
- **通知帧的三个 id 键改为省略**（`eventId` / `sessionId` / `platformMessageId`）：基线事件的
  `sessionId` 是空串，而 `skip_serializing_if` 对空串无效 —— 构造时显式映射成 `None`。
  撤回帧**仍然不填** `platformMessageId`：本仓事件里的 `rawid` 是**行号**，而拉取面的
  `platformMessageId` 用的是 `seq`，两号不可混用。
- **`/chatlab/sessions` 的 `messageCount` 落成真值**（索引里该会话的条数）。此前恒为 0，
  而「键要留着：下游按它排序」这条注释让一列无意义的数字看起来像承诺。
- 契约 pin 升到 `v0.4.0`（新增 `media_shape_in_pull`、`chatlab_envelope_page_keys` 两条具名
  不变量与消息面用例；鉴权探测收窄为两条；夹具登记 `messages_chatlab` 端点）。

### 修复

- **按名取字节的同名多命中**：候选内容一致才服务（先比 size 短路，必要时逐字节比较），不一致给 404。
  「取第一个」在这里是错的：按名解析是**跨会话**的，别的会话里可能躺着同名但内容不同的文件 ——
  随便挑一个等于把「出现即可取」变成「出现即可取到某个东西」，而调用方无从察觉。
  **句柄因此只对内容摘要派生的名字给出**（平台名/原文件名回落只作元数据）。
- **kinds 白名单与穿越拒绝迁到 `{id}`**：只有四个类型目录参与解析；路径段沿用
  `pathsafe::safe_segment` ＋ canonicalize ＋ `starts_with(export_root)`。
- `group-members` 的排序稳定：先按 `messageCount` 降序、再按 uid 升序。只按计数排时，
  一大批计数为 0 的潜水成员顺序随哈希遍历顺序抖动，同一个群两次请求的顺序可能不同。
- 未命中的措辞统一为「媒体不存在」（含缓存被清理的情形）——「文件被清理」与「没这个名字」
  对调用方是同一种失败。
- 注销函数里那段英文注释改回与代码一致（它写着重放历史「保持不动」，而代码是**清空条目 ＋
  保留 id 计数器 ＋ 推进 `generation`**）。

### 文档

- `docs/qqflow-server-api.md` 与 `docs/architecture.md` 随本批改动同步：路由清单（含
  `/chatlab` 四条）、鉴权两条通道、读端点只留 GET、媒体按名解析与同名消歧规则、
  `GET /chatlab/messages` 的信封与四条口径、群成员集合与排序、`messageCount` 真值。
- `/chatlab/sessions` 的响应示例补上漏掉的 `count` 键（实际响应一直有它）。

### 迁移

过渡期一律**立即生效**。

| 改了什么 | 怎么迁 |
|---|---|
| 老面不再输出 ChatLab 形状（`chatlab=1` / `format=chatlab`） | 改用 `GET /chatlab/messages`（参数同名，见上）；会话发现面用 `/chatlab/sessions` |
| 老面不再识别 `cursor` | 老面用 `offset`；**注意失败模式**：继续传 `cursor` 不会报错，而是**静默回到第一页** |
| 旧参数拼写（`chatlab=1`、`format=chatlab`、`meiti`、`tupian`、`vioce`） | 一律**被静默忽略**（未知参数不报错）：改成 `media=1` 与类型参数，或改用新面 |
| 三段式媒体路由已删 | 改用 `GET /api/v1/media/{id}`（`{id}` 是导出文件名）；未导出前先请求 `media=1` |
| 鉴权只剩 Bearer 与 `?access_token=` | 删掉 `X-Api-Key`、`?token=` 与「把 token 放进 JSON body」的写法 |
| 消息行的 `mediaType` 改名 `type` | 按键名替换；值与位置不变 |
| `/chatlab/sessions` 的 `messageCount` 由恒 0 变真值 | 按它排序的地方现在拿到的是真实条数（更准，但也更大） |
| 读端点的 POST 变 405 | 改用 GET（参数走查询串） |
| `group-members` 的 `talker`/`withCounts` 别名与 `forceRefresh` 已删、成员集合并了名册 | 用 `chatroomId` 与 `includeMessageCounts`；按 `count` 分配 UI 的地方要接受更大的成员数 |
| 原生面 `media.exportPath` 未导出时省略 | 判空从「空串」改为「键不存在」 |
| 通知帧的 `eventId`/`sessionId`/`platformMessageId` 改为省略 | 按「键存在且非空」判断；基线帧不再有空的 `sessionId` |
## [0.6.1] - 2026-10-01

门禁与文档收口。**响应形状未变** —— 新增的是接口描述里的两条操作与更严的门禁。

### 新增

- **`/openapi.json` 补上两条真实存在的操作**：`POST /api/v1/sessions`、`GET /api/v1/sync`。
  它们一直能被调用却不在描述里，从描述生成客户端的人看不到它们。
- 路由现在有**唯一事实源**（`src/server/routes.rs`）：`build_router` 由它构建，
  与端点表的对等由 `documented_routes_match_the_openapi_table` 强制 —— 集合必须等于
  「路由 − 豁免」（本仓没有豁免），未声明的方法必须 405。

### 变更（对门禁，不对接口）

- 一致性套件带上跳过即失败：有用例被跳过时整套失败（此前跳过不影响退出码，
  「夹具少声明一个端点」会让用例静默变成不跑，而 CI 仍是绿的）。缺 `FLOW_CONTRACT_DIR`
  同样由静默通过改为失败。
- 契约 pin 升到 `v0.3.3`（`v0.3.1` 引入跳过即失败；`v0.3.2` 让 runner 校验 tag；
  `v0.3.3` 修公共段措辞）。夹具的 `contractVersion` 与 `conformance.pin` 由
  `pinned_contract_version_matches_the_fixture` 钉在一起，只改一处不再能溜过。
- golden 快照**缺失即失败**（此前缺失会被静默重建，drift 检测随之失效）。

### 文档

- `docs/architecture.md` 补「工程与工具链」「测试与夹具」两节。
- `docs/qqflow-server-api.md` 登记 `members[].roles` 为**有意不输出**（与 `isOwner` 同义，
  且受同一个「群主可能不在本页」的限制）。

### 迁移

无。
## [0.6.0] - 2026-09-27

ChatLab 适配层上线，**并接受一次破坏性发布**（三项，见下）。下游需按迁移表逐项核对。

### 破坏性变更

| 变更 | 改了什么 | 怎么迁 | 过渡期 |
|---|---|---|---|
| **SSE `sync` 载荷** | qqflow 由平铺的 `lastRowidGroup`/`lastRowidC2c` 收敛为 `{event,watermarks:[{table,watermark}]}`（weflow 同批统一为带 `generation` 的水位线数组） | 订阅者若解析 `sync`，按新形状改 | **不适用** —— 唯一已知下游零影响（实测：显式忽略 `sync`）|
| **注销后重放** | 清重放条目 ＋ **保留** id 计数器 ＋ 基线带 `generation` | 依赖 `Last-Event-ID` 的下游需处理 `generation`（它区分「换了个账号」与「自己漏收了」）| **不适用** —— 唯一已知下游零影响 |
| **媒体地址改根相对** | `mediaUrl` 由 `http://host:port/api/v1/media/…` 变为 `/api/v1/media/…`，不再把服务基址烤进响应（本仓此前**从未内嵌 token**，与 weflow 的「去 token」不是同一件事）| 按自己的 `base_url` 拼接后再请求；把相对路径当完整 URL 直接用的写法**会失败** | **不适用** —— 发布即生效、无并行期，需随升级同步改 |

### 新增

- **ChatLab 适配面 `/chatlab/*`**（`baseUrl` 指向 `http://127.0.0.1:PORT/chatlab` 即可）：
  - `GET /chatlab/sessions` —— Pull 形状的发现面（`keyword` / `limit` / `cursor`）；
  - `GET /chatlab/sessions/{id}/messages` —— Pull 面；
  - `GET /chatlab/push/messages` —— **通知面**：只发元信息（`eventId` / `sessionId` / `timestamp` /
    `platformMessageId?`），**不发消息体**。规范对这条通道的定位是「仅通知，不假设事件可靠送达」，
    客户端收到后**去拉**那一页。
  - **老面 `/api/v1/*` 的路由集合与默认语义不变** —— 不关心新能力可以不迁。
- **SSE 连接建立就发一帧 `sync` 基线**：没有它，客户端在「连上」到「第一次水位变化」之间是盲的。
- **`generation`**：注销时递增，两个 SSE 面发同一个计数器。
- **群名册**（读 QQ 的 `group_info.db`）：`/chatlab/sessions` 现在发 `memberCount`（**我们知道的**成员数，
  本地缓存，可能少于真值），群名片与群昵称也可用了。
- **接入配方写在 `docs/*-api.md` 里**（四阶段 ＋ 实测数字 ＋ 接入者会踩的坑）。

### 修复

- `mediaId` **只在取得到字节时才通告**（「出现即可取」是承诺，不是尽力而为）。
- 空 talker 的行不再生成会话：真库里因此出现过一个 id 为空串的会话（`GET /api/v1/sessions`
  会多出这一项，`GET /api/v1/messages?talker=`（空）也会把这些行当正常会话返回）——现在两者
  都按「无此会话」处理。

## [0.5.1] - 2026-08-28

启动日志补一条账号扫描计数，其余为文档修订。**无接口变更，0.5.0 客户端无需改动。**

### 变更

- 启动扫描新增一条计数日志（`发现 N 个账号目录，等待注册` / `未发现本机 QQ 账号目录`）。
  此前扫描结果完全不打印，启动日志无从判断本机有没有可注册的账号。**只报数量、不打印
  QQ 号**：`/health` 为「免鉴权不得枚举账号」付了类型级代价（`AccountPhase` 没有
  `AwaitingKey` 变体），日志打印清单会把这层设计绕过去；号码仍由需鉴权的
  `GET /api/v1/accounts` 提供。与 weflow-server 同形。

### 文档

- 开篇明确 "WeFlow" 在本文中一律指**安装版 WeFlow**（5031），不是同族的 weflow-server
  （5033）。全文 6 处 "WeFlow" 从未定义过基线，§3.2 第 ⑥ 项的「404 错误体差异」因此容易被
  误读成与 weflow-server 的现存差异——后者自其 0.5.0 起已同样统一为 envelope，该差异仅对
  安装版成立。第 ⑥ 项一并补注。
- SSE 保活补明帧形态：`ping` 是**注释帧**（线上字节 `:ping`），不是 `data:` 帧。只解析
  `data:` 行的客户端收不到它，事件分派无需处理 `ping` 类型（此前表述不足，下游据此写过
  不可达的防御分支）。

## [0.5.0] - 2026-08-28

本版本收敛账号管理面：`/health` 不再泄露账号清单，账号明细移到需鉴权的接口，
并补上了此前缺失的注销能力。**含破坏性变更，不兼容 0.4.x 客户端。**

### 破坏性变更

- `/health`（及 `/api/v1/health`）的响应从 `accounts` 数组改为单个标量字段
  `account`，取值 `unregistered | indexing | ready | error`。该接口免鉴权，
  而启动扫描会为本机每个 QQ 目录建立一条记录，因此原来的数组（乃至它的长度）
  等于向任何未鉴权的调用方枚举本机存在哪些账号、各自进行到哪一步。
  账号号码、消息数、数据库路径与错误详情改由 `GET /api/v1/accounts` 提供。
  `awaiting_key` 不再对外出现——进入该状态的唯一途径就是扫描发现，
  故一律折叠为 `unregistered`。
- 重复注册**不再覆写**：已有另一个账号持有绑定时，`POST /api/v1/accounts`
  返回 `state: "account_conflict"`（HTTP 200）并附 `occupied_by` /
  `occupied_status`。内存索引没有账号维度，覆写会把第二个账号的数据写进
  第一个账号的索引里。换账号需先注销。

### 新增

- `GET /api/v1/accounts`：账号明细（`qq` / `state` / `message_count` /
  `error` / `db_path`）。Token 保护，**不受就绪门控**——客户端正是在账号
  `indexing`（服务尚未就绪）时轮询它。
- `DELETE /api/v1/accounts/{qq}`：注销账号，把服务恢复到刚启动的未注册状态
  （停止同步与文件监听、丢弃内存索引、清空 SSE 重放缓冲、广播归零的 `sync`
  基线事件）。别名 `POST /api/v1/accounts/{qq}/deregister` 供无法发 DELETE
  的客户端与代理使用。
  - 路径里的 `qq` 是**安全联锁而非选择器**：绑定全局只有一个，传错账号报
    `qq_mismatch` 并且完全不动占用方，而不是顺手注销当前绑定的那个。
  - 三种结果一律 HTTP 200，判定写在 `state`：`deregistered` /
    `not_registered`（幂等）/ `qq_mismatch`。
  - `purge_media` **默认 false**：导出媒体是派生数据、删除不可撤销。开启后
    也只删 `<exportPath>/<talker>/<images|voices|videos|emojis>`，talker
    目录仅在变空后移除，导出根目录永不递归删除。
  - 允许在 `indexing` 中途注销：进行中的初始化会被作废，构建完成后不会把
    索引装回来，账号也不会"复活"。

### 修复

- 文件监听任务此前只在进程退出时结束，`JoinHandle` 未被跟踪；现由
  `SyncEngine` 持有并在注销时 abort，否则注销后的写入仍会驱动一次同步、
  把数据写进刚被清空的索引里，SSE 订阅方也会继续收到一个"已注销"账号的消息。
- `error` 状态不再释放绑定：一次瞬时解密失败不应把服务交给另一个账号。
  同一账号仍可直接重试注册来恢复。

### 已知限制

- 内存中的 SQLCipher 密钥未做 `zeroize`：仅存活于进程内存、不落盘，但注销与
  进程退出时不做显式擦除，仍可能残留在内存或崩溃转储中。威胁模型假定本机
  可信（默认只监听 `127.0.0.1`）。
- 注销不是锁：持有 token 的客户端可以立刻重新注册。要真正阻止访问请轮换
  token 或停止进程。
