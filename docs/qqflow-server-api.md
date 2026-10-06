# qqflow-server HTTP API / Push 文档

qqflow-server 提供本地 HTTP API（已支持 GET 和 POST 请求），便于外部脚本或工具读取 QQ NT 本地聊天记录（会话、消息、联系人、群成员）；也支持通过固定 SSE 地址推送新消息事件。接口形态参考 WeFlow HTTP API，字段与语义以本文档为准（v1 实现差异见各节"说明"）。

> **术语**：**本文中 "WeFlow" 一律指安装版 WeFlow HTTP API（默认端口 5031）**，不是同族的 weflow-server（默认 5033，另有独立 API 文档）——下文所有「与 WeFlow 的差异」均以前者为基线。

## 启用方式

**无配置文件**；运行参数全部由命令行指定（均有默认值）：`--port`（5032）/ `--host`（127.0.0.1）/ `--log`（info）/ `--watch-debounce-ms`（350）/ `--watch-fallback-ms`（30000），`qqflow-server.exe` 直接启动即为默认状态。

- 默认监听地址：`127.0.0.1`
- 默认端口：`5032`
- 基础地址：`http://127.0.0.1:5032`
- **账号为客户端驱动**：启动时仅做平台路径扫描，发现的账号列为 `awaiting_key`（零账号启动合法）；由客户端调用 `POST /api/v1/accounts` 传入 `{qq, key, db_path}` 注册账号后，服务在后台以只读直连方式打开源库（偏移 VFS 虚拟剥离自定义头）、解密并构建索引（见 §1.1）
- API Token：首次启动自动生成（32 字节随机数的 64 字符十六进制）并持久化到系统凭据库（`--show-token` 获取）。两种情形启动日志**会打印 token 值**：凭据库为空首次生成时（info 级），凭据库不可用而降级为会话级 token 时（warn 级）——后者的 token 重启即变
- 索引就绪前（账号处于 `indexing` / `error` 时），业务接口返回 `503`（见 §8 错误）；`/health` 返回 `starting` 状态。例外：SSE 接口 `/api/v1/push/messages` 与 `/api/v1/accounts`（含明细、注销）不检查就绪状态，可随时调用
- 新消息检测：后台以**文件系统事件**驱动（Windows ReadDirectoryChangesW / Linux inotify / macOS FSEvents，`--watch-debounce-ms` 默认 350ms 防抖，辅以 `--watch-fallback-ms` 默认 30s 慢速兜底轮询防事件丢失），源数据库文件变化时执行完整同步（直连活库的增量读取，零拷贝），经 `GET /api/v1/push/messages` 推送 SSE；客户端亦可主动调用 `POST /api/v1/sync` 立即同步

## 命令行子命令（`cli`）

裸跑仍是「起服务」；既有旗标（`--port`／`--help`／`--show-token` …）**逐字不变**。新增的子命令面与退出码：

| 码 | 含义 | 例子 |
| --- | --- | --- |
| `0` | 成功 | `qqflow-server sessions` |
| `1` | 运行期错误：连不上、被拒、缺 token／缺配置文件 | 未设 `QQFLOW_TOKEN` 就跑查询 |
| `2` | 用法错误：未知子命令、缺必需参数 | `qqflow-server bogus`、`search` 不带 `--keyword` |

| 子命令 | 打哪个面 | 要点 |
| --- | --- | --- |
| `serve` | — | 起服务；等价裸跑 |
| `token` | 系统凭据库 | 打印 API token 并退出（等价 `--show-token`）|
| `sessions` | SDK `list_all_sessions` | `page_size=10000`（服务端硬上限），一次取尽 |
| `messages` | SDK `list_messages` | HTTP 形态必须给 `--talker`；`--since` 接受 unix 秒或 `YYYYMMDD` |
| `search` | SDK `list_messages` 带 `keyword` | `--keyword` 必填；搜不到不是错误（退出 0）|
| `contacts` | SDK `contacts` | **只走 HTTP**：本仓的嵌入面没有联系人读面 |
| `accounts` | SDK `accounts` | **只有 HTTP 形态**：这一面问的是「服务端此刻实际绑定了什么」|
| `sync` | SDK `sync_now` | **写动作**：立刻跑一次增量同步；没有 `--embedded` |
| `export` | SDK `list_all_sessions` ＋ `drain_session` ＋ `chatlab_messages` | **只走 HTTP**：批量导出到 ChatLab Format 文件；`--with-media` 时另用 `chatlab_messages(media=1)` 触发导出并下载字节，见下一节 |

环境变量：`QQFLOW_BASE_URL`（默认 `http://127.0.0.1:5032`）、`QQFLOW_TOKEN`（API token）。
**token 一律不经命令行传递** —— 命令行会落进 shell history 与进程列表。

`--embedded`（进程内直读本地库，不起也不打 HTTP）只开放给 `sessions`／`messages`／`search`：
配置路径由 `QQFLOW_EMBED_CONFIG` 给出，JSON 形态 `{"db_path": "<nt_msg.db 文件>", "key": "<库密钥>"}`
（本仓的嵌入面是 `api::open(db_path, key)`：一库一钥）。关键词在嵌入形态只对**解析后的正文**判命中
（本仓不保存原始 XML）—— 这是与 weflow-server 的能力差异，写在这里而不是留给用户猜。

输出：默认人类可读紧凑行，`--json` 给机器可读形状。

回归位置：`tests/cli.rs`。

## 批量导出（`export`）

`qqflow-server export --out <目录> [--format jsonl|json] [--session <id> …] [--since <t>] [--resume] [--with-media]`

**只走 HTTP**：这个面不提供 `--embedded`。服务端已经把数据库密钥握在内存里，CLI 只做编排与落盘；
否则一个可能跑几分钟的任务会长时间持有密钥，还得把密钥带上命令行。

产物布局：

- **每个会话一个文件**（`<slug>.jsonl` 或 `<slug>.json`）。`<slug>` 由会话显示名经 `pathsafe::slugify`
  得到；**折叠后不含任何 ASCII 字母数字时**（纯中文群名会被折成一串下划线，既不可读也极易互撞）
  **回落到会话 id 的 slug**；同名会话按出现顺序追加 `-2`／`-3`。文件名是确定性函数 —— 这是
  `--resume` 成立的前提。两个补充口径：

  - **去重按大小写折叠**（Windows 卷默认大小写不敏感，`Team` 与 `team` 是同一个文件），而交付名
    保留原大小写；
  - **编排文件名 `index` 永远留给清单**：显示名恰好是 `index` 的会话拿到 `index-2`，否则 json 形态下
    会话信封会被清单原地覆盖。

  回归位置：`export::tests::slug_collision_gets_deterministic_suffix`。
- **JSONL 形态**：第一行是 `_type: header`（含 `chatlab` 与 `meta`），其后是 `_type: message` 行。
  规范建议按时间升序，因此**页内**排序（跨页排序会把内存恒定这条承诺打破）。**不写 member 行**：
  流式写不出「先集齐成员再写消息」的顺序，而消息行自带 `accountName` 与 `groupNickname`，信息不丢。
- **JSON 形态**：一个会话一个完整信封（`chatlab`／`meta`／`members`／`messages`）。整会话留在内存，
  因此大语料请用 jsonl。
- **`index.json`**：本服务自造的编排清单。**它不属于 ChatLab 规范，导入请用单个 `<slug>.jsonl`／`.json`。**
- **`--with-media`**：把本会话用到的媒体字节下载到 `<目录>/media/`，并把导出物里的 `media.fileName`
  **限定为确实落盘的那些句柄**。实现上先走 `/chatlab/messages?media=1` 触发导出（该面**每请求最多导出
  200 项**，超出部分靠翻页续传），再取字节；顺序不能反 —— 服务端只有在真的写出了本地副本之后，才把
  `fileName` 回填成可取句柄。注意**两个面给的名字不必相同**：消息面回填的是导出后的内容摘要名，
  拉取面携带的仍是索引里的原始名，所以句柄**按消息 id 对账**（不是按名字比对），导出物里写的是实际落盘
  的那个名字。外链媒体与未能导出的媒体**不会**留下句柄：宁可少一个 `media` 字段，也不给一个指向不存在
  文件的句柄。单个媒体取不到只跳过，不升级成会话级失败。
- **响应里的媒体名要过本地路径校验**：服务端回传的 `media.fileName` 要拿去拼 `<目录>/media/` 下的路径，
  `../`、盘符、ADS、Win32 设备名这类值配合 `join` 能写到导出目录之外（URL 段编码只防 HTTP 层）。
  因此落盘前先过 `pathsafe::safe_segment`，非法名按 404 同级跳过**并计数可见**（汇总行给出个数）。
  同名（含仅大小写不同，折叠口径与会话名一致）的媒体只下载一份字节，引用它的每条消息都映射到**实际落盘的
  那个名字**。
  回归位置：`cli_e2e::with_media_rejects_unsafe_response_file_names` 与
  `cli_e2e::with_media_maps_exported_names_back_to_message_ids`。

三条硬约束：

1. **导出物里不得出现访问令牌**。服务端的媒体是**根相对路径**（`/api/v1/media/<file>`，**不含令牌** ——
   令牌只走请求头或 `?access_token=`，响应体从不嵌它）。导出仍然不写任何 URL（相对路径换台机器就失效），
   传上网盘。因此导出**不写任何 URL**，媒体只以 `{type, fileName}` 表达；并且每一行写盘前会拿调用方给的
   令牌做一次子串检查，**命中即整轮中止**（不是跳过该会话）并删掉半成品。
2. **会话级失败不静默**：取数失败的会话被记入 `skipped`，而只要 `skipped` 非空，CLI 就以退出码 1 结束。
   静默少导几个会话是这类工具最坏的失败方式。**`--resume` 的命中不算失败**：记入 `reused`（幂等完成），
   同参数续跑以退出码 0 收场。
3. **最终名是唯一的完成标记**：会话先写 `<slug>.<格式>.part`，整个会话成功收尾后才 `rename` 成最终名。
   因此 `.part` 残留意味着「没写完」，续跑一律重写；失败的那一轮只删 `.part`，**不会**碰上一轮已经
   交付的完整产物。清单 `index.json` 同样先 `index.json.part` 再改名 —— 它是续跑唯一的「完成记录」来源，
   半路被杀的截断清单会让下一轮误判「没有上一轮」。
4. **`--resume` 复用要同时满足四条**（缺一条就重写，并在日志点名原因）：上一轮清单记着该会话、
   且它登记的**就是本轮这个文件名**（换格式 `jsonl`／`json` 后盘上的另一扩展名文件是孤儿，
   不算本轮产物）、没有 `.part` 残留、本轮带 `--with-media` 时上一轮也带过媒体。
   「有个同名文件」单独不构成完成记录：清单被删或被截断时，那会让从没导出过的会话被静默判成已完成，
   而它在新清单里没有条目，既看不见也不能自愈。
5. **媒体承诺按轮成立**：`--with-media --resume` 复用上一轮产物时，CLI 会逐个核对复用会话里的
   `fileName` 在 `media/` 下确有字节；有悬空就点名该会话并以退出码 1 结束（媒体目录被清理或
   搬走过的交付包不该静默通过）。处理办法：对点名的会话去掉 `--resume` 重跑，或恢复 `media/`。
6. **单个会话的起手／收尾失败只跳过该会话**（记入 `skipped`），不中止整轮 —— 整轮中止会让本轮已写出
   的会话留在盘上却不进清单，比留一个 `.part` 更难恢复。令牌泄漏仍是整轮中止。

   回归位置：`export::tests::resume_rewrites_an_incomplete_artifact`、
   `export::tests::failed_rerun_keeps_the_previous_complete_artifact`、
   `cli_e2e::export_writes_a_file_against_a_live_service`（含续跑第二次退 0）。

内存：JSONL 逐页写盘、写完即丢，**峰值常驻集与消息条数无关**；`--with-media` 的句柄集合是
**O(不同媒体数)**、同秒组的消息会被服务端扩页带出（Pull 段的「同秒扩页」），这两项不在「与条数无关」的承诺内。
大语料的实测口径与造库工具（隐藏的 `--rows`
参数，仅 `testing` feature 下编译进二进制）见 `docs/architecture.md` 的「测试与夹具」。

回归位置：`export::tests::*`（行形状、slug、`--resume`、令牌熔断、`--with-media` 的句柄契约）与
`tests/cli.rs` 的 `export_help_does_not_advertise_the_rows_param`／`export_requires_out_dir_as_usage_error`。

## 鉴权规范

除健康检查接口外，所有 `/api/v1/*` 与 `/chatlab/*` 接口均受 Token 保护。支持**两种**传参方式（任选其一）：

1. **HTTP Header (推荐)**: `Authorization: Bearer <您的Token>`
2. **Query 参数**: `?access_token=<您的Token>`（SSE 长连接推荐此方式）

> **迁移**：`X-Api-Key`、`?token=` 与 POST body 里的凭据键**已删除**。每多一条通道就多一处
> 凭据会被复制到的地方（请求体、代理日志、客户端抓包），而它们鉴权的是同一个东西。
> body 里的 `access_token`/`token` 现在会被**跳过**（既不鉴权、也不覆盖查询串里的值）。

## 接口列表

- `GET|POST /health`（免鉴权）
- `GET|POST /api/v1/health`（免鉴权）
- `POST /api/v1/accounts`（注册账号：qq + key + db_path）
- `GET /api/v1/accounts`（账号明细，需鉴权，见 §1.2）
- `DELETE /api/v1/accounts/{qq}`（注销账号，见 §1.3；**POST 别名已删除**）
- `GET /api/v1/messages`（原生/富数据形状，见 §3）
- `GET /api/v1/media/{id}`（**唯一**的取字节路由：store 键或导出文件名，见 §3.1）
- `GET /api/v1/sessions`（原生形状；只认 `offset`）
- `GET /api/v1/sessions/{id}/messages`（ChatLab Pull；与 `/chatlab/...` 同一处理器、同一形状）
- `GET /api/v1/contacts`
- `GET /api/v1/group-members`（成员集合是**名册 ∪ 发言人**）
- `GET /api/v1/push/messages`（SSE，带正文的富推送面）
- `GET|POST /api/v1/sync`（手动同步，动作端点）
- `GET /chatlab/sessions`（ChatLab 发现面，认 `cursor`）
- `GET /chatlab/messages`（ChatLab 消息面：原 `?chatlab=1` 的新家）
- `GET /chatlab/sessions/{id}/messages`（ChatLab Pull）
- `GET /chatlab/push/messages`（SSE 通知面，只发元信息）
- `GET /openapi.json`（接口描述，**免鉴权**，见 §1.0）

> **读端点只有 GET**：`messages`/`sessions`/`contacts`/`group-members`/`media/{id}`/`push/messages`
> 的 POST 一律 **405**。动作端点（注册、同步）与免鉴权的 `/health` 保留两个方法。

> v1 未实现：`/api/v1/sns/*`（朋友圈）——QQ NT 本地库不含朋友圈数据。媒体双通道：`/api/v1/media/{id}` 直接服务 QQ 本地缓存里的媒体文件（常开）；`media=1` 按需导出到 `exportPath`（§3.2，WeFlow 形状）。

---

## 1.0 接口描述（GET /openapi.json，免鉴权）

返回本服务的 OpenAPI 3 描述，由 `server/dto.rs` 里的响应类型经 `#[derive(ToSchema)]` 生成。

免鉴权是刻意的：它描述的是**形状**，不含账号、路径或密钥，而且正是给尚未拿到 token 的接入方
看的 —— 拿到它就能生成客户端，再凭 token 调真正的接口。

两点使用提示：

- **多形状端点用 `oneOf`**。`/api/v1/sessions`、`/api/v1/messages`、`/api/v1/accounts`（POST）
  的响应形状由参数或状态决定，描述里列的是若干可能形状的并集 —— 生成客户端时应按 `oneOf`
  处理，而不是当成「所有字段都可能存在」。
- **描述随 DTO 变**。改 DTO 就会改它；`tests/openapi.rs` 保证描述自身自洽（每个 `$ref` 都能
  解析、operationId 唯一、多形状确实用 `oneOf`），golden 快照保证它的变更有人看过。
- **字节面与 SSE 面如实标注媒体类型**：媒体路由是 `application/octet-stream`（binary），
  推送面是 `text/event-stream`；端点级 `description` 携带各面的对外闸门（limit 默认/上限、
  单页 5000、重放缓冲 1000 条/600 秒等）——生成的客户端不必再翻散文文档找这些数字。

错误响应（401/404/405/400 等）**不在描述里**：它们是跨端点的统一信封，见上文「鉴权规范」后的
错误信封说明。

---
## 1. 健康检查

**请求**

```http
GET /health
```

或

```http
GET /api/v1/health
```

免鉴权，GET/POST 均可。

**响应**

```json
{
  "status": "ok",
  "version": "<version>",
  "account": "ready"
}
```

| 字段 | 说明 |
| ---- | ---- |
| `status` | `ok`（索引就绪）或 `starting`（未就绪） |
| `version` | 服务版本号 |
| `account` | 单个标量阶段值 ∈ `unregistered \| indexing \| ready \| error` |

`account` 的取值：

| 值 | 说明 |
| -- | ---- |
| `unregistered` | 无账号注册（含"启动扫描发现了账号但尚未注册密钥"，见下） |
| `indexing` | 已注册，正在构建索引 |
| `ready` | 索引就绪，业务接口可用 |
| `error` | 初始化失败（如密钥错误）；账号仍占用绑定 |

**本接口刻意不列出账号。** 它免鉴权，而启动扫描会为本机每个 QQ 目录建立一条记录，因此把账号数组（乃至它的长度）放在这里，等于向任何未鉴权的调用方泄露本机存在哪些账号、各自进行到哪一步。账号号码、消息数、数据库路径、错误详情改由需鉴权的 `GET /api/v1/accounts`（§1.2）提供。

同理，`awaiting_key` 不会出现在这里：账号进入该状态的唯一途径就是启动扫描发现了它，所以对外一律折叠为 `unregistered`。

> 说明：`error` 表示初始化失败（如密钥错误），客户端重新调用 `POST /api/v1/accounts` 传入正确参数即可恢复，进程不会退出。

---

## 1.2 账号明细（GET /api/v1/accounts）

`/health` 不再披露的账号明细。Token 保护（两条通道，但 GET 建议走 Header——查询串会进代理日志与 shell 历史）；**不受就绪门控**，因为客户端正是在账号还在 `indexing`（即服务尚未就绪）时轮询它。

**请求**

```http
GET /api/v1/accounts
Authorization: Bearer YOUR_TOKEN
```

**响应**

```json
{
  "success": true,
  "accounts": [
    {
      "qq": "1234567890",
      "state": "ready",
      "message_count": 28314,
      "db_path": "C:\\Users\\<用户名>\\Documents\\Tencent Files\\1234567890\\nt_qq\\nt_db\\nt_msg.db"
    }
  ]
}
```

| 字段 | 说明 |
| ---- | ---- |
| `accounts[].qq` | 账号 |
| `accounts[].state` | `awaiting_key` / `indexing` / `ready` / `error`（完整状态机，不折叠） |
| `accounts[].message_count` | 已索引消息数（仅 ready 后有效） |
| `accounts[].error` | 出错时的错误信息（仅 `error` 状态；否则该键省略） |
| `accounts[].db_path` | 服务端解析到的 `nt_msg.db` 路径；未知时该键省略 |

数组里可能有多条，但**最多一条处于 `indexing` / `ready` / `error`**（即"绑定"）。其余条目是启动扫描发现、但从未注册密钥的账号，恒为 `awaiting_key`；它们不参与就绪判定，也不构成绑定（见 §10）。

---

## 1.1 注册账号（POST /api/v1/accounts）

客户端驱动启动：下游客户端传入账号（`qq`）、数据库密钥（`key`）与可选数据库路径（`db_path`），服务在后台以只读长连接直连活库完成解密 + 索引构建，账号进入 `ready`。仅 POST；Token 保护（两条通道）；**不受就绪门控**。

**请求**

```http
POST /api/v1/accounts
```

```json
{
  "qq": "1234567890",
  "key": "<16字节ASCII密钥>",
  "db_path": "C:\\Users\\<用户名>\\Documents\\Tencent Files",
  "access_token": "YOUR_TOKEN"
}
```

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `qq` | string | 是 | 账号（数字字符串） |
| `key` | string | 是 | SQLCipher 密钥（16 字节可打印 ASCII，由外部工具提取） |
| `db_path` | string | 否 | `nt_msg.db` 文件路径，或 Tencent Files 风格目录（`<dir>/<qq>/nt_qq/nt_db/nt_msg.db`）；省略时使用启动扫描发现的路径 |

**响应**

```json
{
  "success": true,
  "qq": "1234567890",
  "state": "accepted",
  "status": "indexing",
  "db_path": "C:\\SomeUser\\Documents\\Tencent Files\\1234567890\\nt_qq\\nt_db\\nt_msg.db"
}
```

| 字段 | 说明 |
| ---- | ---- |
| `state` | **本次注册请求的结果**（见下表） |
| `status` | **账号当前状态机值** ∈ `awaiting_key \| indexing \| ready \| error`，与 `GET /api/v1/accounts` 的 `accounts[].state` 同一枚举；账号此前从未出现过时省略 |
| `db_path` | 服务端**实际解析到的** `nt_msg.db` 路径；无法解析时省略 |

| `state` | 说明 | 伴随的 `status` |
| ------- | ---- | ---- |
| `accepted` | 参数合法，后台开始初始化（`GET /api/v1/accounts` 可见 `indexing` → `ready`） | `indexing` |
| `invalid_key` | 密钥未通过校验（非 16 字节可打印 ASCII）；**仅在账号/路径解析通过后评估** | 账号原状态（未变） |
| `invalid_db_path` | `db_path` 不存在或目录下无 `nt_msg.db` | 账号原状态（未变） |
| `unknown_qq` | 未扫描到该账号且未提供 `db_path` | 账号原状态（未变） |
| `already_ready` | 账号已就绪（幂等无操作） | `ready` |
| `in_progress` | 账号正在索引 | `indexing` |
| `account_conflict` | **已有另一个账号占用绑定**，本次注册被拒；额外返回 `occupied_by`（占用方 qq）与 `occupied_status`（占用方状态） | 省略（本次请求的 qq 未变化） |

**判定顺序**（与实现一致）：① 参数与密钥格式；② 幂等/占用守卫——同一 qq 已 `ready`/`indexing` 时返回 `already_ready`/`in_progress`，**不同** qq 已持有绑定则返回 `account_conflict`；③ 账号/库路径解析——未扫描到该账号且未提供 `db_path` → `unknown_qq`；提供了 `db_path` 但无法解析 → `invalid_db_path`；④ 路径解析通过后才校验密钥格式 → `invalid_key`。因此对未扫描到的账号传任何 key 都只会得到 `unknown_qq`（`invalid_key` 在该分支不可达）；`invalid_key` 只出现在账号已存在（扫描到或路径已解析）但密钥格式错误的情形。

`account_conflict` 示例：

```json
{
  "success": true,
  "qq": "10002",
  "state": "account_conflict",
  "occupied_by": "10001",
  "occupied_status": "ready"
}
```

**重复注册不再覆写。** 内存索引没有账号维度，第二个账号写进来会污染第一个账号的数据，所以换账号必须先调用 §1.3 注销。`error` 状态**不释放绑定**——一次瞬时解密失败不应把服务交给另一个账号；同一个 qq 可以直接重试注册来恢复。

`status` 的用途是免去注册后立刻再打一次 `GET /api/v1/accounts`：拒绝类响应（`invalid_key` / `invalid_db_path` / `unknown_qq`）不改变账号状态，`status` 因此告诉客户端账号**此刻仍处于什么状态**——例如密钥填错重注册一个此前失败的账号，会得到 `state=invalid_key` + `status=error`，即"这次被拒且账号仍然坏着"。

**`status: "indexing"` 不代表密钥正确**：本接口在账号/路径解析通过后只校验密钥格式，真正的解密验证在后台初始化中完成（失败 → `error`）。客户端仍需轮询 `/health`（或 §1.2）等到 `ready`。

`db_path` 回显的是解析结果而非请求原值：请求里的 `db_path` 可以是文件、可以是 Tencent Files 风格根目录、也可以省略（走启动扫描），回显让客户端确认服务端最终读的是哪个库。幂等分支（`already_ready` / `in_progress`）回显的是**运行中账号当初使用的路径**，本次请求携带的 `db_path` 在这些分支下被忽略。

密钥仅保存在内存中，**不持久化**；进程退出后需重新注册。密钥错误时账号进入 `error` 状态（`GET /api/v1/accounts` 的 `accounts[].error` 给出原因），重新调用本接口传入正确参数即可恢复。

---

## 1.3 注销账号（DELETE /api/v1/accounts/{qq}）

撤销注册，把服务恢复到刚启动时的未注册状态：停止同步与文件监听、丢弃内存索引、清空 SSE 重放缓冲并广播一条归零的 `sync` 基线事件。Token 保护；**不受就绪门控**——卡在 `error` 的账号正是最需要清掉的。

**请求**

```http
DELETE /api/v1/accounts/1234567890?purge_media=1
Authorization: Bearer YOUR_TOKEN
```

> **迁移**：`POST /api/v1/accounts/{qq}/deregister` 别名**已删除**（现在返回 404）。
> 注销只有 `DELETE` 一条路 —— 两条路做同一件事时，其中一条迟早漏掉一次改动。

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `qq` | string(path) | 是 | **安全联锁**，非选择器（见下） |
| `purge_media` | bool(`1`/`0`/`true`/`false`) | 否 | 是否同时删除已导出的媒体文件，**默认 `false`** |

**响应**

```json
{
  "success": true,
  "qq": "1234567890",
  "state": "deregistered",
  "previous_status": "ready",
  "index_cleared": true,
  "purged_media": true,
  "purged_dirs": 3
}
```

| 字段 | 说明 |
| ---- | ---- |
| `state` | `deregistered` / `not_registered` / `qq_mismatch` |
| `previous_status` | 请求到达时账号所处的状态（仅 `deregistered`）——用于区分"取消了一次进行中的构建"与"解绑了一个就绪账号" |
| `index_cleared` | 是否真的丢弃了一份索引（`indexing` 中途注销时为 `false`） |
| `purged_media` | 本次是否**请求**了清理媒体（回显入参） |
| `purged_dirs` | 实际删除的导出目录数；`purged_media=false` 时恒为 `0` |
| `occupied_by` / `occupied_status` | 占用绑定的账号及其状态（仅 `qq_mismatch`） |

| `state` | 语义 | 副作用 |
| ------- | ---- | ---- |
| `deregistered` | 注销成功 | 索引、同步、监听、SSE 缓冲均已清理 |
| `not_registered` | 当前无账号绑定 | 无（**幂等**：重试一次已完成的注销得到 200，而非报错） |
| `qq_mismatch` | 另一个账号持有绑定，路径里的 qq 不是它 | **无，占用方完全不受影响** |

三种结果**一律返回 HTTP 200**，判定写在 `state` 里，与 §1.1 的拒绝态报告方式一致。

**路径里的 `qq` 是联锁而不是选择器**：绑定全局只有一个，所以传错账号说明客户端状态和服务端不一致，值得报 `qq_mismatch` 让它发现，而不是顺手把当前绑定的那个注销掉。

**`purge_media` 默认 false，且只删已知布局。** 导出媒体是派生数据，客户端可能还在用自己的缓存对外提供服务，而删文件不可撤销，所以必须显式索取。开启后也只删 `<exportPath>/<talker>/<kind>`（`kind` ∈ `images` / `voices` / `videos` / `emojis`，即服务自己写的四类），talker 目录本身仅在变空后以"非空即拒"的方式移除；`--media-export-dir` 可能指向操作员另有他用的目录，因此**递归删除导出根目录从来不是选项**，根目录自身永不删除。

**索引中途注销是允许的。** 不必等 `ready`：注销会作废进行中的初始化（`indexing` 的构建完成后不会把索引装回来，账号也不会"复活"）。

注销**不阻止**持有 token 的客户端立刻重新注册（这是恢复手段，不是锁）；注销后 `state=not_registered` 的幂等语义也意味着并发的两次注销不会互相报错。

**已扫描账号与客户端注册账号的差别**：启动扫描发现过的账号，注销后条目**保留**并回到 `awaiting_key`、`db_path` 仍在（下次启动扫描还会找到它，声称它不存在是假话）；纯客户端引入的账号（靠请求里的 `db_path` 才知道）条目**整条移除**，路径一并遗忘。

---

## 2. 主动推送（SSE）

通过 SSE 长连接接收新消息事件，端口与 HTTP API 共用。

**请求**

```http
GET /api/v1/push/messages
```

或 POST（参数仍走 Query/Header）。

### 说明

- 响应类型为 `text/event-stream`
- 连接建立后**先收到一个 `ready` 事件**（`{"status":"ok"}`，表示流已就绪，对齐 WeFlow 契约），随后重放断线期间错过的事件（见下），再收到 `sync` 事件（qqflow-server 扩展，携带当前 rowid 水位线），之后是 `message.new` / `message.revoke`
- **断线续传（Last-Event-ID 重放）**：每个 `message.new` / `message.revoke` 帧都带 `id:`（服务端单调递增序号）。客户端重连时携带 `Last-Event-ID: <序号>` 请求头（或 `?last_event_id=<序号>` 查询参数——浏览器 `EventSource` 无法设置自定义头），服务端重放序号之后、10 分钟 TTL 窗口内的历史事件；窗口外/无序号则从当前水位线重新开始。事件缓冲上限 1000 条
- KeepAlive 每 25 秒发送 `ping` **注释帧**（线上字节为 `:ping`，非 `data:` 帧）保活——只解析 `data:` 行的客户端收不到它，事件分派无需处理 `ping` 类型
- 订阅端落后于广播缓冲（1024 条）时会重新收到 `sync` 事件对齐
- 进程收到退出信号（Ctrl+C）时，服务端主动结束所有 SSE 流——客户端看到连接正常关闭，不会等到 3 秒宽限期超时
- 建议接收端按 `event + rawid` 去重
- **媒体路径不出现在推送里**：`media` 对象为无路径元数据视图（`localPath` 永不下发——QQ 缓存路径多为本机失效路径且无下游可用性）；媒体字节一律经 `GET /api/v1/media/{id}` 获取，键取 `mediaId`（仅当服务端已注册可读取的本地缓存时携带，与 messages 的 `mediaId` 同一规则，见 §3/§3.1）

### 事件字段

| 字段 | 说明 |
| ---- | ---- |
| `event` | `ready` / `sync` / `message.new` / `message.revoke`（`ready` 仅携带 `status`） |
| `sessionId` | 会话 ID：群聊为群号，私聊为对方 UID（`u_` 前缀） |
| `sessionType` | `group` 或 `private` |
| `rawid` | 消息 rowid（字符串） |
| `avatarUrl` | v1 恒省略（序列化时跳过该字段） |
| `sourceName` | 发送者显示名，与 §3 的 `senderName` 同一解析链路、同一取值（群聊：本群群名片（40090）> 备注 > 最新昵称 > 档案昵称 > UID；私聊无群名片，从备注起算——群名片只在所属群内显示，不会泄漏进私聊/联系人）。混用推送与 REST 的客户端，同一发送者在两个通道拿到的名字一致 |
| `groupName` | 会话显示名（群聊：群备注 > 改名消息群名 > 群信息库群名 > 群号；私聊：备注 > 对方昵称（会话名） > 档案昵称 > UID）；仅 `message.new` / `message.revoke` 携带，缺失时省略该字段 |
| `content` | 消息内容 |
| `timestamp` | 消息时间，秒级 Unix 时间戳 |
| `media` | 仅图片/语音/视频消息：媒体元数据对象（`uuid`/`md5`/`fileName`/`size`/`width`/`height`/`urls`，**不含 `localPath`**——推送不携带任何本地路径），缺失时省略 |
| `mediaId` | 仅 `message.new`：媒体获取键（md5 hex 或 uuid），用于 `GET /api/v1/media/{id}` 直取字节；**仅当索引注册了可读取的本地缓存路径时提供**（与 REST `messages.mediaId` 同规则），否则省略——出现即保证可取，绝不 404 承诺 |
| （`sync` 无上面这些字段）| **`sync` 是唯一的例外**：它的载荷收敛成 `{"event":"sync","watermarks":[…]}`，见下 |

**`sync` 的水位线是数组，不是字段。** 原来是 `lastRowidGroup` / `lastRowidC2c` 两个平铺字段 —— 那等于把
「哪张表」编码进**字段名**里，加第三张表就必须再加一个字段，消费方得靠约定去配对。现在它是数据：

```json
{"event":"sync","watermarks":[{"table":"group_msg_table","watermark":{"rowid":12345}},
                                  {"table":"c2c_msg_table","watermark":{"rowid":678}}]}
```

- **没有水位的表那一项直接不出现** —— 「还没扫过」与「扫过但没数据」在下游是两件事。
- **`watermark` 的对象形状与 weflow 不同**：这里是 `{"rowid": N}`，weflow 是
  `{create_time, local_id, sort_seq}` 三元组。**跨仓库的消费方必须按 `table` 分支**，不能因为
  字段都叫 `watermark` 就当成同一套语义。

### 示例

```bash
curl -N "http://127.0.0.1:5032/api/v1/push/messages?access_token=YOUR_TOKEN"
```

```text
event: ready
data: {"status":"ok"}

event: sync
data: {"event":"sync","watermarks":[{"table":"group_msg_table","watermark":{"rowid":12345}},{"table":"c2c_msg_table","watermark":{"rowid":678}}]}

id: 1
event: message.new
data: {"event":"message.new","sessionId":"10001","sessionType":"group","groupName":"10001","rawid":"1234567890123","sourceName":"张三","content":"你好","timestamp":1782864123}
```

---

### 接入配方（四阶段，已用真实账号走通）

把 ChatLab 的 `baseUrl` 设为 `http://127.0.0.1:5032/chatlab` 即可。下面是规范的四阶段与每一步
在本服务上的**实测结果**（真实账号，一个 6321 条的群）：

| 阶段 | 请求 | 实测 |
|---|---|---|
| ① 发现 | `GET /chatlab/sessions?limit=50` | 29 个会话（`page.hasMore=false` ⇒ 单页全量）|
| ② 全量 | `GET /chatlab/sessions/{id}/messages?since=0&limit=500`，用 `sync.nextSince` 续拉 | **6321 条 / 13 页**收敛 |
| ③ 增量 | `GET …/messages?since={lastPullAt}` | 返回 `since` 之后的增量（实测 321 条）|
| ④ 通知 | `GET /chatlab/push/messages`（SSE，可选）| 立刻收到 `ready` 基线帧 |

**服务是零账号启动的**（客户端驱动）：先 `POST /api/v1/accounts` 传入 `{qq, key, db_path}`，再轮询
`GET /api/v1/accounts` 到 `state == "ready"`。在此之前业务端点返回 `503`（`/health` 返回 `starting`），
这是**有意的** —— 索引没建完确实查不了。

**分页语义**（阶段二的关键）：

- `sync.hasMore` 为真时必须**继续拉**，并把 `sync.nextSince` 原样作为下次的 `since`。
- `since` 是**排他**下界：用 `nextSince` 续拉不会重复取到边界那一秒，也不会跳过它。
- 支持 `limit` 分页时 `sync` 块**必须**给（规范：缺了它 ChatLab 不保证自动续拉）。

**`messageCount` 是索引里的真实条数**（该会话在索引中有多少条消息）。键**恒保留** —— 缺键与
「是 0」在下游不是同一件事。`memberCount` 来自群名册，**只在名册加载得到时出现**。

### ChatLab 发现面（GET `/chatlab/sessions`）

规范把 ChatLab 的 `baseUrl` 定义为 `/chatlab`，这条是其中的会话发现入口。与 `/api/v1/sessions`
**共用同一份实现**，差别只在**形状与分页参数**：老面只输出原生形状、只认 `offset`；新面**天生
就是** ChatLab 形状，并接受 `cursor`（`page.nextCursor` 的回传入参）。

响应（规范形状）：

```json
{
  "sessions": [
    { "id": "10001", "name": "…", "platform": "qq", "type": "group",
      "messageCount": 128, "lastMessageAt": 1782864000 }
  ],
  "page": { "hasMore": true, "nextCursor": "2" }
}
```

参数 `keyword`（按名称或 id 模糊匹配）、`limit`、`cursor`（原样回传上一页的 `nextCursor`）。

- **推荐 `cursor`**：规范明确不建议在发现接口用 `offset`（列表变化时会出现重复或漏项）。
  `cursor` 解析失败时**退回 `offset`**（与其它参数一样「坏值退化为默认而不是报错」），因此
  `offset` 仍然可用。
- **`page` 总是给出**：规范说客户端在响应里**未发现** `page` 时按「单次全量结果」处理 —— 那比
  「靠条数猜有没有截断」明确。
- **`messageCount` 是真实条数**（索引里该会话的消息数）。键恒保留 —— 缺键与「是 0」在下游
  不是同一件事。
- **`memberCount`** 来自群名册（`group_info.db` 的 `group_member3`），**只在名册加载得到时出现**
  （私聊、或名册缺失时**不出现这个键**）。
  - 它是「**我们知道的**成员数」，**不是「群的确切人数」**：来源是本地缓存，可能少于真实值。
    拿它做展示预估可以，拿它做「群里一共几个人」的断言不行。
  - 「没有名册」与「名册是空的」在下游是两件事 —— 前者不该被读成 `0`，所以是可选键而不是给 0。

排序为最后消息时间降序、`id` 升序 —— 稳定，游标翻页因此不会跳项或重复。

### ChatLab 通知面（GET `/chatlab/push/messages`）

规范把 ChatLab 的 `baseUrl` 定义为 `/chatlab`，这条是其中的**通知通道**。与 `/api/v1/push/messages`
是**同一条总线、同一套连接机制**（鉴权、`Last-Event-ID` 重放、保活、滞后重基线、关机自收），
差别只有**帧的形状**：

| | `/api/v1/push/messages` | `/chatlab/push/messages` |
|---|---|---|
| 载荷 | 整个事件（含 `content` 与媒体元数据）| **只带标识与时间** |
| 定位 | WeFlow 兼容面 —— 已有客户端在解析它 | 规范里的通知通道 |

```json
{ "event": "message.new", "eventId": "…", "platformMessageId": null, "sessionId": "…", "timestamp": 1700000000 }
```

- **为什么不带正文**：规范对这条通道的定位是「仅通知：ChatLab 不假设 SSE 事件可靠送达」——
  客户端收到后**去拉**那一页。带正文会诱导调用方把它当数据源，而它并不保证送达；不带，语义就
  没有歧义。**契约套件里有一条断言就查这个**（帧里出现 `content` 或 `messages` 即失败）。
- 基线类事件（`session.sync`）额外带 **`generation`**（注销时递增）：客户端据此区分「注销后新
  账号刚开始」（该丢弃本地状态重新拉）与「自己漏收了」（该补拉）。少了它，两者在协议上是同一
  件事。
- **老面一行未改。**


## 3. 获取消息

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）；Body 字段优先于 Query 参数

读取指定会话的消息，支持原始 JSON 和 ChatLab 格式。

**请求**

```http
GET /api/v1/messages
```

### 参数

| 参数      | 类型   | 必填 | 说明                                                  |
| --------- | ------ | ---- | ----------------------------------------------------- |
| `talker`  | string | 是   | 会话 ID：群聊为群号，私聊为对方 UID（`u_` 前缀）     |
| `limit`   | number | 否   | 返回条数，默认 `100`，范围 `1~10000`                  |
| `offset`  | number | 否   | 分页偏移，默认 `0`                                    |
| `start`   | string | 否   | 开始时间，支持 `YYYYMMDD` 或秒级时间戳                |
| `end`     | string | 否   | 结束时间，支持 `YYYYMMDD` 或秒级时间戳                |
| `keyword` | string | 否   | 基于消息显示文本过滤                                  |
| `media`   | string | 否 | `1`/`true` 触发该页媒体导出，填充 `mediaFileName`/`mediaUrl`/`mediaLocalPath`，并让 `media.exportPath` **出现**；不带它时 `exportPath` **省略**（不是空串），媒体元数据仍随消息返回 |
| `image` / `voice` / `video` / `emoji` | string | 否 | 媒体导出子开关，默认开启，`0`/`false` 关闭 |

> **迁移**（三个旧参数行为已删除）：
> - `chatlab=1` 与 `format=chatlab`：老面**不再输出** ChatLab 形状，改用 `GET /chatlab/messages`；
> - 拼音别名 `meiti` / `tupian` / `vioce`：**静默忽略**（未知参数不报错，所以它们不再有任何作用）；
> - POST：读端点只留 GET，POST 一律 **405**（body 既不是参数通道，也不是鉴权通道）。

### 示例

```bash
curl "http://127.0.0.1:5032/api/v1/messages?talker=10001&limit=20&access_token=YOUR_TOKEN"
curl "http://127.0.0.1:5032/chatlab/messages?talker=10001&limit=20&access_token=YOUR_TOKEN"
curl "http://127.0.0.1:5032/api/v1/messages?talker=u_abc123&start=20260101&end=20260131&access_token=YOUR_TOKEN"
```

### JSON 响应字段

> v1 说明：`talker` 对应的会话不存在时不报错，返回 `success=true`、`messages=[]`、`count=0`（与 §4.1 的 404 行为不同）。

顶层字段：`success`、`talker`、`count`、`hasMore`、`media.enabled`、`media.count`、`messages`，
以及**仅在 `media=1` 时出现**的 `media.exportPath`。

单条消息字段（按时间倒序，最新在前）：

| 字段 | 说明 |
| ---- | ---- |
| `localId` | 本地 rowid（消息唯一标识，数字） |
| `serverId` | 消息 seq（字符串） |
| `localType` | 消息类型码（见下表） |
| `createTime` | 秒级 Unix 时间戳（优先 `40050` 列，缺列时回退 seq 高位） |
| `isSend` | 方向：`1`=本人发送，`0`=他人/系统（来自 `40013` 列；QQ 版本缺列或值非 1/2 时恒 `0`） |
| `senderUsername` | 发送者 UID（稳定标识，客户端据此去重） |
| `senderName` | 发送者显示名，按**本会话**解析：群聊为本群群名片（`40090`）> 备注（`20009`）> 最新昵称（`40093`）> 档案昵称（`20002`）> UID；私聊无群名片，从备注起算。同一 UID 在不同会话可得不同显示名（群名片只在本群生效，不会外泄到私聊或联系人列表）。回退链末端是 UID 本身，故 `senderUsername` 非空时该字段必非空；系统消息等无发送者的行 `senderUsername` 为空，此字段同为空串（真实账号抽样 2568 条中 369 条属此类）。**有此字段后，客户端无需为显示名再调用 `/api/v1/contacts` 与 `/api/v1/group-members`** |
| `content` / `rawContent` / `parsedContent` | v1 三者相同，为解析后文本 |
| `type` | 仅图片/语音/视频消息：`image` / `voice` / `video`（该键曾叫 `mediaType`，与媒体对象内部的名字空间无关） |
| `media` | 仅媒体消息：元数据对象（`uuid`/`md5`/`fileName`/`size`/`width`/`height`/`localPath`/`urls`，均为可选字段，缺失即省略） |
| `mediaId` | 媒体获取键（md5 hex 或 uuid，统一小写），用于 `GET /api/v1/media/{id}`；**仅当索引注册了本地缓存路径（`media.localPath` 非空）时提供**——否则省略（`media` 对象仍在，但该键无法取到字节，承诺了就是必 404） |
| `mediaFileName` / `mediaUrl` / `mediaLocalPath` | 仅 `media=1` **确实写出本地副本**后出现：导出文件名、**根相对路径** `/api/v1/media/{导出文件名}`（单段形式；调用方按自己的基址拼接，取字节带鉴权头）、导出目录绝对路径 |

消息类型码：

| 类型 | 码 |
| ---- | -- |
| 文本 | 0 |
| 其他 | 1 |
| 图片 | 3 |
| 语音 | 4 |
| 视频 | 5 |
| 撤回 | 6 |
| 系统 | 7 |

> v1 差异：有 `replyToMessageId`（**仅在目标唯一时输出**，见 §4.1 末的判据），无 `quote` 字段；媒体通过 `media` 对象 + `mediaId` 提供，字节经 `GET /api/v1/media/{id}` 获取。

**示例响应**

```json
{
  "success": true,
  "talker": "10001",
  "count": 2,
  "hasMore": true,
  "media": { "enabled": true, "count": 1 },
  "messages": [
    {
      "localId": 1234567890123,
      "serverId": "1234567890123",
      "localType": 3,
      "createTime": 1782864000,
      "isSend": 0,
      "senderUsername": "u_a",
      "senderName": "张三（群名片）",
      "content": "[image]",
      "rawContent": "[image]",
      "parsedContent": "[image]",
      "type": "image",
      "media": {
        "uuid": "R020-...",
        "md5": "9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d",
        "fileName": "9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d.png",
        "size": 44540,
        "width": 507,
        "height": 307,
        "localPath": "C:\\SomeUser\\nt_qq\\nt_data\\Pic\\2026-08\\Ori\\9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d.png"
      },
      "mediaId": "9f2a1c2d3e4f5a6b7c8d9e0f1a2b3c4d"
    },
    {
      "localId": 1234567890199,
      "serverId": "1234567890199",
      "localType": 0,
      "createTime": 1782863900,
      "isSend": 0,
      "senderUsername": "u_b",
      "senderName": "李四",
      "content": "你好",
      "rawContent": "你好",
      "parsedContent": "你好"
    }
  ]
}
```

### ChatLab 消息面（GET /chatlab/messages）

原 `/api/v1/messages?chatlab=1` 的**新家**。它天生就是 ChatLab 形状 —— 调用方不必知道还有
另一种，也不会因为漏传一个开关而拿到另一种。

**请求**：`talker`（必填）、`limit`（默认 100、上限 10000）、`offset`、`cursor`
（`page.nextCursor` 的回传入参；解析失败退回 `offset`）、`start`、`end`、`keyword`、
`media` 与四个分类型子开关（语义与 §3 相同）。鉴权与就绪门控与原生面一致。

**信封**（**不带 `success`** —— 它输出的是数据信封，而 `success` 是「操作结果」的语言）：

```json
{
  "talker": "10001",
  "count": 2,
  "page": { "hasMore": true, "nextCursor": "2" },
  "chatlab": { "version": "0.0.2", "exportedAt": 1782864000, "generator": "qqflow-server" },
  "meta": { "groupId": "10001", "name": "项目群", "ownerId": "10001", "platform": "qq", "type": "group" },
  "members": [ … ],
  "messages": [ … ]
}
```

四条容易踩的口径：

1. `count` ＝**本页条数**（不是总数；总数不在这个面上表达，分页信息一律走 `page`）；
2. 消息**升序**（原生面是降序）；
3. `media=1` **真正执行导出**（收集任务 → 把确实落盘的那些回填成可取句柄）。`media` 是
   **元数据**：`{type, fileName, md5}`，无媒体时**整个键省略**、`md5` 取不到则省略该键。
   `fileName` 只有在**确实写出了本地副本、且名字由内容摘要派生**时才是可取句柄；
4. `members` 仍是**本页出现的发送者**（去重），**不是**名册 —— 名册只并进
   `/api/v1/group-members`。

字段语义与 §4.1 ChatLab Pull 完全一致，注意两点：

- `accountName`（账号名：备注 > 昵称 > UID）与 `groupNickname`（本群群名片）
  是**两个不同的字段**，不是同一个值的别名。上面 §3 的 `senderName` 是二者的
  合并值（群名片优先），语义不同，不受影响。
- `messages[].type` 是 ChatLab 0.0.2 标准枚举，与本节 `localType` 的**平台原生
  编码是两套独立体系**（同一张图片：`type: 1` vs `localType: 3`）。枚举全表和
  "码 6 未分配"的说明见 §4.1。

`meta.type` 按标准只有 `group`/`private` 两个取值，公众号/订阅号等会归入
`private`。需要更细的会话分类请用 §4 `/api/v1/sessions` 的 `type`。

不输出的可选字段（`meta.groupAvatar`、`members[].aliases`、`messages[].mediaPath`）与本面
同样适用，清单见 §4.1"与标准 / 安装版的已知差异"。`messages[].replyToMessageId` 在
**原生面、消息面、拉取面三个面上同规**：无引用时**省略该键**（不是给 `null`）。

---

## 3.1 获取媒体文件（GET /api/v1/media/{id}）

**请求**

```
GET /api/v1/media/{id}?access_token=YOUR_TOKEN
```

| 参数 | 类型 | 必填 | 说明 |
| ---- | ---- | ---- | ---- |
| `id` | string | 是 | **store 键**（消息里的 `mediaId`，md5 hex 或 uuid）或**导出文件名**（`media=1` 写进导出根的名字） |
| `access_token` | string | 是 | 鉴权（或 `Authorization: Bearer`） |

**响应**：媒体文件字节流，`content-type` 按扩展名（`image/jpeg` / `image/png` / `image/gif` / `image/webp` / `video/mp4` / `audio/wav` / `audio/amr` / `audio/silk` 等），携带 `content-length`。

**说明**：`id` 有**两个来源**，按顺序解析：

1. **store 键**：索引登记过的本地缓存路径（消息 `media.localPath`，即 `45812` 字段）。命中即服务，
   这是 `mediaId` 承诺「出现即可取」的落点；键命中但文件已被 QQ 清理 ⇒ `404`。
2. **导出文件名**：在导出根下按名解析，布局 `<exportPath>/<talker>/<images|voices|videos|emojis>/<file>`。
   只有这四个类型目录参与解析（别的目录里就算躺着同名文件也不服务）；路径段按统一规则拒绝
   尾点、尾空格与冒号，规范化后必须仍落在导出根内。

**同名多命中**（规则与 weflow-server 一致）：候选**内容一致** ⇒ 取排序后的第一个；
**不一致** ⇒ `404`（同名内容冲突）。比较先比 size 短路，只有 size 相同才逐字节读整份文件 ——
读放大 ＝ 候选数 × 文件大小，是线性成本。这条规则是「出现即可取」的前提：按名解析是**跨会话**的，
别的会话里可能躺着同名但内容不同的文件。

**未命中的措辞统一为「媒体不存在」**（含缓存被清理的情形）——「文件被清理」与「没这个名字」
对调用方是同一种失败，两种措辞只会让它多写一条分支。

> **迁移**：三段式 `/api/v1/media/{talker}/{mediaType}/{file}` 已删除，改用本路由。

## 3.2 媒体导出（WeFlow 形状，`media=1`）

`GET /api/v1/messages?media=1`（`/chatlab/messages` 同样支持）把**本页**媒体导出到导出根目录（`--media-export-dir`，默认 `<data-dir>/api-media`），布局 `<exportPath>/<talker>/<images|voices|videos>/<file>`，并：

- 每条**确实写出本地副本**的媒体消息填充 `mediaFileName` / `mediaUrl`（**根相对路径**
  `/api/v1/media/{导出文件名}`，不含 token）/ `mediaLocalPath`（绝对路径）
- envelope 多出 `"exportPath": "<导出根>"`（未请求导出时该键**省略**，不是空串），
  `count` 是本次成功导出的条数
- 子开关 `image` / `voice` / `video` / `emoji` 默认开启，`0`/`false` 关闭对应类型导出
- 导出在阻塞线程池执行（`spawn_blocking`），不阻塞 HTTP worker（并发导出/SSE 互不影响）
- 幂等：目标已存在且同尺寸即跳过（不重写、mtime 保留）——目标文件名由内容键（md5 hex / uuid，小写）派生，同键必同内容，不会出现"同名不同字节"互相覆盖/错指

**获取导出文件**：`GET /api/v1/media/{导出文件名}?access_token=YOUR_TOKEN`（见 §3.1 的按名解析规则：
只有四个类型目录参与、同名异内容 404、路径穿越拒绝）。注意：链接仅在 `media=1` 导出过对应消息后
可访问（WeFlow 同语义）。

> **句柄只对「内容摘要派生」的名字给出**：导出名是 `<内容键>.<扩展名>`（md5 hex / uuid）时它
> 同时是 ChatLab 面 `media.fileName` 的可用句柄；平台给的原文件名回落**只作元数据**，
> 因为按名解析是跨会话的、同名可能是别的文件。

**与 WeFlow 的已知差异**（不可避免，文档化）：① `emoji` 开关接受但不产生导出（QQ 表情仅有显示文本，无文件；gif 图片走 `images/`）；② 语音导出**原始** `.silk`/`.amr` 文件（未转码为 wav）；③ `mediaUrl` 的 base 默认取 `http://{host}:{port}`（默认 127.0.0.1:5032，WeFlow 为 5031）——绑定 `0.0.0.0`/`::` 时该地址不可达，自动回退 `127.0.0.1` 并告警；局域网/容器部署请用 `--base-url http://<可达地址>:<port>` 显式指定；④ 导出根默认 `<data-dir>/api-media`（可 `--media-export-dir` 覆盖）；⑤ 文件名按内容键派生（`<md5|uuid>.<源扩展名>`，如 `9f2a1c...4d.png`）而非保留 QQ 原始 `fileName`——同名不同内容的文件因此永不冲突，跨页重复导出幂等（无键可派生时才保留 QQ 原始名）；⑥ 404 错误体为统一 envelope，而安装版 WeFlow 为 `{"error":"Media not found"}`（注：weflow-server 自其 0.5.0 起亦已统一为 envelope，此项差异仅对安装版 WeFlow 成立）。`mediaId` 直服（§3.1）与 `media` 元数据对象为 qqflow 扩展，导出后仍可用。

## 4. 获取会话列表

**请求**

```http
GET /api/v1/sessions
```

> 读端点只留 GET（POST 返回 405）；`/api/v1/sync` 与 `/health` 仍接受 POST。

### 参数

| 参数      | 类型   | 必填 | 说明                             |
| --------- | ------ | ---- | -------------------------------- |
| `keyword` | string | 否   | 匹配 `username` 或 `displayName` |
| `limit`   | number | 否   | 默认 `100`，范围 `1~10000`       |
| `offset`  | number | 否   | 分页偏移，默认 `0`               |
| `cursor`  | string | 否   | 翻页游标：把上一次响应的 `page.nextCursor` 原样传回。解析不了就退回 `offset`；两者做同一件事（本面按偏移翻页），`cursor` 只是免去调用方自己算下一个偏移 |
| `format`  | string | 否   | `chatlab` 时输出 ChatLab 格式    |

### 响应字段（按最后消息时间倒序）

- `success`
- `count`
- `sessions[].username`
- `sessions[].displayName`（群聊：群备注 > 改名消息群名 > 群信息库群名 > 群号；私聊：备注 > 对方昵称（会话名） > 档案昵称 > UID）
- `sessions[].type`（`2`=群聊，`1`=私聊）
- `sessions[].lastTimestamp`
- `sessions[].unreadCount`（v1 恒为 `0`）

**示例响应**

```json
{
  "success": true,
  "count": 2,
  "sessions": [
    { "username": "10001", "displayName": "项目群", "type": 2, "lastTimestamp": 1782864000, "unreadCount": 0 },
    { "username": "u_abc123", "displayName": "张三", "type": 1, "lastTimestamp": 1803700000, "unreadCount": 0 }
  ]
}
```

### ChatLab 形状（`GET /chatlab/sessions`）

`/api/v1/sessions` 只输出原生形状；ChatLab 形状走这个面（详见 §4 的 ChatLab 发现面一节）。

```json
{
  "sessions": [
    { "id": "10001", "name": "项目群", "platform": "qq", "type": "group", "messageCount": 128, "lastMessageAt": 1782864000 }
  ],
  "count": 1,
  "page": { "hasMore": true, "nextCursor": "1" }
}
```

`platform` 固定 `"qq"`；`messageCount` 是**索引里的真实条数**。`count` 是**本页条数**。
老面**不再识别** `cursor`：继续传不会报错，而是静默回到第一页。

**`page` 块是必需的。** ChatLab 把「没有 `page` 块」的响应读作「这就是完整一页」，
所以不带 `page` 的截断会被下游当成全量——默认 `limit` 是 100，普通账号就能撞上，
表现是「第 101 个会话凭空消失」且不报错。`hasMore` 为假时 `nextCursor` 为 `null`；
把 `nextCursor` 原样回传即可继续翻页。

---

## 4.1 拉取会话消息（ChatLab Pull）

返回 ChatLab 标准格式的聊天数据，支持增量拉取和分页。**仅 GET**。

**请求**

```http
GET /api/v1/sessions/{id}/messages
```

### 参数

| 参数     | 类型   | 必填 | 说明                                     |
| -------- | ------ | ---- | ---------------------------------------- |
| `:id`    | string | 是   | 会话 ID（Path 参数）                     |
| `since`  | string | 否   | 秒级时间戳或 `YYYYMMDD`，仅返回该时间之后（**不含**）的消息；同一秒的消息会在同一页内完整返回 |
| `end`    | string | 否   | 秒级时间戳或 `YYYYMMDD`，时间上界        |
| `limit`  | number | 否   | 单次返回上限，默认且最大 `5000`          |
| `offset` | number | 否   | 分页偏移，默认 `0`                       |

会话不存在时返回 `404`（错误信封，见 §8）。

`limit`/`offset` 容错解析：WeFlow 的 Pull 契约对分页参数没有 400 语义，因此
`?limit=abc`、`?limit=0`、`?offset=` 等一律退化为默认值，不报错。

### 响应

**名字是两个字段，不是一个。** `accountName` = 账号自己的名字（备注 `20009` >
昵称 > UID），`groupNickname` = 本群群名片（`40090`），群聊无名片或私聊时为空
串。二者不同源、可以不同值，客户端要显示"群里的称呼"取 `groupNickname` 并回落
到 `accountName`。

§3 `/api/v1/messages` 的 `senderName` 是**另一回事**：它是两者的合并值（群聊
名片优先），下游已依赖，不随本节改变。

```json
{
  "chatlab": {
    "version": "0.0.2",
    "exportedAt": 1738713600,
    "generator": "qqflow-server"
  },
  "meta": {
    "name": "项目群",
    "platform": "qq",
    "type": "group",
    "groupId": "10001",
    "ownerId": "10000"
  },
  "members": [
    { "platformId": "u_a", "accountName": "张三", "groupNickname": "张三群名片", "avatar": "" }
  ],
  "messages": [
    { "sender": "u_a", "accountName": "张三", "groupNickname": "张三群名片", "timestamp": 1738713600, "type": 0, "content": "你好", "platformMessageId": "123456" }
  ],
  "sync": {
    "hasMore": true,
    "nextSince": 1738713600,
    "nextOffset": 0,
    "watermark": 1738714000
  }
}
```

`meta.ownerId` = 当前绑定账号的 QQ 号；未绑定时为空串。
`members` 仅含**本页**出现过的发送者，且已去重。

### messages[].type

采用 **ChatLab 0.0.2 标准枚举**（`docs.chatlab.fun/standard/chatlab-format`），
不是 QQ 原生编号：

| 码 | 含义 | | 码 | 含义 |
| -- | ---- |-| -- | ---- |
| 0 | TEXT | | 24 | SHARE |
| 1 | IMAGE | | 25 | REPLY |
| 2 | VOICE | | 27 | CONTACT |
| 3 | VIDEO | | 80 | SYSTEM |
| 4 | FILE | | 81 | RECALL |
| 5 | EMOJI | | 99 | OTHER |
| 7 | LINK | | | |
| 8 | LOCATION | | | |

**`6` 在标准中未分配，任何情况下都不会出现。**

**本项目实际只产出 `0` / `1` / `2` / `3` / `80` / `81` / `99` 七个码。** 上表是标准
全集，其余码位（`4` FILE、`5` EMOJI、`7` LINK、`8` LOCATION、`24` SHARE、`25`
REPLY、`27` CONTACT）在 QQ 侧没有对应的**类型码**解析：没有名片 / 位置 / 链接的细分识别，
这些消息统一落到 `99` OTHER。（**引用关系本身是抽得出的**——见 §4.1 末的
`replyToMessageId`；类型码与引用关系是两件事，不要因为其中一个没做就以为另一个也没有。）

⚠️ **与 weflow-server 的覆盖面不对等。** weflow-server 能输出
`0/1/2/3/4/5/7/8/24/25/27/80/81/99`。同一个逻辑消息在两个平台上可能一边是 `25`
REPLY、另一边是 `99` OTHER。下游做类型分支时应把未覆盖码按 `99` 兜底，不要假设两个
上游的枚举分布一致。

⚠️ 这与 §3 `/api/v1/messages` 的 `localType` 是**两套独立编码**：同一张图片在
本节是 `type: 1`，在 §3 是 `localType: 3`。`localType` 是平台原生码、下游已按
它分支，两者不可互换使用。

### sync 块

| 字段         | 说明 |
| ------------ | ---- |
| `hasMore`    | 是否还有更多数据 |
| `nextSince`  | 有更多时 = 本页最后一条消息时间；否则 = `watermark` |
| `nextOffset` | 正常恒为 `0`，仅在时间戳无法推进的退化场景下才为累计偏移 |
| `watermark`  | 本次拉取的时间上界（未传 `end` 时为当前时间） |

`nextOffset` **有意不同于 WeFlow（安装版）文档里的 `5000`**：`nextSince` 是
排他上界且分页永不切断同一秒，客户端把两个游标原样回传时，`nextSince` 已经过
滤掉本页所有行、下一条未读行正好落在 offset 0。此时再叠加一个累计 offset 会
二次跳过同样的行（double-skip）。两个游标照文档原样回传即可，不需要客户端做
特殊处理。

### 与标准 / 安装版的已知差异

以下是有意不实现、或按本仓数据条件取舍的部分。本仓**没有「键恒出现、值为 `null`」的响应字段**
（唯一的例外是分页游标，见下节）——可选字段一律**省略键**：

| 字段 | 标准 | 安装版 | 本项目 | 原因 |
| ---- | ---- | ------ | ------ | ---- |
| `meta.groupId` | 群 ID（**仅群聊**） | 字段清单里有 | **私聊也输出**，值等于会话 id | 省略键、给空串、给会话 id 是三种不同的契约；改用「群聊时 `groupId` 等于路径 id」限定语义（契约套件的 `meta_groupId_matches_id`） |
| `meta.groupAvatar` | 可选；CN 的「头像格式说明」接受 Data URL 与网络 URL 两种，EN 字段表只写 Data URL | 字段清单里有 | **不输出** | QQ 侧没有可用的群头像来源 |
| `members[].aliases` | 可选，`string[]` | 未列出 | **不输出** | 多来源名字已收敛进 `accountName`（备注 > `uid_names` > 昵称 > UID） |
| `members[].avatar` | 可选；**CN 的「头像格式说明」明确接受网络 URL**，EN 字段表只写 Data URL | 真实 URL | **恒为空串** | QQ 侧没有可用的头像来源；字段保留以满足形状。空串而非省略键，是为了让 `members[]` 的键集恒定 |
| `messages[].mediaPath` | **规范里没有这个字段**（EN / CN 字段表都没有） | 字段清单里有 | **两个面都不输出** | 媒体字节请走 §3 `/api/v1/messages` 的媒体导出 |
| `members[].roles` | 可选，`[{id}]`（CN 表列出，EN 表未列） | 未列出 | **不输出** | **有意不做**，不是遗漏：它与 `members[].isOwner` 是同一件事的两种表达，而 `isOwner` 已经在输出（源 `group_info.db::group_detail_info_ver1.[60002]`）；且两者受同一个限制——群主不在本页时都无从判断 |
| `messages[].replyToMessageId` | 不在标准（WeFlow 私有扩展） | 仅 ChatLab 面有 | **目标唯一时输出；否则省略该键**（原生面、消息面、拉取面**同规**） | 键名与语义对齐 WeFlow；判据见下 |

**`replyToMessageId` 的判据（为什么有时不给）**：它取自表列 `40850`（被回复消息的**会话内序号**），
再在同一会话里找 `40003` 等于它的那一行，输出**那一行的 `40001`**（也就是 `platformMessageId`）。

问题在于 `(会话, 40003)` **并不唯一**——实测某真实库 31,820 行群消息里重复了 **1371 组**，而
文档只说了「可跨群复用」，漏了「**群内也复用**」。直接查会随机命中一行，于是客户端把引用挂到
**错的消息**上，而它无法分辨。因此实现只在**恰好一个候选**（同一 `40003` 且不晚于本条的时间）
时输出，否则**省略该键**：实测覆盖 **1522/1616 = 94.2%** 的回复，其余 5.8% 的表现是
「看不到引用」，而不是「看到错的引用」。

> 列 `40900`（引用场景的消息快照）经实测**不含**目标身份——四种编码 × 两列 × 发送者维度都试过，
> 不能用来消歧。上游文档在这一点上与本库形态不同，**以本库实测为准**。

### 字段无值时怎么表示：省略键 / `null` / 空串

同一个响应里会出现三种「没有值」，它们是**三种不同的契约**：**省略键**＝这个对象不存在或本次没请求；
**`null`**＝有这个概念、此刻没有值；**空串**＝类型上恒为字符串的字段没有内容。本仓的可选字段
**一律省略键**，唯一的 `null` 是分页游标（那里「已排空」本身是有意义的状态）。

| 面 | 字段 | 表示 | 说明 |
| --- | --- | --- | --- |
| `GET /api/v1/messages` | `messages[].media`、`mediaId`、`mediaFileName`、`mediaUrl`、`mediaLocalPath`、`replyToMessageId` | 无值时**省略键** | 本仓消息面没有「键恒出现、值为 `null`」的字段 |
| `GET /api/v1/messages` | `media.exportPath` | **只在真正执行了导出时出现** | 未请求 `media=1` 时省略该键——「没导出」与「导出到空路径」是两件事（回归见 `messages_without_media_param_omits_export_path`） |
| `GET /api/v1/accounts` | `accounts[].dbPath`、`accounts[].error` | 无值时**省略键** | |
| `POST /api/v1/accounts` | `dbPath`、`status` | 无值时**省略键** | 注册的三个分支键集不同，见该端点小节 |
| `GET /chatlab/sessions` | `sessions[].memberCount` | 不掌握名册时**省略键** | 可选字段，断言不得写成必填 |
| `GET /chatlab/sessions`、`GET /chatlab/messages` | `page.nextCursor` | 键恒出现，已排空时 `null` | 与「整个 `page` 块不存在」（＝完整单页）是两件事 |
| `GET /chatlab/messages`、拉取面（两条路径同形） | `messages[].media`、`replyToMessageId` | 无值时**省略键** | `media.md5` 取不到摘要时也省略 |
| 拉取面 | `page` | **不出现在响应里** | 进度走 `sync` 块（`hasMore` / `nextSince` / `nextOffset` / `watermark`），四个键恒出现 |
| `GET /api/v1/group-members` | `members[].messageCount` | **只在 `includeMessageCounts=1` 时出现** | 未请求计数时省略，而不是给 `0`（见第 6 节） |
| `GET /api/v1/group-members` | `members[].isOwner` | 键**恒保留**（值为 `false` 时也出现） | 与 `messageCount` 相反：它表达的是「判定过，不是群主」 |
| SSE `message.new` | `groupName`、`avatarUrl`、`sourceName`、`media`、`mediaId` | 都是**条件键**：没有就不出现 | 不是给 `null`（回归见 `message_new_text_payload_keys_are_pinned`、`message_new_media_payload_carries_no_local_path`）。其中 `avatarUrl` 目前**恒不出现**：QQ 侧没有头像来源，构造点一律传 `None`；它留在类型里，是为了将来源可用时不必改形状 |
| SSE `message.revoke` | `groupName`、`avatarUrl`、`sourceName` | 条件键（`avatarUrl` 同上，恒不出现） | 撤销事件**没有** `media`（回归见 `message_revoke_payload_has_no_media`） |
| SSE `sync` | `event`、`generation`、`watermarks` | 三个键恒出现 | 某张表没有水位时，是**数组里少一项**，而不是给 `null` 或 `0`（回归见 `sync_omits_tables_without_a_watermark`） |
| 通知面 `/chatlab/push/messages` | `eventId`、`sessionId`、`platformMessageId`、`generation` | 都是**条件键** | 基线事件（`session.sync` 一类）带 `generation`、不带 `eventId` / `sessionId`；消息事件反之。`sessionId` 为空串时**显式映射成省略**——空串与「没有会话」在下游是两件事 |

**空串是另一套约定**：`group-members` 的 `alias` / `avatarUrl`（v1 恒为空串）、`remark`（未设置时
为空串），以及 ChatLab `members[].avatar`（恒为空串），在没有该值时给空串而不是 `null`；
名册-only 成员的 `displayName` 回落 UID 而不是空串。判据是这些字段在类型上恒为字符串。

**跨仓的允许差异**（两仓都成立，但取值不同，按一端写判空逻辑会出错）：

| 项 | 本仓 | weflow |
| --- | --- | --- |
| 未请求导出时的 `media.exportPath` | 省略键 | 键恒出现（值是导出根） |
| SSE `message.new` 的 `groupName` / `media` | 条件键（没有就不出现） | 键恒出现，无值时 `null` |
| 通知帧 `platformMessageId` | 恒省略 | 键恒出现：`message.new` 为 `null`，`message.revoke` 为平台号 |
| 未请求计数时的 `members[].messageCount` | 省略键 | 键恒出现，值为 `0`（见第 6 节） |

---

## 5. 获取联系人列表

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

v1 联系人来源：消息中出现过的 UID ∪ 档案/映射表中出现的 UID（无聊天记录的 UID 也会出现）。昵称来自联系人档案（`profile_info.db` 的 `20002`，经 ground-truth 探针确认），QQ 号来自 `nt_msg.db` 的 `nt_uid_mapping_table` + 档案（版本相关，列结构按值探测，缺表/缺列时退化为消息昵称列表）。备注（remark）：来自 `profile_info.db` 的 `20009` 列（QQDecrypt 字段 id，真库 ground-truth 确认——本账号实测 17 个联系人设置备注）；分类按字段 id `20009` 直接识别（绕过 CJK 门槛，兼容拉丁备注），列名/字段 id 提示（`remark`/`20003`/`60026`）保留作兜底。

**请求**

```http
GET /api/v1/contacts
```

### 参数

| 参数      | 类型   | 必填 | 说明                          |
| --------- | ------ | ---- | ----------------------------- |
| `keyword` | string | 否   | 匹配 `username` 或 `nickname` |
| `limit`   | number | 否   | 默认 `100`，范围 `1~10000`    |
| `offset`  | number | 否   | 分页偏移，默认 `0`            |

### 响应字段（按 `(displayName, username)` 排序）

> 仅为「显示消息发送者名字」而调用本接口已无必要：§3 的每条消息自带
> `senderName`，且它按会话解析、含本群群名片，比本接口的 `displayName`（全局，
> 无群名片）更准。本接口留给需要**完整通讯录**的场景（例如列出无聊天记录的联系人、
> 读取 `alias`/QQ 号）。

**必须翻页**：`limit` 默认 100，不传就只拿到前 100 条，静默丢掉其余联系人。
按 `offset` 递增直到 `hasMore=false`：

- `total` 为过滤后总数，与 `offset` 无关；`count` 是本页条数；
- 排序加了 `username` 次键——显示名不唯一，仅按显示名排序时并列项在多次请求间
  顺序不定，offset 翻页会漏行/重复行；
- `offset` 超出末尾返回空页且 `hasMore=false`。

- `success`
- `count`（本页条数）
- `total`（过滤后总数）
- `hasMore`（`offset + count < total`）
- `contacts[].username`（UID）
- `contacts[].displayName`（备注 > 消息昵称 > 档案昵称 > UID）
- `contacts[].nickname`（档案昵称 > 消息昵称；均无时为空串）
- `contacts[].remark`（来自 `profile_info.db` 的 `20009`；未设置备注的联系人为空串）
- `contacts[].alias`（WeFlow 的微信号槽位；QQ 场景存放该联系人 **QQ 号**——uid 映射表/profile 可读时返回，无数据源为空串；原 `qq` 字段已迁移至此）
- `contacts[].avatarUrl`（v1 恒为空串）
- `contacts[].type`（v1 恒为 `"friend"`）

---

## 6. 获取群成员列表

v1 成员来源：**名册 ∪ 发言人** —— `group_info.db::group_member3` 的群名册与索引里该群的发言者
取并集。只列发言人会让「群里有谁」的答案取决于谁最近说过话，潜水成员永远不出现，而他们恰恰是
「这个群还有谁」的主要部分。代价是出现 `messageCount` 为 0 的成员，这是接受的。

> 同 §5：只为显示发送者名字不必调用本接口，§3 每条消息的 `senderName` 已是同一条
> 解析链路（含本群群名片）。本接口留给需要成员名单本身的场景（发言数统计等）。

**请求**

```http
GET /api/v1/group-members
```

### 参数

| 参数                   | 类型   | 必填 | 说明                          |
| ---------------------- | ------ | ---- | ----------------------------- |
| `chatroomId`           | string | 是   | 群 ID（`talker` 别名已删除） |
| `includeMessageCounts` | string | 否   | `1/true` 时附带成员发言数（`withCounts` 别名已删除） |

> **迁移**：`talker` 与 `withCounts` 别名、`forceRefresh` 占位参数都已删除；
> 它们被**静默忽略**（未知参数不报错）。同步统一走 `POST /api/v1/sync`。

**群不存在时返回 404**；名册有而消息无时**返回成员**（不再 404 —— 那时名册恰好是唯一能回答
「群里有谁」的来源）。成员按 `messageCount` 降序、`wxid` 升序**稳定排序**：只按计数排时，
一大批计数为 0 的潜水成员顺序会随哈希遍历顺序抖动，下游按它做 diff 会看到满屏假变化。

### 响应字段

- `success`
- `chatroomId`
- `count`
- `fromCache`（v1 恒为 `false`）
- `updatedAt`（毫秒时间戳，取值是**本次响应的生成时刻**）。**它不表示索引新鲜度** ——
  本仓没有记录索引构建时刻；weflow 的同名字段是索引构建完成时刻，两仓含义不同（允许差异）。
- `members[].wxid`（发送者 UID）
- `members[].displayName`（该群消息内首见昵称；**名册-only 成员回落 UID**，而不是空串）；
  `members[].nickname`（消息里带的昵称）；`members[].groupNickname`（本群群名片（40090）> 备注 > 最新昵称 > 档案昵称 > UID）
- `members[].remark`（来自 `profile_info.db` 的 `20009`；未设置备注的成员为空串）
- `members[].alias` / `members[].avatarUrl`（v1 恒为空串）
- `members[].isOwner`（群主标记：由 `group_info.db::group_detail_info_ver1.[60002]` 解析——
  **每群恰一个 `true`**；群主不在本页、或缺 `group_info.db`/该表时全为 `false`，键恒保留）
- `members[].isFriend`（v1 恒为 `false`）
- `members[].messageCount`（仅 `includeMessageCounts=1` 时返回）。**未请求计数时这个键不出现**
  （不是给 `0`）—— weflow 在同名参数下给 `0`：`0` 是「统计过，这个成员没发过言」，而缺键是
  「没统计」。两仓取值不同是**允许差异**，写在这里以免调用方按另一端写判空逻辑。

---

## 7. 手动同步

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

立即对所有账号执行一次完整同步（直连活库的增量读取，**绕过后台变化检测循环**），
返回本次新增的**条数**。客户端初始化或手动刷新时调用；新增消息同时广播给 SSE
订阅端，也可随后用 §3 `/api/v1/messages` 读回。

**这是一个触发器，不返回消息体**。消息的唯一读取面是 §3 / §4.1，避免同一批数据
出现第二种形状。响应结构与 weflow-server 的 `/api/v1/sync` 完全一致。

**请求**

```http
POST /api/v1/sync
```

### 参数

无（除鉴权）。`limit` 等分页参数会被接受但忽略。

### 响应字段

- `success`
- `newMessages`（本次新增的普通消息条数）
- `revokeMessages`（本次新增的撤回消息条数，不计入 `newMessages`）

**示例响应**

```json
{
  "success": true,
  "newMessages": 3,
  "revokeMessages": 0
}
```

> 说明：账号注册后索引已全量构建，之后无新消息时两个计数均为 `0`；QQ 运行中产生
> 新消息后调用可立即同步。WeFlow（安装版）没有这个接口，因此本接口没有可对齐的
> 上游契约，形状与 weflow-server 对齐。

---

## 8. 错误响应

除健康检查与未知路径外，所有错误使用统一信封：

```json
{ "success": false, "code": 400, "message": "缺少必填参数 talker" }
```

| HTTP 状态码 | 场景 |
| ----------- | ---- |
| `400` | 缺少必填参数、Body 参数类型无效（报错 `body 参数无效`） |
| `401` | 未携带有效 Token |
| `404` | 会话/群不存在；**未知路径同样走信封**（不再是框架默认的空响应体） |
| `405` | 路径存在但方法不对（同样走信封） |
| `503` | 索引构建中（"服务正在建立索引，请稍后重试"） |
| `500` | 内部错误 |

> 说明：非 JSON 的 POST Body 会被忽略（仅记录日志），请求沿用 Query 参数，不会报 400；`start`/`end` 无法解析时该过滤条件被忽略。
>
> Query 数值参数类型错误（如 `limit=abc`）返回 **400 统一信封**。框架默认给的是**纯文本空响应体**，于是客户端只能靠状态码特判——而「参数错了」恰恰是它能自己修的那一类，不该与「传输返回了无法解析的东西」长得一样。适用于 `/api/v1/messages`、`/api/v1/sessions`、`/api/v1/contacts`。**例外是 ChatLab 拉取** `/api/v1/sessions/{id}/messages`：该接口的 `limit` / `offset` 为容错解析，非法值退化为默认值而不报错（见 §4.1），因为 WeFlow 的 Pull 契约对分页参数没有 400 语义。

---

## 9. 使用示例

### cURL

```bash
TOKEN=$(Get-Content "$env:LOCALAPPDATA\qqflow-server\系统凭据库（--show-token 获取）")   # PowerShell
# 注册账号（客户端驱动启动；密钥仅内存保存）
curl -X POST http://127.0.0.1:5032/api/v1/accounts \
  -H "Content-Type: application/json" \
  -d "{\"qq\": \"1234567890\", \"key\": \"<16字节密钥>\", \"db_path\": \"C:\\\\Users\\\\<用户名>\\\\Documents\\\\Tencent Files\", \"access_token\": \"$TOKEN\"}"
# 账号明细（需鉴权；/health 只给标量 account 阶段）
curl -H "Authorization: Bearer $TOKEN" http://127.0.0.1:5032/api/v1/accounts
# 注销账号（恢复未注册状态；加 purge_media=1 才删导出媒体）
curl -X DELETE -H "Authorization: Bearer $TOKEN" \
  "http://127.0.0.1:5032/api/v1/accounts/1234567890?purge_media=1"
# GET 带 Token Header
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:5032/api/v1/messages?talker=10001&limit=20"
# POST 带 JSON Body（参数走 Body，token 亦可走 Body）
curl -X POST http://127.0.0.1:5032/api/v1/messages \
  -H "Content-Type: application/json" \
  -d "{\"access_token\": \"$TOKEN\", \"talker\": \"10001\", \"limit\": 50}"
# SSE
curl -N "http://127.0.0.1:5032/api/v1/push/messages?access_token=$TOKEN"
```

### Python

```python
import requests

BASE_URL = "http://127.0.0.1:5032"
headers = {"Authorization": "Bearer YOUR_TOKEN", "Content-Type": "application/json"}

messages = requests.post(
    f"{BASE_URL}/api/v1/messages",
    json={"talker": "10001", "limit": 50},
    headers=headers,
).json()

sessions = requests.get(f"{BASE_URL}/api/v1/sessions", params={"limit": 20}, headers=headers).json()
```

---

## 10. 注意事项

1. API 仅监听本机 `127.0.0.1`，不对外网开放（`host` 可在命令行参数中修改，需自行承担风险）。
2. `start` / `end` 支持 `YYYYMMDD` 与秒级时间戳；纯 `YYYYMMDD` 的 `end` 会扩展到当天 `23:59:59`。
3. 账号注册后全量构建索引（消息 → 内存），就绪前业务接口返回 `503`（SSE 接口与 `/api/v1/accounts` 除外）；构建耗时取决于库大小（真实库 2.8 万条约 2~5 秒）。注册后由文件系统事件驱动增量同步（防抖 `--watch-debounce-ms`，兜底 `--watch-fallback-ms`），也可用 `POST /api/v1/sync` 手动触发。
4. 会话 ID 判定：全数字 → 群聊；`u_` 前缀或含非数字字符 → 私聊。查询时若按此判定未命中会话，会再尝试另一种类型（支持全数字 UID 的私聊）。
5. 消息内容优先按 40800 结构化 wire 解码（文本取 `45101`、媒体取精确元数据），非结构化 blob 回退到启发式文本提取（QQ 消息体 schema 无稳定文档，QQ 升级可能导致解析退化）；媒体消息输出 `[image]` / `[voice]` / `[video]` 占位文本 + `media` 元数据对象。
6. 撤回消息 `localType=6`，content 保留原文（含"你猜猜撤回了什么"提示行）。
7. 媒体交付双通道：`/api/v1/media/{id}` 直服本地缓存（常开）；`media=1` 按需导出（§3.2，WeFlow 形状）。v1 未实现：朋友圈（SNS）、未读数（`unreadCount`）。
8. 端口：默认 `127.0.0.1:5032`（WeFlow 为 5031；`--port` 可改）——与 WeFlow 的差异为既定决策。
9. **单账号绑定**：内存索引没有账号维度，同时只能有一个账号处于 `indexing` / `ready` / `error`。第二个账号注册被拒（`account_conflict`，见 §1.1）而**不是覆写**；换账号必须先调 `DELETE /api/v1/accounts/{qq}`（§1.3）。`error` 不释放绑定，但同一 qq 可直接重试。
10. **注销不是锁**：它只是把服务恢复到未注册状态，持有 token 的客户端可以立刻重新注册。要真正阻止访问请轮换 token 或停止进程。
11. **密钥在内存中未做 `zeroize`**：`key` 仅存活于进程内存、不落盘，但注销/进程退出时不做显式擦除，因此仍可能残留在内存或崩溃转储中。威胁模型假定本机可信（服务默认只监听 `127.0.0.1`）。

## 类型化客户端（SDK）

本仓库提供两种语言的同构客户端：`clients/rust`（Rust）与 `clients/python`（Python，`qqflow-sdk`）。
两者分层一致：生成类型 + 手写行为层，行为方法的语义（503 是等待、游标原样回传、`Last-Event-ID` 重连、
404 后先导出再取）在两侧保持相同。

本仓库自带类型化 Rust 客户端：`clients/rust`（crate 名 `qqflow-client`，workspace 成员）。

- **类型与操作客户端是生成的**：出处是 `/openapi.json` 的描述（生成工具 `clients/regen`，
  `cargo run -p qqflow-regen` 重新生成；生成物入库，CI 断言「重生成无 diff」）。**不要手改**
  `clients/rust/src/generated/` 下的任何文件。
- **行为层是手写的**（`clients/rust/src/client.rs`）：下面这张表就是**公共面**——
  每个方法都有具名测试（Rust 在 `clients/rust/tests/behavior.rs`，Python 在
  `clients/python/tests/test_behavior.py`），没有测试的能力不进这张表。注册载荷是
  `qq`/`key`/`db_path`。

  | 方法 | 打哪个面 | 语义要点 |
  | --- | --- | --- |
  | `health()` | `GET /health` | **免鉴权**，只给标量阶段与版本；客户端不带凭据（有测试钉住） |
  | `accounts()` | `GET /api/v1/accounts` | 账号明细；`error` 与 `messageCount` 只在这里 |
  | `register(body)` | `POST /api/v1/accounts` | **非阻塞**，返回原始 `state`/`status`（`RegisterOutcome`）；拒绝态是**值**不是错误 |
  | `ensure_ready(account, body, timeout)` | 注册 ＋ 轮询 | `register` ＋ `wait_ready` 的组合；**200 的拒绝态立即失败**（`account_conflict`/`invalid_key`/`invalid_db_path`/`unknown_qq`），不等超时 |
  | `wait_ready(account, timeout)` | `GET /api/v1/accounts` | **只等待、不注册**（wait-only）；中间态是等待不是错误 |
  | `pull_page(talker, since, offset, limit)` | Pull 面 | **一页语义**：`since` 排他、`offset` 是同一时间组内的游标；两游标必须原样回传。`limit` 是单页上限（服务端封顶 5000），`None` 即服务端默认 |
  | `drain_session(talker, since, on_page)` | Pull 面 | **取尽语义**（内部逐页调到 `hasMore=false`，每页回调）；游标（`nextSince`/`nextOffset`）原样回传，按 (时间组, offset) 翻页 |
  | `list_messages(query)` | `GET /api/v1/messages` | **原生面，一页语义**：`offset` 进、`hasMore` 出；带 `rawContent`/`isSend`/`localType`，且只有它能 `media=1` 导出。时间界收 `YYYYMMDD` 或 unix 秒，客户端先校验 |
  | `chatlab_messages(talker, …)` | `GET /chatlab/messages` | **ChatLab 形状面，一页语义**：升序、ChatLab type 码、`media` 在消息上、`count`/`page` 翻页、**没有 `success`**；查询参数与 `list_messages` 同一套（含 `keyword`） |
  | `contacts(query)` | `GET /api/v1/contacts` | **一页语义**；ChatLab 面完全不覆盖联系人 |
  | `list_all_sessions(page_size, keyword)` | `GET /api/v1/sessions` | **取尽语义**（内部翻页到空页），跨页重复折叠并告警；`keyword` 是**服务端过滤**（翻的是过滤后的列表，不是取回来再剪）；`page_size` 上限 10000 |
  | `media_bytes(message, talker)` | `GET /api/v1/media/{id}` | 从 ChatLab 消息取；`talker` 是**会话 id**（不是 `accountName` 显示名）；404 后按「先 `media=1` 导出再取」自动重试一次 |
  | `media_bytes_by_id(id)` | `GET /api/v1/media/{id}` | 按**单段句柄**取（原生面的 `mediaId`，或 `mediaUrl` 末段）；不触发导出 |
  | `group_members(chatroom, include_message_counts)` | `GET /api/v1/group-members` | 成员集合＝**名册 ∪ 发言人**；`messageCount` 是**条件键**（只在要求计数时出现，不是占位 0）；计数开关关闭时不发参数；**空群号本地拒绝**（空名册会被读成「这个群没有成员」） |
  | `sync_now()` | `POST /api/v1/sync` | **写动作**（推进水位、可能导出媒体）：刻意不进入任何轮询路径，只有显式调用才触发（有测试钉住读路径零命中） |
  | `watch()` | SSE `/api/v1/push/messages` | `Last-Event-ID` 重连（游标**只在帧被消费时推进**：只有 `id:` 没有 `data:` 的悬空帧不推进，否则那条事件会从重放窗口消失）、心跳注释帧过滤、`generation` 变化上报；老面 `message.new`/`message.revoke` 载荷由客户端自有类型解码——它们不在描述 schema 里 |

  **两个容易读错的地方**：① `list_all_sessions` 与 `drain_session` 是取尽，而 `pull_page`/
  `list_messages`/`contacts` 只取一页（后两者那个面没有 `hasMore`，翻页由调用方按 `offset` 推进）；② 时间界收
  `YYYYMMDD` **或** unix 秒，`end` 作为上界时裸日期覆盖**整天**。
- **错误按性质分派变体**：HTTP 非 2xx → `Status`；连接/超时/重置/**URL 解析不出来** → `Transport`；
  响应是合法 JSON 但不合承诺形状 → `Shape`。解码是**先取字节再单独解析**的：`resp.json::<T>()` 会把解码失败也包成
  传输错误，于是「服务端答错了」与「网络断了」混成一类 —— 而调用方正是按变体分流的（重试传输故障
  合理，重试形状错误不合理）。`Status.url` **恒等于请求 URL**，不掺描述文字（按 url 归因的调用方会静默错分类），
  拒绝态的 `state` 另走 `detail` 字段。
- **超时默认（Rust 与 Python 一致；TS 仅示例，不在承诺面内）**：连接 **5s**（`CONNECT_TIMEOUT`）——服务端不在时要立刻失败；普通 JSON 请求读 **30s**
  （`READ_TIMEOUT`）；**按构造无上界**的请求不带读上界：`group_members(..., include_message_counts=True)`
  （整名册计数＝全会话扫描）、`media_bytes`／`media_bytes_by_id`（体积由发送方决定）、`sync_now()`（索引＋可能导出媒体）、
  `watch()`（长连接；服务端每 25s 发一次 keep-alive ping，读上界是按每次读操作计时的，30s 只剩 5s 余量，代理缓冲或一次事件循环卡顿就会掐断健康的空闲流）。
  判据是「慢不等于坏」：把 30s 套到这些面上会把「这个群很大」变成客户端错误。
  回归位置：`test_published_timeout_budgets_travel_per_request`（钉到 transport 收到的 per-request timeout 上，
  不是只读常量）、`test_watch_stream_is_not_bounded_by_the_json_read_timeout`；Rust 侧因 reqwest 不暴露已建
  Client 的配置，`published_timeouts_match_the_documented_budgets` 只钉公开常量数值。
- 鉴权走 `Authorization: Bearer`；客户端从不把 token 放进 URL（`/health` 是唯一免鉴权端点）。
- 本轮**不发布** crates.io：本地 `cargo build -p qqflow-client` 即可使用。

- **Python 侧**：`clients/python`（包 `qqflow-sdk`）。模型生成走 `scripts/regen.py`
  （spec 经 Rust 生成工具的 `--dump-spec` 取得，绕开 golden 的占位掩码）；行为层是
  `httpx.AsyncClient` 异步实现，`from qqflow_sdk import Client` 即用；老面 SSE 事件由
  客户端自有 `MessageEvent` 模型解码（camelCase 载荷），`sync` 帧用生成的 `SyncFrame`。
  测试对进程内 ASGI mock 跑：`clients/python/.venv/Scripts/python -m pytest tests/`。
  本轮**不发布** PyPI：本地 `pip install -e clients/python` 即可使用。
