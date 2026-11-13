# qqflow-server HTTP API

QQ NT 本地消息库的只读 HTTP / SSE 接口参考。以源码 `src/server/` 为准。

- 默认监听 `127.0.0.1:5032`（`--port` 可改）。
- 除 `/health`、`/api/v1/health` 与 `/openapi.json` 外，所有端点都需要鉴权。
- token 存于系统凭据库；无凭据库平台为会话级 token，随启动日志打印。
- 与 weflow-server 的有意差异见 [flow-contract ADAPTATION.md](https://github.com/ren-yc/flow-contract/blob/master/ADAPTATION.md)。

## 目录

- [鉴权](#鉴权)
- [错误信封与错误码](#错误信封与错误码)
- [通用查询参数](#通用查询参数)
- [端点](#端点)
- [ChatLab 适配面](#chatlab-适配面)
- [数据获取与安全模型](#数据获取与安全模型)
- [类型化客户端sdk](#类型化客户端sdk)

## 鉴权

两种传输等价：HTTP 头 `Authorization: Bearer <token>`，或查询参数 `?access_token=<token>`（SSE 长连接推荐后者）。

> [!WARNING]
> 只有这两条。`X-Api-Key`、`?token=` 与 POST JSON body 都不是通道；body 里的 `access_token` / `token` 会被跳过（既不鉴权、也不覆盖查询串里的值）。

两种传输的 token 比对均为常时比较。

## 错误信封与错误码

所有错误共用同一信封。

```json
{ "success": false, "code": 400, "message": "..." }
```

| 码 | 含义 | 何时发生 | 处置 | 可否重试 |
|---|---|---|---|---|
| 400 | 参数错误 | 缺必填参数、取值非法、密钥校验失败 | 修正请求 | 否 |
| 401 | 鉴权失败 | token 缺失或不匹配 | 补 token | 否 |
| 404 | 未找到 | 未知路径、未知群、媒体不存在 | 核对 id | 否 |
| 405 | 方法不允许 | 对只读端点用了 POST | 改用 GET | 否 |
| 500 | 服务异常 | 内部错误 | 记录并上报 | 视情况 |
| 503 | 未就绪 | 账号未注册或仍在 indexing | 轮询到 ready | 是 |

- 未知会话在**消息面**返回 200 空列表（与 weflow 相反），但在**拉取面**返回 404；群成员面对未知群号返回 404。
- 没有 403：本服务只有「有 token / 没 token」这一种授权判断。

## 通用查询参数

| 参数 | 适用面 | 说明 |
|---|---|---|
| `limit` | 全部列表面 | 条数上限，默认 100、上限 10000 |
| `offset` | 原生面 | 偏移量，默认 0 |
| `cursor` | 仅两个 `/chatlab` 面 | 原样回传 `page.nextCursor`；解析失败退回 `offset` |
| `start` / `end` | 消息面、拉取面 | Unix 秒或 `YYYYMMDD`；`start` 取当天 0 点，`end` 取当天 23:59:59（包含） |
| `keyword` | sessions / contacts / messages | 关键词过滤，小写匹配 |
| `media` + `image`/`voice`/`video`/`emoji` | 消息面、ChatLab 消息面 | `1` 触发本页媒体导出，类型开关收窄范围 |

无法解析的时间或游标不报 400，退回默认值。上限分属四个面，不要互相换算：

| 上限 | 属于哪个面 | 含义 |
|---|---|---|
| 10000 | sessions、contacts、messages、chatlab messages | 单请求条数硬上限（默认 100） |
| 5000 | 两个 Pull 面 | 单页上限，且默认也是 5000 |
| 200 | MCP 工具参数 | 工具层默认 50、上限 200 |
| 200 | CLI 的 `--with-media` 页大小 | 调用方自设的请求条数，不是服务端闸门 |

`GET /api/v1/group-members` 没有 `limit`，整名册一次给全。SDK 形参 `page_size` 不是 HTTP 参数名，它内部发的就是 `limit`。

> [!NOTE]
> 本仓 `media=1` **没有导出条数闸门**（weflow 服务端有 `jobs.truncate(200)`）。本仓也没有朋友圈面：`/api/v1/sns/*` 是 weflow-only，QQ NT 本地库不含朋友圈数据。

## 端点

16 个端点小节覆盖 17 条路由：`/health` 两条路径合并一节，`/chatlab/sessions/{id}/messages` 与原生 Pull 同一节。

路由表在 `server/routes.rs` 的 `ROUTES`，端点与路由的等价性由 api_smoke 的路由对等测试钉住。

### GET `/openapi.json` — 接口描述（免鉴权）

返回 OpenAPI 3 描述，由 `server/dto.rs` 的响应类型生成。

**请求**：
```http
GET /openapi.json
```

**参数**：无

**响应**：OpenAPI 3 JSON 文档（`application/json`）。

- 多形状端点用 `oneOf`：`POST /api/v1/accounts` 与 `DELETE /api/v1/accounts/{qq}` 的响应形状由状态决定。
- 字节面标注 `application/octet-stream`，推送面标注 `text/event-stream`；端点级 `description` 携带对外闸门。错误响应不在描述里。

### GET/POST `/health`、`/api/v1/health` — 就绪状态与账号阶段（免鉴权）

两条路径等价，GET / POST 均可。

**请求**：
```http
GET /health
```

**参数**：无

**响应**：
```json
{ "status": "ok", "version": "<version>", "account": "ready" }
```

- `status`：`ok`（已绑定账号且 ready）或 `starting`（未注册 / indexing / error）。
- `account` ∈ `unregistered | indexing | ready | error`。
- 刻意不列出账号：它免鉴权，而账号数组本身会泄露这台机器上有哪些账号。账号身份、消息数与库路径改由 `GET /api/v1/accounts` 提供。`awaiting_key` 不会出现在这里。

### POST `/api/v1/accounts` — 注册账号（客户端驱动启动）

**前置**：无。零账号启动合法。

**请求**：
```bash
curl -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"qq":"<QQ 号>","key":"<16 字节 ASCII 库密钥>"}' http://127.0.0.1:5032/api/v1/accounts
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `qq` | string | 是 | 账号（数字字符串） |
| `key` | string | 是 | SQLCipher 密钥（16 字节可打印 ASCII，由外部工具提取） |
| `db_path` | string | 否 | `nt_msg.db` 文件路径，或 Tencent Files 风格目录；省略时用启动扫描发现的路径 |

**响应**：
```json
{ "success": true, "qq": "<QQ 号>", "state": "accepted", "status": "indexing",
  "db_path": "C:\\...\\nt_msg.db" }
```

- `state`：`accepted` / `already_ready` / `in_progress` / `account_conflict`；密钥/路径/账号非法时是 400 拒绝态 `invalid_key` / `invalid_db_path` / `unknown_qq`。`status` 与 `db_path` 都是条件键，未知时不出现。
- `account_conflict` 时不含 `status` / `db_path`，改附 `occupied_by` / `occupied_status`（HTTP 仍为 200，`success` 仍为 `true`）。
- **强制单账号**：一个进程同时只绑定一个 qq；换账号必须先注销。**判定顺序**：冲突检查在密钥校验之前。
- **密钥仅存进程内存**，重启后需重新注册；注册时对目标库做页 1 HMAC 预校验，错钥立即 400。
- 重复注册同一 qq 且状态为 `ready` / `indexing` 时不重建索引；仅 `error` / `awaiting_key` 会被替换重建。`error` 账号仍持有绑定。

### GET `/api/v1/accounts` — 账号明细（需鉴权）

**请求**：
```bash
curl -H "Authorization: Bearer <token>" http://127.0.0.1:5032/api/v1/accounts
```

**参数**：无

**响应**：
```json
{ "success": true, "accounts": [ { "qq": "<QQ 号>", "state": "ready", "message_count": 0,
  "db_path": "C:\\...\\nt_msg.db" } ] }
```

- `state` ∈ `awaiting_key | indexing | ready | error`；`error` 与 `db_path` 都是条件键，不知道就不出现（不是 `null`）。
- 除绑定账号外，还包含启动扫描发现但尚未注册的账号（`awaiting_key`）。列表按 `qq` 升序，不受就绪门控。
- 键名是蛇形（`message_count` / `db_path`），与 weflow-server 的 `message_count` / `db_storage` 不是同一套。

`indexing` 时列表项额外带可选对象 `progress`：`{"tables_done":3,"tables_total":12,"messages_indexed":184321}`。其余状态整块省略；`tables_total` 为 `0` 表示分母未定；三个数不是同一瞬间的快照。

### DELETE `/api/v1/accounts/{qq}` — 注销账号（需鉴权）

释放绑定、清空内存索引、退场后台任务。

**请求**：
```bash
curl -X DELETE -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/accounts/<QQ 号>?purge_media=1"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `qq` | string(path) | 是 | 要注销的账号；与在位账号不一致时什么都不做 |
| `purge_media` | bool(`1`/`0`/`true`/`false`) | 否 | 是否同时删除已导出的媒体文件，默认 `false` |

**响应**（三种结果，HTTP 均为 200）：
```json
{ "success": true, "qq": "<QQ 号>", "state": "deregistered", "previous_status": "ready",
  "index_cleared": true, "purged_media": false, "purged_dirs": 0 }
```

- `deregistered`——已注销；`previous_status` 是注销前状态，`index_cleared` 表示内存索引确有内容被清空。
- `not_registered`——本就没有绑定账号，幂等。`wxid_mismatch` 对偶的 `qq_mismatch`——路径里的 qq 不是在位账号，附 `occupied_by` / `occupied_status`，在位账号毫发无损。
- 允许在 `indexing` 中途注销；尚未提交的构建结果会被丢弃。注销后旧任务不能重新置为 `ready`、挂接 watcher 或撤销同账号的新注册。注销时清掉 SSE 重放条目并推进 `generation`。
- `purge_media=1` 只删已知布局，仅在会话目录已空时删它；导出根目录永不触碰。

### GET `/api/v1/messages` — 消息查询（原生/富数据形状）

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/messages?talker=<id>&limit=20"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `talker` | string | 是 | 会话标识；缺失或为空返回 400 |
| `limit` | integer | 否 | 本页条数，默认 100、上限 10000 |
| `offset` | integer | 否 | 分页偏移，默认 0 |
| `start` / `end` | string | 否 | 时间边界，Unix 秒或 `YYYYMMDD` |
| `keyword` | string | 否 | 关键词过滤 |
| `media` / `image` / `voice` / `video` / `emoji` | string | 否 | `media=1` 触发本页媒体导出，其余收窄类型 |

**响应**：
```json
{ "success": true, "talker": "<id>", "count": 20, "hasMore": true,
  "media": { "enabled": false, "count": 0 },
  "messages": [ { "localId": 1, "serverId": "8280000000000000001", "localType": 1,
    "createTime": 1700000100, "isSend": 0, "senderUsername": "<qq>", "senderName": "<昵称>",
    "content": "你好", "rawContent": "你好", "parsedContent": "你好",
    "replyToMessageId": "8200000000000000000", "type": "image",
    "media": { "md5": "<md5>", "fileName": "<name>.jpg", "size": 0, "localPath": "C:\\...\\<name>.jpg" },
    "mediaId": "<md5>" } ] }
```

- `serverId` 为字符串：i64 超出 JS 安全整数范围。`isSend` 为数字 `0` / `1`，不是布尔。
- `replyToMessageId` 无引用时省略该键。`type`、`media`、`mediaId` 都是**条件键**：无媒体时整个不出现（不是 `null`）。
- 本仓的原生消息项**没有** `baseType` / `appmsgSubtype` / `quote` / `sortSeq`——`localType` 就是 QQ 原生类型码，不像 weflow 那样打包两个字段。
- `mediaId` 是 **store 键**（md5 hex 或 uuid），可直接喂给 `GET /api/v1/media/{id}`，不需要先导出。
- `media` 对象是 QQ 侧的原始媒体元数据：`uuid` / `md5` / `fileName` / `size` / `width` / `height` / `localPath` / `urls`，缺值的键整块省略。

媒体导出：`media=1` 时服务端按需导出本页媒体，并把 `mediaFileName` / `mediaUrl` / `mediaLocalPath` 三个**顶层消息键**填上（未导出时都不出现）。

顶层 `media` 为汇总：`{ "enabled": true, "exportPath": "C:\\...\\api-media", "count": 5 }`。`exportPath` 是**条件键**：只在本次请求真的执行了导出时出现。

- `media.count` 是本次实际导出的条数。`mediaFileName` 只在**确实写出本地副本**时才填；`mediaUrl` 只在名字可作句柄时给（`/api/v1/media/<name>`）。
- `mediaLocalPath` 是绝对路径。导出物里不写任何 URL 之外的凭据。

文件附件（`file`）不参与导出：导出仅覆盖图片、语音、视频三类。`emoji` 开关保留以对齐 WeFlow 形状，但本仓没有可导出的 emoji 文件（QQ 的 emoji 只带展示文本）。

安装版契约里的 `messages[].mediaPath` 本项目不输出。

### GET `/api/v1/sessions` — 会话列表（原生形状）

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/sessions?limit=50&offset=0"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `limit` | integer | 否 | 本页条数，默认 100、上限 10000 |
| `offset` | integer | 否 | 分页偏移，默认 0；本面只按 `offset` 翻页 |
| `keyword` | string | 否 | 关键词过滤 |

**响应**：
```json
{ "success": true, "count": 315, "sessions": [ { "username": "<qq>", "displayName": "项目群",
  "type": 2, "lastTimestamp": 1700000100, "unreadCount": 0 } ] }
```

- `type` 为数值枚举：`1` 私聊、`2` 群聊。`unreadCount` 在本仓 **v1 恒为 `0`**。本仓的原生会话项**没有** `sessionType` / `messageCount` / `summary`——这与 weflow-server 不同，别照抄那边的读法。
- 排序按 `lastTimestamp` 降序、`username` 次键。

### GET `/api/v1/sessions/{id}/messages` — ChatLab 拉取（消息游标）

同一份实现也挂在 `GET /chatlab/sessions/{id}/messages` 上，两条路径逐字节同形。**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/sessions/<id>/messages?since=0&limit=500"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `since` | string | 否 | 排他下界，Unix 秒或 `YYYYMMDD`（取当天 0 点）；缺省 = 全量 |
| `end` | string | 否 | 上界，Unix 秒或 `YYYYMMDD`（含当天 23:59:59） |
| `limit` | integer | 否 | 单页条数，默认与上限均为 5000 |
| `offset` | integer | 否 | 页内偏移，默认 0 |

**响应**：
```json
{ "chatlab": { "version": "0.0.2", "generator": "qqflow-server", "exportedAt": 1700000000 },
  "meta": { "name": "项目群", "platform": "qq", "type": "group", "groupId": "<id>", "ownerId": "<qq>" },
  "members": [ { "platformId": "<qq>", "accountName": "<昵称>", "groupNickname": "<群名片>", "avatar": "" } ],
  "messages": [ { "sender": "<qq>", "accountName": "<昵称>", "groupNickname": "<群名片>",
    "timestamp": 1700000103, "type": 0, "content": "大家好", "platformMessageId": "8200000000000000000",
    "media": { "type": "image", "fileName": "<md5>.jpg", "md5": "<md5>" } } ],
  "sync": { "hasMore": true, "nextSince": 1700000103, "nextOffset": 0, "watermark": 1700000200 } }
```

`messages[].mediaId` 是「此刻取得到字节」的承诺：出现时按它去 `GET /api/v1/media/{id}` 必须成功。判据是本仓的句柄来自 **QQ 本地缓存的登记路径**，不需要先执行导出；缺一个就整个键省略。

`media` 的键集固定为 `{type, fileName, md5}`；`mediaId` 在消息这一层，不在 `media` 里。本面不接受 `media` 参数——要还没导出过的媒体拿到字节，走 `/chatlab/messages?media=1`。

游标语义：

- `since` 排他（`create_time > since`），`end` 包含（`<= end`）。
- 页面按时间戳整秒组补齐：达到 `limit` 后仍会把当前秒的剩余消息取完，故一页可能略多于 `limit`。这保证 `nextSince` 一定能前进。
- `nextSince` 是本页最后一条的时间戳，不是整个会话的最大时间戳。`nextOffset` 常为 `0`；两个游标都应原样回传，叠加会二次跳过同一批行。`watermark` 是本次拉取的时间上界；排空后 `nextSince` 停在该上界。

### GET `/api/v1/contacts` — 联系人列表

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/contacts?limit=100&offset=0"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `limit` | integer | 否 | 本页条数，默认 100、上限 10000 |
| `offset` | integer | 否 | 分页偏移，默认 0 |
| `keyword` | string | 否 | 关键词过滤 |

**响应**：
```json
{ "success": true, "count": 100, "total": 0, "hasMore": true, "contacts": [
  { "username": "<qq>", "displayName": "<昵称>", "nickname": "<昵称>", "remark": "<备注>",
    "alias": "", "avatarUrl": "", "type": "friend" } ] }
```

- `displayName` 按 `remark > nickname > username` 解析，恒为字符串。`nickname`、`remark`、`alias`、`avatarUrl` 缺值时为空字符串。
- **必须翻页**：`limit` 默认 100，不传就只拿到前 100 条，截断外的联系人在下游会退化为显示 UID。`total` 为过滤后总数；`count` 是本页条数。
- 排序键为 `(displayName, username)`，保证全序稳定。`offset` 超出末尾返回空页且 `hasMore=false`。

### GET `/api/v1/group-members` — 群成员列表

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/group-members?chatroomId=<id>&includeMessageCounts=1"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `chatroomId` | string | 是 | 群 id（`talker` 别名已删除，传入被静默忽略） |
| `includeMessageCounts` | string | 否 | `1` 时 `messageCount` 为真实值，否则恒为 `0` |

**响应**：
```json
{ "success": true, "chatroomId": "<id>", "count": 81, "fromCache": false, "updatedAt": 1700000100000,
  "members": [ { "wxid": "<qq>", "displayName": "<昵称>", "nickname": "<昵称>", "remark": "",
    "alias": "", "groupNickname": "", "avatarUrl": "", "isOwner": false, "isFriend": false } ] }
```

- 成员标识键为 `wxid`（值就是 QQ 号，键名沿用 weflow 的 `wxid`）。
- 成员集合是「名册 ∪ 发言人」：潜水成员也会出现，其 `messageCount` 为 `0`；发过言但已不在名册里的照旧保留。
- 名册里的成员可能连联系人档案都没有：那时 `displayName` 回落 `wxid`。排序按 `messageCount` 降序、`wxid` 升序。
- `messageCount` 是**条件键**：只有 `includeMessageCounts=1` 时才出现（不是 `0`，是没有这个键）。
- `isOwner` 由群详情解析；群主不在本页时全为 `false`。`isFriend` 在本仓**恒为 `false`**（v1 不解析好友关系）。
- `fromCache` 恒为 `false`：本请求既不读盘、也不触发同步。`updatedAt` 是索引构建/更新完成时刻，毫秒。

### POST `/api/v1/media/export` — 定向导出消息媒体

在当前账号整个会话中唯一定位目标，并导出可取的本地字节。

**请求**

```bash
curl -X POST -H "Authorization: Bearer <token>" -H "Content-Type: application/json" \
  -d '{"talker":"<session-id>","platformMessageId":"<message-id>","sender":"<sender-id>","timestamp":1700000000}' \
  "http://127.0.0.1:5032/api/v1/media/export"
```

将 `<session-id>`、`<message-id>` 和 `<sender-id>` 替换为消息所在会话、消息 ID 和发送者 ID。

**参数**

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `talker` | string | 是 | 会话 ID |
| `platformMessageId` | string | 是 | 平台消息 ID |
| `sender` | string | 是 | 发送者 ID |
| `timestamp` | integer | 是 | 发送时刻，Unix 秒 |

**响应**

```json
{"success":true,"mediaId":"<64-hex>.png","fileName":"fake_image_01.jpg","mediaUrl":"/api/v1/media/<64-hex>.png"}
```

- `mediaId` 是可取句柄：URL 安全单段 token，由落盘字节的摘要派生。`fileName` 是消息自带的展示名（取不到时为空串），**不能拿它取字节**，两者通常不相等。
- `mediaUrl` 是不含凭据的根相对地址，末段就是 `mediaId`；随后通过 `GET /api/v1/media/{id}` 取字节。接口不下载任意外链。
- 字段非法为 400、未鉴权为 401、目标或源文件缺失为 404、目标不唯一或注册切换为 409。
- 无可导出的本地媒体为 422、读取或导出失败为 500、账号未就绪为 503。旧消息不受默认分页大小限制。

### GET `/api/v1/media/{id}` — 按 id 取媒体字节（唯一的字节面）

`{id}` 是 store 键或导出产物文件名（消息的 `mediaId`）；消息面的 `media.fileName` 只在导出回填后才与产物名相同。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/api/v1/media/<md5>.jpg" -o out.jpg
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `id` | string(path) | 是 | store 键或导出产物文件名，即消息的 `mediaId` |

**响应**：
```text
200 OK
content-type: image/jpeg
content-length: <字节数>
<媒体字节流>

404 Not Found
{ "success": false, "code": 404, "message": "media not found" }
```

- 解析范围仅限导出根与 QQ 本地缓存的媒体目录，不接受任何路径。路径段先过 `pathsafe` 的边界规则，再在 `canonicalize` 后校验仍落在根内。
- 同名多命中：候选内容一致时服务排序后的第一个；内容不一致时返回 404。找不到、被拒绝、内容冲突都返回同一个 404 信封。
- 无就绪门控：媒体文件已在磁盘上，其可读性与当前是否有账号绑定无关。

### GET `/api/v1/push/messages` — SSE 富推送面（带正文）

**前置**：无。零账号时连接返回 200，先收到 `ready` 基线。

**请求**：
```bash
curl -N "http://127.0.0.1:5032/api/v1/push/messages?access_token=<token>"
```

**参数**：无（`qq` 查询参数仅作语义提示，不影响订阅内容）

**响应**：SSE 事件流（`content-type: text/event-stream`）。

```json
{"event":"message.new","sessionId":"...","sessionType":"group","rawid":"...","sourceName":"...","groupName":"...","content":"...","timestamp":1700000100,"media":{"type":"image","fileName":"...","md5":"..."}}
```

事件名：`ready`（`{"status":"ok"}`）、`message.new`、`message.revoke`（完整消息，含 `content` 与媒体元数据）、`sync`（`{"event":"sync","generation":N,"watermarks":[…]}`）。

- 连接建立就发一帧 `sync` 基线。帧携带 `id:` 序号；`Last-Event-ID` 头（或查询参数）可回放最近 1000 条 / 10 分钟。每 25 秒发送 `ping` 注释帧保活。
- 广播缓冲为 1024 条：订阅端落后超过这个数就收不到逐条事件，改为收到一条 `sync` 对齐。它与重放缓冲（1000 条）不是一回事。
- `mediaId` 只在 store 登记过这条媒体的可读本地缓存路径时才出现；QQ 会清理媒体缓存，缓存里的文件没了，这个键就跟着消失。`media` 是元数据：不含 `url` / `localPath`，也绝不含解密用的 `aes_key`。
- SSE 事件不带 `localType` / `baseType` / `appmsgSubtype`；需要类型分支请按 `content` 与 `media.type`，或用 `sessionId` 回查 `/api/v1/messages`。进程收到退出信号时主动结束所有 SSE 流。

### GET/POST `/api/v1/sync` — 手动同步（动作端点）

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -X POST -H "Authorization: Bearer <token>" http://127.0.0.1:5032/api/v1/sync
```

GET 与 POST 均可，body 里放什么都会被忽略。

**参数**：无

**响应**：
```json
{ "success": true, "newMessages": 3, "revokeMessages": 0 }
```

立即跑一次水位增量同步。正常情况下由文件监视自动触发，此端点用于强制对账。

- 撤回计数键为 `revokeMessages`，不计入 `newMessages`。新消息同时会通过 SSE 推给已订阅的客户端。这是一个触发器，不返回消息体。

## ChatLab 适配面

`/chatlab/*` 是同一批数据在另一套形状上的入口：发现面、消息面、拉取面、通知面四条路由。它们与 `/api/v1/*` 共用同一份实现与同一条事件总线，差别只在形状；鉴权、错误信封、`Last-Event-ID` 重放与保活都与原生面完全一致。

### GET `/chatlab/sessions` — ChatLab 发现面

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/chatlab/sessions?limit=50&cursor=<nextCursor>"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `keyword` | string | 否 | 按名称或 id 模糊匹配 |
| `limit` | integer | 否 | 本页条数，默认 100、上限 10000 |
| `cursor` | string | 否 | 原样回传上一页的 `page.nextCursor`；缺省或解析失败退到 `offset` |
| `offset` | integer | 否 | `cursor` 的退路，默认 0 |

**响应**：
```json
{ "sessions": [ { "id": "<id>", "name": "项目群", "platform": "qq", "type": "group",
      "messageCount": 58000, "memberCount": 86, "lastMessageAt": 1711468800 } ],
  "page": { "hasMore": true, "nextCursor": "2" } }
```

- `page` 总是给出；未发现 `page` 时客户端按单次全量处理。`cursor` 与查询条件绑定：`keyword` 变化后旧游标应视为失效。
- `memberCount` 是可选键：群名册加载得到才出现。`type` 只有 `group` / `private` 两个取值；排序为 `lastMessageAt` 降序、`id` 升序。

### GET `/chatlab/messages` — ChatLab 消息面

**前置**：账号 `state == "ready"`。

**请求**：
```bash
curl -H "Authorization: Bearer <token>" \
  "http://127.0.0.1:5032/chatlab/messages?talker=<id>&limit=20"
```

**参数**：

| 参数 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `talker` | string | 是 | 会话标识 |
| `limit` | integer | 否 | 本页条数，默认 100、上限 10000 |
| `offset` | integer | 否 | 分页偏移，默认 0 |
| `cursor` | string | 否 | 解析不了退回 `offset` |
| `start` / `end` | string | 否 | 时间边界，Unix 秒或 `YYYYMMDD` |
| `keyword` | string | 否 | 关键词过滤 |
| `media` | string | 否 | `1` 触发导出；类型开关 `image`/`voice`/`video`/`emoji` |

**没有 `format` 参数**：这条面天生就是 ChatLab 形状。

**响应**：
```json
{ "talker": "<id>", "count": 20, "page": { "hasMore": true, "nextCursor": "20" },
  "chatlab": { "version": "0.0.2", "generator": "qqflow-server", "exportedAt": 1700000000 },
  "meta": { "name": "项目群", "platform": "qq", "type": "group", "groupId": "<id>", "ownerId": "<qq>" },
  "members": [ { "platformId": "<qq>", "accountName": "<昵称>", "groupNickname": "<群名片>", "avatar": "" } ],
  "messages": [ { "sender": "<qq>", "accountName": "<昵称>", "groupNickname": "<群名片>",
    "timestamp": 1700000103, "type": 0, "content": "大家好", "platformMessageId": "8200000000000000000" } ] }
```

- 没有 `success`：它输出的是数据信封。`count` 是本页条数，不是总数；截断由 `page` 报告（`hasMore` 为假时 `nextCursor` 为 `null`）。消息按时间升序（与原生面的降序相反）。
- `members` 只含本页出现过的发送者（去重后），不是群名册。

`media=1` 时真正执行导出（`file` 永不导出），并把确实写出摘要派生本地文件的条目的 `messages[].media.fileName` 回填成实际导出文件名。

`messages[].media` 是媒体元数据 `{type, fileName, md5}`：无媒体时整个键省略，`md5` 取不到时省略该键。

### GET `/chatlab/push/messages` — ChatLab 通知面（只发元信息）

**前置**：无。

**请求**：
```bash
curl -N "http://127.0.0.1:5032/chatlab/push/messages?access_token=<token>"
```

**参数**：无

**响应**：SSE 帧。

```json
{ "event": "message.new", "eventId": "...", "sessionId": "...", "timestamp": 1700000000 }
```

与 `/api/v1/push/messages` 唯一的差别是帧的形状：

| | `/api/v1/push/messages` | `/chatlab/push/messages` |
|---|---|---|
| 载荷 | 完整消息（含 `content` 与媒体元数据） | 只带标识与时间 |
| 定位 | 富推送面 | 规范里的通知通道 |

- `message.revoke` 帧同形。不带正文：规范对这条通道的定位是「仅通知」，客户端收到后去拉那一页。
- `eventId` 是事件通道自己的标识。`platformMessageId` **本面所有帧都不出现**（含撤回帧）——键名留在类型里是为了与规范同形，不代表线上有值。`sync` 基线帧带 `generation`（注销时递增）。

### ChatLab 字段语义

`messages[].type` 采用 ChatLab 0.0.2 标准枚举，不是 QQ 原生类型码：

- `0` TEXT / `1` IMAGE / `2` VOICE / `3` VIDEO / `80` SYSTEM / `81` RECALL / `99` OTHER

本仓只输出这 7 个码（QQ 侧没有引用关系抽取，也没有名片/位置/链接的细分解析）。标准中 `6` 未分配，任何情况下都不会出现。weflow-server 能输出 14 个码；下游做类型分支时应把未覆盖码按 `99` 兜底。

`meta.type` 按标准只有 `group` / `private`。本仓的原生会话项没有更细的分类字段可用。

与标准 / 安装版的已知差异：

- `meta.groupId` 私聊也输出（值等于会话 id）。
- `meta.groupAvatar`、`members[].aliases`、`members[].roles`、`messages[].mediaPath` 均不输出；`members[].avatar` 给 HTTP URL 或空串。

字段无值时的三种表示：**省略键**＝对象不存在或本次没请求；**`null`**＝有概念、此刻没值；**空串**＝恒为字符串的字段没有内容。

| 面 | 键恒出现（无值为 `null` 或 `0`） | 无值时省略键 |
|---|---|---|
| messages | `media.enabled`、`media.count` | `replyToMessageId`、`type`、`media`、`mediaId`、`mediaFileName` / `mediaUrl` / `mediaLocalPath`、`media.exportPath` |
| 两个 ChatLab 列表 | `page.nextCursor`（排空时 `null`） | `sessions[].memberCount`、`messages[].replyToMessageId`、`media`、`mediaId` |
| 拉取面 | `sync` 四键 | `messages[].replyToMessageId`、`media`、`mediaId`（`page` 整块不出现） |
| 两个 SSE 面 | — | `groupName`、`media`、消息层 `mediaId`、`platformMessageId` |
| 群成员面 | — | `messageCount`（只在 `includeMessageCounts=1` 时出现） |
| 账号面 | `message_count` | `db_path`、`error`、`progress` |

本仓**没有**「键恒出现、值为 `null`」的响应字段（唯一例外是分页游标 `nextCursor`）。这与 weflow 不同——那边 `quote` / `media` / `summary` 都是恒出现、无值 `null`。

`/api/v1/contacts`、`/api/v1/group-members` 与 ChatLab 的 `members[]` 在没有该值时给空串而不是 `null`。

涉及字段：`nickname` / `remark` / `alias` / `avatarUrl` / `avatar` 等——它们在类型上恒为字符串。

跨仓的允许差异：

- 未请求导出时的 `media.exportPath`：本仓省略键，weflow 键恒出现。
- SSE `message.new` 的 `groupName` / `media`：本仓条件键，weflow 键恒出现、无值 `null`。
- 通知帧 `platformMessageId`：本仓恒省略，weflow 键恒出现。
- 未请求计数时的 `members[].messageCount`：本仓省略键，weflow 键恒出现、值为 `0`。

逐条见 [flow-contract ADAPTATION.md](https://github.com/ren-yc/flow-contract/blob/master/ADAPTATION.md)。

## 数据获取与安全模型

- **活库直读**：对 QQ NT 加密库持只读长连接，全程不写源库、不在磁盘生成明文镜像/快照。库文件带 QQ 自有的明文头偏移，由自定义 VFS 虚拟剥离。
- **密钥策略**：注册入内存、重启失效需重注册；日志、导出、响应均不打码密钥值。本仓只有一把库密钥（没有 weflow 的独立图片键）。
- **媒体**：直接从 QQ 本地缓存登记路径取字节（常开），`media=1` 按需导出到 `exportPath`。

## 类型化客户端（SDK）

本仓库提供 `clients/rust`（crate `qqflow-client`）与 `clients/python`（包 `qqflow-sdk`），分层一致：生成类型 + 手写行为层。

**只读查询**：

- 账号与就绪：`health()` / `accounts()` / `wait_ready()`
- 消息：`pull_page()` / `drain_session()` / `list_messages()` / `chatlab_messages()`
- 其它面：`contacts()` / `group_members()` / `list_all_sessions()`
- 媒体与推送：`media_bytes()` / `media_bytes_by_id()` / `watch()`

**动作**：`register()`（非阻塞，返回原始 `state`/`status`）、`ensure_ready()`（注册 + 轮询，200 的拒绝态立即失败）、`sync_now()`（写动作，只有显式调用才触发）。

- 一页语义 vs 取尽语义：`pull_page` / `list_messages` / `chatlab_messages` / `contacts` 只取一页；`drain_session` / `list_all_sessions` 内部翻页到 `hasMore=false`。
- 游标必须原样回传：`pull_page` 的 `nextSince` / `nextOffset` 叠加会二次跳过同一批行。
- `group_members` 的计数开关关闭时不发参数（不是发 `0`）；`list_all_sessions` 的 `page_size` 上限 10000。`media_bytes` 在 404 后按「先 `media=1` 导出再取」自动重试一次；`media_bytes_by_id` 不触发导出。
- 重试时导出行按（平台消息 id、sender、timestamp）三元组定位：恰好一行用它的落盘名重试，零行报「导出页不含目标」，多行报「无法确定哪一行才是本条」。
- `watch()` 的游标只在帧被消费时推进；只有 `id:` 没有 `data:` 的悬空帧不推进。类型与操作客户端由 `/openapi.json` 生成（`cargo run -p qqflow-regen` / `python scripts/regen.py`），生成物入库、CI 断言无 diff；不要手改生成物。
- 行为层手写：`clients/rust/src/client.rs` 与 `clients/python/src/qqflow_sdk/client.py`。Python 另有 `SyncClient`，方法面与 `Client` 一一对应，内部跑专属事件循环线程。
- 错误按性质分派：HTTP 非 2xx → `Status`；连接/超时/重置/URL 解析失败 → `Transport`；JSON 合法但不合承诺形状 → `Shape`。
- 超时按单次操作计时、有进展即复位，没有总时限：连接 5s、普通 JSON 请求 30s、成本由数据量决定的一族 90s（`group_members` 带计数、媒体取字节、`sync_now`、`watch`）。鉴权走 `Authorization: Bearer`；客户端从不把 token 放进 URL。
