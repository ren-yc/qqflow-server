# qqflow-server HTTP API / Push 文档

qqflow-server 提供本地 HTTP API（已支持 GET 和 POST 请求），便于外部脚本或工具读取 QQ NT 本地聊天记录（会话、消息、联系人、群成员）；也支持通过固定 SSE 地址推送新消息事件。接口形态参考 WeFlow HTTP API，字段与语义以本文档为准（v1 实现差异见各节"说明"）。

## 启用方式

配置由当前目录 `./qqflow-server.json` 提供（详见 README），运行 `qqflow-server.exe` 即启动。

- 默认监听地址：`127.0.0.1`
- 默认端口：`5031`
- 基础地址：`http://127.0.0.1:5031`
- API Token：首次启动自动生成（32 字节随机数的 64 字符十六进制）并持久化到 `<data-dir>/token.txt`（启动日志仅打印保存路径，不打印 token 值）；也可在配置 `token` 字段指定
- 索引就绪前，业务接口返回 `503`（见 §8 错误）；`/health` 返回 `starting` 状态。例外：SSE 接口 `/api/v1/push/messages` 不检查就绪状态，可随时连接（此时 `sync` 事件携带的水位线为当前已索引值）；索引构建完成时会向订阅者广播新的 `sync` 事件，索引期间连接的客户端将自动获得正确基线，无需重连
- 新消息检测：后台以**文件系统事件**驱动（Windows ReadDirectoryChangesW / Linux inotify / macOS FSEvents，`watch_debounce_ms` 默认 350ms 防抖，辅以 `watch_fallback_ms` 默认 30s 慢速兜底轮询防事件丢失），源数据库文件变化时执行完整同步（镜像刷新 + 增量读取），经 `GET /api/v1/push/messages` 推送 SSE；客户端亦可主动调用 `POST /api/v1/sync` 立即同步

## 鉴权规范

除健康检查接口外，所有 `/api/v1/*` 接口均受 Token 保护。支持三种传参方式（任选其一）：

1. **HTTP Header (推荐)**: `Authorization: Bearer <您的Token>`
2. **Query 参数**: `?access_token=<您的Token>`（SSE 长连接推荐此方式）
3. **JSON Body**: `{"access_token": "<您的Token>"}`（仅限 POST 请求；SSE 接口除外，仅支持前两种）

## 接口列表

- `GET|POST /health`（免鉴权）
- `GET|POST /api/v1/health`（免鉴权）
- `GET|POST /api/v1/messages`
- `GET|POST /api/v1/sessions`
- `GET /api/v1/sessions/{id}/messages`（ChatLab Pull，仅 GET）
- `GET|POST /api/v1/contacts`
- `GET|POST /api/v1/group-members`
- `GET|POST /api/v1/push/messages`（SSE）
- `GET|POST /api/v1/sync`（手动同步）

> v1 未实现：`/api/v1/media/*`（媒体导出）、`/api/v1/sns/*`（朋友圈）——QQ NT 本地库不含朋友圈数据，媒体存于独立加密缓存。

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
  "version": "0.1.0",
  "accounts": [
    { "qq": "123456789", "state": "ready", "message_count": 28314 }
  ]
}
```

| 字段 | 说明 |
| ---- | ---- |
| `status` | `ok`（全部索引就绪）或 `starting`（索引构建中） |
| `version` | 服务版本号 |
| `accounts[].qq` | 账号 |
| `accounts[].state` | `indexing` / `ready` / `error` |
| `accounts[].message_count` | 已索引消息数 |
| `accounts[].error` | 出错时的错误信息（仅 error 状态） |

> v1 说明：`error` 状态当前实现中不会出现——任一账号的索引构建失败都会终止整个服务（不降级继续运行）。

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
- 连接建立后**先收到一个 `sync` 事件**（qqflow-server 扩展，携带当前 rowid 水位线），之后是 `message.new` / `message.revoke`
- KeepAlive 每 15 秒发送 `ping`
- 订阅端落后于广播缓冲（1024 条）时会重新收到 `sync` 事件对齐
- 建议接收端按 `event + rawid` 去重

### 事件字段

| 字段 | 说明 |
| ---- | ---- |
| `event` | `sync` / `message.new` / `message.revoke` |
| `sessionId` | 会话 ID：群聊为群号，私聊为对方 UID（`u_` 前缀） |
| `sessionType` | `group` 或 `private` |
| `rawid` | 消息 rowid（字符串） |
| `avatarUrl` | v1 恒省略（序列化时跳过该字段） |
| `sourceName` | 发送者昵称（无昵称时为空串） |
| `groupName` | 会话显示名：群聊为群名，私聊为对方昵称；仅 `message.new` / `message.revoke` 携带，缺失时省略该字段 |
| `content` | 消息内容 |
| `timestamp` | 消息时间，秒级 Unix 时间戳 |
| `lastRowidGroup` / `lastRowidC2c` | 仅 `sync` 事件：群/私聊表当前水位线（rowid 最大值） |

### 示例

```bash
curl -N "http://127.0.0.1:5031/api/v1/push/messages?access_token=YOUR_TOKEN"
```

```text
event: sync
data: {"event":"sync","sessionId":"","sessionType":"","rawid":"","content":"","timestamp":1760000000,"lastRowidGroup":7746845930483315306,"lastRowidC2c":7580296796597902190}

event: message.new
data: {"event":"message.new","sessionId":"881467592","sessionType":"group","groupName":"881467592","rawid":"7746845930483315306","sourceName":"张三","content":"你好","timestamp":1760000123}
```

---

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
| `chatlab` | string | 否   | `1/true` 时输出 ChatLab 格式                          |
| `format`  | string | 否   | `json` 或 `chatlab`                                   |
| `media`   | string | 否   | 兼容占位参数，v1 恒为 `media.enabled=false`           |

### 示例

```bash
curl "http://127.0.0.1:5031/api/v1/messages?talker=881467592&limit=20&access_token=YOUR_TOKEN"
curl "http://127.0.0.1:5031/api/v1/messages?talker=881467592&chatlab=1&access_token=YOUR_TOKEN"
curl "http://127.0.0.1:5031/api/v1/messages?talker=u_abc123&start=20260101&end=20260131&access_token=YOUR_TOKEN"
```

### JSON 响应字段

> v1 说明：`talker` 对应的会话不存在时不报错，返回 `success=true`、`messages=[]`、`count=0`（与 §4.1 的 404 行为不同）。

顶层字段：`success`、`talker`、`count`、`hasMore`、`media.enabled`、`media.exportPath`、`media.count`、`messages`

单条消息字段（按时间倒序，最新在前）：

| 字段 | 说明 |
| ---- | ---- |
| `localId` | 本地 rowid（消息唯一标识，数字） |
| `serverId` | 消息 seq（字符串） |
| `localType` | 消息类型码（见下表） |
| `createTime` | 秒级 Unix 时间戳 |
| `isSend` | v1 恒为 `0`（方向无法可靠推导） |
| `senderUsername` | 发送者 UID |
| `content` / `rawContent` / `parsedContent` | v1 三者相同，为解析后文本 |
| `mediaType` | 仅图片/语音/视频消息：`image` / `voice` / `video` |

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

> v1 差异：无 `replyToMessageId` / `quote` / `mediaFileName` / `mediaUrl` / `mediaLocalPath` 字段。

**示例响应**

```json
{
  "success": true,
  "talker": "881467592",
  "count": 2,
  "hasMore": true,
  "media": { "enabled": false, "exportPath": "", "count": 0 },
  "messages": [
    {
      "localId": 7746845930483315306,
      "serverId": "7746845930483315306",
      "localType": 3,
      "createTime": 1803703124,
      "isSend": 0,
      "senderUsername": "u_a",
      "content": "[image]",
      "rawContent": "[image]",
      "parsedContent": "[image]",
      "mediaType": "image"
    },
    {
      "localId": 7746845930483315299,
      "serverId": "7746845930483315299",
      "localType": 0,
      "createTime": 1803703100,
      "isSend": 0,
      "senderUsername": "u_b",
      "content": "你好",
      "rawContent": "你好",
      "parsedContent": "你好"
    }
  ]
}
```

### ChatLab 响应

当 `chatlab=1` 或 `format=chatlab` 时，返回 ChatLab 结构（消息按时间正序）：

- `chatlab.version`（`"0.0.2"`）、`chatlab.exportedAt`、`chatlab.generator`（`"qqflow-server"`）
- `meta.name`（会话显示名）、`meta.platform`（`"qq"`）、`meta.type`（`group`/`private`）、`meta.groupId`（群聊为群号，私聊为对方 UID）
- `members[].platformId`、`members[].accountName`、`members[].groupNickname`、`members[].avatar`（恒空）
- `messages[].sender`、`messages[].accountName`、`messages[].timestamp`、`messages[].type`、`messages[].content`、`messages[].platformMessageId`

---

## 4. 获取会话列表

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

**请求**

```http
GET /api/v1/sessions
```

### 参数

| 参数      | 类型   | 必填 | 说明                             |
| --------- | ------ | ---- | -------------------------------- |
| `keyword` | string | 否   | 匹配 `username` 或 `displayName` |
| `limit`   | number | 否   | 默认 `100`，范围 `1~10000`       |
| `offset`  | number | 否   | 分页偏移，默认 `0`               |
| `format`  | string | 否   | `chatlab` 时输出 ChatLab 格式    |

### 响应字段（按最后消息时间倒序）

- `success`
- `count`
- `sessions[].username`
- `sessions[].displayName`（群聊为群名，未知时回落为群号；私聊为对方昵称）
- `sessions[].type`（`2`=群聊，`1`=私聊）
- `sessions[].lastTimestamp`
- `sessions[].unreadCount`（v1 恒为 `0`）

**示例响应**

```json
{
  "success": true,
  "count": 2,
  "sessions": [
    { "username": "881467592", "displayName": "项目群", "type": 2, "lastTimestamp": 1803703124, "unreadCount": 0 },
    { "username": "u_abc123", "displayName": "张三", "type": 1, "lastTimestamp": 1803700000, "unreadCount": 0 }
  ]
}
```

### ChatLab 格式（`format=chatlab`）

```json
{
  "sessions": [
    { "id": "881467592", "name": "项目群", "platform": "qq", "type": "group", "messageCount": 0, "lastMessageAt": 1803703124 }
  ]
}
```

`platform` 固定 `"qq"`；`messageCount` v1 恒为 `0`。

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

### 响应

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
    "groupId": "881467592"
  },
  "members": [
    { "platformId": "u_a", "accountName": "张三", "groupNickname": "张三", "avatar": "" }
  ],
  "messages": [
    { "sender": "u_a", "accountName": "张三", "timestamp": 1738713600, "type": 0, "content": "你好", "platformMessageId": "123456" }
  ],
  "sync": {
    "hasMore": true,
    "nextSince": 1738713600,
    "nextOffset": 5000,
    "watermark": 1738714000
  }
}
```

### sync 块

| 字段         | 说明 |
| ------------ | ---- |
| `hasMore`    | 是否还有更多数据 |
| `nextSince`  | 有更多时 = 本页最后一条消息时间；否则 = `watermark` |
| `nextOffset` | 有更多时 = 下次请求的 offset；否则 = `0` |
| `watermark`  | 本次拉取的时间上界（未传 `end` 时为当前时间） |

---

## 5. 获取联系人列表

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

v1 联系人来源：消息中出现过的 UID → 最新昵称映射（无独立联系人库）。

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

### 响应字段（按 displayName 排序）

- `success`
- `count`
- `contacts[].username`（UID）
- `contacts[].displayName` / `contacts[].nickname`（均为最新昵称）
- `contacts[].remark` / `contacts[].alias` / `contacts[].avatarUrl`（v1 恒为空串）
- `contacts[].type`（v1 恒为 `"friend"`）

---

## 6. 获取群成员列表

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

v1 成员来源：该群消息中出现过的发送者 UID + 昵称（无独立群成员库）。

**请求**

```http
GET /api/v1/group-members
```

### 参数

| 参数                   | 类型   | 必填 | 说明                          |
| ---------------------- | ------ | ---- | ----------------------------- |
| `chatroomId`           | string | 是   | 群 ID，兼容使用 `talker` 传入 |
| `includeMessageCounts` | string | 否   | `1/true` 时附带成员发言数     |
| `withCounts`           | string | 否   | `includeMessageCounts` 的别名 |
| `forceRefresh`         | string | 否   | 兼容占位参数，v1 无缓存可刷新 |

群不存在时返回 `404`。

### 响应字段

- `success`
- `chatroomId`
- `count`
- `fromCache`（v1 恒为 `false`）
- `updatedAt`（毫秒时间戳）
- `members[].wxid`（发送者 UID）
- `members[].displayName` / `members[].nickname` / `members[].groupNickname`（均为昵称）
- `members[].remark` / `members[].alias` / `members[].avatarUrl`（v1 恒为空串）
- `members[].isOwner` / `members[].isFriend`（v1 恒为 `false`）
- `members[].messageCount`（仅 `includeMessageCounts=1` 时返回）

---

## 7. 手动同步

> 当使用 POST 时，请将参数放在 JSON Body 中（Content-Type: application/json）

立即对所有账号执行一次完整同步（镜像刷新 + 增量读取，**绕过后台 stat 检测循环**），并返回本次新增的最近若干条消息。客户端初始化或手动刷新时调用，用于主动拉取最新消息；新增消息同时也会广播给 SSE 订阅端。

**请求**

```http
POST /api/v1/sync
```

### 参数

| 参数    | 类型   | 必填 | 说明                       |
| ------- | ------ | ---- | -------------------------- |
| `limit` | number | 否   | 返回条数，默认 `100`，范围 `1~10000` |

### 响应字段

- `success`
- `count`（本次返回条数）
- `synced`（本次同步新增消息总数，`count` 可能因 `limit` 截断而小于它）
- `hasMore`（恒为 `false`）
- `messages`（新增消息，按时间倒序；字段同 §3 单条消息）

**示例响应**

```json
{
  "success": true,
  "count": 3,
  "synced": 3,
  "hasMore": false,
  "messages": [
    { "localId": 7746845930483315306, "serverId": "7746845930483315306", "localType": 0, "createTime": 1803703124, "isSend": 0, "senderUsername": "u_a", "content": "你好", "rawContent": "你好", "parsedContent": "你好" }
  ]
}
```

> 说明：启动时索引已全量构建，之后无新消息时 `synced` 为 `0`；QQ 运行中产生新消息后调用，可立即取回。

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
| `404` | 会话/群不存在（业务 404 走信封；**未知路径为 axum 默认空响应**） |
| `503` | 索引构建中（"服务正在建立索引，请稍后重试"） |
| `500` | 内部错误 |

> 说明：非 JSON 的 POST Body 会被忽略（仅记录日志），请求沿用 Query 参数，不会报 400；`start`/`end` 无法解析时该过滤条件被忽略。Query 参数类型错误（如 `limit=abc`）由框架直接拒绝，返回 `400` 空响应体，不走本信封。

---

## 9. 使用示例

### cURL

```bash
TOKEN=$(Get-Content "$env:LOCALAPPDATA\qqflow-server\token.txt")   # PowerShell
# GET 带 Token Header
curl -H "Authorization: Bearer $TOKEN" "http://127.0.0.1:5031/api/v1/messages?talker=881467592&limit=20"
# POST 带 JSON Body（参数走 Body，token 亦可走 Body）
curl -X POST http://127.0.0.1:5031/api/v1/messages \
  -H "Content-Type: application/json" \
  -d "{\"access_token\": \"$TOKEN\", \"talker\": \"881467592\", \"limit\": 50}"
# SSE
curl -N "http://127.0.0.1:5031/api/v1/push/messages?access_token=$TOKEN"
```

### Python

```python
import requests

BASE_URL = "http://127.0.0.1:5031"
headers = {"Authorization": "Bearer YOUR_TOKEN", "Content-Type": "application/json"}

messages = requests.post(
    f"{BASE_URL}/api/v1/messages",
    json={"talker": "881467592", "limit": 50},
    headers=headers,
).json()

sessions = requests.get(f"{BASE_URL}/api/v1/sessions", params={"limit": 20}, headers=headers).json()
```

---

## 10. 注意事项

1. API 仅监听本机 `127.0.0.1`，不对外网开放（`host` 可在配置中修改，需自行承担风险）。
2. `start` / `end` 支持 `YYYYMMDD` 与秒级时间戳；纯 `YYYYMMDD` 的 `end` 会扩展到当天 `23:59:59`。
3. 索引在启动时全量构建（消息 → 内存），期间业务接口返回 `503`（SSE 接口 `/api/v1/push/messages` 除外）；构建耗时取决于库大小（真实库 2.8 万条约 2~5 秒）。启动后由文件系统事件驱动增量同步（防抖 `watch_debounce_ms`，兜底 `watch_fallback_ms`），也可用 `POST /api/v1/sync` 手动触发。
4. 会话 ID 判定：全数字 → 群聊；`u_` 前缀或含非数字字符 → 私聊。查询时若按此判定未命中会话，会再尝试另一种类型（支持全数字 UID 的私聊）。
5. 消息内容为启发式解析结果（QQ 消息体为无固定 schema 的 protobuf 形态），QQ 升级可能导致解析退化；媒体消息输出 `[image]` / `[voice]` / `[video]` 占位。
6. 撤回消息 `localType=6`，content 保留原文（含"你猜猜撤回了什么"提示行）。
7. v1 未实现：媒体导出、朋友圈（SNS）、消息方向（`isSend`）、未读数（`unreadCount`）。
