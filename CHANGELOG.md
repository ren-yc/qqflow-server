# 更新日志

本文件记录 qqflow-server 的版本变更，自 v0.5.0 起维护。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

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
