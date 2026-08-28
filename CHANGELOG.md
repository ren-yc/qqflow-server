# 更新日志

本文件记录 qqflow-server 的版本变更，自 v0.5.0 起维护。
格式参考 [Keep a Changelog](https://keepachangelog.com/zh-CN/1.1.0/)，版本号遵循 [语义化版本](https://semver.org/lang/zh-CN/)。

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
