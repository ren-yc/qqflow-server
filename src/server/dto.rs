//! 响应体的**类型化定义**（HTTP 形状的单一事实源）。
//!
//! 与 `weflow-server` 的同名模块是同一套纪律，但**形状不共用**：两个仓库的平台差异
//! （QQ NT 与微信 4.x）落在具体键上——`qq` vs `wxid`、`friend` vs `private`、原生消息的
//! `mediaId`/`mediaType`、`group-members` 的 `fromCache`/`updatedAt`。共用一套 DTO 会让
//! 任何一边的形状变更都牵动另一边。
//!
//! ## 三条纪律
//!
//! 1. **字段按字母序声明。** `json!` 走 `serde_json::Map`（默认 BTreeMap），因此现有响应
//!    的键**是按字母序输出的**；struct 的序列化顺序是**声明顺序**，所以按字母序声明能让
//!    DTO 的输出与现状**逐字节一致**。快照的 `keys` 字段会抓出任何顺序变化。
//! 2. **`null` 与「省略」是两件事，逐键保留现状**：要输出 `null` 就写 `Option<T>` 且
//!    **不加** `skip_serializing_if`；要省略才加。
//! 3. **同名键在两端点若类型或来源不同，必须各自建 struct**。

use serde::Serialize;

// ── 健康检查 ──────────────────────────────────────────────

/// `GET|POST /health` 与 `/api/v1/health`（**免鉴权**）。
///
/// 刻意是标量：未鉴权方可访问，因此**不能**列出账号 —— 连数组长度都会泄露
/// 「本机有几个 QQ 配置、各自到哪一步」。
#[derive(Debug, Serialize)]
pub struct Health {
    /// 账号阶段枚举（取值集合封闭，用类型表达比字符串稳）。
    pub account: crate::server::AccountPhase,
    pub status: String,
    pub version: String,
}

// ── 手工增量同步 ──────────────────────────────────────────

/// `POST /api/v1/sync`。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SyncResult {
    pub new_messages: usize,
    pub revoke_messages: usize,
    pub success: bool,
}

// ── 账号面 ────────────────────────────────────────────────

/// `GET /api/v1/accounts`。
#[derive(Debug, Serialize)]
pub struct AccountsList {
    pub accounts: Vec<AccountView>,
    pub success: bool,
}

/// 账号列表项。
///
/// 键名是**蛇形**（`message_count` / `db_path`），与 `weflow-server` 的 `message_count` /
/// `db_storage` 不是同一套 —— 不要跨仓库统一。
///
/// `error` 与 `db_path` 都是**条件键**：不知道就**不出现**，而不是 `null`。
#[derive(Debug, Serialize)]
pub struct AccountView {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_path: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub error: Option<String>,
    pub message_count: usize,
    pub qq: String,
    pub state: crate::server::AccountStatus,
}

/// 注册**受理**（或幂等命中）。
///
/// `status` 与 `db_path` 都是**条件键**：未知时不出现。`db_path` 回显的是服务器**实际解析
/// 到的**库（请求里的 `db_path` 很松散：可以是文件、可以是 Tencent Files 风格的根目录、
/// 也可以省略走启动扫描），所以回显解析结果才告诉客户端「在跟哪个库说话」。
#[derive(Debug, Serialize)]
pub struct AccountRegistered {
    #[serde(skip_serializing_if = "Option::is_none")]
    pub db_path: Option<String>,
    pub qq: String,
    pub state: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub status: Option<crate::server::AccountStatus>,
    pub success: bool,
}

/// 注册**被别的账号占位**（单绑定互锁）。
#[derive(Debug, Serialize)]
pub struct AccountConflict {
    pub occupied_by: String,
    pub occupied_status: crate::server::AccountStatus,
    pub qq: String,
    pub state: String,
    pub success: bool,
}

/// 注销：**已成功解绑**。
#[derive(Debug, Serialize)]
pub struct AccountDeregistered {
    pub index_cleared: bool,
    /// 请求落地时账号处于什么状态 —— 让客户端能区分「我取消了正在进行的构建」与
    /// 「我解绑了一个就绪账号」。
    pub previous_status: crate::server::AccountStatus,
    pub purged_dirs: usize,
    pub purged_media: bool,
    pub qq: String,
    pub state: String,
    pub success: bool,
}

/// 注销：**本来就没有绑定**。刻意幂等 —— 重试已完成的注销得到 200 而不是错误。
#[derive(Debug, Serialize)]
pub struct AccountNotRegistered {
    pub index_cleared: bool,
    pub purged_dirs: usize,
    pub purged_media: bool,
    pub qq: String,
    pub state: String,
    pub success: bool,
}

/// 注销：**互锁触发**（另一个账号持有绑定，它被完全不动地留下）。
#[derive(Debug, Serialize)]
pub struct AccountQqMismatch {
    pub index_cleared: bool,
    pub occupied_by: String,
    pub occupied_status: crate::server::AccountStatus,
    pub purged_dirs: usize,
    pub purged_media: bool,
    pub qq: String,
    pub state: String,
    pub success: bool,
}
// ── 会话列表 ──────────────────────────────────────────────

/// `GET /api/v1/sessions`（原生面）。
#[derive(Debug, Serialize)]
pub struct SessionsNative {
    pub count: usize,
    pub sessions: Vec<SessionNative>,
    pub success: bool,
}

/// 原生面的会话项。
///
/// `type` 是**平台数值枚举**，取值集合与 `weflow-server` **不同**：这里 `1` 是私聊、
/// `2` 是群（weflow 是 `0/1/2/3`）。下游不要跨仓库复用这个数字，用 ChatLab 面的字符串。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionNative {
    pub display_name: String,
    pub last_timestamp: i64,
    pub r#type: i64,
    pub unread_count: i64,
    pub username: String,
}

/// `GET /api/v1/sessions?format=chatlab`。
#[derive(Debug, Serialize)]
pub struct SessionsChatlab {
    pub count: usize,
    pub page: Page,
    pub sessions: Vec<SessionChatlab>,
}

/// 翻页信息。**`nextCursor` 始终出现**（排空时为 `null`）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// ChatLab 面的会话项。`type` 是字符串（`group` / `private`）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionChatlab {
    pub id: String,
    pub last_message_at: i64,
    pub message_count: i64,
    pub name: String,
    pub platform: String,
    pub r#type: String,
}

// ── 联系人 ────────────────────────────────────────────────

/// `GET|POST /api/v1/contacts`。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Contacts {
    /// 复用既有的 `ContactOut`（它已带 `camelCase` 与字段级注释），不另建一份平行
    /// 定义 —— 两份定义迟早会漂移。
    pub contacts: Vec<crate::server::handlers::contacts::ContactOut>,
    pub count: usize,
    pub has_more: bool,
    pub success: bool,
    pub total: usize,
}

// ── 群成员 ────────────────────────────────────────────────

/// `GET|POST /api/v1/group-members`。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMembers {
    pub chatroom_id: String,
    pub count: usize,
    /// `true` 表示这份成员表来自缓存而不是刚扫的。**与 weflow 的 `refreshed` 语义相反**
    /// （那边是「刚刷新过」），不要照抄。
    pub from_cache: bool,
    pub members: Vec<GroupMember>,
    pub success: bool,
    /// **毫秒级**墙钟（缓存写入时间）。快照靠时钟哨兵掩码。
    pub updated_at: i64,
}

/// 群成员项。字段缺失同样压平成**空串**（与 `contacts` 同规）。
#[derive(Debug, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub alias: String,
    pub avatar_url: String,
    pub display_name: String,
    pub group_nickname: String,
    pub is_friend: bool,
    pub is_owner: bool,
    /// **条件键**：只有请求带 `includeMessageCounts=1` 时才出现 —— 不是 `0`，是**没有这个
    /// 键**。客户端据「键在不在」判断自己拿到的是计数还是占位，恒出现会让这个判据失效。
    ///
    /// **注意与 `weflow-server` 不同**：那边无论是否要求计数都输出该键（值 0）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub message_count: Option<usize>,
    pub nickname: String,
    pub remark: String,
    pub wxid: String,
}
