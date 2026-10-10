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
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct Health {
    /// 账号阶段枚举（取值集合封闭，用类型表达比字符串稳）。
    pub account: crate::server::AccountPhase,
    pub status: String,
    pub version: String,
}

// ── 手工增量同步 ──────────────────────────────────────────

/// `POST /api/v1/sync`。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SyncResult {
    pub new_messages: usize,
    pub revoke_messages: usize,
    pub success: bool,
}

// ── 账号面 ────────────────────────────────────────────────

/// `GET /api/v1/accounts`。
#[derive(Debug, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AccountConflict {
    pub occupied_by: String,
    pub occupied_status: crate::server::AccountStatus,
    pub qq: String,
    pub state: String,
    pub success: bool,
}

/// 注销：**已成功解绑**。
#[derive(Debug, Serialize, utoipa::ToSchema)]
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
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct AccountNotRegistered {
    pub index_cleared: bool,
    pub purged_dirs: usize,
    pub purged_media: bool,
    pub qq: String,
    pub state: String,
    pub success: bool,
}

/// 注销：**互锁触发**（另一个账号持有绑定，它被完全不动地留下）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
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
// ── 消息（原生面）─────────────────────────────────────────

/// `GET /api/v1/messages`（原生面）。
///
/// 消息项直接复用 `store::query::MessageOut` —— 它已经是类型化定义，条件键也已用
/// `skip_serializing_if` 表达，不另建平行 struct（两份迟早漂移）。
///
/// `media=1` 导出**本页**消息的媒体文件，页大小受 `limit` 约束（默认 100、上限 10000）——
/// 这个闸门必须留在描述里：下游只能靠它规划分批策略，而它此前只存在于散文文档。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MessagesNative {
    pub count: usize,
    pub has_more: bool,
    pub media: MediaEnvelope,
    pub messages: Vec<crate::store::query::MessageOut>,
    pub success: bool,
    pub talker: String,
}

/// 本页的导出能力/状态。
///
/// `exportPath` 只在**本次请求真的执行了导出**时出现（那时它是导出根）。空串会被读成
/// 「有路径、只是空的」，而「没导出」与「导出到空路径」在下游是两件事。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaEnvelope {
    pub count: usize,
    pub enabled: bool,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub export_path: Option<String>,
}

// ── 消息（ChatLab 混合面）─────────────────────────────────

/// `GET /chatlab/messages`（消息面）。
///
/// **不带 `success`**：它输出的是数据信封，而 `success` 是「操作结果」的语言 —— 两者同时
/// 出现时，读者无法判断 `count`/`page` 是否可信。翻页信息一律走 `page`，与发现面同规。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatlabMessages {
    pub chatlab: ChatlabHeader,
    /// **本页条数**（不是总数）—— 总数不在这个面上表达，分页语义由 `page` 承担。
    pub count: usize,
    pub members: Vec<ChatlabMember>,
    pub messages: Vec<ChatlabMessage>,
    pub meta: ChatlabMeta,
    pub page: Page,
    pub talker: String,
}

/// 一条消息的媒体**元数据**（拉取面与消息面同形）。
///
/// 它**不是**「字节可取」的承诺：无媒体时整个键省略；`fileName` 只有在导出确实写出了本地
/// 副本、且名字由内容键派生之后才是可取句柄（那时它是**实际导出文件名**），否则它只是这条消息
/// 自带的文件名。`md5` 取不到时省略该键 —— 未导出不等于没有摘要。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct MediaBrief {
    pub file_name: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub md5: Option<String>,
    pub r#type: String,
}

/// ChatLab 信封头。`exportedAt` 是**墙钟**（每次请求都不同）—— 快照里靠时钟哨兵掩码。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatlabHeader {
    pub exported_at: i64,
    pub generator: String,
    pub version: String,
}

/// 会话元信息。`ownerId` 未绑定时是空串。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatlabMeta {
    pub group_id: String,
    pub name: String,
    pub owner_id: String,
    pub platform: String,
    pub r#type: String,
}

/// 本页出现过的发送者（去重）。
///
/// `accountName` 与 `groupNickname` 在 ChatLab 里是**两件事**：前者是账号自己的名字，
/// 后者是本会话的群名片（40090）。原生面的 `senderName` 是二者「名片优先」的合并结果，
/// 含义不同 —— 所以这里各自解析，不复用 `senderName` 填两个键。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatlabMember {
    pub account_name: String,
    /// QQ 没有头像来源；ChatLab 0.0.2 里该字段可选，空串是诚实的答案。
    pub avatar: String,
    pub group_nickname: String,
    pub platform_id: String,
}

/// 消息面的 ChatLab 消息项。
///
/// 三个面（原生面、消息面、拉取面）在 `replyToMessageId` 与 `media` 上**同规**：无引用时
/// 省略该键（规范把它列为可选 *string*，给 `null` 会让信任类型的读者拿到解析不了的值），
/// 无媒体时省略整个 `media` 键。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ChatlabMessage {
    pub account_name: String,
    pub content: String,
    pub group_nickname: String,
    /// 媒体元数据；无媒体时**整个键省略**（见 `MediaBrief`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaBrief>,
    pub platform_message_id: String,
    /// **无引用时省略该键**（不是给 `null`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<String>,
    pub sender: String,
    pub timestamp: i64,
    pub r#type: i64,
}

// ── Pull 面（/api/v1/sessions/{id}/messages）─────────────

/// ChatLab Pull 信封：**顶层就是那五块**，没有 `success` / `count`。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct PullEnvelope {
    pub chatlab: ChatlabHeader,
    pub members: Vec<ChatlabMember>,
    pub messages: Vec<PullMessage>,
    pub meta: ChatlabMeta,
    pub sync: PullSync,
}

/// Pull 面的消息项。
///
/// 与混合面**不是同一个 struct**：本面的 `replyToMessageId` 在无引用或**目标不唯一**时
/// **省略该键**（规范把它列为可选 *string*；给 `null` 会让信任类型的读者拿到解析不了的
/// 值）。目标不唯一时宁可不说 —— 猜错的 id 会让客户端把回复挂到另一条消息上，而它无从
/// 分辨。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PullMessage {
    pub account_name: String,
    pub content: String,
    pub group_nickname: String,
    /// 媒体元数据；无媒体时**整个键省略**（见 `MediaBrief`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<MediaBrief>,
    /// 本条消息的媒体**此刻就能取字节**时给出的句柄（`GET /api/v1/media/{id}`）。
    ///
    /// 「出现即可取」是承诺而非尽力而为：判据与各面**同一条**（store 登记过这条媒体的本地缓存
    /// 路径才算，见 `store::query::fetchable_media_id`）；不成立时**整个键省略**而不是给 `null`
    /// —— 通告一个必 404 的 id 比不给更坏：调用方拿到 404 只会以为服务坏了，而它无从区分。
    /// QQ 会清理媒体缓存：缓存里的文件没了，这个键就跟着消失（原生面与 SSE 同理）。
    ///
    /// 它在**消息这一层**而不在 `media` 对象里：`media` 的键集由契约钉死为
    /// `{type, fileName, md5}`（`media_shape_in_pull` 拒绝多余键）；而 `fileName` 是元数据名，
    /// 「有名字」与「取得到」是两件事，塞进同一个键会把它们混谈。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    pub platform_message_id: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<String>,
    pub sender: String,
    pub timestamp: i64,
    pub r#type: i64,
}

/// 翻页与水位。**两个游标都要原样回传**：`nextSince` 是排他下界，`nextOffset` 只用于
/// 时间戳没能前进的退化情形。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct PullSync {
    pub has_more: bool,
    pub next_offset: usize,
    pub next_since: i64,
    pub watermark: i64,
}
// ── 会话列表 ──────────────────────────────────────────────

/// `GET /api/v1/sessions`（原生面）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SessionsNative {
    pub count: usize,
    pub sessions: Vec<SessionNative>,
    pub success: bool,
}

/// 原生面的会话项。
///
/// `type` 是**平台数值枚举**，取值集合与 `weflow-server` **不同**：这里 `1` 是私聊、
/// `2` 是群（weflow 是 `0/1/2/3`）。下游不要跨仓库复用这个数字，用 ChatLab 面的字符串。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionNative {
    pub display_name: String,
    pub last_timestamp: i64,
    pub r#type: i64,
    pub unread_count: i64,
    pub username: String,
}

/// `GET /chatlab/sessions`（ChatLab 形状的会话发现面）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SessionsChatlab {
    pub count: usize,
    pub page: Page,
    pub sessions: Vec<SessionChatlab>,
}

/// 翻页信息。**`nextCursor` 始终出现**（排空时为 `null`）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct Page {
    pub has_more: bool,
    pub next_cursor: Option<String>,
}

/// ChatLab 面的会话项。`type` 是字符串（`group` / `private`）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct SessionChatlab {
    /// 会话在数据源里的唯一标识，可直接用作拉取路径。
    pub id: String,
    /// 最新消息时间戳（秒）。
    pub last_message_at: i64,
    /// 消息总数（索引里该会话的条数）。**键恒保留** —— 缺键与「是 0」在下游不是同一件事。
    pub message_count: i64,
    /// 会话名称（群名/联系人名）。
    pub name: String,
    /// 平台标识。
    pub platform: String,
    /// `group` / `private`。
    pub r#type: String,
    /// 群成员数 —— **可选**：只有群名册加载得到时才出现（私聊、或名册缺失时不出现这个键）。
    ///
    /// **它是「我们知道的成员数」，不是「群的确切人数」**：来源是本地缓存，可能少于真实值。
    /// 拿它做展示预估可以，拿它做「群里一共几个人」的断言不行。
    ///
    /// 「没有名册」与「名册是空的」在下游是两件事：前者不该被读成 0，所以是可选键而不是给 0。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub member_count: Option<usize>,
}

// ── 联系人 ────────────────────────────────────────────────

/// `GET /api/v1/contacts`。
#[derive(Debug, Serialize, utoipa::ToSchema)]
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

/// `GET /api/v1/group-members`（成员集合是名册 ∪ 发言人）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupMembers {
    pub chatroom_id: String,
    pub count: usize,
    /// v1 恒为 `false`：名册与消息都在内存索引里，本请求既不读盘也不触发同步
    /// （要强制对账请调 `/api/v1/sync`）。与 weflow 的同名字段同语义、同为常量。
    pub from_cache: bool,
    pub members: Vec<GroupMember>,
    pub success: bool,
    /// **毫秒级**时间戳，取值是**索引构建/更新的完成时刻**（store 里的 `index_built_at_ms`），
    /// 不是本次响应的墙钟：空转同步不会推进它，真吸收了新数据才前移。客户端据此判断
    /// 「这份成员表有多旧」。与 weflow 的同名字段同语义。
    /// 迁移提示：0.9.0 之前这里是响应生成时刻，依赖「每次请求都拿到新值」的调用方需要改用
    /// 自己的时钟，或按「值不变＝数据未变」判读。
    pub updated_at: i64,
}

/// 群成员项。字段缺失同样压平成**空串**（与 `contacts` 同规）。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct GroupMember {
    pub alias: String,
    pub avatar_url: String,
    pub display_name: String,
    pub group_nickname: String,
    pub is_friend: bool,
    /// 群主标记：由 `group_detail_info_ver1.[60002]` 解析，**每群恰一个 `true`**；
    /// 群主不在本页（发言者集合）或缺群主数据时全为 `false`。键恒保留。
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

// ── ChatLab 通知帧 ────────────────────────────────────────

/// `/chatlab/push/messages` 的通知帧：**只带元信息，不带正文**。
///
/// 规范对这条通道的定位是「仅通知：不假设事件可靠送达」—— 客户端收到后**去拉**那一页。
/// 带正文会诱导调用方把它当数据源，而它并不保证送达；不带，语义就没有歧义。
///
/// `eventId` 与 `platformMessageId` 是**两个不同的号**：前者是事件通道自己的标识，后者是那条
/// 消息在平台上的 id（拉取时用它定位）。
///
/// **本面不下发 `platformMessageId`**：取不到值时该键**整个省略**（见字段上的
/// `skip_serializing_if`），而不是给 `null`。原因：事件里的 `rawid` 是本仓库自己的**行号**，
/// 不是平台消息号（拉取面的 `platformMessageId` 用的是 `seq`）；把它翻过去要在**推送热路径**上
/// 逐事件查一次索引，而规范里这个字段是**可选**的。定位消息请用**拉取面**返回的同名字段。
/// **撤回帧同样不带**（本仓与 weflow 在此有意不同：weflow 的事件编号本身就是平台号）。
///
/// 基线事件（`sync`）没有对应的消息，两个 id 键都不出现 —— 它只告诉客户端「水位变了，去拉」。
#[derive(Debug, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct NotificationFrame {
    /// 事件名。帧头（`event:` 行）与载荷里各有一份，**不是**重复：只解析 `data:` 行的客户端
    /// 也要能分辨类型。
    pub event: String,
    /// 事件通道自己的标识；基线事件**省略该键**（不是给 `null`）。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub event_id: Option<String>,
    /// 平台消息 id；**本面所有帧都取不到 ⇒ 该键恒不出现**（含撤回帧，见类型头注释）。
    /// 键名留在类型里是为了与规范同形，不代表线上一定有值。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub platform_message_id: Option<String>,
    /// 所属会话；基线事件也可能带（它属于某个账号）。**空串当作没有** ——
    /// 基线事件的 `session_id` 是空串，而 `skip_serializing_if` 对空串无效，
    /// 所以由调用方在构造时显式映射成 `None`。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub session_id: Option<String>,
    /// 事件时刻（秒）。
    pub timestamp: i64,
    /// **基线事件才有**：事件基线代号，注销时递增。
    ///
    /// 客户端据此区分「注销后新账号刚开始」（该丢弃本地状态重新拉）与「自己漏收了」（该补拉）。
    /// 少了它，这两种情况在协议上是同一件事。消息类事件不带这个键。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub generation: Option<u64>,
}

// ── SSE `sync` 帧 ────────────────────────────────────────

/// `sync` 帧 —— 老面与通知面**共用同一形状**（收敛之后）。
///
/// 水位线是**数组**而不是两个具名字段：原来它靠 `lastRowidGroup` / `lastRowidC2c` 两个字段名
/// 承载，加第三张表就得再加一个字段，而消费方只能靠「字段名 ←→ 表」的约定配对。数组把这件事
/// 变成数据。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct SyncFrame {
    /// 事件名（`sync`）。
    pub event: String,
    /// 事件基线代号，注销时递增。**恒出现**（不是条件键）：客户端据此区分「注销后新账号刚开始」
    /// （该丢弃本地状态重新拉）与「自己漏收了」（该补拉）。少了它，这两种情况在协议上是同一件事。
    pub generation: u64,
    /// 各表的水位线。
    pub watermarks: Vec<WatermarkEntry>,
}

/// 一张表的水位。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WatermarkEntry {
    /// 表名。
    pub table: String,
    /// 水位值。
    pub watermark: WatermarkValue,
}

/// 水位值。
///
/// **与 weflow 不是同一套语义**：那边是 `{create_time, local_id, sort_seq}` 三元组，这里是
/// SQLite 行号（`read_new` 就是按它取新行的）。跨仓库的消费方必须按 `table` 分支，
/// 而不是靠「名字一样」蒙混过去。
#[derive(Debug, Serialize, utoipa::ToSchema)]
pub struct WatermarkValue {
    /// SQLite 行号。
    pub rowid: i64,
}

