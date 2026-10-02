#[allow(unused_imports)]
pub use progenitor_client::{ByteStream, ClientInfo, Error, ResponseValue};
#[allow(unused_imports)]
use progenitor_client::{encode_path, ClientHooks, OperationInfo, RequestBuilderExt};
/// Types used as operation parameters and responses.
#[allow(clippy::all)]
pub mod types {
    ///注册**被别的账号占位**（单绑定互锁）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountConflict {
        pub occupied_by: ::std::string::String,
        pub occupied_status: AccountStatus,
        pub qq: ::std::string::String,
        pub state: ::std::string::String,
        pub success: bool,
    }
    ///注销：**已成功解绑**。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountDeregistered {
        pub index_cleared: bool,
        pub previous_status: AccountStatus,
        pub purged_dirs: u64,
        pub purged_media: bool,
        pub qq: ::std::string::String,
        pub state: ::std::string::String,
        pub success: bool,
    }
    ///注销：**本来就没有绑定**。刻意幂等 —— 重试已完成的注销得到 200 而不是错误。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountNotRegistered {
        pub index_cleared: bool,
        pub purged_dirs: u64,
        pub purged_media: bool,
        pub qq: ::std::string::String,
        pub state: ::std::string::String,
        pub success: bool,
    }
    /**What `/health` may disclose about the bound account — deliberately NOT
[`AccountStatus`].

`/health` is unauthenticated, so it must not reveal which QQ accounts
exist on this machine, how many there are, or where their databases live.
The startup scan seeds one `AwaitingKey` entry per account directory it
finds, which makes the *count* of those entries a disclosure in itself.
This enum has no `AwaitingKey` variant at all, so leaking discovery
results through `/health` is a type error rather than a review item.*/
    #[derive(
        ::serde::Deserialize,
        ::serde::Serialize,
        Clone,
        Copy,
        Debug,
        Eq,
        Hash,
        Ord,
        PartialEq,
        PartialOrd
    )]
    pub enum AccountPhase {
        #[serde(rename = "unregistered")]
        Unregistered,
        #[serde(rename = "indexing")]
        Indexing,
        #[serde(rename = "ready")]
        Ready,
        #[serde(rename = "error")]
        Error,
    }
    impl ::std::fmt::Display for AccountPhase {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            match *self {
                Self::Unregistered => f.write_str("unregistered"),
                Self::Indexing => f.write_str("indexing"),
                Self::Ready => f.write_str("ready"),
                Self::Error => f.write_str("error"),
            }
        }
    }
    impl ::std::str::FromStr for AccountPhase {
        type Err = self::error::ConversionError;
        fn from_str(
            value: &str,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            match value {
                "unregistered" => Ok(Self::Unregistered),
                "indexing" => Ok(Self::Indexing),
                "ready" => Ok(Self::Ready),
                "error" => Ok(Self::Error),
                _ => Err("invalid value".into()),
            }
        }
    }
    impl ::std::convert::TryFrom<&str> for AccountPhase {
        type Error = self::error::ConversionError;
        fn try_from(
            value: &str,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }
    impl ::std::convert::TryFrom<::std::string::String> for AccountPhase {
        type Error = self::error::ConversionError;
        fn try_from(
            value: ::std::string::String,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }
    ///注销：**互锁触发**（另一个账号持有绑定，它被完全不动地留下）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountQqMismatch {
        pub index_cleared: bool,
        pub occupied_by: ::std::string::String,
        pub occupied_status: AccountStatus,
        pub purged_dirs: u64,
        pub purged_media: bool,
        pub qq: ::std::string::String,
        pub state: ::std::string::String,
        pub success: bool,
    }
    /**注册**受理**（或幂等命中）。

`status` 与 `db_path` 都是**条件键**：未知时不出现。`db_path` 回显的是服务器**实际解析
到的**库（请求里的 `db_path` 很松散：可以是文件、可以是 Tencent Files 风格的根目录、
也可以省略走启动扫描），所以回显解析结果才告诉客户端「在跟哪个库说话」。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountRegistered {
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub db_path: ::std::option::Option<::std::string::String>,
        pub qq: ::std::string::String,
        pub state: ::std::string::String,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub status: ::std::option::Option<AccountStatus>,
        pub success: bool,
    }
    /**Per-account readiness state (serialized as-is into the token-protected
`GET /api/v1/accounts`; `/health` reports the coarser [`AccountPhase`]).*/
    #[derive(
        ::serde::Deserialize,
        ::serde::Serialize,
        Clone,
        Copy,
        Debug,
        Eq,
        Hash,
        Ord,
        PartialEq,
        PartialOrd
    )]
    pub enum AccountStatus {
        #[serde(rename = "awaiting_key")]
        AwaitingKey,
        #[serde(rename = "indexing")]
        Indexing,
        #[serde(rename = "ready")]
        Ready,
        #[serde(rename = "error")]
        Error,
    }
    impl ::std::fmt::Display for AccountStatus {
        fn fmt(&self, f: &mut ::std::fmt::Formatter<'_>) -> ::std::fmt::Result {
            match *self {
                Self::AwaitingKey => f.write_str("awaiting_key"),
                Self::Indexing => f.write_str("indexing"),
                Self::Ready => f.write_str("ready"),
                Self::Error => f.write_str("error"),
            }
        }
    }
    impl ::std::str::FromStr for AccountStatus {
        type Err = self::error::ConversionError;
        fn from_str(
            value: &str,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            match value {
                "awaiting_key" => Ok(Self::AwaitingKey),
                "indexing" => Ok(Self::Indexing),
                "ready" => Ok(Self::Ready),
                "error" => Ok(Self::Error),
                _ => Err("invalid value".into()),
            }
        }
    }
    impl ::std::convert::TryFrom<&str> for AccountStatus {
        type Error = self::error::ConversionError;
        fn try_from(
            value: &str,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }
    impl ::std::convert::TryFrom<::std::string::String> for AccountStatus {
        type Error = self::error::ConversionError;
        fn try_from(
            value: ::std::string::String,
        ) -> ::std::result::Result<Self, self::error::ConversionError> {
            value.parse()
        }
    }
    /**账号列表项。

键名是**蛇形**（`message_count` / `db_path`），与 `weflow-server` 的 `message_count` /
`db_storage` 不是同一套 —— 不要跨仓库统一。

`error` 与 `db_path` 都是**条件键**：不知道就**不出现**，而不是 `null`。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountView {
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub db_path: ::std::option::Option<::std::string::String>,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub error: ::std::option::Option<::std::string::String>,
        pub message_count: u64,
        pub qq: ::std::string::String,
        pub state: AccountStatus,
    }
    ///`GET /api/v1/accounts`。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct AccountsList {
        pub accounts: ::std::vec::Vec<AccountView>,
        pub success: bool,
    }
    ///ChatLab 信封头。`exportedAt` 是**墙钟**（每次请求都不同）—— 快照里靠时钟哨兵掩码。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ChatlabHeader {
        #[serde(rename = "exportedAt")]
        pub exported_at: i64,
        pub generator: ::std::string::String,
        pub version: ::std::string::String,
    }
    /**本页出现过的发送者（去重）。

`accountName` 与 `groupNickname` 在 ChatLab 里是**两件事**：前者是账号自己的名字，
后者是本会话的群名片（40090）。原生面的 `senderName` 是二者「名片优先」的合并结果，
含义不同 —— 所以这里各自解析，不复用 `senderName` 填两个键。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ChatlabMember {
        #[serde(rename = "accountName")]
        pub account_name: ::std::string::String,
        ///QQ 没有头像来源；ChatLab 0.0.2 里该字段可选，空串是诚实的答案。
        pub avatar: ::std::string::String,
        #[serde(rename = "groupNickname")]
        pub group_nickname: ::std::string::String,
        #[serde(rename = "platformId")]
        pub platform_id: ::std::string::String,
    }
    /**消息面的 ChatLab 消息项。

三个面（原生面、消息面、拉取面）在 `replyToMessageId` 与 `media` 上**同规**：无引用时
省略该键（规范把它列为可选 *string*，给 `null` 会让信任类型的读者拿到解析不了的值），
无媒体时省略整个 `media` 键。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ChatlabMessage {
        #[serde(rename = "accountName")]
        pub account_name: ::std::string::String,
        pub content: ::std::string::String,
        #[serde(rename = "groupNickname")]
        pub group_nickname: ::std::string::String,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub media: ::std::option::Option<MediaBrief>,
        #[serde(rename = "platformMessageId")]
        pub platform_message_id: ::std::string::String,
        ///**无引用时省略该键**（不是给 `null`）。
        #[serde(
            rename = "replyToMessageId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub reply_to_message_id: ::std::option::Option<::std::string::String>,
        pub sender: ::std::string::String,
        pub timestamp: i64,
        #[serde(rename = "type")]
        pub type_: i64,
    }
    /**`GET /chatlab/messages`（消息面）。

**不带 `success`**：它输出的是数据信封，而 `success` 是「操作结果」的语言 —— 两者同时
出现时，读者无法判断 `count`/`page` 是否可信。翻页信息一律走 `page`，与发现面同规。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ChatlabMessages {
        pub chatlab: ChatlabHeader,
        ///**本页条数**（不是总数）—— 总数不在这个面上表达，分页语义由 `page` 承担。
        pub count: u64,
        pub members: ::std::vec::Vec<ChatlabMember>,
        pub messages: ::std::vec::Vec<ChatlabMessage>,
        pub meta: ChatlabMeta,
        pub page: Page,
        pub talker: ::std::string::String,
    }
    ///会话元信息。`ownerId` 未绑定时是空串。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ChatlabMeta {
        #[serde(rename = "groupId")]
        pub group_id: ::std::string::String,
        pub name: ::std::string::String,
        #[serde(rename = "ownerId")]
        pub owner_id: ::std::string::String,
        pub platform: ::std::string::String,
        #[serde(rename = "type")]
        pub type_: ::std::string::String,
    }
    ///`ContactOut`
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct ContactOut {
        /**WeFlow's alias slot: for QQ this carries the contact's QQ number
(from the uid mapping table / profile, when the version exposes it;
empty otherwise) — the old qqflow `qq` field migrated here.*/
        pub alias: ::std::string::String,
        #[serde(rename = "avatarUrl")]
        pub avatar_url: ::std::string::String,
        #[serde(rename = "displayName")]
        pub display_name: ::std::string::String,
        pub nickname: ::std::string::String,
        pub remark: ::std::string::String,
        #[serde(rename = "type")]
        pub type_: ::std::string::String,
        pub username: ::std::string::String,
    }
    ///`GET /api/v1/contacts`。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct Contacts {
        /**复用既有的 `ContactOut`（它已带 `camelCase` 与字段级注释），不另建一份平行
定义 —— 两份定义迟早会漂移。*/
        pub contacts: ::std::vec::Vec<ContactOut>,
        pub count: u64,
        #[serde(rename = "hasMore")]
        pub has_more: bool,
        pub success: bool,
        pub total: u64,
    }
    ///`DeleteApiV1AccountsQqResponse`
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    #[serde(untagged)]
    pub enum DeleteApiV1AccountsQqResponse {
        Deregistered(AccountDeregistered),
        NotRegistered(AccountNotRegistered),
        QqMismatch(AccountQqMismatch),
    }
    impl ::std::convert::From<AccountDeregistered> for DeleteApiV1AccountsQqResponse {
        fn from(value: AccountDeregistered) -> Self {
            Self::Deregistered(value)
        }
    }
    impl ::std::convert::From<AccountNotRegistered> for DeleteApiV1AccountsQqResponse {
        fn from(value: AccountNotRegistered) -> Self {
            Self::NotRegistered(value)
        }
    }
    impl ::std::convert::From<AccountQqMismatch> for DeleteApiV1AccountsQqResponse {
        fn from(value: AccountQqMismatch) -> Self {
            Self::QqMismatch(value)
        }
    }
    ///群成员项。字段缺失同样压平成**空串**（与 `contacts` 同规）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct GroupMember {
        pub alias: ::std::string::String,
        #[serde(rename = "avatarUrl")]
        pub avatar_url: ::std::string::String,
        #[serde(rename = "displayName")]
        pub display_name: ::std::string::String,
        #[serde(rename = "groupNickname")]
        pub group_nickname: ::std::string::String,
        #[serde(rename = "isFriend")]
        pub is_friend: bool,
        /**群主标记：由 `group_detail_info_ver1.[60002]` 解析，**每群恰一个 `true`**；
群主不在本页（发言者集合）或缺群主数据时全为 `false`。键恒保留。*/
        #[serde(rename = "isOwner")]
        pub is_owner: bool,
        #[doc = "**条件键**：只有请求带 `includeMessageCounts=1` 时才出现 —— 不是 `0`，是**没有这个\n键**。客户端据「键在不在」判断自己拿到的是计数还是占位，恒出现会让这个判据失效。\n\n**注意与 `weflow-server` 不同**：那边无论是否要求计数都输出该键（值 0）。"]
        #[serde(
            rename = "messageCount",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub message_count: ::std::option::Option<u64>,
        pub nickname: ::std::string::String,
        pub remark: ::std::string::String,
        pub wxid: ::std::string::String,
    }
    ///`GET /api/v1/group-members`（成员集合是名册 ∪ 发言人）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct GroupMembers {
        #[serde(rename = "chatroomId")]
        pub chatroom_id: ::std::string::String,
        pub count: u64,
        /**`true` 表示这份成员表来自缓存而不是刚扫的。**与 weflow 的 `refreshed` 语义相反**
（那边是「刚刷新过」），不要照抄。*/
        #[serde(rename = "fromCache")]
        pub from_cache: bool,
        pub members: ::std::vec::Vec<GroupMember>,
        pub success: bool,
        #[doc = "**毫秒级**墙钟，取值是**本次响应的生成时刻**。\n\n**它不表示索引新鲜度** —— 想要「这份成员表有多旧」的客户端拿不到答案（本仓没有记录索引\n构建时刻）。weflow 的同名字段是索引构建完成时刻，两仓含义不同：这是**允许差异**，\n写在这里而不是留给调用方猜。（快照靠时钟哨兵掩码。）"]
        #[serde(rename = "updatedAt")]
        pub updated_at: i64,
    }
    /**`GET|POST /health` 与 `/api/v1/health`（**免鉴权**）。

刻意是标量：未鉴权方可访问，因此**不能**列出账号 —— 连数组长度都会泄露
「本机有几个 QQ 配置、各自到哪一步」。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct Health {
        pub account: AccountPhase,
        pub status: ::std::string::String,
        pub version: ::std::string::String,
    }
    /**一条消息的媒体**元数据**（拉取面与消息面同形）。

它**不是**「字节可取」的承诺：无媒体时整个键省略；`fileName` 只有在导出确实写出了本地
副本、且名字由内容键派生之后才是可取句柄（那时它是**实际导出文件名**），否则它只是这条消息
自带的文件名。`md5` 取不到时省略该键 —— 未导出不等于没有摘要。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct MediaBrief {
        #[serde(rename = "fileName")]
        pub file_name: ::std::string::String,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub md5: ::std::option::Option<::std::string::String>,
        #[serde(rename = "type")]
        pub type_: ::std::string::String,
    }
    /**本页的导出能力/状态。

`exportPath` 只在**本次请求真的执行了导出**时出现（那时它是导出根）。空串会被读成
「有路径、只是空的」，而「没导出」与「导出到空路径」在下游是两件事。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct MediaEnvelope {
        pub count: u64,
        pub enabled: bool,
        #[serde(
            rename = "exportPath",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub export_path: ::std::option::Option<::std::string::String>,
    }
    /**Media metadata parsed from a structured message segment (image/voice/
video) — field ids per the upstream 40800 analysis (45424 md5 hex,
45405 size, 45411/45412 dims, 45503 uuid, 45812 local cache path, CDN
urls 45802/45803/45804). All optional: absent fields stay absent.*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug, Default)]
    pub struct MediaInfo {
        ///File name (45402), often "md5.ext".
        #[serde(
            rename = "fileName",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub file_name: ::std::option::Option<::std::string::String>,
        ///Image height (45412).
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub height: ::std::option::Option<i32>,
        ///Local cache path (45812) — served by /api/v1/media/{id}.
        #[serde(
            rename = "localPath",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub local_path: ::std::option::Option<::std::string::String>,
        ///Image MD5 hex string (45424) — the media store lookup key.
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub md5: ::std::option::Option<::std::string::String>,
        ///File size in bytes (45405).
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub size: ::std::option::Option<i64>,
        ///CDN URLs (45802 thumb / 45803 preview / 45804 original).
        #[serde(default, skip_serializing_if = "::std::vec::Vec::is_empty")]
        pub urls: ::std::vec::Vec<::std::string::String>,
        ///File UUID (45503).
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub uuid: ::std::option::Option<::std::string::String>,
        ///Image width (45411).
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub width: ::std::option::Option<i32>,
    }
    ///WeFlow-style message row.
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct MessageOut {
        pub content: ::std::string::String,
        #[serde(rename = "createTime")]
        pub create_time: i64,
        #[serde(rename = "isSend")]
        pub is_send: i64,
        #[serde(rename = "localId")]
        pub local_id: i64,
        #[serde(rename = "localType")]
        pub local_type: i64,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub media: ::std::option::Option<MediaInfo>,
        /**WeFlow media-export fields — filled by the messages handler when
`media=1` exports this page's media (absent otherwise).*/
        #[serde(
            rename = "mediaFileName",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub media_file_name: ::std::option::Option<::std::string::String>,
        ///Media store key (md5 hex or uuid) — fetch bytes via /api/v1/media/{id}.
        #[serde(
            rename = "mediaId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub media_id: ::std::option::Option<::std::string::String>,
        #[serde(
            rename = "mediaLocalPath",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub media_local_path: ::std::option::Option<::std::string::String>,
        #[serde(
            rename = "mediaUrl",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub media_url: ::std::option::Option<::std::string::String>,
        #[serde(rename = "parsedContent")]
        pub parsed_content: ::std::string::String,
        #[serde(rename = "rawContent")]
        pub raw_content: ::std::string::String,
        /**`platformMessageId` of the message this one replies to.

**Omitted when the target cannot be pinned down** — see
[`resolve_reply_to`]. The key is absent rather than `null` so that a
reader which trusts the declared type never receives a value it cannot
use.*/
        #[serde(
            rename = "replyToMessageId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub reply_to_message_id: ::std::option::Option<::std::string::String>,
        /**Resolved sender display name (WeFlow `senderName`): group card ("40090")
for THIS conversation > remark ("20009") > message nick > profile nick >
UID. Needs the name maps, so only [`shape_record`] fills it;
`from_record` leaves it empty. Saves every client from rebuilding the
same mapping out of /api/v1/contacts + /api/v1/group-members.*/
        #[serde(rename = "senderName")]
        pub sender_name: ::std::string::String,
        #[serde(rename = "senderUsername")]
        pub sender_username: ::std::string::String,
        #[serde(rename = "serverId")]
        pub server_id: ::std::string::String,
        /**消息行上的媒体类型（image / voice / video）。键名是 type：它与媒体对象内部的名字空间
无关，改名的理由是「同一个概念在两面用两个名字」比位置差异更难记。*/
        #[serde(rename = "type", skip_serializing_if = "::std::option::Option::is_none")]
        pub type_: ::std::option::Option<::std::string::String>,
    }
    /**`GET /api/v1/messages`（原生面）。

消息项直接复用 `store::query::MessageOut` —— 它已经是类型化定义，条件键也已用
`skip_serializing_if` 表达，不另建平行 struct（两份迟早漂移）。

`media=1` 导出**本页**消息的媒体文件，页大小受 `limit` 约束（默认 100、上限 10000）——
这个闸门必须留在描述里：下游只能靠它规划分批策略，而它此前只存在于散文文档。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct MessagesNative {
        pub count: u64,
        #[serde(rename = "hasMore")]
        pub has_more: bool,
        pub media: MediaEnvelope,
        pub messages: ::std::vec::Vec<MessageOut>,
        pub success: bool,
        pub talker: ::std::string::String,
    }
    /**`/chatlab/push/messages` 的通知帧：**只带元信息，不带正文**。

规范对这条通道的定位是「仅通知：不假设事件可靠送达」—— 客户端收到后**去拉**那一页。
带正文会诱导调用方把它当数据源，而它并不保证送达；不带，语义就没有歧义。

`eventId` 与 `platformMessageId` 是**两个不同的号**：前者是事件通道自己的标识，后者是那条
消息在平台上的 id（拉取时用它定位）。

**本面当前不下发 `platformMessageId`**（键保留、值恒为 `null`）：事件里的 `rawid` 是本仓库
自己的行号，**不是**平台消息号（拉取面的 `platformMessageId` 用的是 `seq`）；把它翻过去
需要在**推送热路径**上逐事件查一次索引，而规范里这个字段是**可选**的。定位消息请用
**拉取面**返回的 `platformMessageId`。

基线类事件（如 `session.sync`）没有对应的消息，两个 id 都为 `null` —— 它们只告诉客户端
「水位变了，去拉」。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct NotificationFrame {
        /**事件名。帧头（`event:` 行）与载荷里各有一份，**不是**重复：只解析 `data:` 行的客户端
也要能分辨类型。*/
        pub event: ::std::string::String,
        ///事件通道自己的标识；基线事件**省略该键**（不是给 `null`）。
        #[serde(
            rename = "eventId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub event_id: ::std::option::Option<::std::string::String>,
        #[doc = "**基线事件才有**：事件基线代号，注销时递增。\n\n客户端据此区分「注销后新账号刚开始」（该丢弃本地状态重新拉）与「自己漏收了」（该补拉）。\n少了它，这两种情况在协议上是同一件事。消息类事件不带这个键。"]
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub generation: ::std::option::Option<i64>,
        /**平台消息 id；取不到时**省略该键**。撤回帧里它是被撤回那条消息的平台号，
`message.new` 不带（见类型头注释）。*/
        #[serde(
            rename = "platformMessageId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub platform_message_id: ::std::option::Option<::std::string::String>,
        /**所属会话；基线事件也可能带（它属于某个账号）。**空串当作没有** ——
基线事件的 `session_id` 是空串，而 `skip_serializing_if` 对空串无效，
所以由调用方在构造时显式映射成 `None`。*/
        #[serde(
            rename = "sessionId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub session_id: ::std::option::Option<::std::string::String>,
        ///事件时刻（秒）。
        pub timestamp: i64,
    }
    ///翻页信息。**`nextCursor` 始终出现**（排空时为 `null`）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct Page {
        #[serde(rename = "hasMore")]
        pub has_more: bool,
        #[serde(
            rename = "nextCursor",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub next_cursor: ::std::option::Option<::std::string::String>,
    }
    ///`PostApiV1AccountsResponse`
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    #[serde(untagged)]
    pub enum PostApiV1AccountsResponse {
        Registered(AccountRegistered),
        Conflict(AccountConflict),
    }
    impl ::std::convert::From<AccountRegistered> for PostApiV1AccountsResponse {
        fn from(value: AccountRegistered) -> Self {
            Self::Registered(value)
        }
    }
    impl ::std::convert::From<AccountConflict> for PostApiV1AccountsResponse {
        fn from(value: AccountConflict) -> Self {
            Self::Conflict(value)
        }
    }
    ///ChatLab Pull 信封：**顶层就是那五块**，没有 `success` / `count`。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct PullEnvelope {
        pub chatlab: ChatlabHeader,
        pub members: ::std::vec::Vec<ChatlabMember>,
        pub messages: ::std::vec::Vec<PullMessage>,
        pub meta: ChatlabMeta,
        pub sync: PullSync,
    }
    /**Pull 面的消息项。

与混合面**不是同一个 struct**：本面的 `replyToMessageId` 在无引用或**目标不唯一**时
**省略该键**（规范把它列为可选 *string*；给 `null` 会让信任类型的读者拿到解析不了的
值）。目标不唯一时宁可不说 —— 猜错的 id 会让客户端把回复挂到另一条消息上，而它无从
分辨。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct PullMessage {
        #[serde(rename = "accountName")]
        pub account_name: ::std::string::String,
        pub content: ::std::string::String,
        #[serde(rename = "groupNickname")]
        pub group_nickname: ::std::string::String,
        #[serde(skip_serializing_if = "::std::option::Option::is_none")]
        pub media: ::std::option::Option<MediaBrief>,
        #[serde(rename = "platformMessageId")]
        pub platform_message_id: ::std::string::String,
        #[serde(
            rename = "replyToMessageId",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub reply_to_message_id: ::std::option::Option<::std::string::String>,
        pub sender: ::std::string::String,
        pub timestamp: i64,
        #[serde(rename = "type")]
        pub type_: i64,
    }
    /**翻页与水位。**两个游标都要原样回传**：`nextSince` 是排他下界，`nextOffset` 只用于
时间戳没能前进的退化情形。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct PullSync {
        #[serde(rename = "hasMore")]
        pub has_more: bool,
        #[serde(rename = "nextOffset")]
        pub next_offset: u64,
        #[serde(rename = "nextSince")]
        pub next_since: i64,
        pub watermark: i64,
    }
    ///ChatLab 面的会话项。`type` 是字符串（`group` / `private`）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SessionChatlab {
        ///会话在数据源里的唯一标识，可直接用作拉取路径。
        pub id: ::std::string::String,
        ///最新消息时间戳（秒）。
        #[serde(rename = "lastMessageAt")]
        pub last_message_at: i64,
        /**群成员数 —— **可选**：只有群名册加载得到时才出现（私聊、或名册缺失时不出现这个键）。

**它是「我们知道的成员数」，不是「群的确切人数」**：来源是本地缓存，可能少于真实值。
拿它做展示预估可以，拿它做「群里一共几个人」的断言不行。

「没有名册」与「名册是空的」在下游是两件事：前者不该被读成 0，所以是可选键而不是给 0。*/
        #[serde(
            rename = "memberCount",
            skip_serializing_if = "::std::option::Option::is_none"
        )]
        pub member_count: ::std::option::Option<u64>,
        ///消息总数（索引里该会话的条数）。**键恒保留** —— 缺键与「是 0」在下游不是同一件事。
        #[serde(rename = "messageCount")]
        pub message_count: i64,
        ///会话名称（群名/联系人名）。
        pub name: ::std::string::String,
        ///平台标识。
        pub platform: ::std::string::String,
        ///`group` / `private`。
        #[serde(rename = "type")]
        pub type_: ::std::string::String,
    }
    /**原生面的会话项。

`type` 是**平台数值枚举**，取值集合与 `weflow-server` **不同**：这里 `1` 是私聊、
`2` 是群（weflow 是 `0/1/2/3`）。下游不要跨仓库复用这个数字，用 ChatLab 面的字符串。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SessionNative {
        #[serde(rename = "displayName")]
        pub display_name: ::std::string::String,
        #[serde(rename = "lastTimestamp")]
        pub last_timestamp: i64,
        #[serde(rename = "type")]
        pub type_: i64,
        #[serde(rename = "unreadCount")]
        pub unread_count: i64,
        pub username: ::std::string::String,
    }
    ///`GET /chatlab/sessions`（ChatLab 形状的会话发现面）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SessionsChatlab {
        pub count: u64,
        pub page: Page,
        pub sessions: ::std::vec::Vec<SessionChatlab>,
    }
    ///`GET /api/v1/sessions`（原生面）。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SessionsNative {
        pub count: u64,
        pub sessions: ::std::vec::Vec<SessionNative>,
        pub success: bool,
    }
    /**`sync` 帧 —— 老面与通知面**共用同一形状**（收敛之后）。

水位线是**数组**而不是两个具名字段：原来它靠 `lastRowidGroup` / `lastRowidC2c` 两个字段名
承载，加第三张表就得再加一个字段，而消费方只能靠「字段名 ←→ 表」的约定配对。数组把这件事
变成数据。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SyncFrame {
        ///事件名（`sync`）。
        pub event: ::std::string::String,
        /**事件基线代号，注销时递增。**恒出现**（不是条件键）：客户端据此区分「注销后新账号刚开始」
（该丢弃本地状态重新拉）与「自己漏收了」（该补拉）。少了它，这两种情况在协议上是同一件事。*/
        pub generation: i64,
        ///各表的水位线。
        pub watermarks: ::std::vec::Vec<WatermarkEntry>,
    }
    ///`POST /api/v1/sync`。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct SyncResult {
        #[serde(rename = "newMessages")]
        pub new_messages: u64,
        #[serde(rename = "revokeMessages")]
        pub revoke_messages: u64,
        pub success: bool,
    }
    ///一张表的水位。
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct WatermarkEntry {
        ///表名。
        pub table: ::std::string::String,
        pub watermark: WatermarkValue,
    }
    /**水位值。

**与 weflow 不是同一套语义**：那边是 `{create_time, local_id, sort_seq}` 三元组，这里是
SQLite 行号（`read_new` 就是按它取新行的）。跨仓库的消费方必须按 `table` 分支，
而不是靠「名字一样」蒙混过去。*/
    #[derive(::serde::Deserialize, ::serde::Serialize, Clone, Debug)]
    pub struct WatermarkValue {
        ///SQLite 行号。
        pub rowid: i64,
    }
    /// Error types.
    pub mod error {
        /// Error from a `TryFrom` or `FromStr` implementation.
        pub struct ConversionError(::std::borrow::Cow<'static, str>);
        impl ::std::error::Error for ConversionError {}
        impl ::std::fmt::Display for ConversionError {
            fn fmt(
                &self,
                f: &mut ::std::fmt::Formatter<'_>,
            ) -> Result<(), ::std::fmt::Error> {
                ::std::fmt::Display::fmt(&self.0, f)
            }
        }
        impl ::std::fmt::Debug for ConversionError {
            fn fmt(
                &self,
                f: &mut ::std::fmt::Formatter<'_>,
            ) -> Result<(), ::std::fmt::Error> {
                ::std::fmt::Debug::fmt(&self.0, f)
            }
        }
        impl From<&'static str> for ConversionError {
            fn from(value: &'static str) -> Self {
                Self(value.into())
            }
        }
        impl From<String> for ConversionError {
            fn from(value: String) -> Self {
                Self(value.into())
            }
        }
    }
}
#[derive(Clone, Debug)]
/**Client for qqflow-server

Version: 0.7.0*/
pub struct Client {
    pub(crate) baseurl: String,
    pub(crate) client: reqwest::Client,
}
impl Client {
    /// Create a new client.
    ///
    /// `baseurl` is the base URL provided to the internal
    /// `reqwest::Client`, and should include a scheme and hostname,
    /// as well as port and a path stem if applicable.
    pub fn new(baseurl: &str) -> Self {
        #[cfg(not(target_arch = "wasm32"))]
        let client = {
            let dur = ::std::time::Duration::from_secs(15u64);
            reqwest::ClientBuilder::new().connect_timeout(dur).timeout(dur)
        };
        #[cfg(target_arch = "wasm32")]
        let client = reqwest::ClientBuilder::new();
        Self::new_with_client(baseurl, client.build().unwrap())
    }
    /// Construct a new client with an existing `reqwest::Client`,
    /// allowing more control over its configuration.
    ///
    /// `baseurl` is the base URL provided to the internal
    /// `reqwest::Client`, and should include a scheme and hostname,
    /// as well as port and a path stem if applicable.
    pub fn new_with_client(baseurl: &str, client: reqwest::Client) -> Self {
        Self {
            baseurl: baseurl.to_string(),
            client,
        }
    }
}
impl ClientInfo<()> for Client {
    fn api_version() -> &'static str {
        "0.7.0"
    }
    fn baseurl(&self) -> &str {
        self.baseurl.as_str()
    }
    fn client(&self) -> &reqwest::Client {
        &self.client
    }
    fn inner(&self) -> &() {
        &()
    }
}
impl ClientHooks<()> for &Client {}
#[allow(clippy::all)]
impl Client {
    /**Sends a `GET` request to `/api/v1/accounts`

*/
    pub async fn get_api_v1_accounts<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::AccountsList>, Error<()>> {
        let url = format!("{}/api/v1/accounts", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_accounts",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `POST` request to `/api/v1/accounts`

*/
    pub async fn post_api_v1_accounts<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::PostApiV1AccountsResponse>, Error<()>> {
        let url = format!("{}/api/v1/accounts", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "post_api_v1_accounts",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**注销账号；只能发 DELETE。

Sends a `DELETE` request to `/api/v1/accounts/{qq}`

*/
    pub async fn delete_api_v1_accounts_qq<'a>(
        &'a self,
        qq: &'a str,
    ) -> Result<ResponseValue<types::DeleteApiV1AccountsQqResponse>, Error<()>> {
        let url = format!(
            "{}/api/v1/accounts/{}", self.baseurl, encode_path(& qq.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .delete(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "delete_api_v1_accounts_qq",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**`limit` 上限 10000。

Sends a `GET` request to `/api/v1/contacts`

*/
    pub async fn get_api_v1_contacts<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::Contacts>, Error<()>> {
        let url = format!("{}/api/v1/contacts", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_contacts",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**成员集合是**名册 ∪ 发言人**（潜水成员也会出现，其 `messageCount` 为 0）。

Sends a `GET` request to `/api/v1/group-members`

*/
    pub async fn get_api_v1_group_members<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::GroupMembers>, Error<()>> {
        let url = format!("{}/api/v1/group-members", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_group_members",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `GET` request to `/api/v1/health`

*/
    pub async fn get_api_v1_health<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::Health>, Error<()>> {
        let url = format!("{}/api/v1/health", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_health",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `POST` request to `/api/v1/health`

*/
    pub async fn post_api_v1_health<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::Health>, Error<()>> {
        let url = format!("{}/api/v1/health", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "post_api_v1_health",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**字节面：`id` 是 store 键（md5 hex / uuid，直服本地缓存）或**导出文件名**（`media=1` 写进导出根的文件，按名在四个类型目录下解析）。同名多命中时内容一致才服务、不一致 404；未命中统一「媒体不存在」。

Sends a `GET` request to `/api/v1/media/{id}`

*/
    pub async fn get_api_v1_media_id<'a>(
        &'a self,
        id: &'a str,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!(
            "{}/api/v1/media/{}", self.baseurl, encode_path(& id.to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_media_id",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**`limit` 默认 100、上限 10000；`media=1` 导出**本页**消息的媒体（页大小受 `limit` 约束）。

Sends a `GET` request to `/api/v1/messages`

*/
    pub async fn get_api_v1_messages<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::MessagesNative>, Error<()>> {
        let url = format!("{}/api/v1/messages", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**SSE：重放缓冲 1000 条 / 600 秒；广播缓冲 1024；保活 25 秒（注释帧）。载荷为完整事件（老面形状）。

Sends a `GET` request to `/api/v1/push/messages`

*/
    pub async fn get_api_v1_push_messages<'a>(
        &'a self,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!("{}/api/v1/push/messages", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_push_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**原生形状的会话列表：`limit` 默认 100、上限 10000；只认 `offset` 翻页。

Sends a `GET` request to `/api/v1/sessions`

*/
    pub async fn get_api_v1_sessions<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::SessionsNative>, Error<()>> {
        let url = format!("{}/api/v1/sessions", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_sessions",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**游标拉取：`limit` 单页上限 5000。

Sends a `GET` request to `/api/v1/sessions/{id}/messages`

*/
    pub async fn get_api_v1_sessions_id_messages<'a>(
        &'a self,
        id: &'a str,
    ) -> Result<ResponseValue<types::PullEnvelope>, Error<()>> {
        let url = format!(
            "{}/api/v1/sessions/{}/messages", self.baseurl, encode_path(& id
            .to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_sessions_id_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `GET` request to `/api/v1/sync`

*/
    pub async fn get_api_v1_sync<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::SyncResult>, Error<()>> {
        let url = format!("{}/api/v1/sync", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_api_v1_sync",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `POST` request to `/api/v1/sync`

*/
    pub async fn post_api_v1_sync<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::SyncResult>, Error<()>> {
        let url = format!("{}/api/v1/sync", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "post_api_v1_sync",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**ChatLab 消息面：`talker` 必填；`cursor`/`offset` 翻页；`media=1` **真正执行导出**（本页媒体）；`count` 是**本页条数**、消息**升序**；无 `success`。

Sends a `GET` request to `/chatlab/messages`

*/
    pub async fn get_chatlab_messages<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::ChatlabMessages>, Error<()>> {
        let url = format!("{}/chatlab/messages", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_chatlab_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**SSE 通知面：只发元信息、不发正文；缓冲与保活同老面；基线帧带 `generation`。

Sends a `GET` request to `/chatlab/push/messages`

*/
    pub async fn get_chatlab_push_messages<'a>(
        &'a self,
    ) -> Result<ResponseValue<ByteStream>, Error<()>> {
        let url = format!("{}/chatlab/push/messages", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self.client.get(url).headers(header_map).build()?;
        let info = OperationInfo {
            operation_id: "get_chatlab_push_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => Ok(ResponseValue::stream(response)),
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Pull 形状的发现面：`keyword`/`limit`/`cursor` 分页；`count`/`page` 报告截断；`memberCount` 为已知名册人数、`messageCount` 为索引里的真实条数。

Sends a `GET` request to `/chatlab/sessions`

*/
    pub async fn get_chatlab_sessions<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::SessionsChatlab>, Error<()>> {
        let url = format!("{}/chatlab/sessions", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_chatlab_sessions",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Pull 面（与 `/api/v1/sessions/{id}/messages` 同一实现）：`limit` 单页上限 5000。

Sends a `GET` request to `/chatlab/sessions/{id}/messages`

*/
    pub async fn get_chatlab_sessions_id_messages<'a>(
        &'a self,
        id: &'a str,
    ) -> Result<ResponseValue<types::PullEnvelope>, Error<()>> {
        let url = format!(
            "{}/chatlab/sessions/{}/messages", self.baseurl, encode_path(& id
            .to_string()),
        );
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_chatlab_sessions_id_messages",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `GET` request to `/health`

*/
    pub async fn get_health<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::Health>, Error<()>> {
        let url = format!("{}/health", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_health",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**Sends a `POST` request to `/health`

*/
    pub async fn post_health<'a>(
        &'a self,
    ) -> Result<ResponseValue<types::Health>, Error<()>> {
        let url = format!("{}/health", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .post(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "post_health",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
    /**免鉴权：只描述形状，不含账号、路径与密钥。

Sends a `GET` request to `/openapi.json`

*/
    pub async fn get_openapi_json<'a>(
        &'a self,
    ) -> Result<
        ResponseValue<::serde_json::Map<::std::string::String, ::serde_json::Value>>,
        Error<()>,
    > {
        let url = format!("{}/openapi.json", self.baseurl,);
        let mut header_map = ::reqwest::header::HeaderMap::with_capacity(1usize);
        header_map
            .append(
                ::reqwest::header::HeaderName::from_static("api-version"),
                ::reqwest::header::HeaderValue::from_static(Self::api_version()),
            );
        #[allow(unused_mut)]
        let mut request = self
            .client
            .get(url)
            .header(
                ::reqwest::header::ACCEPT,
                ::reqwest::header::HeaderValue::from_static("application/json"),
            )
            .headers(header_map)
            .build()?;
        let info = OperationInfo {
            operation_id: "get_openapi_json",
        };
        self.pre(&mut request, &info).await?;
        let result = self.exec(request, &info).await;
        self.post(&result, &info).await?;
        let response = result?;
        match response.status().as_u16() {
            200u16 => ResponseValue::from_response(response).await,
            _ => Err(Error::UnexpectedResponse(response)),
        }
    }
}
/// Items consumers will typically use such as the Client.
pub mod prelude {
    #[allow(unused_imports)]
    pub use super::Client;
}
