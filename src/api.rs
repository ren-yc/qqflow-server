//! 嵌入者承诺面。
//!
//! **这是本 crate 唯一的对外承诺。** 其余模块在默认构建下是 `pub(crate)` —— 外部不可达，边界由
//! 编译器强制。承诺面有 rustdoc、有 semver 承诺，`#![deny(missing_docs)]` 也只作用在这里。
//!
//! ## 为什么需要这层间接
//!
//! [`crate::store::Store`] 的字段是 `pub`（内部模块要写它）。直接把 `Store` 放出去，等于把
//! **每一个字段**都变成对外契约 —— 它们一变动下游就断，而它们本来是内部布局。所以这里只放
//! **读**：嵌入者能查，不能改。
//!
//! ## 与 HTTP 面的关系
//!
//! HTTP 面的响应形状由 `server::dto` 描述。这里**不是**它的副本：嵌入者拿到的就是索引里的类型，
//! 不经 JSON 往返。

use std::sync::Arc;

use parking_lot::RwLock;

use crate::parser::types::{ChatType, MessageRecord};
use crate::store::Store;

pub use crate::store::{Conversation, NameMaps, conv_key};

/// 从账号库建索引，**不起 HTTP**。
///
/// 这是嵌入者通常的第一个调用。`db_path` 是 `nt_msg.db` **文件**（不是目录）：本仓库的库带 QQ
/// 自有的明文头偏移，由自定义 VFS 处理 —— 所以必须经这条路径打开，裸 `Connection::open` 读不了。
///
/// `key` 是那个库的密钥（32 字节按 hex 或它本身的形态，与注册时给服务的一致）。**密钥从哪来由
/// 调用方负责**：本 crate 不做密钥提取。
///
/// **同步**调用：真实账号是秒级到十几秒，放哪个线程由调用方决定。
pub fn open(db_path: &std::path::Path, key: &str) -> anyhow::Result<Index> {
    let store = crate::store::index::open_and_build(db_path, key)?;
    Ok(Index::new(Arc::new(RwLock::new(store))))
}

/// 一个会话的摘要 —— **不含消息**。
///
/// 单独一个类型而不是直接给 [`Conversation`]：后者的 `msgs` 是整段消息，列一遍会话就会把
/// 每条消息都克隆一次。要消息的调用方用 [`Index::messages`] 单独取。
#[derive(Debug, Clone)]
pub struct ConversationSummary {
    /// 群聊还是私聊。
    pub chat_type: ChatType,
    /// 对端标识（群号或 uid）。
    pub talker: String,
    /// 展示名（群名或对方昵称）；取不到时为空串。
    pub name: String,
    /// 消息条数。
    pub message_count: usize,
}

/// 一个账号的只读索引句柄。
///
/// 克隆是廉价的（内部是 `Arc`）—— 嵌入者可以放心把它分给多个线程。
#[derive(Clone)]
pub struct Index {
    store: Arc<RwLock<Store>>,
}

impl Index {
    /// 包住一个索引。
    ///
    /// **不是 `pub`**：它要一个 `Arc<RwLock<Store>>`，而 `Store` 不在承诺面上 —— 留成 `pub` 会逼
    /// 嵌入者去命名一个我们没承诺的类型（等于把内部布局漏出去）。索引由 [`open`] 给出。
    pub(crate) fn new(store: Arc<RwLock<Store>>) -> Self {
        Self { store }
    }

    /// 索引里既没有会话也没有消息。账号库是空的、或没读到时为真。
    pub fn is_empty(&self) -> bool {
        self.store.read().convs.values().all(|c| c.msgs.is_empty())
    }

    /// 全部会话摘要，按 `(chat_type, talker)` 升序 —— 顺序稳定，便于调用方做 diff。
    pub fn conversations(&self) -> Vec<ConversationSummary> {
        let store = self.store.read();
        let mut out: Vec<ConversationSummary> = store
            .convs
            .values()
            .map(|c| ConversationSummary {
                chat_type: c.chat_type,
                talker: c.talker.clone(),
                name: c.name.clone(),
                message_count: c.msgs.len(),
            })
            .collect();
        // 按 `as_str()` 排而不是派生 `Ord`：`ChatType` 没有 `Ord`，而给它加一个会把「枚举声明
        // 顺序」变成对外可见的排序语义 —— 那是实现细节，不该被承诺面依赖。用字符串形式排，
        // 结果与 JSON 里看到的一致，也不会因为将来调整枚举顺序而变。
        out.sort_by(|a, b| (a.chat_type.as_str(), &a.talker).cmp(&(b.chat_type.as_str(), &b.talker)));
        out
    }

    /// 一个会话的消息，按 `(ts, rowid)` 升序。未知会话返回空。
    ///
    /// **返回的是副本，且是整段会话** —— 大群可能是几千条。只要最近几条的调用方自己 `take`
    /// 即可，但代价已经付过了（这个面刻意不引入分页：分页状态该由调用方持有，而它想要的切片
    /// 方式未必和我们猜的一样）。
    pub fn messages(&self, chat_type: ChatType, talker: &str) -> Vec<MessageRecord> {
        let store = self.store.read();
        let mut out = store
            .conversation(chat_type, talker)
            .map(|c| c.msgs.clone())
            .unwrap_or_default();
        out.sort_by_key(|m| (m.ts, m.rowid));
        out
    }

    /// 会话的展示名（取不到时为空串）。
    pub fn display_name(&self, chat_type: ChatType, talker: &str) -> String {
        self.store.read().display_name(chat_type, talker)
    }

    /// 一条消息发送者的展示名。
    ///
    /// **群名片与全局昵称不是一回事**：名片只在它出现的那个群里显示，私聊与全局列表都不用它。
    pub fn display_sender(&self, chat_type: ChatType, talker: &str, uid: &str) -> String {
        self.store.read().display_sender(chat_type, talker, uid)
    }

    /// 某人在某个群里的群名片（`40090`）；没有时为空串。
    pub fn group_card(&self, chat_type: ChatType, talker: &str, uid: &str) -> String {
        let store = self.store.read();
        store
            .group_cards
            .get(&conv_key(chat_type, talker))
            .and_then(|m| m.get(uid))
            .cloned()
            .unwrap_or_default()
    }
}
