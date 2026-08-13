//! In-memory index over decrypted chat records.
//!
//! Design rationale (inherited from QQFlow): nt_msg.db message columns have
//! no useful indexes, so SQL filtering would mean a full table scan per
//! query (30-60 s on a 190 MB database). Instead we scan each table once at
//! startup into a HashMap index and keep it incrementally updated by the
//! poller — a single source of truth for both HTTP queries and SSE events.

pub mod index;
pub mod query;

use std::collections::HashMap;
use std::sync::Arc;

use parking_lot::RwLock;

use crate::parser::types::{ChatType, MessageRecord};
use crate::sync;

/// Key for the conversation map: "g:<groupId>" or "c:<peerUid>".
pub fn conv_key(chat_type: ChatType, talker: &str) -> String {
    match chat_type {
        ChatType::Group => format!("g:{talker}"),
        ChatType::C2c => format!("c:{talker}"),
    }
}

#[derive(Debug, Default)]
pub struct Conversation {
    pub chat_type: ChatType,
    pub talker: String,
    /// Display name: group name (from "修改群名" system messages) or peer nickname.
    pub name: String,
    /// Messages ordered by (ts, rowid); `dirty` marks append-only changes
    /// that need a lazy re-sort before querying.
    pub msgs: Vec<MessageRecord>,
    pub dirty: bool,
}

impl Conversation {
    pub fn ensure_sorted(&mut self) {
        if self.dirty {
            self.msgs.sort_by_key(|m| (m.ts, m.rowid));
            self.dirty = false;
        }
    }
}

#[derive(Debug, Default)]
pub struct Store {
    pub convs: HashMap<String, Conversation>,
    /// uid -> latest known nickname ("40093").
    pub uid_names: HashMap<String, String>,
    /// Highest rowid seen per table (poller watermark).
    pub watermark_group: i64,
    pub watermark_c2c: i64,
}

impl Store {
    pub fn conversation(&self, chat_type: ChatType, talker: &str) -> Option<&Conversation> {
        self.convs.get(&conv_key(chat_type, talker))
    }

    /// Look up a conversation by talker string, falling back to the other
    /// chat type when the primary classification misses (an all-digit c2c
    /// peer uid would otherwise only ever be looked up under `g:`).
    pub fn find_conversation(&self, talker: &str) -> Option<&Conversation> {
        let (primary, _) = crate::store::query::classify_talker(talker);
        let alt = match primary {
            ChatType::Group => ChatType::C2c,
            ChatType::C2c => ChatType::Group,
        };
        self.conversation(primary, talker)
            .or_else(|| self.conversation(alt, talker))
    }
}

/// Shared application state handed to the HTTP layer and poller tasks.
pub struct AppState {
    pub store: Arc<RwLock<Store>>,
    pub events: tokio::sync::broadcast::Sender<sync::Event>,
    /// One entry per loaded account: qq number -> readiness state.
    pub accounts: Arc<RwLock<Vec<crate::server::AccountState>>>,
    /// True once all account indexes are built.
    pub ready: Arc<std::sync::atomic::AtomicBool>,
    /// Access token (Bearer header / access_token query / POST body).
    pub token: Arc<String>,
    /// Per-account sync engines; powers the manual-sync endpoint and the
    /// change-driven poll tasks.
    pub sync: Arc<sync::SyncEngine>,
    /// Client-driven account registry (paths, watch config, shutdown).
    pub init: crate::server::AccountRegistry,
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn find_conversation_falls_back_to_other_type() {
        let mut store = Store::default();
        let conv = Conversation {
            chat_type: ChatType::C2c,
            talker: "12345".into(),
            name: "数字UID好友".into(),
            msgs: Vec::new(),
            dirty: false,
        };
        store.convs.insert(conv_key(ChatType::C2c, "12345"), conv);
        let found = store
            .find_conversation("12345")
            .expect("all-digit c2c peer must resolve via fallback");
        assert_eq!(found.chat_type, ChatType::C2c);
    }
}
