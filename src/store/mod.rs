//! In-memory index over decrypted chat records.
//!
//! Design rationale (inherited from QQFlow): nt_msg.db message columns have
//! no useful indexes, so SQL filtering would mean a full table scan per
//! query (30-60 s on a 190 MB database). Instead we scan each table once at
//! startup into a HashMap index and keep it incrementally updated by the
//! poller — a single source of truth for both HTTP queries and SSE events.

pub mod index;
pub mod media_export;
pub mod names;
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

/// uid/群 name maps loaded from QQ's mapping sources (see `names`) —
/// best-effort: on schema churn these stay empty and display falls back to
/// message-derived names.
#[derive(Debug, Default)]
pub struct NameMaps {
    /// uid -> remark name (联系人备注). Ground truth on current QQ
    /// versions: no readable table stores remarks — this stays empty and
    /// display falls back to the nicknames.
    pub uid_remark: HashMap<String, String>,
    /// uid -> authoritative contact nickname (联系人档案 `20002`).
    pub uid_nick: HashMap<String, String>,
    /// uid -> bare QQ number, when the version exposes it (optional).
    pub uid_qq: HashMap<String, String>,
    /// groupId -> group name, from a group-info source (sibling DB or
    /// mapping tables inside nt_msg.db).
    pub group_name: HashMap<String, String>,
    /// groupId -> group remark (loaded but not exposed in v1).
    pub group_remark: HashMap<String, String>,
}

/// One fetchable media file (served by /api/v1/media/{id}).
#[derive(Debug, Clone)]
pub struct MediaEntry {
    /// Local cache path ("45812") — absolute, or relative to `media_root`.
    pub local_path: String,
    pub file_name: Option<String>,
}

#[derive(Debug, Default)]
pub struct Store {
    pub convs: HashMap<String, Conversation>,
    /// uid -> latest known nickname ("40093").
    pub uid_names: HashMap<String, String>,
    /// uid/群 name maps from the mapping sources (see `names`).
    pub names: NameMaps,
    /// Media lookup: md5 hex / uuid -> local cache file, built from the
    /// structured media metadata at index time (first-wins).
    pub media: HashMap<String, MediaEntry>,
    /// `nt_data` root of the account — relative "45812" paths resolve here.
    pub media_root: Option<std::path::PathBuf>,
    /// Highest rowid seen per table (poller watermark).
    pub watermark_group: i64,
    pub watermark_c2c: i64,
}

impl Store {
    pub fn conversation(&self, chat_type: ChatType, talker: &str) -> Option<&Conversation> {
        self.convs.get(&conv_key(chat_type, talker))
    }

    /// Session display name (pure lookup — never mutates `conv.name`):
    /// c2c: remark > profile nick (uid_nick) > message nick (conv.name) > uid;
    /// group: group remark (60026, QQ 客户端也优先显示备注) > group-info
    /// name > rename-message name (conv.name) > group id.
    pub fn display_name(&self, chat_type: ChatType, talker: &str) -> String {
        let from_map = match chat_type {
            ChatType::C2c => self
                .names
                .uid_remark
                .get(talker)
                .filter(|s| !s.is_empty())
                .or_else(|| self.names.uid_nick.get(talker).filter(|s| !s.is_empty())),
            ChatType::Group => self
                .names
                .group_remark
                .get(talker)
                .filter(|s| !s.is_empty())
                .or_else(|| self.names.group_name.get(talker).filter(|s| !s.is_empty())),
        };
        if let Some(name) = from_map.filter(|s| !s.is_empty()) {
            return name.clone();
        }
        if let Some(name) = self
            .conversation(chat_type, talker)
            .map(|c| c.name.clone())
            .filter(|s| !s.is_empty())
        {
            return name;
        }
        talker.to_string()
    }

    /// Person display name: remark > profile nick (uid_nick) > latest
    /// message nick (uid_names) > uid.
    pub fn display_uid(&self, uid: &str) -> String {
        self.names
            .uid_remark
            .get(uid)
            .filter(|s| !s.is_empty())
            .or_else(|| self.names.uid_nick.get(uid).filter(|s| !s.is_empty()))
            .or_else(|| self.uid_names.get(uid).filter(|s| !s.is_empty()))
            .cloned()
            .unwrap_or_else(|| uid.to_string())
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
    /// Media export root (`media=1` on /api/v1/messages copies here, WeFlow
    /// exportPath semantics); `--media-export-dir`, default `<data-dir>/api-media`.
    pub export_root: Arc<std::path::PathBuf>,
    /// Base URL for exported media links (`http://{host}:{port}`).
    pub base_url: Arc<String>,
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

    fn conv(chat_type: ChatType, talker: &str, name: &str) -> Conversation {
        Conversation {
            chat_type,
            talker: talker.into(),
            name: name.into(),
            msgs: Vec::new(),
            dirty: false,
        }
    }

    #[test]
    fn display_name_c2c_prefers_remark_over_profile_nick_over_nick_over_uid() {
        let mut store = Store::default();
        store
            .convs
            .insert(conv_key(ChatType::C2c, "u_a"), conv(ChatType::C2c, "u_a", "张三"));
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三", "message nick only");

        store.names.uid_nick.insert("u_a".into(), "档案昵称".into());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "档案昵称", "profile nick wins");

        store.names.uid_remark.insert("u_a".into(), "张三备注".into());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三备注", "remark wins");

        store.names.uid_remark.insert("u_a".into(), String::new());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "档案昵称", "empty remark falls back");

        assert_eq!(store.display_name(ChatType::C2c, "u_unknown"), "u_unknown", "no data -> uid");
    }

    #[test]
    fn display_name_group_prefers_remark_over_info_name_over_rename_msg_over_id() {
        let mut store = Store::default();
        store
            .convs
            .insert(conv_key(ChatType::Group, "10001"), conv(ChatType::Group, "10001", "改名消息群名"));
        assert_eq!(
            store.display_name(ChatType::Group, "10001"),
            "改名消息群名",
            "rename-message name"
        );

        store.names.group_name.insert("10001".into(), "群信息库群名".into());
        assert_eq!(
            store.display_name(ChatType::Group, "10001"),
            "群信息库群名",
            "group-info name wins over rename message"
        );

        store.names.group_remark.insert("10001".into(), "群备注名".into());
        assert_eq!(
            store.display_name(ChatType::Group, "10001"),
            "群备注名",
            "group remark wins (QQ 客户端行为)"
        );

        store.names.group_remark.insert("10001".into(), String::new());
        assert_eq!(
            store.display_name(ChatType::Group, "10001"),
            "群信息库群名",
            "empty group remark falls back"
        );

        assert_eq!(store.display_name(ChatType::Group, "99999"), "99999", "no data -> group id");
    }

    #[test]
    fn display_uid_prefers_remark_over_profile_nick_over_message_nick_over_uid() {
        let mut store = Store::default();
        store.uid_names.insert("u_a".into(), "张三".into());
        assert_eq!(store.display_uid("u_a"), "张三", "message nick only");

        store.names.uid_nick.insert("u_a".into(), "档案昵称".into());
        assert_eq!(store.display_uid("u_a"), "档案昵称", "profile nick wins");

        store.names.uid_remark.insert("u_a".into(), "张三备注".into());
        assert_eq!(store.display_uid("u_a"), "张三备注", "remark wins");

        assert_eq!(store.display_uid("u_zzz"), "u_zzz", "no data -> uid");
    }
}
