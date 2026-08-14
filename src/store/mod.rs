//! In-memory index over decrypted chat records.
//!
//! Design rationale (inherited from QQFlow): nt_msg.db message columns have
//! no useful indexes, so SQL filtering would mean a full table scan per
//! query (30-60 s on a 190 MB database). Instead we scan each table once at
//! startup into a HashMap index and keep it incrementally updated by the
//! poller — a single source of truth for both HTTP queries and SSE events.

pub mod index;
pub mod media;
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
    /// uid -> remark name (联系人备注). Ground truth: `profile_info.db`
    /// `20009` (QQDecrypt field id, `classify_remark` 绕过 CJK 门槛)。
    /// Loaded when the hint fires, empty otherwise.
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
    /// uid -> latest known nickname ("40093") — the context-free global
    /// fallback. Group cards ("40090") never enter here: they only display
    /// inside the conversation they were seen in (`group_cards`).
    pub uid_names: HashMap<String, String>,
    /// uid/群 name maps from the mapping sources (see `names`).
    pub names: NameMaps,
    /// Group cards ("40090") per conversation: conv_key -> (uid -> card).
    /// Display scope is the group the card was seen in (SSE source_name,
    /// chatlab members) — never c2c chats or the global contact lists.
    pub group_cards: HashMap<String, HashMap<String, String>>,
    /// Media lookup: md5 hex / uuid -> local cache file, built from the
    /// structured media metadata at index time. First-wins, but a stale
    /// entry (QQ cleared its cache) is refreshed by a later row with a
    /// live local path — see `index::apply_record`.
    pub media: HashMap<String, MediaEntry>,
    /// Cache-index fallback snapshot: files under nt_data's media dirs
    /// keyed by stem (see `media::scan_cache_index`). Consulted by
    /// `index::apply_record` when a media row has no usable "45812" path —
    /// pure map lookups there; the walk itself runs once per registration
    /// and again on manual sync.
    pub media_fallback: Option<media::CacheIndex>,
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
    /// c2c: remark (20009) > session name (conv.name = 首行 40093) >
    /// profile nick (uid_nick) > uid; group: group remark (60026, QQ 客户端
    /// 也优先显示备注) > rename-message name (conv.name, 仅真实改名名) >
    /// group-info name (group_name) > group id.
    pub fn display_name(&self, chat_type: ChatType, talker: &str) -> String {
        let remark = match chat_type {
            ChatType::C2c => self.names.uid_remark.get(talker),
            ChatType::Group => self.names.group_remark.get(talker),
        };
        if let Some(name) = remark.filter(|s| !s.is_empty()) {
            return name.clone();
        }
        if let Some(name) = self
            .conversation(chat_type, talker)
            .map(|c| c.name.as_str())
            .filter(|s| !s.is_empty())
            // 群的 conv.name 以群号占位（未改名群）——占位不参与显示，
            // 否则群号会压过群信息库群名。
            .filter(|s| chat_type == ChatType::C2c || *s != talker)
        {
            return name.to_string();
        }
        let nick = match chat_type {
            ChatType::C2c => self.names.uid_nick.get(talker),
            ChatType::Group => self.names.group_name.get(talker),
        };
        if let Some(name) = nick.filter(|s| !s.is_empty()) {
            return name.clone();
        }
        talker.to_string()
    }

    /// Person display name: remark (20009) > latest message nick
    /// (uid_names) > profile nick (uid_nick) > uid.
    pub fn display_uid(&self, uid: &str) -> String {
        self.names
            .uid_remark
            .get(uid)
            .filter(|s| !s.is_empty())
            .or_else(|| self.uid_names.get(uid).filter(|s| !s.is_empty()))
            .or_else(|| self.names.uid_nick.get(uid).filter(|s| !s.is_empty()))
            .cloned()
            .unwrap_or_else(|| uid.to_string())
    }

    /// Sender display name in a conversation context. In a group, the
    /// sender's group card ("40090") for THIS conversation wins over the
    /// global name (remark 20009 > message nick > profile nick) — cards are
    /// per-group and must never leak into c2c chats or contacts.
    pub fn display_sender(&self, chat_type: ChatType, talker: &str, uid: &str) -> String {
        if chat_type == ChatType::Group
            && let Some(card) = self
                .group_cards
                .get(&conv_key(chat_type, talker))
                .and_then(|cards| cards.get(uid))
                .filter(|s| !s.is_empty())
        {
            return card.clone();
        }
        self.display_uid(uid)
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
    fn display_name_c2c_prefers_remark_over_conv_name_over_profile_nick_over_uid() {
        let mut store = Store::default();
        store
            .convs
            .insert(conv_key(ChatType::C2c, "u_a"), conv(ChatType::C2c, "u_a", "张三"));
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三", "会话名(首行 40093) only");

        store.names.uid_nick.insert("u_a".into(), "档案昵称".into());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三", "会话名 wins over 档案昵称");

        store.names.uid_remark.insert("u_a".into(), "张三备注".into());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三备注", "remark wins");

        store.names.uid_remark.insert("u_a".into(), String::new());
        assert_eq!(store.display_name(ChatType::C2c, "u_a"), "张三", "empty remark falls back to 会话名");

        // 无会话记录 -> 档案昵称
        let mut store2 = Store::default();
        store2.names.uid_nick.insert("u_b".into(), "档案昵称".into());
        assert_eq!(store2.display_name(ChatType::C2c, "u_b"), "档案昵称", "no conversation -> profile nick");

        assert_eq!(store.display_name(ChatType::C2c, "u_unknown"), "u_unknown", "no data -> uid");
    }

    #[test]
    fn display_name_group_prefers_remark_over_rename_msg_over_info_name_over_id() {
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
            "改名消息群名",
            "rename-message name wins over group-info name"
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
            "改名消息群名",
            "empty group remark falls back to rename-message name"
        );

        // 未改名群：conv.name 是群号占位，不得压过群信息库群名。
        store
            .convs
            .insert(conv_key(ChatType::Group, "10002"), conv(ChatType::Group, "10002", "10002"));
        store.names.group_name.insert("10002".into(), "群信息库名".into());
        assert_eq!(
            store.display_name(ChatType::Group, "10002"),
            "群信息库名",
            "placeholder group id never shadows the info name"
        );

        assert_eq!(store.display_name(ChatType::Group, "99999"), "99999", "no data -> group id");
    }

    #[test]
    fn display_uid_prefers_remark_over_message_nick_over_profile_nick_over_uid() {
        let mut store = Store::default();
        store.uid_names.insert("u_a".into(), "张三".into());
        assert_eq!(store.display_uid("u_a"), "张三", "message nick only");

        store.names.uid_nick.insert("u_a".into(), "档案昵称".into());
        assert_eq!(store.display_uid("u_a"), "张三", "message nick wins over profile nick");

        store.names.uid_remark.insert("u_a".into(), "张三备注".into());
        assert_eq!(store.display_uid("u_a"), "张三备注", "remark wins");

        store.names.uid_remark.insert("u_a".into(), String::new());
        assert_eq!(store.display_uid("u_a"), "张三", "empty remark falls back to message nick");

        assert_eq!(store.display_uid("u_zzz"), "u_zzz", "no data -> uid");
    }

    #[test]
    fn display_sender_card_stays_inside_its_group() {
        let mut store = Store::default();
        store.uid_names.insert("u_a".into(), "张三".into());
        store
            .group_cards
            .insert(conv_key(ChatType::Group, "10001").into(), {
                let mut m = HashMap::new();
                m.insert("u_a".to_string(), "1群名片".to_string());
                m
            });
        store
            .group_cards
            .insert(conv_key(ChatType::Group, "10002").into(), {
                let mut m = HashMap::new();
                m.insert("u_a".to_string(), "2群名片".to_string());
                m
            });
        assert_eq!(
            store.display_sender(ChatType::Group, "10001", "u_a"),
            "1群名片",
            "card for THIS conversation"
        );
        assert_eq!(
            store.display_sender(ChatType::Group, "10002", "u_a"),
            "2群名片",
            "different group, different card"
        );
        assert_eq!(
            store.display_sender(ChatType::Group, "10003", "u_a"),
            "张三",
            "no card in this group -> global name"
        );
        assert_eq!(
            store.display_sender(ChatType::C2c, "u_a", "u_a"),
            "张三",
            "cards never leak into c2c chats"
        );
        assert_eq!(store.display_uid("u_a"), "张三", "global display ignores cards");
    }
}
