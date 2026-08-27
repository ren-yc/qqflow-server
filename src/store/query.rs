//! Read-side queries over the in-memory index (WeFlow-compatible shapes).

use crate::parser::types::{ChatType, MediaInfo};
use crate::store::Store;

/// WeFlow-style session row.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct SessionInfo {
    pub username: String,
    pub display_name: String,
    pub r#type: i64,
    pub last_timestamp: i64,
    pub unread_count: i64,
}

/// WeFlow-style message row.
#[derive(Debug, serde::Serialize)]
#[serde(rename_all = "camelCase")]
pub struct MessageOut {
    pub local_id: i64,
    pub server_id: String,
    pub local_type: i64,
    pub create_time: i64,
    pub is_send: i64,
    pub sender_username: String,
    /// Resolved sender display name (WeFlow `senderName`): group card ("40090")
    /// for THIS conversation > remark ("20009") > message nick > profile nick >
    /// UID. Needs the name maps, so only [`shape_record`] fills it;
    /// `from_record` leaves it empty. Saves every client from rebuilding the
    /// same mapping out of /api/v1/contacts + /api/v1/group-members.
    pub sender_name: String,
    pub content: String,
    pub raw_content: String,
    pub parsed_content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
    /// Structured media metadata (image/voice/video); absent for text etc.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media: Option<crate::parser::types::MediaInfo>,
    /// Media store key (md5 hex or uuid) — fetch bytes via /api/v1/media/{id}.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_id: Option<String>,
    /// WeFlow media-export fields — filled by the messages handler when
    /// `media=1` exports this page's media (absent otherwise).
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_file_name: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_url: Option<String>,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_local_path: Option<String>,
}

impl MessageOut {
    pub fn from_record(r: &crate::parser::types::MessageRecord) -> Self {
        Self {
            local_id: r.rowid,
            server_id: r.seq.to_string(),
            local_type: r.parsed.msg_type.code(),
            create_time: r.ts,
            // Direction from the "40013" column when present; degrades to 0
            // when the QQ version lacks it.
            is_send: r.direction.map(crate::parser::types::direction_to_is_send).unwrap_or(0),
            sender_username: r.from_uid.clone(),
            // Left empty here: resolving it needs the store's name maps, which
            // a plain record conversion has no access to. `shape_record` fills
            // it, and every emitting path goes through that.
            sender_name: String::new(),
            content: r.parsed.content.clone(),
            raw_content: r.parsed.content.clone(),
            parsed_content: r.parsed.content.clone(),
            media_type: r.parsed.msg_type.media_type_str().map(String::from),
            media: r.parsed.media.clone(),
            media_id: r.parsed.media.as_ref().and_then(|m| m.key()).map(str::to_string),
            media_file_name: None,
            media_url: None,
            media_local_path: None,
        }
    }
}

/// The one way a record becomes an API row: convert, then apply every
/// store-dependent field (`mediaId` fetchability, `senderName`).
///
/// Both emitting paths — the messages query and the manual-sync response —
/// call this, so neither can advertise an unfetchable `mediaId` nor return an
/// empty `senderName` while the other fills it.
///
/// `senderName` resolves against `(rec.chat_type, rec.talker)`, the record's
/// own conversation key (see `index::apply_record`), because the group card
/// ("40090") is scoped to one group and must not leak across conversations.
pub fn shape_record(store: &Store, rec: &crate::parser::types::MessageRecord) -> MessageOut {
    let mut row = with_fetchable_media_id(store, MessageOut::from_record(rec));
    row.sender_name = store.display_sender(rec.chat_type, &rec.talker, &row.sender_username);
    row
}

/// `mediaId` advertises a fetchable /api/v1/media/{id} — the store only
/// registers media with a local cache path ("45812"), so a key with no
/// entry would guarantee a 404. Omit it instead (the `media` object still
/// carries the md5/uuid for reference). Applied by every message-emitting
/// path (messages query and the manual-sync endpoint) so the promise can
/// never be made by one and broken by the other.
pub fn with_fetchable_media_id(store: &Store, mut row: MessageOut) -> MessageOut {
    if let Some(id) = row.media_id.as_deref()
        && !store.media.contains_key(id)
    {
        row.media_id = None;
    }
    row
}

/// The single fetchability rule behind every `mediaId` in the API: a media
/// key is advertised only when the store has registered a live local path
/// for it (REST rows via [`with_fetchable_media_id`], SSE events directly).
/// One rule, two channels — an advertised key is always servable.
pub(crate) fn fetchable_media_id(store: &Store, m: &MediaInfo) -> Option<String> {
    let key = m.key()?;
    store.media.contains_key(key).then(|| key.to_string())
}

pub struct MessageQuery<'a> {
    pub talker: &'a str,
    pub limit: usize,
    pub offset: usize,
    pub start: Option<i64>,
    pub end: Option<i64>,
    pub keyword: Option<&'a str>,
}

/// Messages for one session, newest first (WeFlow semantics), with hasMore.
pub fn query_messages(store: &Store, q: &MessageQuery) -> (Vec<MessageOut>, bool) {
    // find_conversation also probes the other chat type, so an all-digit
    // c2c peer uid (which classifies as "group") still resolves.
    let Some(conv) = store.find_conversation(q.talker) else {
        return (Vec::new(), false);
    };
    // Work on a sorted snapshot of indexes.
    let mut idx: Vec<usize> = conv.msgs.iter().enumerate().map(|(i, _)| i).collect();
    idx.sort_by(|&a, &b| {
        let x = &conv.msgs[a];
        let y = &conv.msgs[b];
        (y.ts, y.rowid).cmp(&(x.ts, x.rowid)) // newest first
    });

    let kw = q.keyword.map(|k| k.to_lowercase());
    let mut out = Vec::new();
    let mut skipped = 0usize;
    let mut has_more = false;
    for i in idx {
        let m = &conv.msgs[i];
        if let Some(s) = q.start
            && m.ts < s {
                continue;
            }
        if let Some(e) = q.end
            && m.ts > e {
                continue;
            }
        if let Some(k) = &kw
            && !m.parsed.content.to_lowercase().contains(k.as_str()) {
                continue;
            }
        if skipped < q.offset {
            skipped += 1;
            continue;
        }
        if out.len() >= q.limit {
            has_more = true;
            break;
        }
        out.push(shape_record(store, m));
    }
    (out, has_more)
}

/// Newest message ts of a conversation. Appends do not re-sort `msgs`, so
/// the newest row is the max over the vec, not the tail.
fn conv_last_ts(c: &crate::store::Conversation) -> i64 {
    c.msgs.iter().map(|m| m.ts).max().unwrap_or(0)
}

/// Sessions sorted by last message time (newest first). Display names
/// resolve through the name maps (remark / group-info > message-derived).
pub fn query_sessions(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> Vec<SessionInfo> {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut all: Vec<&crate::store::Conversation> = store.convs.values().collect();
    all.sort_by_key(|c| std::cmp::Reverse(conv_last_ts(c)));
    all.into_iter()
        .filter(|c| {
            if let Some(k) = &kw {
                store.display_name(c.chat_type, &c.talker).to_lowercase().contains(k.as_str())
                    || c.talker.to_lowercase().contains(k.as_str())
            } else {
                true
            }
        })
        .skip(offset)
        .take(limit)
        .map(|c| SessionInfo {
            username: c.talker.clone(),
            display_name: store.display_name(c.chat_type, &c.talker),
            r#type: c.chat_type.weflow_code(),
            last_timestamp: conv_last_ts(c),
            unread_count: 0,
        })
        .collect()
}

/// Contacts: every UID known to chat or to the name maps (a profile-only
/// uid with no chat history appears too — that is the point of the
/// mapping), with nickname (profile > message-derived), remark, and the
/// QQ number exposed in the WeFlow `alias` slot.
///
/// Returns `(page, total_after_filter)`: the caller needs the pre-pagination
/// count to answer `hasMore` without running the query twice.
pub fn query_contacts(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> (Vec<crate::server::handlers::contacts::ContactOut>, usize) {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut uid_set: std::collections::BTreeSet<&String> = store.uid_names.keys().collect();
    uid_set.extend(store.names.uid_remark.keys());
    uid_set.extend(store.names.uid_nick.keys());
    let mut rows: Vec<crate::server::handlers::contacts::ContactOut> = uid_set
        .into_iter()
        .map(|uid| {
            let nick = store
                .names
                .uid_nick
                .get(uid)
                .or_else(|| store.uid_names.get(uid))
                .cloned()
                .unwrap_or_default();
            crate::server::handlers::contacts::ContactOut {
                username: uid.clone(),
                display_name: store.display_uid(uid),
                nickname: nick,
                remark: store.names.uid_remark.get(uid).cloned().unwrap_or_default(),
                // WeFlow's alias slot carries the QQ number here (migrated
                // from the old `qq` field; empty when the version lacks a
                // uid->QQ mapping source).
                alias: store.names.uid_qq.get(uid).cloned().unwrap_or_default(),
                avatar_url: String::new(),
                r#type: "friend".into(),
            }
        })
        .collect();
    // Sort by (display_name, username): display names are not unique, so a
    // display-name-only key leaves ties in arbitrary order between requests and
    // offset paging would skip or repeat those rows.
    rows.sort_by(|a, b| {
        a.display_name
            .to_lowercase()
            .cmp(&b.display_name.to_lowercase())
            .then_with(|| a.username.cmp(&b.username))
    });
    let filtered: Vec<_> = rows
        .into_iter()
        .filter(|c| {
            if let Some(k) = &kw {
                c.username.to_lowercase().contains(k.as_str())
                    || c.display_name.to_lowercase().contains(k.as_str())
                    || c.nickname.to_lowercase().contains(k.as_str())
            } else {
                true
            }
        })
        .collect();
    let total = filtered.len();
    (
        filtered.into_iter().skip(offset).take(limit).collect(),
        total,
    )
}

/// Distinguish group ids from peer uids: groups are all-digit QQ group
/// numbers in "40021"; c2c peers are "u_..." style uids. Fallback: try
/// group first, then c2c.
pub fn classify_talker(talker: &str) -> (ChatType, &str) {
    if talker.starts_with("u_") || talker.starts_with('u') && talker.len() > 4 && !talker.chars().all(|c| c.is_ascii_digit()) {
        (ChatType::C2c, talker)
    } else if talker.chars().all(|c| c.is_ascii_digit()) {
        // All-digit: could be a group number (common case for QQ groups).
        (ChatType::Group, talker)
    } else {
        (ChatType::C2c, talker)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::parser::types::{MessageRecord, MsgType, ParsedMessage};
    use crate::store::{conv_key, Conversation, Store};

    fn rec(rowid: i64, ts: i64) -> MessageRecord {
        MessageRecord {
            rowid,
            seq: (ts << 32) | rowid,
            ts,
            chat_type: ChatType::Group,
            talker: "10001".into(),
            from_uid: "u_a".into(),
            from_nick: "张三".into(),
            card: None,
            direction: Some(0),
            parsed: ParsedMessage { msg_type: MsgType::Text, content: "x".into(), media: None },
        }
    }

    #[test]
    fn sessions_last_ts_is_max_not_tail() {
        let mut store = Store::default();
        // Sorted at build, then a backfilled older row lands at the tail —
        // last_timestamp must still be the newest ts (max), not the tail.
        let conv = Conversation {
            chat_type: ChatType::Group,
            talker: "10001".into(),
            name: "项目群".into(),
            msgs: vec![rec(1, 200), rec(2, 100)],
            dirty: false,
        };
        store.convs.insert(conv_key(ChatType::Group, "10001"), conv);
        let sessions = query_sessions(&store, None, 10, 0);
        assert_eq!(sessions.len(), 1);
        assert_eq!(sessions[0].last_timestamp, 200, "newest ts, not the unsorted tail");
    }
}
