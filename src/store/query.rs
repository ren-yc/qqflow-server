//! Read-side queries over the in-memory index (WeFlow-compatible shapes).

use crate::parser::types::ChatType;
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
    pub content: String,
    pub raw_content: String,
    pub parsed_content: String,
    #[serde(skip_serializing_if = "Option::is_none")]
    pub media_type: Option<String>,
}

impl MessageOut {
    pub fn from_record(r: &crate::parser::types::MessageRecord) -> Self {
        Self {
            local_id: r.rowid,
            server_id: r.seq.to_string(),
            local_type: r.parsed.msg_type.code(),
            create_time: r.ts,
            is_send: 0, // v1 limitation: sender direction is not reliably derivable
            sender_username: r.from_uid.clone(),
            content: r.parsed.content.clone(),
            raw_content: r.parsed.content.clone(),
            parsed_content: r.parsed.content.clone(),
            media_type: match r.parsed.msg_type {
                crate::parser::types::MsgType::Image => Some("image".into()),
                crate::parser::types::MsgType::Voice => Some("voice".into()),
                crate::parser::types::MsgType::Video => Some("video".into()),
                _ => None,
            },
        }
    }
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
    let (chat_type, talker) = classify_talker(q.talker);
    let Some(conv) = store.conversation(chat_type, talker) else {
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
        out.push(MessageOut::from_record(m));
    }
    (out, has_more)
}

/// Sessions sorted by last message time (newest first).
pub fn query_sessions(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> Vec<SessionInfo> {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut all: Vec<&crate::store::Conversation> = store.convs.values().collect();
    all.sort_by_key(|c| std::cmp::Reverse(c.last_ts()));
    all.into_iter()
        .filter(|c| {
            if let Some(k) = &kw {
                c.name.to_lowercase().contains(k.as_str())
                    || c.talker.to_lowercase().contains(k.as_str())
            } else {
                true
            }
        })
        .skip(offset)
        .take(limit)
        .map(|c| SessionInfo {
            username: c.talker.clone(),
            display_name: c.name.clone(),
            r#type: c.chat_type.weflow_code(),
            last_timestamp: c.last_ts(),
            unread_count: 0,
        })
        .collect()
}

/// Contacts: every UID that appeared in chat, with the latest nickname.
pub fn query_contacts(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> Vec<crate::server::handlers::contacts::ContactOut> {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut rows: Vec<crate::server::handlers::contacts::ContactOut> = store
        .uid_names
        .iter()
        .map(|(uid, nick)| crate::server::handlers::contacts::ContactOut {
            username: uid.clone(),
            display_name: nick.clone(),
            nickname: nick.clone(),
            remark: String::new(),
            alias: String::new(),
            avatar_url: String::new(),
            r#type: "friend".into(),
        })
        .collect();
    rows.sort_by_key(|a| a.display_name.to_lowercase());
    rows.into_iter()
        .filter(|c| {
            if let Some(k) = &kw {
                c.username.to_lowercase().contains(k.as_str())
                    || c.display_name.to_lowercase().contains(k.as_str())
                    || c.nickname.to_lowercase().contains(k.as_str())
            } else {
                true
            }
        })
        .skip(offset)
        .take(limit)
        .collect()
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
