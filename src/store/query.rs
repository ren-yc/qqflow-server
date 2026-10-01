//! Read-side queries over the in-memory index (WeFlow-compatible shapes).

use std::collections::HashMap;

use crate::parser::types::{ChatType, MediaInfo, MessageRecord};
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
    /// 该会话在索引里的消息条数（= 真实条数，不是占位）。ChatLab 面的 messageCount 用它 ——
    /// 恒 0 的占位会让下游按它排序时拿到一列无意义的数字。
    pub message_count: usize,
}

/// WeFlow-style message row.
// `ToSchema` 只在服务层需要（schema 是 HTTP 面的东西）。核心面不该依赖 utoipa ——
// 否则 `--no-default-features` 丢不掉它。
#[cfg_attr(feature = "server", derive(utoipa::ToSchema))]
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
    /// 消息行上的媒体类型（image / voice / video）。键名是 type：它与媒体对象内部的名字空间
    /// 无关，改名的理由是「同一个概念在两面用两个名字」比位置差异更难记。
    #[serde(skip_serializing_if = "Option::is_none")]
    pub r#type: Option<String>,
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
    /// 本行的 mediaFileName 能不能当**句柄**用（导出批次填写，未导出恒 false）。
    ///
    /// 判据是「名字按内容唯一」：取自 store 键（md5 hex / uuid）或形状是 32 位十六进制摘要。
    /// 同名异内容的文件可能躺在别的会话里，而按名取字节是跨会话解析的 —— 只有这两类名字才
    /// 承诺「出现即可取」。
    ///
    /// 它**不序列化**：判据只服务于「这个名字能不能回填成句柄」，字段一旦下发就会有人拿它
    /// 当契约。
    #[serde(skip)]
    pub media_export_handle_ok: bool,
    /// `platformMessageId` of the message this one replies to.
    ///
    /// **Omitted when the target cannot be pinned down** — see
    /// [`resolve_reply_to`]. The key is absent rather than `null` so that a
    /// reader which trusts the declared type never receives a value it cannot
    /// use.
    #[serde(skip_serializing_if = "Option::is_none")]
    pub reply_to_message_id: Option<String>,
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
            r#type: r.parsed.msg_type.media_type_str().map(String::from),
            media: r.parsed.media.clone(),
            media_id: r.parsed.media.as_ref().and_then(|m| m.key()).map(str::to_string),
            media_file_name: None,
            media_url: None,
            media_local_path: None,
            media_export_handle_ok: false,
            // Filled by the query layer, which is the only place that can see
            // the whole conversation a candidate must be found in.
            reply_to_message_id: None,
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

/// `40003` -> every `(ts, seq)` carrying it in this conversation.
///
/// Built once per query: resolving row by row would be O(n²), and a long
/// conversation is exactly where a reply is most likely to exist.
/// `None` when no row in the conversation replies to anything, so the cost is
/// not paid for the common case.
pub(crate) fn build_inner_index(
    conv: &crate::store::Conversation,
) -> Option<HashMap<i64, Vec<(i64, i64)>>> {
    if !conv.msgs.iter().any(|m| m.reply_inner_seq.is_some()) {
        return None;
    }
    let mut map: HashMap<i64, Vec<(i64, i64)>> = HashMap::new();
    for m in &conv.msgs {
        if let Some(k) = m.inner_seq {
            map.entry(k).or_default().push((m.ts, m.seq));
        }
    }
    Some(map)
}

/// Resolve a reply target to a `platformMessageId`, or `None`.
///
/// **Deliberately conservative.** "40003" is not unique inside a conversation
/// (measured on a real database: 1371 repeating key groups, only 9 of them at
/// the same second), so the obvious `(conversation, "40003")` lookup can land
/// on several rows. Picking one would be worse than emitting nothing: a client
/// matches `replyToMessageId` against another message in the same conversation
/// and **cannot tell a wrong match from a right one**.
///
/// So a value is emitted only when exactly one candidate both carries the
/// wanted "40003" and is not newer than the reply. Measured on a real
/// database: 1522 of 1616 replies (94.2%) resolve uniquely; the rest omit the
/// field. The upstream field notes omit this ambiguity — the local database
/// shape is what the rule is built on.
pub(crate) fn resolve_reply_to(
    index: &HashMap<i64, Vec<(i64, i64)>>,
    m: &MessageRecord,
) -> Option<i64> {
    let want = m.reply_inner_seq?;
    let mut hit: Option<i64> = None;
    for (ts, seq) in index.get(&want)? {
        if *ts > m.ts {
            continue;
        }
        if hit.is_some() {
            return None; // more than one candidate: refuse to guess
        }
        hit = Some(*seq);
    }
    hit
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

    let inner_index = build_inner_index(conv);
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
        let mut row = shape_record(store, m);
        if let Some(index) = &inner_index {
            row.reply_to_message_id = resolve_reply_to(index, m).map(|s| s.to_string());
        }
        out.push(row);
    }
    (out, has_more)
}

/// Newest message ts of a conversation. Appends do not re-sort `msgs`, so
/// the newest row is the max over the vec, not the tail.
fn conv_last_ts(c: &crate::store::Conversation) -> i64 {
    c.msgs.iter().map(|m| m.ts).max().unwrap_or(0)
}

/// 会话是否匹配关键词。**这是唯一的过滤谓词**——`query_sessions` 与
/// `count_sessions` 共用它：两份各写一遍必然漂移，而漂移的表现就是
/// 「还有没有下一页」与实际返回的内容对不上。
fn matches_keyword(store: &Store, c: &crate::store::Conversation, kw: Option<&str>) -> bool {
    match kw {
        Some(k) => {
            store.display_name(c.chat_type, &c.talker).to_lowercase().contains(k)
                || c.talker.to_lowercase().contains(k)
        }
        None => true,
    }
}

/// 过滤后的会话总数：调用方用它判断「还有没有下一页」。不做切片。
pub fn count_sessions(store: &Store, keyword: Option<&str>) -> usize {
    let kw = keyword.map(|k| k.to_lowercase());
    store
        .convs
        .values()
        .filter(|c| matches_keyword(store, c, kw.as_deref()))
        .count()
}

/// Sessions sorted by last message time (newest first). Display names
/// resolve through the name maps (remark / group-info > message-derived).
pub fn query_sessions(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> Vec<SessionInfo> {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut all: Vec<&crate::store::Conversation> = store.convs.values().collect();
    all.sort_by_key(|c| std::cmp::Reverse(conv_last_ts(c)));
    all.into_iter()
        .filter(|c| matches_keyword(store, c, kw.as_deref()))
        .skip(offset)
        .take(limit)
        .map(|c| SessionInfo {
            username: c.talker.clone(),
            display_name: store.display_name(c.chat_type, &c.talker),
            r#type: c.chat_type.weflow_code(),
            last_timestamp: conv_last_ts(c),
            unread_count: 0,
            message_count: c.msgs.len(),
        })
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
            inner_seq: None,
            reply_inner_seq: None,
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
        assert_eq!(sessions[0].message_count, 2, "messageCount 是索引里的真实条数");
    }
}
