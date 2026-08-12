//! Parsed message types.

use serde::Serialize;

/// Message category recognized by the heuristic parser.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
#[serde(rename_all = "lowercase")]
pub enum MsgType {
    Text,
    Image,
    Voice,
    Video,
    Recall,
    System,
    Other,
}

impl MsgType {
    /// Numeric `localType` used by WeFlow-style clients.
    pub fn code(self) -> i64 {
        match self {
            MsgType::Text => 0,
            MsgType::Image => 3,
            MsgType::Voice => 4,
            MsgType::Video => 5,
            MsgType::Recall => 6,
            MsgType::System => 7,
            MsgType::Other => 1,
        }
    }
}

/// Result of parsing one message BLOB.
#[derive(Debug, Clone, Serialize)]
pub struct ParsedMessage {
    pub msg_type: MsgType,
    pub content: String,
}

/// A single chat record (row from group_msg_table / c2c_msg_table).
#[derive(Debug, Clone)]
pub struct MessageRecord {
    pub rowid: i64,
    /// Column "40001": message seq (high 32 bits carry the unix timestamp).
    pub seq: i64,
    pub ts: i64,
    pub chat_type: ChatType,
    /// Group: group id ("40021"); c2c: peer uid ("40020").
    pub talker: String,
    /// Sender uid ("40020" in group table; c2c peer for c2c table).
    pub from_uid: String,
    /// Sender nickname ("40093").
    pub from_nick: String,
    pub parsed: ParsedMessage,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize, Default)]
#[serde(rename_all = "lowercase")]
pub enum ChatType {
    #[default]
    Group,
    C2c,
}

impl ChatType {
    /// WeFlow session `type`: 2 = group, 1 = private.
    pub fn weflow_code(self) -> i64 {
        match self {
            ChatType::Group => 2,
            ChatType::C2c => 1,
        }
    }
    pub fn as_str(self) -> &'static str {
        match self {
            ChatType::Group => "group",
            ChatType::C2c => "private",
        }
    }
}

/// QQ NT stores the message time in the high 32 bits of the "40001" seq
/// value (verified against a real database: seq = (ts << 32) | low32).
pub fn seq_to_time(seq: i64) -> i64 {
    seq >> 32
}
