//! GET /api/v1/group-members — 群成员（**名册 ∪ 发言人**）＋ 可选的发言计数。
//!
//! 成员集合为什么要并名册：只列发言人会让「群里有谁」这个问题的答案取决于谁最近说过话 ——
//! 潜水成员永远不出现，而他们恰恰是「这个群还有谁」的主要部分。代价是出现 messageCount 为 0
//! 的成员，这是接受的（名册里没有发言记录的人本来就没有计数）。

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::parser::types::ChatType;
use crate::server::dto::{GroupMember, GroupMembers};
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub chatroom_id: Option<String>,
    #[serde(default)]
    pub include_message_counts: Option<String>,
    // 键名显式钉住：结构体是 camelCase，而凭据参数按契约就叫 `access_token`。
    #[serde(default, rename = "access_token")]
    pub access_token: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let room = params
        .chatroom_id
        .as_deref()
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request("缺少必填参数 chatroomId"))?;
    let with_counts = matches!(params.include_message_counts.as_deref(), Some("1") | Some("true"));

    let store = state.store.read();
    let conv = store.conversation(ChatType::Group, room);
    let roster = store.chatroom_roster.get(room);
    // 名册**也算这个群存在**：只按消息判存在会让「有群、但一条消息都没索引到」变成 404，
    // 而那时名册恰好是唯一能回答「群里有谁」的来源。
    if conv.is_none() && roster.is_none() {
        return Err(ApiError::not_found(format!("群聊不存在: {room}")));
    }

    // 发言人计数（整个会话，与本页无关）＋ 消息里带的昵称。
    let mut counts: HashMap<&str, usize> = HashMap::new();
    let mut nicks: HashMap<&str, String> = HashMap::new();
    if let Some(conv) = conv {
        for m in &conv.msgs {
            if m.from_uid.is_empty() {
                continue;
            }
            *counts.entry(m.from_uid.as_str()).or_insert(0) += 1;
            nicks.entry(m.from_uid.as_str()).or_insert_with(|| m.from_nick.clone());
        }
    }
    // 名册并进来；发言过但不在名册里的（退群、名册缺失）照旧保留 —— 两个方向的差集都要。
    let mut uids: Vec<&str> = counts.keys().copied().collect();
    if let Some(roster) = roster {
        for uid in roster {
            if !uid.is_empty() && !counts.contains_key(uid.as_str()) {
                uids.push(uid.as_str());
            }
        }
    }

    let mut members: Vec<GroupMember> = uids
        .into_iter()
        .map(|uid| {
            let nick = nicks.get(uid).cloned().unwrap_or_default();
            let remark = store.names.uid_remark.get(uid).cloned().unwrap_or_default();
            // groupNickname 优先用本会话的群名片（64003）；nickname 保持消息里带的昵称。
            let group_nick = store.display_sender(ChatType::Group, room, uid);
            GroupMember {
                alias: String::new(),
                avatar_url: String::new(),
                // 名册里的潜水成员可能连消息都没有：那时 displayName **回落 uid**，而不是给空串
                // —— 空串会让下游把每一行都显示成一样的空白，而 uid 至少能定位到人。
                display_name: if nick.is_empty() { uid.to_string() } else { nick.clone() },
                group_nickname: group_nick,
                is_friend: false,
                // 群主：本群恰一个 true（群主不在成员集合、或缺群主数据时全 false）。
                // 判据与真库实测见 `store::group_meta` 模块头注释。
                is_owner: store
                    .chatroom_owner
                    .get(room)
                    .is_some_and(|owner| owner == uid),
                message_count: with_counts.then(|| counts.get(uid).copied().unwrap_or(0)),
                nickname: nick,
                remark,
                wxid: uid.to_string(),
            }
        })
        .collect();

    // 排序**必须稳定**：只按计数排时，一大批计数为 0 的潜水成员的相对顺序取决于哈希表的遍历
    // 顺序，同一个群两次请求的顺序就可能不同 —— 下游按它做 diff 时会看到满屏假变化。
    members.sort_by(|a, b| {
        let ca = counts.get(a.wxid.as_str()).copied().unwrap_or(0);
        let cb = counts.get(b.wxid.as_str()).copied().unwrap_or(0);
        cb.cmp(&ca).then_with(|| a.wxid.cmp(&b.wxid))
    });

    let body = serde_json::to_value(GroupMembers {
        chatroom_id: room.to_string(),
        count: members.len(),
        // 名册与消息都在内存索引里：这个请求既不读盘、也不触发同步（同步走 /api/v1/sync）。
        from_cache: false,
        members,
        success: true,
        updated_at: chrono::Utc::now().timestamp_millis(),
    })
    .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?;
    Ok(Json(body))
}
