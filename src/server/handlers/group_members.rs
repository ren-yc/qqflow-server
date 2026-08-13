//! GET|POST /api/v1/group-members — member list of a group chat.
//! v1 derives members from the messages of that group (uid + nickname),
//! with optional per-member message counts.

use std::collections::HashMap;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::parser::types::ChatType;
use crate::server::error::ApiError;
use crate::store::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
#[serde(rename_all = "camelCase")]
pub struct Params {
    pub chatroom_id: Option<String>,
    #[serde(default)]
    pub talker: Option<String>, // alias for chatroomId
    #[serde(default)]
    pub include_message_counts: Option<String>,
    #[serde(default)]
    pub with_counts: Option<String>, // alias
    #[serde(default)]
    pub force_refresh: Option<String>,
    #[serde(default, rename = "access_token")]
    pub access_token: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
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
        .or(params.talker.as_deref())
        .ok_or_else(|| ApiError::bad_request("缺少必填参数 chatroomId"))?;
    let with_counts = params.include_message_counts.as_deref() == Some("1")
        || params.include_message_counts.as_deref() == Some("true")
        || params.with_counts.as_deref() == Some("1")
        || params.with_counts.as_deref() == Some("true");
    let _ = params.force_refresh; // v1: no member cache to refresh

    let store = state.store.read();
    let Some(conv) = store.conversation(ChatType::Group, room) else {
        return Err(ApiError::not_found(format!("群聊不存在: {room}")));
    };

    let mut uid_order: Vec<String> = Vec::new();
    let mut counts: HashMap<String, usize> = HashMap::new();
    let mut nicks: HashMap<String, String> = HashMap::new();
    for m in &conv.msgs {
        if m.from_uid.is_empty() {
            continue;
        }
        if !uid_order.contains(&m.from_uid) {
            uid_order.push(m.from_uid.clone());
        }
        *counts.entry(m.from_uid.clone()).or_insert(0) += 1;
        nicks.entry(m.from_uid.clone()).or_insert_with(|| m.from_nick.clone());
    }

    let members: Vec<Value> = uid_order
        .iter()
        .map(|uid| {
            let nick = nicks.get(uid).cloned().unwrap_or_default();
            let remark = store.names.uid_remark.get(uid).cloned().unwrap_or_default();
            let mut m = json!({
                "wxid": uid,
                "displayName": nick,
                "nickname": nick,
                "remark": remark,
                "alias": "",
                "groupNickname": nick,
                "avatarUrl": "",
                "isOwner": false,
                "isFriend": false,
            });
            if with_counts {
                m["messageCount"] = json!(counts.get(uid).copied().unwrap_or(0));
            }
            m
        })
        .collect();

    Ok(Json(json!({
        "success": true,
        "chatroomId": room,
        "count": members.len(),
        "fromCache": false,
        "updatedAt": chrono::Utc::now().timestamp_millis(),
        "members": members,
    })))
}
