//! GET /api/v1/sessions/:id/messages — ChatLab Pull protocol:
//! incremental sync with a `sync` pagination block.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::{json, Value};

use crate::parser::types::ChatType;
use crate::server::error::ApiError;
use crate::store::AppState;

use super::{authorized, parse_time_bound};

/// Every field is a lenient `Option<String>`: WeFlow's Pull contract has no
/// 400 semantics for malformed pagination, so garbage degrades to the default
/// instead of rejecting the request (`?limit=abc` used to 400 here while the
/// same value on WeFlow's own endpoint is ignored).
#[derive(Debug, Default, Deserialize)]
pub struct Params {
    pub since: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    pub offset: Option<String>,
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    Query(params): Query<Params>,
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }

    let limit = params
        .limit
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5000)
        .clamp(1, 5000);
    let offset = params.offset.as_deref().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
    let since = params.since.as_deref().and_then(|s| parse_time_bound(s, false));
    let end = params.end.as_deref().and_then(|s| parse_time_bound(s, true));
    let watermark = end.unwrap_or_else(|| chrono::Utc::now().timestamp());

    let store = state.store.read();
    // find_conversation falls back to the other chat type when the primary
    // classification misses (an all-digit c2c peer uid classifies as group).
    let Some(conv) = store.find_conversation(&id) else {
        return Err(ApiError::not_found(format!("会话不存在: {id}")));
    };
    let chat_type = conv.chat_type;
    let talker = conv.talker.clone();

    // Chronological messages within (since, end] — `since` is exclusive, so
    // a client resuming with `nextSince` never re-fetches the boundary
    // second and can neither loop nor see duplicates.
    let mut idx: Vec<usize> = conv.msgs.iter().enumerate().map(|(i, _)| i).collect();
    idx.sort_by_key(|&i| (conv.msgs[i].ts, conv.msgs[i].rowid));
    let filtered: Vec<usize> = idx
        .into_iter()
        .filter(|&i| {
            let m = &conv.msgs[i];
            since.is_none_or(|s| m.ts > s) && end.is_none_or(|e| m.ts <= e)
        })
        .collect();

    let total = filtered.len();
    // Page from `offset`, extending to the end of the last second's ts
    // group: pages never split a second, so `nextSince` strictly advances.
    let start = offset.min(total);
    let mut page_end = start;
    let mut prev_ts = None;
    while page_end < total {
        let ts_j = conv.msgs[filtered[page_end]].ts;
        if prev_ts.is_some_and(|p| p != ts_j) && page_end - start >= limit {
            break;
        }
        prev_ts = Some(ts_j);
        page_end += 1;
    }
    let page = &filtered[start..page_end];
    let has_more = page_end < total;

    // WeFlow's Pull contract keeps these two SEPARATE: `accountName` is the
    // account's own name, `groupNickname` is the per-conversation group card
    // ("40090"). Collapsing both onto `display_sender` (card-wins) would make
    // them identical in groups, leaving a consumer unable to tell "no card" from
    // "card equals the account name". `senderName` on /api/v1/messages keeps its
    // card-wins semantics — that field is downstream-visible and unchanged.
    let account_name = |uid: &str| store.display_uid(uid);
    let group_card = |uid: &str| -> String {
        if chat_type != ChatType::Group {
            return String::new();
        }
        store
            .group_cards
            .get(&crate::store::conv_key(chat_type, &talker))
            .and_then(|cards| cards.get(uid))
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_default()
    };

    let messages: Vec<Value> = page
        .iter()
        .map(|&i| {
            let m = &conv.msgs[i];
            json!({
                "sender": m.from_uid,
                "accountName": account_name(&m.from_uid),
                "groupNickname": group_card(&m.from_uid),
                "timestamp": m.ts,
                // Canonical ChatLab 0.0.2 code — NOT the native `localType`.
                "type": m.parsed.msg_type.chatlab_type(),
                "content": m.parsed.content,
                "platformMessageId": m.seq.to_string(),
            })
        })
        .collect();

    // Senders in THIS page, deduped — the roster describes what was exported,
    // matching WeFlow. Scanning the whole conversation instead made `members`
    // unbounded and cost a full pass per request.
    let members: Vec<Value> = {
        let mut seen: Vec<&str> = Vec::new();
        let mut out = Vec::new();
        for &i in page {
            let uid = conv.msgs[i].from_uid.as_str();
            if !uid.is_empty() && !seen.contains(&uid) {
                seen.push(uid);
                out.push(json!({
                    "platformId": uid,
                    "accountName": account_name(uid),
                    "groupNickname": group_card(uid),
                    // QQ exposes no avatar source; the field is optional in
                    // ChatLab 0.0.2, so an empty string is the honest answer.
                    "avatar": "",
                }));
            }
        }
        out
    };

    // `ownerId` = the bound account (WeFlow emits its own wxid here). Only one
    // account ever holds the binding, so the first Ready entry is unambiguous.
    let owner_id = state
        .accounts
        .read()
        .iter()
        .find(|a| a.state.is_ready())
        .map(|a| a.qq.clone())
        .unwrap_or_default();

    let next_since = page.last().map(|&i| conv.msgs[i].ts).unwrap_or(since.unwrap_or(0));
    Ok(Json(json!({
        "chatlab": {
            "version": "0.0.2",
            "exportedAt": chrono::Utc::now().timestamp(),
            "generator": "qqflow-server",
        },
        "meta": {
            "name": store.display_name(chat_type, &talker),
            "platform": "qq",
            "type": chat_type.as_str(),
            "groupId": talker,
            "ownerId": owner_id,
        },
        "members": members,
        "messages": messages,
        "sync": {
            "hasMore": has_more,
            "nextSince": if has_more { next_since } else { watermark },
            // Both cursors are meant to be echoed back verbatim, so they must
            // not skip the same rows twice. `nextSince` is exclusive and the
            // page ends on a complete ts group, so re-filtering with it drops
            // exactly the rows already served — leaving the next unseen row at
            // offset 0. `nextOffset` therefore only carries weight in the
            // degenerate case where the timestamp could not advance at all.
            "nextOffset": if has_more && next_since <= since.unwrap_or(i64::MIN) {
                start.saturating_add(page.len())
            } else {
                0
            },
            "watermark": watermark,
        }
    })))
}
