//! GET|POST /api/v1/contacts — people who appeared in chat records.
//! v1 derives contacts from the uid->nickname map (no separate contact DB).

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::Store;
use crate::server::dto::Contacts;
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub keyword: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

fn default_limit() -> usize {
    100
}

#[derive(Debug, Clone, Serialize, utoipa::ToSchema)]
#[serde(rename_all = "camelCase")]
pub struct ContactOut {
    pub username: String,
    pub display_name: String,
    pub nickname: String,
    pub remark: String,
    /// WeFlow's alias slot: for QQ this carries the contact's QQ number
    /// (from the uid mapping table / profile, when the version exposes it;
    /// empty otherwise) — the old qqflow `qq` field migrated here.
    pub alias: String,
    pub avatar_url: String,
    pub r#type: String,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    // 形状见 `crate::server::dto::Contacts`。
    let params = merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let limit = params.limit.clamp(1, 10000);
    let store = state.store.read();
    let (contacts, total) = query_contacts(
        &store,
        params.keyword.as_deref(),
        limit,
        params.offset,
    );
    let count = contacts.len();
    // `total` / `hasMore` let clients page deterministically instead of
    // inferring the end from "page shorter than limit" — which silently breaks
    // if the server-side default limit ever changes.
    let has_more = params.offset.saturating_add(count) < total;
    // 构造 DTO 后 `to_value`：`json!` 与 `to_value` 都经 BTreeMap（键被排序），因此**输出
    // 逐字节不变**；而类型化构造让「键名写错」变成编译错误。
    //
    // 这里**不用** `Json<Contacts>` 直接序列化：`ContactOut` 的字段声明不是字母序，直接
    // 序列化会让响应里的键序变化。`to_value` 对声明顺序免疫。
    let body = serde_json::to_value(Contacts {
        contacts,
        count,
        has_more,
        success: true,
        total,
    })
    .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?;
    Ok(Json(body))
}

/// Contacts: every UID known to chat or to the name maps (a profile-only
/// uid with no chat history appears too — that is the point of the
/// mapping), with nickname (profile > message-derived), remark, and the
/// QQ number exposed in the WeFlow `alias` slot.
///
/// Returns `(page, total_after_filter)`: the caller needs the pre-pagination
/// count to answer `hasMore` without running the query twice.
///
/// 它返回的是 HTTP 面的 DTO（[`ContactOut`]），所以住在这一层 —— 之前它在 `store::query` 里，
/// 于是核心面反过来依赖服务层（`--no-default-features` 下得为它加 cfg 才能编译）。
pub fn query_contacts(store: &Store, keyword: Option<&str>, limit: usize, offset: usize) -> (Vec<ContactOut>, usize) {
    let kw = keyword.map(|k| k.to_lowercase());
    let mut uid_set: std::collections::BTreeSet<&String> = store.uid_names.keys().collect();
    uid_set.extend(store.names.uid_remark.keys());
    uid_set.extend(store.names.uid_nick.keys());
    let mut rows: Vec<ContactOut> = uid_set
        .into_iter()
        .map(|uid| {
            let nick = store
                .names
                .uid_nick
                .get(uid)
                .or_else(|| store.uid_names.get(uid))
                .cloned()
                .unwrap_or_default();
            ContactOut {
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

