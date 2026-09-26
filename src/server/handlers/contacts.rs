//! GET|POST /api/v1/contacts — people who appeared in chat records.
//! v1 derives contacts from the uid->nickname map (no separate contact DB).

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::server::dto::Contacts;
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::store::AppState;

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

#[derive(Debug, Clone, Serialize)]
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
    let (contacts, total) = crate::store::query::query_contacts(
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
