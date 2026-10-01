//! `GET /chatlab/push/messages` —— **通知面**。
//!
//! 与 `/api/v1/push/messages` 是**同一条总线、同一套连接机制**（鉴权、`Last-Event-ID` 重放、
//! 保活、滞后重基线、关机自收），差别只有**帧的形状**：
//!
//! | | `/api/v1/push/messages` | `/chatlab/push/messages` |
//! |---|---|---|
//! | 载荷 | 整个事件（含 `content` 与媒体元数据）| **只带元信息** |
//! | 定位 | WeFlow 兼容面 —— 已有客户端在解析它 | 规范里的通知通道 |
//!
//! 规范对这条通道的定位是「**仅通知**：ChatLab 不假设 SSE 事件可靠送达」——收到事件后**去拉**
//! 那一页，而不是把事件内容当数据。所以这里不带正文：带了会诱导客户端把它当数据源，而它并不
//! 保证送达。
//!
//! 老面**不动**（「不改老路由」）：它的形状是既有客户端在解析的，改它要走破坏性发布。

use std::sync::Arc;

use axum::extract::State;
use axum::http::{HeaderMap, HeaderName};
use axum::response::IntoResponse;

use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;
use crate::sync::events::Event;

use super::authorized;

#[derive(Debug, Default, serde::Deserialize)]
pub struct Params {
    #[serde(default)]
    pub access_token: Option<String>,
    /// Last-Event-ID 作为查询参数，给设不了头的客户端（浏览器 `EventSource` 就没法发头）。
    #[serde(default, alias = "last_event_id")]
    pub last_event_id: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
) -> Result<impl IntoResponse, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    let last_id = headers
        .get(HeaderName::from_static("last-event-id"))
        .and_then(|v| v.to_str().ok())
        .and_then(|s| s.parse::<u64>().ok())
        .or_else(|| {
            params
                .last_event_id
                .as_deref()
                .and_then(|s| s.parse::<u64>().ok())
        })
        .unwrap_or(0);
    // 与老面共用整套连接机制，只换序列化器。
    Ok(super::push_events::sse_from(
        state,
        last_id,
        serialize_notification,
    ))
}

/// 把一个总线事件映射成**通知帧**：只带标识与时间，不带正文。
///
/// `Event` 在这边是**扁平结构体**（不是枚举），类型由 `event` 字段区分 —— 所以这里按它分派，
/// 而不是按变体匹配。
fn serialize_notification(ev: Event) -> (String, serde_json::Value) {
    use crate::server::dto::NotificationFrame;
    let name = ev.event.clone();
    // `session.sync` 一类的基线事件没有对应的消息 —— 它只告诉客户端「水位变了，去拉」。
    // 通知面因此把它压成同一形状：有 id 就带，没有就为 null。
    let is_message = name == "message.new" || name == "message.revoke";
    // 基线事件（`sync` 一类的 `session.*`）带代号；消息类不带这个键（见 DTO 的字段说明）。
    let generation = if is_message {
        None
    } else {
        Some(crate::server::current_generation())
    };
    let frame = NotificationFrame {
        event: name.clone(),
        // 空串**显式映射成 None**：基线事件（如 session.sync）没有会话，而
        // skip_serializing_if 对空串无效 —— 不映射就会下发一个空的 sessionId，
        // 读者无法把它与「真的有一个空 id 的会话」区分开。
        event_id: if is_message { Some(ev.rawid.clone()) } else { None },
        platform_message_id: None,
        session_id: (!ev.session_id.is_empty()).then(|| ev.session_id.clone()),
        timestamp: ev.timestamp,
        generation,
    };
    (
        name,
        serde_json::to_value(frame).expect("通知帧必须可序列化"),
    )
}
