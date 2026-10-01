//! GET /api/v1/push/messages — SSE event stream.
//!
//! WeFlow contract: `ready` first, then a `sync` event carrying the current
//! rowid watermarks (qqflow-server extension), then `message.new` /
//! `message.revoke` with `id:` frames. Last-Event-ID replay (1000 events /
//! 10 min TTL), 25 s keep-alive ping. On a broadcast lag the client is
//! re-synced with a fresh `sync` event.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{State};
use axum::http::{HeaderMap, HeaderName};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;
use crate::sync::events::Event;

use super::authorized;

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    #[serde(default)]
    pub access_token: Option<String>,
    /// Last-Event-ID as a query param, for clients that cannot set the header
    /// (the browser `EventSource` API has no way to send one).
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

    // Last-Event-ID replay (header first, then query param; WeFlow contract).
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

    Ok(sse_from(state, last_id, serialize_weflow))
}

/// 事件的序列化器：把一个总线事件变成一个 SSE 帧（`(事件名, 载荷)`）。
///
/// 两个面对同一批事件有**不同的形状要求** —— WeFlow 兼容面发完整消息，ChatLab 面只发元信息
/// （规范：「ChatLab 不假设事件可靠送达」，通知只负责告诉客户端去拉）。参数化这一处，
/// 其余（鉴权、重放、保活、滞后重基线、关机）两面对**完全一致**。
pub(crate) type Serializer = fn(Event) -> (String, serde_json::Value);

/// WeFlow 兼容面的形状：**整个事件原样序列化**（既有客户端在解析它）。
///
/// **只有 `sync` 是例外**：它被收敛成与 weflow 同形的 `{event, watermarks:[{table, watermark}]}`。
/// 原来它是这个扁平结构体的一个"投影" —— 水位线靠 `lastRowidGroup` / `lastRowidC2c` 两个字段名
/// 承载，加第三张表就必须再加一个字段，而消费方得靠"字段名 ←→ 表"的约定来配对。
/// 数组形态把这件事变成数据。
///
/// 代价是这一帧**变成破坏性**的（解析旧字段名的下游会拿到缺键）——
/// 这正是要走 0.6.0 的那一项。
fn serialize_weflow(ev: Event) -> (String, serde_json::Value) {
    let name = ev.event.clone();
    let payload = if name == "sync" {
        sync_payload(&ev)
    } else {
        serde_json::to_value(&ev).unwrap_or_default()
    };
    (name, payload)
}

/// 给键集护栏用的入口。
///
/// 测试在**独立 crate** 里，看不见 `pub(crate)`；而这条护栏的价值恰恰在于它断言的是**线上形状**
/// 而不是某个内部函数 —— 所以这里把它开一个口，而不是让测试自己拼一份等价实现
/// （自己拼的那份会跟着测试一起漂移，护栏就白设了）。
pub fn serialize_for_test(ev: Event) -> (String, serde_json::Value) {
    serialize_weflow(ev)
}

/// 把扁平事件里的两个 rowid 水位收敛成数组。
///
/// `rowid` 是 SQLite 的行号，也是本仓库增量的**全部**游标语义（`read_new` 就按它取新行）。
/// weflow 那边的水位是 `{create_time, local_id, sort_seq}` 三元组 —— **两者不是同一套语义**，
/// 跨仓库的消费方必须按 `table` 分支。这一点写在字段文档里，而不是靠"名字一样"蒙混过去。
fn sync_payload(ev: &Event) -> serde_json::Value {
    use crate::server::dto::{SyncFrame, WatermarkEntry, WatermarkValue};
    // 走**类型**而不是 `json!`：这一帧此前是手拼的，键名写错编译不过；而它是 SSE 面上的
    // 输出，没有 golden 快照护栏（快照的模型是一次请求一次响应）——键集由 sse_shape 的断言
    // 盯着，形状本身则由这里的类型盯住。`to_value` 与 `json!` 一样按字母序输出，
    // 因此键序不变。
    let mut watermarks: Vec<WatermarkEntry> = Vec::new();
    for (table, rowid) in [
        (crate::store::index::GROUP_TABLE, ev.last_rowid_group),
        (crate::store::index::C2C_TABLE, ev.last_rowid_c2c),
    ] {
        if let Some(n) = rowid {
            watermarks.push(WatermarkEntry {
                table: table.to_string(),
                watermark: WatermarkValue { rowid: n },
            });
        }
    }
    serde_json::to_value(SyncFrame {
        event: "sync".to_string(),
        // 基线代号**恒出现**：注销时递增，客户端据此区分「注销后新账号刚开始」与「自己漏收了」。
        // 少了它，这两种情况在协议上是同一件事。
        generation: crate::server::current_generation(),
        watermarks,
    })
    .unwrap_or_default()
}

/// 组装 SSE 响应。`serialize` 决定帧的形状，其余部分是两面的公共部分。
pub(crate) fn sse_from(state: Arc<AppState>, last_id: u64, serialize: Serializer) -> impl IntoResponse {
    let replay = state.history.lock().replay_since(last_id);
    let rx = state.events.subscribe();
    let (wm_g, wm_c) = {
        let store = state.store.read();
        (store.watermark_group, store.watermark_c2c)
    };
    let now = chrono::Utc::now().timestamp();
    let history = state.history.clone();
    // An SSE stream never ends on its own, so it would hold graceful shutdown
    // open for the whole grace period. Watching the shutdown channel lets the
    // stream close itself and the drain finish promptly.
    let mut shutdown = state.shutdown.subscribe();
    let lag_state = state.clone();

    let stream = async_stream::stream!({
        yield Ok::<_, std::convert::Infallible>(
            SseEvent::default().event("ready").data("{\"status\":\"ok\"}"),
        );
        for (id, ev) in replay {
            let (name, payload) = serialize(ev);
            yield Ok(SseEvent::default()
                .id(id.to_string())
                .event(name)
                .json_data(payload)
                .unwrap_or_else(|_| SseEvent::default().event("message.new").data("{}")));
        }
        // Connection baseline: current watermarks, so a client that had
        // nothing to replay still knows where it stands.
        //
        // **必须走 `serialize`**，不能直接吐 `Event::sync(…)` —— 那样会绕过面的形状：
        // 通知面的基线要带 `generation`，而这里曾经把老面的载荷原样发出去（新面收到的是
        // 另一个面的形状，且只在「注销后重放」那条用例里才看得出来）。
        {
            let (name, payload) = serialize(Event::sync(wm_g, wm_c, now));
            yield Ok(SseEvent::default()
                .event(name)
                .json_data(payload)
                .unwrap_or_else(|_| SseEvent::default().event("sync").data("{}")));
        }

        let mut bstream = BroadcastStream::new(rx);
        loop {
            let item = tokio::select! {
                biased;
                _ = shutdown.changed() => break,
                item = bstream.next() => match item {
                    Some(item) => item,
                    None => break,
                },
            };
            let ev = match item {
                Ok(ev) => ev,
                Err(_lagged) => {
                    // Subscriber fell behind: re-sync from the CURRENT
                    // watermarks. No history id — this frame is specific to
                    // this lagging subscriber, so it must not consume a
                    // bus-level sequence number other clients would then skip.
                    let (g, c) = {
                        let store = lag_state.store.read();
                        (store.watermark_group, store.watermark_c2c)
                    };
                    // 与连接基线同理：**走 `serialize`**，否则通知面会收到老面的形状。
                    let (name, payload) =
                        serialize(Event::sync(g, c, chrono::Utc::now().timestamp()));
                    yield Ok(SseEvent::default()
                        .event(name)
                        .json_data(payload)
                        .unwrap_or_else(|_| SseEvent::default().event("sync").data("{}")));
                    continue;
                }
            };
            let id = history.lock().append(ev.clone());
            let (name, payload) = serialize(ev);
            yield Ok(SseEvent::default()
                .id(id.to_string())
                .event(name)
                .json_data(payload)
                .unwrap_or_else(|_| SseEvent::default().event("message.new").data("{}")));
        }
    });

    Sse::new(stream).keep_alive(KeepAlive::new().interval(Duration::from_secs(25)).text("ping"))
}
