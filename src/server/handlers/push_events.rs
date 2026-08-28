//! GET|POST /api/v1/push/messages — SSE event stream.
//!
//! WeFlow contract: `ready` first, then a `sync` event carrying the current
//! rowid watermarks (qqflow-server extension), then `message.new` /
//! `message.revoke` with `id:` frames. Last-Event-ID replay (1000 events /
//! 10 min TTL), 25 s keep-alive ping. On a broadcast lag the client is
//! re-synced with a fresh `sync` event.

use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::{HeaderMap, HeaderName};
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures_util::StreamExt;
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;

use crate::server::error::ApiError;
use crate::store::AppState;
use crate::sync::events::Event;

use super::authorized;

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
    /// Last-Event-ID as a query param, for clients that cannot set the header
    /// (the browser `EventSource` API has no way to send one).
    #[serde(default, alias = "last_event_id")]
    pub last_event_id: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
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
        for (id, name, payload) in replay {
            yield Ok(SseEvent::default()
                .id(id.to_string())
                .event(name)
                .json_data(payload)
                .unwrap_or_else(|_| SseEvent::default().event("message.new").data("{}")));
        }
        // Connection baseline: current watermarks, so a client that had
        // nothing to replay still knows where it stands.
        yield Ok(SseEvent::default()
            .event("sync")
            .json_data(Event::sync(wm_g, wm_c, now))
            .unwrap_or_else(|_| SseEvent::default().event("sync").data("{}")));

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
                    let resync = Event::sync(g, c, chrono::Utc::now().timestamp());
                    yield Ok(SseEvent::default()
                        .event("sync")
                        .json_data(resync)
                        .unwrap_or_else(|_| SseEvent::default().event("sync").data("{}")));
                    continue;
                }
            };
            let name = ev.event.clone();
            let payload = serde_json::to_value(&ev).unwrap_or_default();
            let id = history.lock().append(name.clone(), payload.clone());
            yield Ok(SseEvent::default()
                .id(id.to_string())
                .event(name)
                .json_data(payload)
                .unwrap_or_else(|_| SseEvent::default().event("message.new").data("{}")));
        }
    });

    Ok(Sse::new(stream)
        .keep_alive(KeepAlive::new().interval(Duration::from_secs(25)).text("ping")))
}
