//! GET|POST /api/v1/push/messages — SSE event stream.
//!
//! On connect the client immediately receives a `sync` event carrying the
//! current rowid watermarks (qqflow-server extension), then `message.new` /
//! `message.revoke` events. KeepAlive ping every 15 s. On a broadcast lag
//! the client is re-synced with a fresh `sync` event.

use std::convert::Infallible;
use std::pin::Pin;
use std::sync::Arc;
use std::time::Duration;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::response::sse::{Event as SseEvent, KeepAlive, Sse};
use axum::response::IntoResponse;
use futures_util::stream::{self, Stream};
use serde::Deserialize;
use tokio_stream::wrappers::BroadcastStream;
use tokio_stream::StreamExt;

use crate::sync::events::Event;
use crate::server::error::ApiError;
use crate::store::AppState;

use super::authorized;

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    #[serde(default)]
    pub access_token: Option<String>,
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
) -> Result<impl IntoResponse, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }

    let rx = state.events.subscribe();
    let (wm_g, wm_c) = {
        let store = state.store.read();
        (store.watermark_group, store.watermark_c2c)
    };
    let now = chrono::Utc::now().timestamp();
    let init = stream::once(async move {
        Ok::<_, Infallible>(SseEvent::default().event("sync").data(
            serde_json::to_string(&Event::sync(wm_g, wm_c, now)).unwrap_or_default(),
        ))
    });

    let events = BroadcastStream::new(rx).map(move |item| {
        let ev = match item {
            Ok(ev) => ev,
            Err(_lagged) => {
                // Subscriber fell behind: re-sync.
                let store = state.store.read();
                Event::sync(store.watermark_group, store.watermark_c2c, chrono::Utc::now().timestamp())
            }
        };
        let json = serde_json::to_string(&ev).unwrap_or_default();
        Ok::<_, Infallible>(SseEvent::default().event(ev.event.clone()).data(json))
    });

    let stream: Pin<Box<dyn Stream<Item = Result<SseEvent, Infallible>> + Send>> =
        Box::pin(init.chain(events));

    Ok(Sse::new(stream).keep_alive(
        KeepAlive::new().interval(Duration::from_secs(15)).text("ping"),
    ))
}
