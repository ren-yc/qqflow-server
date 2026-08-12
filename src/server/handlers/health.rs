//! GET|POST /health  and  GET|POST /api/v1/health  (no auth required).

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::store::AppState;

pub async fn handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let status = if state.ready.load(Ordering::SeqCst) { "ok" } else { "starting" };
    let accounts: Vec<_> = state.accounts.read().iter().cloned().collect();
    Json(json!({
        "status": status,
        "version": env!("CARGO_PKG_VERSION"),
        "accounts": accounts,
    }))
}
