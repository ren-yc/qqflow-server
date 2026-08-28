//! GET|POST /health  and  GET|POST /api/v1/health  (no auth required).
//!
//! Unauthenticated, so the response is deliberately scalar: `status` for
//! readiness, `version`, and a single `account` phase. It must NOT list the
//! accounts — the startup scan seeds one entry per QQ profile directory found
//! on this machine, so the array (and even its length) told any unauthenticated
//! caller which accounts exist here and how far along each one is. Account
//! identities, message counts, database paths and error details are served by
//! the token-protected `GET /api/v1/accounts` instead.

use std::sync::atomic::Ordering;
use std::sync::Arc;

use axum::extract::State;
use axum::response::IntoResponse;
use axum::Json;
use serde_json::json;

use crate::server::{bound_account, AccountPhase};
use crate::store::AppState;

pub async fn handler(State(state): State<Arc<AppState>>) -> impl IntoResponse {
    let status = if state.ready.load(Ordering::SeqCst) { "ok" } else { "starting" };
    let account = bound_account(&state.accounts.read())
        .map(|a| AccountPhase::from(a.state))
        .unwrap_or(AccountPhase::Unregistered);
    Json(json!({
        "status": status,
        "version": env!("CARGO_PKG_VERSION"),
        "account": account,
    }))
}
