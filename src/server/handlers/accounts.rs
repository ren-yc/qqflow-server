//! POST /api/v1/accounts — client-driven account registration.
//!
//! A downstream client supplies the account (qq), the SQLCipher key, and
//! optionally the database path; the server then initializes the account
//! in the background (mirror + decrypt + index + SSE baseline + watch).
//! Token-protected; deliberately NOT gated on readiness — without an
//! account the server would never become ready, so this is the bootstrap
//! endpoint. Keys live in memory only.

use std::path::Path;
use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::scan::{self, DbInfo};
use crate::keystore::validate_key;
use crate::server::error::ApiError;
use crate::server::{begin_indexing, init_account, AccountStatus};
use crate::store::AppState;

use super::{authorized, merge_body};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub qq: Option<String>,
    pub key: Option<String>,
    pub db_path: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
}

/// Resolve `(qq, db_path)` to a `DbInfo`: an explicit path (nt_msg.db file
/// or Tencent Files-style root dir) registers or overrides the account in
/// the registry; without one, the startup scan must have found it.
fn resolve_db_path(state: &AppState, qq: &str, db_path: Option<&str>) -> Option<DbInfo> {
    let Some(p) = db_path.filter(|p| !p.is_empty()) else {
        return state.init.find_db(qq);
    };
    // Resolve outside the registry lock (the stat calls are syscalls);
    // only the find-or-insert needs the lock.
    let info = scan::resolve_account(qq, Path::new(p))?;
    state.init.upsert_db(info.clone());
    Some(info)
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
    let Some(qq) = params.qq.as_deref().filter(|s| !s.is_empty()) else {
        return Err(ApiError::bad_request("缺少必填参数 qq"));
    };
    let Some(key) = params.key.as_deref().filter(|s| !s.is_empty()) else {
        return Err(ApiError::bad_request("缺少必填参数 key"));
    };

    let reply = |state_name: &str| Json(json!({ "success": true, "qq": qq, "state": state_name }));

    // Idempotent guards for accounts already past the waiting stage. Check
    // before resolving the path so a ready account's reply wins over
    // unknown-qq / invalid-db-path.
    let current = {
        let accs = state.accounts.read();
        accs.iter().find(|a| a.qq == qq).map(|a| a.state)
    };
    match current {
        Some(AccountStatus::Ready) => return Ok(reply("already_ready")),
        Some(AccountStatus::Indexing) => return Ok(reply("in_progress")),
        _ => {} // awaiting_key / error / unknown -> accept
    }

    let Some(info) = resolve_db_path(&state, qq, params.db_path.as_deref()) else {
        // An explicit path that does not resolve, or an unknown qq.
        let state_name = if params.db_path.as_deref().is_some_and(|p| !p.is_empty()) {
            "invalid_db_path"
        } else {
            "unknown_qq"
        };
        return Ok(reply(state_name));
    };

    if validate_key(key).is_err() {
        return Ok(reply("invalid_key"));
    }

    // Flip to indexing atomically with the guard: a concurrent duplicate
    // registration serializes here and observes the new state instead of
    // spawning a second initialization.
    match begin_indexing(&state, qq) {
        Some(AccountStatus::Ready) => return Ok(reply("already_ready")),
        Some(AccountStatus::Indexing) => return Ok(reply("in_progress")),
        _ => {}
    }

    // Kick off the background build.
    let state_for_init = state.clone();
    let key_owned = key.to_string();
    tokio::spawn(async move { init_account(&state_for_init, info, key_owned).await });
    Ok(reply("accepted"))
}
