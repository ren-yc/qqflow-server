//! POST /api/v1/accounts — client-driven account registration.
//!
//! A downstream client supplies the account (qq), the SQLCipher key, and
//! optionally the database path; the server then initializes the account
//! in the background (mirror + decrypt + index + SSE baseline + watch).
//! Token-protected; deliberately NOT gated on readiness — without an
//! account the server would never become ready, so this is the bootstrap
//! endpoint. Keys live in memory only.

use std::sync::Arc;

use axum::extract::{Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::scan::{DbInfo, NT_MSG_DB};
use crate::server::error::ApiError;
use crate::server::{init_account, set_account_state};
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
    let mut registry = state.init.accounts_db.lock();
    match db_path.filter(|p| !p.is_empty()) {
        Some(p) => {
            let path = std::path::Path::new(p);
            let resolved = if path.is_file() {
                Some(DbInfo { qq: qq.to_string(), path: path.to_path_buf() })
            } else if path.is_dir() {
                let db = path.join(qq).join("nt_qq").join("nt_db").join(NT_MSG_DB);
                db.is_file().then(|| DbInfo { qq: qq.to_string(), path: db })
            } else {
                None
            };
            if let Some(info) = &resolved {
                // Register (or override) the account location.
                match registry.iter_mut().find(|a| a.qq == qq) {
                    Some(a) => *a = info.clone(),
                    None => registry.push(info.clone()),
                }
            }
            resolved
        }
        None => registry.iter().find(|a| a.qq == qq).cloned(),
    }
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

    // Idempotent guards for accounts already past the waiting stage.
    let current = {
        let accs = state.accounts.read();
        accs.iter().find(|a| a.qq == qq).map(|a| a.state.clone())
    };
    match current.as_deref() {
        Some("ready") => return Ok(reply("already_ready")),
        Some("indexing") => return Ok(reply("in_progress")),
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

    if !state.init.key_store.lock().insert_validated(qq, key) {
        return Ok(reply("invalid_key"));
    }

    // Mark indexing and kick off the background build.
    set_account_state(&state, qq, "indexing", 0, None);
    let state_for_init = state.clone();
    let key_owned = key.to_string();
    tokio::spawn(async move { init_account(&state_for_init, info, key_owned).await });
    Ok(reply("accepted"))
}
