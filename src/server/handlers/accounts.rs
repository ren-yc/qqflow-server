//! POST /api/v1/accounts — client-driven account registration.
//!
//! A downstream client supplies the account (qq), the SQLCipher key, and
//! optionally the database path; the server then initializes the account
//! in the background (live open + decrypt + index + SSE baseline + watch).
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
    #[serde(default, alias = "token")]
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

    // `state` = this registration's outcome; `status` = the account's state
    // machine value (same enum /health reports), so a client learns whether
    // the account is usable without a second /health round-trip. `db_path`
    // echoes the database the server actually resolved — the request's own
    // db_path is loose (file, Tencent Files-style root, or omitted → the
    // startup scan), so the resolved path is what tells the client which
    // database is in play. Both are omitted when unknown.
    let reply = |state_name: &str, status: Option<AccountStatus>, db_path: Option<&Path>| {
        let mut out = json!({ "success": true, "qq": qq, "state": state_name });
        let obj = out.as_object_mut().expect("json! object literal");
        if let Some(st) = status {
            obj.insert("status".into(), json!(st));
        }
        if let Some(p) = db_path {
            obj.insert("db_path".into(), json!(p.to_string_lossy()));
        }
        Json(out)
    };

    // Idempotent guards for accounts already past the waiting stage. Check
    // before resolving the path so a ready account's reply wins over
    // unknown-qq / invalid-db-path.
    let current = {
        let accs = state.accounts.read();
        accs.iter().find(|a| a.qq == qq).map(|a| a.state)
    };
    // For the idempotent replies the registry path is the one the running
    // account was built from — NOT the (ignored) db_path of this request.
    let registered = || state.init.find_db(qq).map(|i| i.path);
    match current {
        Some(AccountStatus::Ready) => {
            return Ok(reply("already_ready", current, registered().as_deref()))
        }
        Some(AccountStatus::Indexing) => {
            return Ok(reply("in_progress", current, registered().as_deref()))
        }
        _ => {} // awaiting_key / error / unknown -> accept
    }

    let Some(info) = resolve_db_path(&state, qq, params.db_path.as_deref()) else {
        // An explicit path that does not resolve, or an unknown qq. `current`
        // (awaiting_key / error / None) is this account's unchanged status.
        let state_name = if params.db_path.as_deref().is_some_and(|p| !p.is_empty()) {
            "invalid_db_path"
        } else {
            "unknown_qq"
        };
        return Ok(reply(state_name, current, None));
    };

    if validate_key(key).is_err() {
        // Rejected before any state change: the path resolved, the status is
        // still awaiting_key / error / unknown.
        return Ok(reply("invalid_key", current, Some(&info.path)));
    }

    // Flip to indexing atomically with the guard: a concurrent duplicate
    // registration serializes here and observes the new state instead of
    // spawning a second initialization.
    match begin_indexing(&state, qq) {
        Some(AccountStatus::Ready) => {
            return Ok(reply("already_ready", Some(AccountStatus::Ready), registered().as_deref()))
        }
        Some(AccountStatus::Indexing) => {
            return Ok(reply(
                "in_progress",
                Some(AccountStatus::Indexing),
                registered().as_deref(),
            ))
        }
        _ => {}
    }

    // Build the reply BEFORE spawning: `begin_indexing` just set `indexing`,
    // and re-reading the state after the spawn would race the background
    // build (which may already have reached ready/error). Note `indexing`
    // does NOT mean the key is correct — only its format was checked here;
    // the real decrypt verification happens in `init_account`, so a client
    // still has to watch /health for ready.
    let out = reply("accepted", Some(AccountStatus::Indexing), Some(&info.path));

    // Kick off the background build.
    let state_for_init = state.clone();
    let key_owned = key.to_string();
    tokio::spawn(async move { init_account(&state_for_init, info, key_owned).await });
    Ok(out)
}
