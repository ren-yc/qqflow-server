//! `/api/v1/accounts` — client-driven account registration and inspection.
//!
//! `POST` registers: a downstream client supplies the account (qq), the
//! SQLCipher key, and optionally the database path; the server then
//! initializes the account in the background (live open + decrypt + index +
//! SSE baseline + watch). `GET` returns the account detail `/health` no
//! longer discloses. `DELETE /api/v1/accounts/{qq}` undoes a registration,
//! returning the server to its unregistered boot state.
//!
//! All three are token-protected and deliberately NOT gated on readiness —
//! without an account the server would never become ready, so `POST` is the
//! bootstrap endpoint, `GET` is how a client watches it get there, and
//! `DELETE` must stay reachable for an account stuck in `error`.
//! Keys live in memory only.

use std::path::Path;
use std::sync::Arc;

use axum::extract::{Path as UrlPath, Query, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::{json, Value};

use crate::db::scan::{self, DbInfo};
use crate::keystore::validate_key;
use crate::server::error::ApiError;
use crate::server::{
    begin_indexing, bound_account, deregister_account, init_account, AccountStatus, BindOutcome,
    DeregisterOutcome,
};
use crate::store::AppState;

use super::{authorized, merge_body, FlexBool};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub qq: Option<String>,
    pub key: Option<String>,
    pub db_path: Option<String>,
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

/// `GET /api/v1/accounts` params — the token only.
///
/// A separate struct from [`Params`] on purpose: this route takes no `key`,
/// and a GET query string is the one transport that routinely lands in
/// proxy logs and shell history.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct ListParams {
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

/// `GET /api/v1/accounts` — the account detail `/health` no longer carries.
///
/// Token-protected and NOT ready-gated: a client polls this while the account
/// is still `indexing`, which is exactly when the server is not ready. No
/// `merge_body` either — GET carries no body, so the token arrives via the
/// headers or the query string.
pub async fn list_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<ListParams>,
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    // Snapshot under the read lock, then resolve paths after releasing it:
    // `find_db` takes the registry mutex, and taking it while holding the
    // accounts lock would nest two locks that are otherwise independent.
    let accounts: Vec<_> = state.accounts.read().iter().cloned().collect();
    let accounts: Vec<Value> = accounts
        .into_iter()
        .map(|a| {
            let mut out = json!({
                "qq": a.qq,
                "state": a.state,
                "message_count": a.message_count,
            });
            let obj = out.as_object_mut().expect("json! object literal");
            if let Some(e) = a.error {
                obj.insert("error".into(), json!(e));
            }
            if let Some(info) = state.init.find_db(&a.qq) {
                obj.insert("db_path".into(), json!(info.path.to_string_lossy()));
            }
            out
        })
        .collect();
    Ok(Json(json!({ "success": true, "accounts": accounts })))
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

    // A different account already holds the single binding. The authoritative
    // check is inside `begin_indexing`'s write lock; this one is a fast path
    // so a misconfigured client does not make the server stat paths on every
    // retry. `occupied_by` names the incumbent so the client can log which
    // account it is actually talking to instead of retrying forever.
    let conflict = |qq_in_use: &str, status: AccountStatus| {
        Json(json!({
            "success": true,
            "qq": qq,
            "state": "account_conflict",
            "occupied_by": qq_in_use,
            "occupied_status": status,
        }))
    };

    // Idempotent guards for accounts already past the waiting stage. Check
    // before resolving the path so a ready account's reply wins over
    // unknown-qq / invalid-db-path.
    let current = {
        let accs = state.accounts.read();
        if let Some(b) = bound_account(&accs).filter(|b| b.qq != qq) {
            return Ok(conflict(&b.qq, b.state));
        }
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

    // Claim the binding atomically with the guard: concurrent registrations
    // serialize here, so a duplicate observes the new state instead of
    // spawning a second initialization, and a different qq loses the race
    // instead of overwriting the winner's index.
    match begin_indexing(&state, qq) {
        BindOutcome::SameQq(AccountStatus::Ready) => {
            return Ok(reply("already_ready", Some(AccountStatus::Ready), registered().as_deref()))
        }
        BindOutcome::SameQq(status) => {
            return Ok(reply("in_progress", Some(status), registered().as_deref()))
        }
        BindOutcome::Occupied { qq: in_use, status } => return Ok(conflict(&in_use, status)),
        BindOutcome::Bound => {}
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

/// `DELETE /api/v1/accounts/{qq}` params.
///
/// `purge_media` defaults to **false**: exported media is derived data the
/// client may still be serving from its own cache, and deleting files is not
/// undoable, so it has to be asked for explicitly.
#[derive(Debug, Default, Deserialize, Serialize)]
pub struct DeleteParams {
    #[serde(default)]
    pub purge_media: FlexBool,
    #[serde(default, alias = "token")]
    pub access_token: Option<String>,
}

/// `DELETE /api/v1/accounts/{qq}` (and the `POST .../{qq}/deregister` alias)
/// — undo a registration and return the server to its unregistered state.
///
/// The `qq` in the path is a safety interlock, not a selector: there is only
/// ever one binding, so naming the wrong account is a client bug worth
/// reporting (`qq_mismatch`) rather than silently deregistering whatever
/// happens to be bound.
///
/// Token-protected, NOT ready-gated (an account stuck in `error` is exactly
/// what a client needs to clear), and every business outcome is HTTP 200 with
/// the verdict in `state` — matching how `POST` reports its rejections.
pub async fn delete_handler(
    State(state): State<Arc<AppState>>,
    UrlPath(qq): UrlPath<String>,
    headers: HeaderMap,
    Query(params): Query<DeleteParams>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if qq.is_empty() {
        return Err(ApiError::bad_request("缺少必填参数 qq"));
    }
    let purge_media = params.purge_media.is_true();

    // `deregister_account` blocks: it takes the store write lock, joins
    // nothing but does file removal when purging, and must not run on the
    // async runtime's poll thread.
    let state_for_task = state.clone();
    let qq_for_task = qq.clone();
    let outcome = tokio::task::spawn_blocking(move || {
        deregister_account(&state_for_task, &qq_for_task, purge_media)
    })
    .await
    .map_err(|e| ApiError::internal(format!("注销任务失败: {e}")))?;

    let out = match outcome {
        DeregisterOutcome::Deregistered { previous, index_cleared, purged_dirs } => json!({
            "success": true,
            "qq": qq,
            "state": "deregistered",
            // The state the account was in when the request landed — lets a
            // client tell "I cancelled an in-flight build" from "I unbound a
            // ready account".
            "previous_status": previous,
            "index_cleared": index_cleared,
            "purged_media": purge_media,
            "purged_dirs": purged_dirs,
        }),
        // Nothing was bound. Idempotent by design: a client that retries a
        // deregistration it already completed gets a 200, not an error.
        DeregisterOutcome::NotRegistered => json!({
            "success": true,
            "qq": qq,
            "state": "not_registered",
            "index_cleared": false,
            "purged_media": false,
            "purged_dirs": 0,
        }),
        // The interlock tripped: a different account holds the binding and is
        // left completely untouched.
        DeregisterOutcome::QqMismatch { occupied_by, status } => json!({
            "success": true,
            "qq": qq,
            "state": "qq_mismatch",
            "occupied_by": occupied_by,
            "occupied_status": status,
            "index_cleared": false,
            "purged_media": false,
            "purged_dirs": 0,
        }),
    };
    Ok(Json(out))
}
