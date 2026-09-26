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

use axum::extract::{Path as UrlPath, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::db::scan::{self, DbInfo};
use crate::keystore::validate_key;
use crate::server::dto::{
    AccountConflict, AccountDeregistered, AccountNotRegistered, AccountQqMismatch,
    AccountRegistered, AccountView, AccountsList,
};
use crate::server::error::{ApiError, EnvelopeQuery};
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
    EnvelopeQuery(params): EnvelopeQuery<ListParams>,
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    // Snapshot under the read lock, then resolve paths after releasing it:
    // `find_db` takes the registry mutex, and taking it while holding the
    // accounts lock would nest two locks that are otherwise independent.
    let accounts: Vec<_> = state.accounts.read().iter().cloned().collect();
    let accounts: Vec<AccountView> = accounts
        .into_iter()
        .map(|a| {
            // 先解析路径（要借用 `a.qq`），再把字段移进来。
            //
            // 类型化赋值：`error` / `db_path` 是**条件键**（不知道就不出现），此前是往
            // `Value` 里按字符串键 `insert` —— 键名写错不会报错，只会静默多一个键。
            let db_path = state
                .init
                .find_db(&a.qq)
                .map(|info| info.path.to_string_lossy().into_owned());
            AccountView {
                db_path,
                error: a.error,
                message_count: a.message_count,
                qq: a.qq,
                state: a.state,
            }
        })
        .collect();
    let body = serde_json::to_value(AccountsList { accounts, success: true })
        .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?;
    Ok(Json(body))
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
    EnvelopeQuery(params): EnvelopeQuery<Params>,
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
        Json(
            serde_json::to_value(AccountRegistered {
                db_path: db_path.map(|p| p.to_string_lossy().into_owned()),
                qq: qq.to_string(),
                state: state_name.to_string(),
                status,
                success: true,
            })
            .expect("账号回复必须可序列化"),
        )
    };

    // A different account already holds the single binding. The authoritative
    // check is inside `begin_indexing`'s write lock; this one is a fast path
    // so a misconfigured client does not make the server stat paths on every
    // retry. `occupied_by` names the incumbent so the client can log which
    // account it is actually talking to instead of retrying forever.
    let conflict = |qq_in_use: &str, status: AccountStatus| {
        Json(
            serde_json::to_value(AccountConflict {
                occupied_by: qq_in_use.to_string(),
                occupied_status: status,
                qq: qq.to_string(),
                state: "account_conflict".to_string(),
                success: true,
            })
            .expect("冲突回复必须可序列化"),
        )
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
    EnvelopeQuery(params): EnvelopeQuery<DeleteParams>,
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

    // 三个分支的**键集不同**，各建 struct、各自序列化 —— 而不是三份 `json!` 字面量
    // （键名写错只有运行时才知道）。
    let out = match outcome {
        DeregisterOutcome::Deregistered { previous, index_cleared, purged_dirs } => {
            serde_json::to_value(AccountDeregistered {
                index_cleared,
                previous_status: previous,
                purged_dirs,
                purged_media: purge_media,
                qq: qq.clone(),
                state: "deregistered".to_string(),
                success: true,
            })
            .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?
        }
        // Nothing was bound. Idempotent by design: a client that retries a
        // deregistration it already completed gets a 200, not an error.
        DeregisterOutcome::NotRegistered => serde_json::to_value(AccountNotRegistered {
            index_cleared: false,
            purged_dirs: 0,
            purged_media: false,
            qq: qq.clone(),
            state: "not_registered".to_string(),
            success: true,
        })
        .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?,
        // The interlock tripped: a different account holds the binding and is
        // left completely untouched.
        DeregisterOutcome::QqMismatch { occupied_by, status } => {
            serde_json::to_value(AccountQqMismatch {
                index_cleared: false,
                occupied_by,
                occupied_status: status,
                purged_dirs: 0,
                purged_media: false,
                qq,
                state: "qq_mismatch".to_string(),
                success: true,
            })
            .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?
        }
    };
    Ok(Json(out))
}
