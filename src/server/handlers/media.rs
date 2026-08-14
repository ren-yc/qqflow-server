//! GET /api/v1/media/{id} — serve a media file from QQ's local cache.
//! GET|POST /api/v1/media/{talker}/{media_type}/{file} — serve a WeFlow-style
//! exported file from the media export root (`media=1` on /api/v1/messages).
//!
//! The single-segment `{id}` route resolves a store key (md5 hex or uuid
//! from the structured media metadata) and streams the file read-only from
//! its local cache path ("45812"). Clients can never inject a filesystem
//! path — only keys that the index registered. The three-segment route
//! serves files exported under `<exportRoot>/<talker>/<mediaType>/<file>`
//! with media-type whitelisting and traversal protection. QQ clears its
//! media cache: missing files yield a 404 with a clear message.

use std::sync::Arc;

use axum::extract::{Path, Query, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use crate::store::AppState;

use super::{authorized, media_content_type};
use crate::server::error::ApiError;

#[derive(Debug, Default, Deserialize, serde::Serialize)]
pub struct Params {
    #[serde(default)]
    pub access_token: Option<String>,
}

/// Stream a local file with a content-type from its extension. The file
/// must exist (canonicalized paths are pre-verified by the callers).
///
/// Extension source: the file-name hint first (entry.file_name / URL
/// segment), then the resolved local file's own extension — uuid-keyed
/// media with a missing or extension-less file name must still be typed
/// from what is actually on disk (e.g. a real `.silk`/`.jpg`).
async fn serve_file(local_path: &std::path::Path, file_name_hint: Option<&str>) -> Result<Response, ApiError> {
    let file = tokio::fs::File::open(local_path)
        .await
        .map_err(|_| ApiError::not_found("本地缓存已清理，媒体文件不存在"))?;
    let len = file
        .metadata()
        .await
        .map_err(|_| ApiError::not_found("本地缓存已清理，媒体文件不存在"))?
        .len();
    let hint_ext = file_name_hint.and_then(|n| std::path::Path::new(n).extension());
    let ext = hint_ext
        .or_else(|| local_path.extension())
        .and_then(|e| e.to_str())
        .unwrap_or("");
    let content_type = media_content_type(ext);
    let stream = tokio_util::io::ReaderStream::new(file);
    let body = axum::body::Body::from_stream(stream);
    let resp = Response::builder()
        .status(axum::http::StatusCode::OK)
        .header(axum::http::header::CONTENT_TYPE, content_type)
        .header(axum::http::header::CONTENT_LENGTH, len)
        .body(body)
        .map_err(|e| ApiError::internal(format!("响应构建失败: {e}")))?;
    Ok(resp)
}

/// Store-key route: `GET|POST /api/v1/media/{id}`.
pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
    Path(id): Path<String>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let params = super::merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let (local_path, file_name) = {
        // Take the entry out of the lock, then resolve the path on the
        // blocking pool — canonicalize is real file IO and must neither run
        // on a tokio worker nor extend the store read lock over a syscall.
        let (entry, media_root) = {
            let store = state.store.read();
            (
                store.media.get(&id).cloned().ok_or_else(|| ApiError::not_found("媒体不存在"))?,
                store.media_root.clone(),
            )
        };
        let path = tokio::task::spawn_blocking(move || {
            crate::store::media::resolve_local_path(&entry.local_path, media_root.as_deref())
        })
        .await
        .map_err(|e| ApiError::internal(format!("媒体路径解析任务异常: {e}")))?
        .ok_or_else(|| ApiError::not_found("本地缓存已清理，媒体文件不存在"))?;
        (path, entry.file_name)
    };
    serve_file(&local_path, file_name.as_deref()).await
}

/// Export-dir route: `GET|POST /api/v1/media/{talker}/{media_type}/{file}`.
/// `media_type` is whitelisted (images/voices/videos/emojis); traversal
/// components are rejected at join time and the canonicalized result must
/// stay under the canonicalized export root.
pub async fn exported_handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Query(params): Query<Params>,
    Path((talker, media_type, file)): Path<(String, String, String)>,
    body: axum::body::Bytes,
) -> Result<Response, ApiError> {
    let params = super::merge_body(params, &body).await?;
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    if !matches!(media_type.as_str(), "images" | "voices" | "videos" | "emojis") {
        return Err(ApiError::not_found("媒体不存在"));
    }
    // Path segments come from the URL: reject empty / "." / ".." / anything
    // carrying a separator (the media_type whitelist already narrows one
    // segment; talker and file get the same treatment here).
    let seg_ok = |s: &str| !s.is_empty() && s != "." && s != ".." && !s.contains('/') && !s.contains('\\');
    if !(seg_ok(&talker) && seg_ok(&media_type) && seg_ok(&file)) {
        return Err(ApiError::not_found("媒体不存在"));
    }
    // Canonicalize both paths on the blocking pool (real file IO), then
    // verify the result stays under the canonicalized export root (defense
    // in depth against any remaining join trickery).
    let (canonical, canonical_root) = {
        let root = state.export_root.as_ref().clone();
        // The original `file` is still needed below as the extension hint.
        let probe_file = file.clone();
        tokio::task::spawn_blocking(move || {
            let joined = root.join(&talker).join(&media_type).join(&probe_file);
            (std::fs::canonicalize(&joined), std::fs::canonicalize(&root))
        })
        .await
        .map_err(|e| ApiError::internal(format!("媒体路径解析任务异常: {e}")))?
    };
    let canonical = canonical.map_err(|_| ApiError::not_found("本地缓存已清理，媒体文件不存在"))?;
    let canonical_root = canonical_root.map_err(|_| ApiError::not_found("媒体导出目录不存在"))?;
    if !canonical.starts_with(&canonical_root) {
        return Err(ApiError::not_found("媒体不存在"));
    }
    serve_file(&canonical, Some(&file)).await
}
