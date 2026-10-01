//! GET /api/v1/media/{id} — 按 id 取字节。
//!
//! `{id}` 有**两个来源**，按顺序解析：
//!
//! 1. **store 键**（md5 hex 或 uuid）：索引登记过的本地缓存路径。命中即服务 —— 它是最短的
//!    一跳，也是 `mediaId` 承诺「出现即可取」的落点。
//! 2. **导出文件名**：在导出根下按名解析，布局是
//!    `<root>/<会话>/<images|voices|videos|emojis>/<file>`。`{id}` 不带会话，所以把四个
//!    已知的类型目录逐一代入（只做 `is_file` 判断、不递归）。
//!
//! 未命中统一「媒体不存在」。QQ 会清理媒体缓存：缓存里的文件没了就是 404，不去猜别的来源。

use std::path::Path;
use std::sync::Arc;

use axum::extract::{Path as AxumPath, State};
use axum::http::HeaderMap;
use axum::response::Response;
use serde::Deserialize;

use crate::server::AppState;

use super::{authorized, media_content_type};
use crate::server::error::{ApiError, EnvelopeQuery};

#[derive(Debug, Default, Deserialize)]
pub struct Params {
    #[serde(default)]
    pub access_token: Option<String>,
}

/// 参与导出的四个类型目录。它同时是**按名取字节的白名单**：别的目录里就算躺着同名文件也不服务
/// —— 那些位置不是导出管线写出来的，服务它们等于把「导出根」变成「任意文件根」。
const ALLOWED_TYPES: [&str; 4] = ["images", "voices", "videos", "emojis"];

/// Stream a local file with a content-type from its extension. The file
/// must exist (canonicalized paths are pre-verified by the callers).
///
/// Extension source: the file-name hint first (entry.file_name / URL segment),
/// then the resolved local file's own extension — uuid-keyed media with a
/// missing or extension-less file name must still be typed from what is
/// actually on disk (e.g. a real `.silk`/`.jpg`).
async fn serve_file(local_path: &std::path::Path, file_name_hint: Option<&str>) -> Result<Response, ApiError> {
    let file = tokio::fs::File::open(local_path)
        .await
        .map_err(|_| ApiError::not_found("媒体不存在"))?;
    let len = file
        .metadata()
        .await
        .map_err(|_| ApiError::not_found("媒体不存在"))?
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

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    AxumPath(id): AxumPath<String>,
) -> Result<Response, ApiError> {
    // 鉴权只看查询串：POST body 不是鉴权通道（本面也不接 body —— 它是只读面）。
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    // 名字要成为路径分量（回落那一步）。与导出侧共用同一条边界规则（pathsafe）：这条路由
    // 同样要挡住尾点、尾空格（Win32 会剥掉）与冒号（NTFS 数据流）—— 它们都不带路径分隔符，
    // 只滤分隔符会漏。违规与「没这个名字」对调用方是同一件事：都拿不到字节。
    if !crate::pathsafe::safe_segment(&id) {
        return Err(ApiError::not_found("媒体不存在"));
    }

    // ① store 键优先：`mediaId` 的承诺落在这里。
    let (entry, media_root) = {
        let store = state.store.read();
        (store.media.get(&id).cloned(), store.media_root.clone())
    };
    if let Some(entry) = entry {
        let path = tokio::task::spawn_blocking(move || {
            crate::store::media::resolve_local_path(&entry.local_path, media_root.as_deref())
        })
        .await
        .map_err(|e| ApiError::internal(format!("媒体路径解析任务异常: {e}")))?;
        // 键命中但文件已被清理：这是「缓存过期」，不是「名字不存在」——不去猜导出根，
        // 因为同一个 id 在那边指的是另一个名字空间。
        let path = path.ok_or_else(|| ApiError::not_found("媒体不存在"))?;
        return serve_file(&path, entry.file_name.as_deref()).await;
    }

    // ② 导出根回落：`{id}` 是导出文件名。扫描与包含性检查都是真实文件 IO，
    //    必须在阻塞池上做（并发取字节不该饿死别的请求，包括 SSE 保活）。
    let root = state.export_root.as_ref().clone();
    let probe = id.clone();
    let found = tokio::task::spawn_blocking(move || {
        let candidate = find_exported(&root, &probe)?;
        let canonical = candidate.canonicalize().ok()?;
        let canonical_root = root.canonicalize().ok()?;
        // 包含性检查：符号链接可以让「文件存在」为真而真实目标在根外，所以必须规范化后再比
        // 前缀；**取不到根就失败** —— 拿未规范化的根去比永远不相等，那会把检查变成永假。
        canonical.starts_with(&canonical_root).then_some(canonical)
    })
    .await
    .map_err(|e| ApiError::internal(format!("媒体路径解析任务异常: {e}")))?;
    let found = found.ok_or_else(|| ApiError::not_found("媒体不存在"))?;
    serve_file(&found, Some(&id)).await
}

/// 在导出根下按文件名查找。布局是 `<root>/<会话>/<类型>/<文件>`，而 id 不带会话，
/// 所以把已知的四个类型目录逐一代入。
///
/// **同名多命中**：候选内容**一致**时取排序后的第一个；**不一致**时返回 `None`（调用方给
/// 404）。为什么不能「取第一个就完事」：按名解析是**跨会话**的，别的会话里可能躺着一个同名
/// 但内容不同的文件 —— 那时随便挑一个，等于把「出现即可取」变成「出现即可取到某个东西」，
/// 而调用方无从察觉。
///
/// 比较顺序与成本：先比 **size** 短路（绝大多数同名异内容在大小上就不同），只有 size 相同才
/// 逐字节读整份文件。读放大 = 候选数 × 文件大小，是**线性**成本 —— 写在这里而不是藏在实现里。
fn find_exported(root: &Path, name: &str) -> Option<std::path::PathBuf> {
    let entries = std::fs::read_dir(root).ok()?;
    // 目录遍历顺序本身不保证稳定 ⇒ 先排序，「取第一个」才是确定的（否则同一份导出根两次请求
    // 可能命中不同的候选，而两个候选内容一致时看不出问题、不一致时就成了随机 404）。
    let mut talkers: Vec<std::path::PathBuf> = entries
        .flatten()
        .map(|e| e.path())
        .filter(|p| p.is_dir())
        .collect();
    talkers.sort();
    // 每个候选**先**规范化并确认仍在导出根内，**再**参与比较：" + q("is_file") + " 会跟随符号链接，
    // 而链接可以把「根内有个文件」变成「根外有个文件」—— 比较阶段会读它的字节。
    // 取不到根就整体不命中（拿未规范化的根去比永远不相等，那会把检查变成永假）。
    let canonical_root = root.canonicalize().ok()?;
    let mut hits: Vec<std::path::PathBuf> = Vec::new();
    for dir in talkers {
        for media_type in ALLOWED_TYPES {
            let candidate = dir.join(media_type).join(name);
            if !candidate.is_file() {
                continue;
            }
            match candidate.canonicalize() {
                Ok(c) if c.starts_with(&canonical_root) => hits.push(c),
                _ => tracing::warn!(
                    "media id {name:?} 的候选 {} 不在导出根内：跳过",
                    candidate.display()
                ),
            }
        }
    }
    let first = hits.first()?.clone();
    if hits.len() > 1 {
        let baseline = std::fs::metadata(&first).ok()?.len();
        for other in &hits[1..] {
            let size = std::fs::metadata(other).ok()?.len();
            if size != baseline || !same_contents(&first, other) {
                tracing::warn!(
                    "media id {name:?} 在导出根下有多个同名但内容不同的文件：拒绝服务（命中 {} 处）",
                    hits.len()
                );
                return None;
            }
        }
    }
    Some(first)
}

/// 逐字节比较两个文件（只在 size 相同时才被调用）。
fn same_contents(a: &Path, b: &Path) -> bool {
    use std::io::Read;
    let (Ok(mut fa), Ok(mut fb)) = (std::fs::File::open(a), std::fs::File::open(b)) else {
        return false;
    };
    let mut ba = [0u8; 64 * 1024];
    let mut bb = [0u8; 64 * 1024];
    loop {
        let (na, nb) = match (fa.read(&mut ba), fb.read(&mut bb)) {
            (Ok(na), Ok(nb)) => (na, nb),
            _ => return false,
        };
        if na != nb || ba[..na] != bb[..nb] {
            return false;
        }
        if na == 0 {
            return true;
        }
    }
}
