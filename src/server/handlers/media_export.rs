//! Targeted media restoration over the whole conversation index.

use std::sync::{Arc, atomic::Ordering};
use axum::{extract::State, http::{HeaderMap, StatusCode}, Json};
use crate::server::{AppState, RegistrationToken};
use crate::server::dto::{MediaExportRequest, MediaExportResult};
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::store::media_export::{ExportContext, export_targeted};
use super::{authorized, media::Params};

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<Json<MediaExportResult>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    let request: MediaExportRequest = serde_json::from_slice(&body)
        .map_err(|_| ApiError::bad_request("媒体导出请求字段缺失或格式错误"))?;
    restore(state, request, || {}).await.map(Json)
}

/// The hook fixes the interleaving after staging and before publication in tests.
#[cfg(feature = "testing")]
pub async fn restore_before_publish(
    state: Arc<AppState>, request: MediaExportRequest, before_publish: impl FnOnce() + Send + 'static,
) -> Result<MediaExportResult, ApiError> {
    restore(state, request, before_publish).await
}

async fn restore(
    state: Arc<AppState>, request: MediaExportRequest, before_publish: impl FnOnce() + Send + 'static,
) -> Result<MediaExportResult, ApiError> {
    if request.talker.is_empty() || request.platform_message_id.is_empty() || request.sender.is_empty() {
        return Err(ApiError::bad_request("talker、platformMessageId 和 sender 不能为空"));
    }
    let (registration, media, root, fallback, kind) = {
        let _lifecycle = state.init.lifecycle.lock();
        if !state.ready.load(Ordering::SeqCst) { return Err(ApiError::not_ready()); }
        let token = RegistrationToken {
            epoch: state.init.epoch.load(Ordering::SeqCst),
            registration_id: state.init.registration.load(Ordering::SeqCst),
        };
        let store = state.store.read();
        let conv = store.find_conversation(&request.talker)
            .ok_or_else(|| ApiError::not_found("目标消息不存在"))?;
        let mut matches = conv.msgs.iter().filter(|m| {
            m.seq.to_string() == request.platform_message_id
                && m.from_uid == request.sender && m.ts == request.timestamp
        });
        let target = matches.next().ok_or_else(|| ApiError::not_found("目标消息不存在"))?;
        if matches.next().is_some() { return Err(conflict("目标消息不唯一")); }
        use crate::parser::types::MsgType;
        let kind = match target.parsed.msg_type {
            MsgType::Image => "images", MsgType::Voice => "voices", MsgType::Video => "videos",
            _ => return Err(unexportable()),
        };
        let media = target.parsed.media.clone().ok_or_else(unexportable)?;
        let fallback = media.key().and_then(|key| store.media.get(key)).map(|m| m.local_path.clone());
        if media.local_path.is_none() && fallback.is_none() && !media.urls.is_empty() {
            return Err(unexportable());
        }
        (token, media, store.media_root.clone(), fallback, kind)
    };
    let context = ExportContext {
        root: state.export_root.as_ref().clone(), base_url: String::new(), talker: request.talker,
    };
    tokio::task::spawn_blocking(move || {
        let result = export_targeted(&context, &media, kind, root.as_deref(), fallback.as_deref());
        before_publish();
        let _lifecycle = state.init.lifecycle.lock();
        if registration.is_cancelled(&state.init) { return Err(conflict("导出期间账号注册发生切换")); }
        let result = result.map_err(|_| ApiError::internal("媒体读取或导出失败"))?
            .ok_or_else(|| ApiError::not_found("原始媒体已不可恢复"))?
            .publish().map_err(|_| ApiError::internal("媒体发布失败"))?;
        Ok(MediaExportResult {
            file_name: result.file_name.clone(), media_id: result.file_name.clone(),
            media_url: format!("/api/v1/media/{}", result.file_name), success: true,
        })
    }).await.map_err(|_| ApiError::internal("媒体导出任务异常"))?

}

fn conflict(message: &str) -> ApiError {
    ApiError { status: StatusCode::CONFLICT, message: message.into() }
}

fn unexportable() -> ApiError {
    ApiError { status: StatusCode::UNPROCESSABLE_ENTITY, message: "目标消息没有可导出的本地媒体".into() }
}
