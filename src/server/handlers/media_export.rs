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
    let (registration, qq, media, root, fallback, kind) = {
        let _lifecycle = state.init.lifecycle.lock();
        if !state.ready.load(Ordering::SeqCst) { return Err(ApiError::not_ready()); }
        let token = RegistrationToken {
            epoch: state.init.epoch.load(Ordering::SeqCst),
            registration_id: state.init.registration.load(Ordering::SeqCst),
        };
        // 绑定账号（与 pull 面 ownerId 的取法同源）：store 没有账号维度，
        // 注册表里唯一的 ready 条目就是它。导出前的世代/指纹采样要在**这个**
        // 账号的同步句柄上做。
        let qq = state
            .accounts
            .read()
            .iter()
            .find(|a| a.state.is_ready())
            .map(|a| a.qq.clone())
            .ok_or_else(|| conflict("导出期间账号注册发生切换"))?;
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
        (token, qq, media, store.media_root.clone(), fallback, kind)
    };
    let context = ExportContext {
        root: state.export_root.as_ref().clone(), base_url: String::new(), talker: request.talker,
    };
    // 记下请求开始时的数据源世代与身份指纹。
    //
    // 只比世代不够：它只在同步路径的采样里推进，而导出的整个阻塞任务都可能持有
    // reader/store 的锁——这期间轮询跑不了，计数器必然不动。所以这里自己采一次
    // 指纹（路径 + 创建时间 + 偏移 1024 起的 salt），发布前再采一次比对。
    let account = state.sync.snapshot().into_iter().find(|a| a.qq == qq).ok_or_else(|| conflict("目标消息不存在"))?;
    let generation = account.generation_handle().load(Ordering::SeqCst);
    let fingerprint = crate::db::source_identity::fingerprint(&account.db_path).ok();
    let account_for_task = account.clone();
    tokio::task::spawn_blocking(move || {
        let result = export_targeted(&context, &media, kind, root.as_deref(), fallback.as_deref());
        before_publish();
        let _lifecycle = state.init.lifecycle.lock();
        if registration.is_cancelled(&state.init) { return Err(conflict("导出期间账号注册发生切换")); }
        if account_for_task.generation_handle().load(Ordering::SeqCst) != generation {
            return Err(source_changed());
        }
        // 指纹比对是这条复查真正起作用的那一半：换库会改变它。起始采样失败时退回
        // 到世代比较（尽力检测，边界写进文档）；起始采到了、现在采不到，按「无法
        // 证明」处理——不发布也不赌。
        if let Some(before) = fingerprint {
            let current = crate::db::source_identity::fingerprint(&account_for_task.db_path).ok();
            if current.as_ref() != Some(&before) {
                return Err(source_changed());
            }
        }
        let result = result.map_err(|_| ApiError::internal("媒体读取或导出失败"))?
            .ok_or_else(|| ApiError::not_found("原始媒体已不可恢复"))?
            .publish().map_err(|_| ApiError::internal("媒体发布失败"))?;
        Ok(MediaExportResult {
            file_name: media.file_name.clone().unwrap_or_default(),
            media_id: result.file_name.clone(),
            media_url: format!("/api/v1/media/{}", result.file_name), success: true,
        })
    }).await.map_err(|_| ApiError::internal("媒体导出任务异常"))?

}

fn conflict(message: &str) -> ApiError {
    ApiError { status: StatusCode::CONFLICT, message: message.into(), reason: None }
}

/// 数据源身份在导出途中变化：与游标失效同一个机器可读原因——客户端的处置一致
/// （丢弃已有进度、从头重来），错误文本会改，`reason` 不会。
fn source_changed() -> ApiError {
    ApiError {
        status: StatusCode::CONFLICT,
        message: "导出期间数据源发生变化".into(),
        reason: Some(crate::server::cursor::REASON_CURSOR_RESET),
    }
}

fn unexportable() -> ApiError {
    ApiError { status: StatusCode::UNPROCESSABLE_ENTITY, message: "目标消息没有可导出的本地媒体".into(), reason: None }
}
