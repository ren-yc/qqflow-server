//! GET /api/v1/messages — 原生/富数据形状的消息查询。
//!
//! ChatLab 形状走 /chatlab/messages（见 chatlab_messages）。本模块保留**两个面共用**的部分
//! （参数合成、查询、导出批次）：同一批参数在两个面上给出不同的页，是最难查的一类漂移 ——
//! 两面各抄一份时，改一处忘另一处不会有任何东西变红。

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};
use serde_json::Value;

use crate::store::media_export::{self, ExportContext, ExportOptions};
use crate::store::query::{query_messages, MessageOut, MessageQuery};
use crate::server::AppState;

use super::{authorized, merge_body, parse_time_bound, FlexBool};
use crate::server::dto::{MediaEnvelope, MessagesNative};
use crate::server::error::{ApiError, EnvelopeQuery};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub talker: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    pub start: Option<String>,
    pub end: Option<String>,
    pub keyword: Option<String>,
    /// 媒体导出开关：true 时**真正导出本页**的媒体，并回填每条消息的导出字段。
    #[serde(default)]
    pub media: FlexBool,
    /// 分类型子开关（默认开；false / "0" 关掉）。
    #[serde(default)]
    pub image: FlexBool,
    #[serde(default)]
    pub voice: FlexBool,
    #[serde(default)]
    pub video: FlexBool,
    /// 本仓库暂无可导出的 emoji 文件（QQ 的 emoji 只带展示文本），保留同样的开关以免
    /// 参数面与 weflow 分叉。
    #[serde(default)]
    pub emoji: FlexBool,
    #[serde(default)]
    pub access_token: Option<String>,
}

fn default_limit() -> usize {
    100
}

/// 把两个面共用的参数合成一次查询。
///
/// **参数怎么解释只有这一处**：时间界（`YYYYMMDD` 的 end 覆盖整天）、关键词大小写、
/// `limit` 上限、`talker` 必填 —— 任一条在两处各写一遍就会漂移，而漂移的表现是
/// 「同一批参数在两个面上给出不同的页」。
pub(crate) fn build_query<'a>(
    talker: Option<&'a str>,
    limit: usize,
    offset: usize,
    start: Option<&str>,
    end: Option<&str>,
    keyword: Option<&'a str>,
) -> Result<MessageQuery<'a>, ApiError> {
    let talker = talker
        .filter(|s| !s.is_empty())
        .ok_or_else(|| ApiError::bad_request("缺少必填参数 talker"))?;
    Ok(MessageQuery {
        talker,
        limit,
        offset,
        start: start.and_then(|s| parse_time_bound(s, false)),
        // 上界取**当天末刻**：end=20250101 读作「到 1 月 1 日为止」，取当天 0 点会让那一整天
        // 被静默排除在外。
        end: end.and_then(|s| parse_time_bound(s, true)),
        keyword: keyword.filter(|k| !k.is_empty()),
    })
}

/// 一次导出请求的分类型开关。
pub(crate) struct MediaSwitches {
    pub image: bool,
    pub voice: bool,
    pub video: bool,
    pub emoji: bool,
}

impl MediaSwitches {
    pub(crate) fn from_flags(
        image: &FlexBool,
        voice: &FlexBool,
        video: &FlexBool,
        emoji: &FlexBool,
    ) -> Self {
        Self {
            image: !image.is_false(),
            voice: !voice.is_false(),
            video: !video.is_false(),
            emoji: !emoji.is_false(),
        }
    }
}

/// 执行一批导出（阻塞池），返回填好导出字段的消息与成功条数。
///
/// 调用点必须在**读 guard 的 scope 之外**：guard 不是 Send，跨 await 拿着它编译不过。
/// store 的媒体表在这里取一次快照（`media_entries`）—— 行自身的缓存路径缺失时
/// （缓存索引兜底救回来的那些）导出走登记项，于是 `media=1` 与取字节用同一个来源。
pub(crate) async fn run_export(
    state: &AppState,
    talker: &str,
    switches: MediaSwitches,
    items: Vec<MessageOut>,
) -> Result<(Vec<MessageOut>, usize), ApiError> {
    let opts = ExportOptions {
        image: switches.image,
        voice: switches.voice,
        video: switches.video,
        emoji: switches.emoji,
    };
    let (media_root, media_entries) = {
        let store = state.store.read();
        (store.media_root.clone(), store.media.clone())
    };
    let ctx = ExportContext {
        root: state.export_root.as_ref().clone(),
        base_url: state.base_url.as_str().to_string(),
        talker: talker.to_string(),
    };
    tokio::task::spawn_blocking(move || {
        media_export::export_page(&ctx, &opts, media_root.as_deref(), &media_entries, items)
    })
    .await
    .map_err(|e| ApiError::internal(format!("媒体导出任务异常: {e}")))
}

/// WeFlow 消息信封形状 —— 两条路径（导出 / 不导出）共用同一个构造点，
/// 契约字段集就不会在它们之间漂移。
fn envelope(
    talker: &str,
    count: usize,
    has_more: bool,
    media: MediaEnvelope,
    messages: Vec<MessageOut>,
) -> Value {
    // 构造 DTO 后 `to_value`：`json!` 与 `to_value` 都经 BTreeMap（键被排序），因此**输出
    // 逐字节不变**，而类型化构造让「键名写错」变成编译错误。
    serde_json::to_value(MessagesNative {
        count,
        has_more,
        media,
        messages,
        success: true,
        talker: talker.to_string(),
    })
    .expect("消息信封必须可序列化")
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<Json<Value>, ApiError> {
    let params = merge_body(params, &body).await?;
    // 鉴权只看**查询串**：POST body 不是鉴权通道。
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let limit = params.limit.clamp(1, 10000);
    let q = build_query(
        params.talker.as_deref(),
        limit,
        params.offset,
        params.start.as_deref(),
        params.end.as_deref(),
        params.keyword.as_deref(),
    )?;
    let media_on = params.media.is_true();
    let switches = MediaSwitches::from_flags(&params.image, &params.voice, &params.video, &params.emoji);

    let (items, has_more) = {
        let store = state.store.read();
        query_messages(&store, &q)
    };

    let (messages, media) = if media_on {
        // WeFlow 形状的导出：把本页的媒体拷进导出根，并回填每条消息的导出字段。
        let (messages, exported) = run_export(&state, q.talker, switches, items).await?;
        (
            messages,
            MediaEnvelope {
                count: exported,
                enabled: true,
                // 只有真的执行了导出才给导出根：「没导出」与「导出到空路径」在下游是两件事。
                export_path: Some(state.export_root.to_string_lossy().into_owned()),
            },
        )
    } else {
        // 未请求导出：能力信封照旧，媒体元数据仍随每条消息下发。
        let media_count = items.iter().filter(|m| m.media.is_some()).count();
        (
            items,
            MediaEnvelope {
                count: media_count,
                enabled: true,
                export_path: None,
            },
        )
    };

    Ok(Json(envelope(
        q.talker,
        messages.len(),
        has_more,
        media,
        messages,
    )))
}
