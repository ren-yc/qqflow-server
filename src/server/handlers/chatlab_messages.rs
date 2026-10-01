//! GET /chatlab/messages —— ChatLab 形状的消息面。
//!
//! 它是原「混合面」（/api/v1/messages?chatlab=1）的**新家**：老面不再输出 ChatLab 形状，
//! 而这里天生就是 ChatLab 形状 —— 调用方不必知道还有另一种。
//!
//! 与原生面的差异都在**参数与信封**上：cursor 翻页（同时接受 offset）、信封是
//! {talker,count,page,chatlab,meta,members,messages}、**没有 success**、消息**升序**。
//! 参数合成、查询与导出**与原生面共用同一份实现**（messages 模块）—— 抄一份就会漂移，
//! 而漂移的后果是两个面对同一批参数给出不同的页。

use std::sync::Arc;

use axum::extract::{State};
use axum::http::HeaderMap;
use axum::Json;
use serde::{Deserialize, Serialize};

use crate::server::dto::{ChatlabMember, ChatlabMessage, ChatlabMessages, Page};
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

use super::messages::{build_query, run_export, MediaSwitches};
use super::{authorized, merge_body, FlexBool};

#[derive(Debug, Default, Deserialize, Serialize)]
pub struct Params {
    pub talker: Option<String>,
    #[serde(default = "default_limit")]
    pub limit: usize,
    #[serde(default)]
    pub offset: usize,
    /// `page.nextCursor` 的回传入参；解析不了就退回 `offset`（与发现面同规）。
    #[serde(default)]
    pub cursor: Option<String>,
    pub start: Option<String>,
    pub end: Option<String>,
    /// 原混合面就有的能力，不得静默丢失。
    pub keyword: Option<String>,
    #[serde(default)]
    pub media: FlexBool,
    #[serde(default)]
    pub image: FlexBool,
    #[serde(default)]
    pub voice: FlexBool,
    #[serde(default)]
    pub video: FlexBool,
    #[serde(default)]
    pub emoji: FlexBool,
    #[serde(default)]
    pub access_token: Option<String>,
}

fn default_limit() -> usize {
    100
}

pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
    body: axum::body::Bytes,
) -> Result<Json<serde_json::Value>, ApiError> {
    let params = merge_body(params, &body).await?;
    // 鉴权只看查询串：POST body 不是鉴权通道。本面也不接 body 参数（它是只读面）。
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }
    let limit = params.limit.clamp(1, 10000);
    let offset = params
        .cursor
        .as_deref()
        .and_then(|c| c.parse::<usize>().ok())
        .unwrap_or(params.offset);
    let q = build_query(
        params.talker.as_deref(),
        limit,
        offset,
        params.start.as_deref(),
        params.end.as_deref(),
        params.keyword.as_deref(),
    )?;
    let media_on = params.media.is_true();
    let switches = MediaSwitches::from_flags(&params.image, &params.voice, &params.video, &params.emoji);

    let (items, has_more) = {
        let store = state.store.read();
        crate::store::query::query_messages(&store, &q)
    };

    // media=1：**真正执行导出**。旧混合面在收集导出任务之前就 return 了，因此 media=1 在
    // ChatLab 形状上从未导出过 —— 于是「先触发导出、再取字节」这条两步走在那个面上并不成立。
    let items = if media_on {
        run_export(&state, q.talker, switches, items).await?.0
    } else {
        items
    };

    let store = state.store.read();
    // find_conversation 会回落另一种会话类型，因此一个全数字的私聊 uid 也能解析到真实会话。
    let conv = store.find_conversation(q.talker);
    let chat_type = conv
        .map(|c| c.chat_type)
        .unwrap_or_else(|| crate::store::query::classify_talker(q.talker).0);
    let name = conv
        .map(|c| store.display_name(c.chat_type, &c.talker))
        .unwrap_or_else(|| q.talker.to_string());
    // `accountName`（账号自己的名字）与 `groupNickname`（本会话的群名片）在 ChatLab 里是
    // **两件事**：`MessageOut.senderName` 是二者「名片优先」的合并结果，在原生面上保持那个
    // 含义，所以这里各自解析，不复用 `senderName` 填两个键。
    let conv_key = conv.map(|c| crate::store::conv_key(c.chat_type, &c.talker));
    let account_name = |uid: &str| store.display_uid(uid);
    let group_card = |uid: &str| -> String {
        if chat_type != crate::parser::types::ChatType::Group {
            return String::new();
        }
        conv_key
            .as_ref()
            .and_then(|key| store.group_cards.get(key))
            .and_then(|cards| cards.get(uid))
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_default()
    };

    // members 是**本页出现的发送者**（去重），不是名册：这个面描述的是这一页，把名册并进来
    // 会让 members 与 messages 的关系在不同页上不一致（名册只见于群成员面）。
    let members: Vec<ChatlabMember> = {
        let uids: Vec<String> = items.iter().map(|m| m.sender_username.clone()).collect();
        crate::server::chatlab::dedup_senders(&uids)
            .iter()
            .map(|uid| crate::server::chatlab::member(uid, account_name(uid), group_card(uid)))
            .collect()
    };

    // 切片是原生面的**降序**（最新在前），这里反转回时间顺序：ChatLab 的读者按正序合并，
    // 倒序会让他们以为最新一条排在最前面。
    let messages: Vec<ChatlabMessage> = items
        .iter()
        .rev()
        .map(|m| {
            let mut media = crate::server::chatlab::media_brief(m.r#type.as_deref(), m.media.as_ref());
            // 回填规则：只有**确实写出了本地副本**（`media_file_name` 只在成功时被填）**且名字
            // 可作句柄**（取自 store 键，或形状是内容摘要 —— 见 `media_export::ExportOut` 的
            // `handle_ok`）的那些，fileName 才是可取句柄：平台给的原名给不出跨会话唯一的句柄。
            // 没落盘的、或名字不可作句柄的，照给元数据。
            if let Some(media) = media.as_mut()
                && m.media_export_handle_ok
                && let Some(name) = m.media_file_name.as_deref()
            {
                media.file_name = name.to_string();
            }
            ChatlabMessage {
                account_name: account_name(&m.sender_username),
                content: m.content.clone(),
                group_nickname: group_card(&m.sender_username),
                media,
                platform_message_id: m.server_id.clone(),
                reply_to_message_id: m.reply_to_message_id.clone(),
                sender: m.sender_username.clone(),
                timestamp: m.create_time,
                // 规范的 ChatLab 0.0.2 码。`localType` 是平台原生空间，所以先还原出变体。
                r#type: crate::parser::types::MsgType::from_code(m.local_type).chatlab_type(),
            }
        })
        .collect();

    let count = messages.len();
    let owner_id = state
        .accounts
        .read()
        .iter()
        .find(|a| a.state.is_ready())
        .map(|a| a.qq.clone())
        .unwrap_or_default();

    let body = serde_json::to_value(ChatlabMessages {
        chatlab: crate::server::chatlab::header(),
        count,
        members,
        messages,
        meta: crate::server::chatlab::meta(chat_type, q.talker.to_string(), name, owner_id),
        page: Page {
            has_more,
            next_cursor: has_more.then(|| (offset + count).to_string()),
        },
        talker: q.talker.to_string(),
    })
    .expect("ChatLab 信封必须可序列化");
    Ok(Json(body))
}
