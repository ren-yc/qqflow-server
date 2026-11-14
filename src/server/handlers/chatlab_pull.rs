//! GET /api/v1/sessions/:id/messages — ChatLab Pull protocol:
//! incremental sync with a `sync` pagination block.

use std::sync::Arc;

use axum::extract::{Path, State};
use axum::http::HeaderMap;
use axum::Json;
use serde::Deserialize;
use serde_json::Value;

use crate::parser::types::ChatType;
use crate::server::dto::{ChatlabMember, PullEnvelope, PullMessage, PullSync};
use crate::server::error::{ApiError, EnvelopeQuery};
use crate::server::AppState;

use super::{authorized, parse_time_bound};

/// Every field is a lenient `Option<String>`: WeFlow's Pull contract has no
/// 400 semantics for malformed pagination, so garbage degrades to the default
/// instead of rejecting the request (`?limit=abc` used to 400 here while the
/// same value on WeFlow's own endpoint is ignored).
#[derive(Debug, Default, Deserialize)]
pub struct Params {
    pub since: Option<String>,
    pub end: Option<String>,
    pub limit: Option<String>,
    pub offset: Option<String>,
    /// 不透明进度游标（`opaqueCursor` 能力）：原样回传由本端点编出的
    /// `sync.nextCursor`。给出时覆盖 `since`/`offset`——进度游标是权威起点。
    pub cursor: Option<String>,
    #[serde(default)]
    pub access_token: Option<String>,
}

/// 编码/解码一律走 `crate::server::cursor`（v2）：它把「进程 / 账号 / 会话 /
/// 注册代次 / 数据源世代」绑进载荷，失效时返回 409 + `reason`，而不是静默
/// 退回时间模式。本文件不再保留 v1 的编解码助手。
pub async fn handler(
    State(state): State<Arc<AppState>>,
    headers: HeaderMap,
    Path(id): Path<String>,
    EnvelopeQuery(params): EnvelopeQuery<Params>,
) -> Result<Json<Value>, ApiError> {
    if !authorized(&state, &headers, params.access_token.as_deref()) {
        return Err(ApiError::unauthorized());
    }
    if !state.ready.load(std::sync::atomic::Ordering::SeqCst) {
        return Err(ApiError::not_ready());
    }

    let limit = params
        .limit
        .as_deref()
        .and_then(|s| s.parse::<usize>().ok())
        .unwrap_or(5000)
        .clamp(1, 5000);
    let offset = params.offset.as_deref().and_then(|s| s.parse::<usize>().ok()).unwrap_or(0);
    let since = params.since.as_deref().and_then(|s| parse_time_bound(s, false));
    let end = params.end.as_deref().and_then(|s| parse_time_bound(s, true));
    let watermark = end.unwrap_or_else(|| chrono::Utc::now().timestamp());

    let store = state.store.read();
    // find_conversation falls back to the other chat type when the primary
    // classification misses (an all-digit c2c peer uid classifies as group).
    let Some(conv) = store.find_conversation(&id) else {
        return Err(ApiError::not_found(format!("会话不存在: {id}")));
    };
    let chat_type = conv.chat_type;
    let talker = conv.talker.clone();

    // 游标上下文：本进程 + 本账号 + 本会话 + (注册代次, 数据源世代)。会话摘要
    // 绑**解析后的** `(chat_type, talker)` 而不是裸 `id`：两个不同的裸 id 落到
    // 同一个会话（`find_conversation` 的回退）时必须编出同一条线，而不是互相
    // 误判成跨会话。注册代次用注册令牌的二元组序数（注销与重注册都推进它），
    // 数据源世代来自该账号同步句柄的指纹采样（库身份变化才推进）。
    let registration = state.init.registration.load(std::sync::atomic::Ordering::SeqCst);
    // 绑定账号（与下面 `ownerId` 的取法同源）：store 没有账号维度，注册表里
    // 唯一的绑定条目就是它。
    let bound_qq = state
        .accounts
        .read()
        .iter()
        .find(|a| a.state.is_ready())
        .map(|a| a.qq.clone())
        .unwrap_or_default();
    let source_generation = state
        .sync
        .snapshot()
        .iter()
        .find(|a| a.qq == bound_qq)
        .map(|a| a.generation_handle().load(std::sync::atomic::Ordering::SeqCst))
        .unwrap_or(0);
    let session_key = crate::store::conv_key(chat_type, &talker);
    let cursor_ctx = crate::server::cursor::CursorContext::for_session(
        &bound_qq,
        &session_key,
        registration,
        source_generation,
    );
    // 解码失败分两种——语法坏是 400，身份/代次不符是 409 + `reason:
    // cursor_reset_required`；两者都不退回时间模式：静默退回等于让客户端以为
    // 续上了、实际从头重放。
    let cursor_rowid = match params.cursor.as_deref() {
        Some(raw) => Some(cursor_ctx.decode(raw).map_err(|e| e.into_api_error())?),
        None => None,
    };

    // Chronological messages within (since, end] — `since` is exclusive, so
    // a client resuming with `nextSince` never re-fetches the boundary
    // second and can neither loop nor see duplicates.
    // 进度游标模式（cursor 给出）下整个分页只用行标识（rowid）：过滤用
    // 「rowid > 游标值」的排他下界，排序与页推进都按 rowid 单调——入库序与
    // 时间序在乱序行上分叉时，续拉严格无重放、无跳行；乱序行的 messages
    // 顺序不再是时间升序，这是「排空判据=入库进度」的直接后果。无 cursor
    // 的时间模式保持规范原行为：(ts, rowid) 复合键升序、页不跨秒推进。
    let mut idx: Vec<usize> = conv.msgs.iter().enumerate().map(|(i, _)| i).collect();
    if cursor_rowid.is_some() {
        idx.sort_by_key(|&i| conv.msgs[i].rowid);
    } else {
        idx.sort_by_key(|&i| (conv.msgs[i].ts, conv.msgs[i].rowid));
    }
    let filtered: Vec<usize> = idx
        .into_iter()
        .filter(|&i| {
            let m = &conv.msgs[i];
            // 进度游标是权威起点：给出时忽略 since/offset（时间只是元数据，
            // 入库进度才是增量判据）。排空后同秒晚入库的行由 rowid 排他性覆盖。
            if let Some(rid) = cursor_rowid {
                return m.rowid > rid && end.is_none_or(|e| m.ts <= e);
            }
            since.is_none_or(|s| m.ts > s) && end.is_none_or(|e| m.ts <= e)
        })
        .collect();

    let total = filtered.len();
    // Page from `offset`, extending to the end of the last second's ts
    // group: pages never split a second, so `nextSince` strictly advances.
    // 进度游标模式下 offset 无意义（游标已定位到行），恒从 0 开始。
    let start = if cursor_rowid.is_some() { 0 } else { offset.min(total) };
    let mut page_end = start;
    if cursor_rowid.is_some() {
        // 游标模式：页推进按 rowid 组——与排序键一致，页边界不切断单调序，
        // 页末 rowid 是严格的「已交付上界」。
        let mut prev_rid = None;
        while page_end < total {
            let rid = conv.msgs[filtered[page_end]].rowid;
            if prev_rid.is_some_and(|p| p != rid) && page_end - start >= limit {
                break;
            }
            prev_rid = Some(rid);
            page_end += 1;
        }
    } else {
        let mut prev_ts = None;
        while page_end < total {
            let ts_j = conv.msgs[filtered[page_end]].ts;
            if prev_ts.is_some_and(|p| p != ts_j) && page_end - start >= limit {
                break;
            }
            prev_ts = Some(ts_j);
            page_end += 1;
        }
    }
    let page = &filtered[start..page_end];
    let has_more = page_end < total;

    // WeFlow's Pull contract keeps these two SEPARATE: `accountName` is the
    // account's own name, `groupNickname` is the per-conversation group card
    // ("40090"). Collapsing both onto `display_sender` (card-wins) would make
    // them identical in groups, leaving a consumer unable to tell "no card" from
    // "card equals the account name". `senderName` on /api/v1/messages keeps its
    // card-wins semantics — that field is downstream-visible and unchanged.
    let account_name = |uid: &str| store.display_uid(uid);
    let group_card = |uid: &str| -> String {
        if chat_type != ChatType::Group {
            return String::new();
        }
        store
            .group_cards
            .get(&crate::store::conv_key(chat_type, &talker))
            .and_then(|cards| cards.get(uid))
            .filter(|s| !s.is_empty())
            .cloned()
            .unwrap_or_default()
    };

    // Built once for the whole page: resolving row by row would rescan the
    // conversation per message.
    let inner_index = crate::store::query::build_inner_index(conv);
    let messages: Vec<PullMessage> = page
        .iter()
        .map(|&i| {
            let m = &conv.msgs[i];
            // Only when the target is unambiguous: a wrong id here would make a
            // client attach this reply to the wrong message, and it could not
            // tell. Absent beats misleading.
            //
            // 类型化赋值：此前是 `out["replyToMessageId"] = …`（往 `Value` 里按字符串键
            // 写），键名写错不会报错，只会静默多一个键。
            let reply_to_message_id = inner_index
                .as_ref()
                .and_then(|index| crate::store::query::resolve_reply_to(index, m))
                .map(|target| target.to_string());
            // 与 `media` 键**同一次求值**里赋值（见下面那个 match）：两个键的一致性不靠约定。
            let mut media_id: Option<String> = None;
            PullMessage {
                account_name: account_name(&m.from_uid),
                content: m.parsed.content.clone(),
                group_nickname: group_card(&m.from_uid),
                // 媒体元数据（无媒体省略整个键）—— 与消息面**同一处**取法。
                // `media` 与 `mediaId` **同源**：brief 省略（类型归不到 image/voice/video）时
                // 句柄也必须跟着省略。「有句柄却声称自己没媒体」是矛盾态，契约的
                // `media_id_shape_in_pull` 直接判红；而下游若按「有 media 键才有媒体」读，会整条
                // 漏掉那份本来取得到的字节。两者写在同一个 `match` 里，就不可能只改一半。
                // 「出现即可取」的判据与原生面（`shape_record`）、手动同步响应、SSE **同一条**
                // 规则（store 登记过本地缓存路径才通告）；写第二遍就会分叉。
                media: match crate::server::chatlab::media_brief(
                    m.parsed.msg_type.media_type_str(),
                    m.parsed.media.as_ref(),
                ) {
                    brief @ Some(_) => {
                        media_id = m
                            .parsed
                            .media
                            .as_ref()
                            .and_then(|media| crate::store::query::fetchable_media_id(&store, media));
                        brief
                    }
                    None => None,
                },
                media_id,
                platform_message_id: m.seq.to_string(),
                reply_to_message_id,
                sender: m.from_uid.clone(),
                timestamp: m.ts,
                // Canonical ChatLab 0.0.2 code — NOT the native `localType`.
                r#type: m.parsed.msg_type.chatlab_type(),
            }
        })
        .collect();

    // Senders in THIS page, deduped — the roster describes what was exported,
    // matching WeFlow. Scanning the whole conversation instead made `members`
    // unbounded and cost a full pass per request.
    let members: Vec<ChatlabMember> = {
        // 去重规则只有一处出处（`server::chatlab`）—— 两个面各写一遍时，改一处忘另一处就会让
        // 同一个会话在两个面上成员不同。
        let uids: Vec<String> = page.iter().map(|&i| conv.msgs[i].from_uid.clone()).collect();
        let mut out = Vec::new();
        for uid in crate::server::chatlab::dedup_senders(&uids) {
            out.push(crate::server::chatlab::member(
                &uid,
                account_name(&uid),
                group_card(&uid),
            ));
        }
        out
    };

    // `ownerId` = the bound account (WeFlow emits its own wxid here). Only one
    // account ever holds the binding, so the first Ready entry is unambiguous.
    let owner_id = state
        .accounts
        .read()
        .iter()
        .find(|a| a.state.is_ready())
        .map(|a| a.qq.clone())
        .unwrap_or_default();

    let next_since = page.last().map(|&i| conv.msgs[i].ts).unwrap_or(since.unwrap_or(0));
    // 进度游标编码「**已交付集合**」的进度上界——不是页末行：
    //
    // - cursor 模式：排序按 rowid 单调，页末行就是已交付集合的最大 rowid，
    //   两者重合；空页时以本次游标值为准（进度不回退）。
    // - 时间模式非空页：排序按时间复合键，页末行 rowid **可能不是已交付集合
    //   里最大的**（回溯入库的行：时间早、rowid 大）。编码页末行会让后续游标
    //   链重放它；必须编码「本次响应已交付的行里最大的 rowid」。
    // - 时间模式空页：本响应没有交付任何行，但请求窗口 (since, end] 为空本身
    //   就是进度陈述——窗口内无行，则一切 ts ≤ end 的行都已确立。游标编码
    //   「end 界内已确立的最大 rowid」（end 缺省时水位线=now 覆盖全部行）。
    //   编码 0 是「从头拉取」，周期轮询的客户端采纳它再丢掉时间过滤就会重放
    //   全历史。
    let last_rowid = if cursor_rowid.is_some() {
        page.iter()
            .map(|&i| conv.msgs[i].rowid)
            .max()
            .or(cursor_rowid)
            .unwrap_or(0)
    } else if page.is_empty() {
        // 水位线内找不到任何行时**不能**退回 0：那种情况下客户端已经拉过一批
        // （否则它的 `since` 从哪来），编码 0 等于让它从头重放。改为退到
        // 「`since` 界内已确立的最大 rowid」——`since` 之前的行客户端见过。
        // 两者都空（首次请求就没有行）才编码 0，那才是真正的「从头拉」。
        conv.msgs
            .iter()
            .filter(|m| m.ts <= watermark)
            .map(|m| m.rowid)
            .max()
            .or_else(|| {
                since.and_then(|s| {
                    conv.msgs
                        .iter()
                        .filter(|m| m.ts <= s)
                        .map(|m| m.rowid)
                        .max()
                })
            })
            .unwrap_or(0)
    } else {
        page.iter().map(|&i| conv.msgs[i].rowid).max().unwrap_or(0)
    };
    let next_cursor = cursor_ctx.encode(last_rowid);
    let body = serde_json::to_value(PullEnvelope {
        chatlab: crate::server::chatlab::header(),
        members,
        messages,
        meta: crate::server::chatlab::meta(
            chat_type,
            talker.clone(),
            store.display_name(chat_type, &talker),
            owner_id,
        ),
        sync: PullSync {
            has_more,
            next_cursor: Some(next_cursor),
            next_since: if has_more { next_since } else { watermark },
            // Both cursors are meant to be echoed back verbatim, so they must
            // not skip the same rows twice. `nextSince` is exclusive and the
            // page ends on a complete ts group, so re-filtering with it drops
            // exactly the rows already served — leaving the next unseen row at
            // offset 0. `nextOffset` therefore only carries weight in the
            // degenerate case where the timestamp could not advance at all.
            next_offset: if has_more && next_since <= since.unwrap_or(i64::MIN) {
                start.saturating_add(page.len())
            } else {
                0
            },
            watermark,
        },
    })
    .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?;
    Ok(Json(body))
}
