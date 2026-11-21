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
    // `since=0` 与省略等价：0 是纪元起点，挡不住任何真实行，却会让游标与回显带上
    // 一个假下界（`sync.since: 0` 读起来像「从 1970 年起」）。在建链入口归一化成
    // 「无下界」，游标里因此统一存 `i64::MIN` 哨兵。回归：`chatlab_pull_echoes_the_window_lower_bound`。
    let since = since.filter(|s| *s != 0);
    let end = params.end.as_deref().and_then(|s| parse_time_bound(s, true));
    let watermark = end.unwrap_or_else(|| chrono::Utc::now().timestamp());

    // 四个量（账号、注册代次、数据源世代）与会话内容必须在**同一把锁**下取：store 的
    // 读锁就是生命周期闸门——注销要拿写锁，而且顺序固定为「清空 store → 推进注册代次
    // → 摘掉账号绑定」。持读锁期间这三件事都发生不了，所以在锁内采样得到的是同一时刻
    // 的一致视图。锁外采样会留下「A 的账号与世代 + B 的 store 内容」这种混合身份：
    // 响应是 B 的数据、游标却绑着 A，下一次请求 409，拉取被中途打断。
    let store = state.store.read();
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
    let registration = state.init.registration.load(std::sync::atomic::Ordering::SeqCst);
    // find_conversation falls back to the other chat type when the primary
    // classification misses (an all-digit c2c peer uid classifies as group).
    let Some(conv) = store.find_conversation(&id) else {
        return Err(ApiError::not_found(format!("会话不存在: {id}")));
    };
    let chat_type = conv.chat_type;
    let talker = conv.talker.clone();

    let session_key = crate::store::conv_key(chat_type, &talker);
    // 游标上下文：本进程 + 本账号 + 本会话 + (注册代次, 数据源世代)。会话摘要绑
    // **解析后的** `(chat_type, talker)` 而不是裸 `id`：`find_conversation` 的类型回退
    // 让同一个裸 id 落到某个会话时，两条请求必须编出同一条线。回归
    // `all_digit_c2c_talker_resolves_via_fallback`、`every_identity_mismatch_is_a_reset_not_a_silent_restart`。
    let cursor_ctx = crate::server::cursor::CursorContext::for_session(
        &bound_qq,
        &session_key,
        registration,
        source_generation,
    );
    // 解码失败分两种——语法坏是 400，身份/代次不符是 409 + `reason:
    // cursor_reset_required`；两者都不退回时间模式：静默退回等于让客户端以为
    // 续上了、实际从头重放。
    let cursor_progress = match params.cursor.as_deref() {
        Some(raw) => Some(cursor_ctx.decode(raw).map_err(|e| e.into_api_error())?),
        None => None,
    };

    // ── 两段式游标：第一段窗口快照扫描，第二段入库补发（契约 0.12.0）──────
    //
    // 首页按时间键交付、续页只按行标识过滤时，两种序下的「已交付集合」互不为前缀：
    // 时间更晚、行标识更小的行会被 `rowid > 水位` 跨过去、此后永不交付。两段式把
    // 集合按「链开始时的行标识上界」切干净，每段内部只剩一种序——第一段时间键、
    // 第二段行标识——「已交付 = 该序的前缀」才成立。判据与载荷见 `server::cursor`。
    //
    // 本仓一个会话恒落一张表（`group_msg_table` / `c2c_msg_table`），所以分片维度
    // 恒为 1：分片号取表名摘要，保留这个维度只是为了让两仓的判据逐字对应。
    //
    // 回归：`cursor_continuation_is_monotonic_when_row_order_diverges_from_time_order`
    // （乱序行经链可达、不重放）、`cursor_end_window_does_not_freeze_the_chain`
    // （`end` 挡住的行留在第一段）、`cursor_snapshot_is_frozen_while_new_rows_wait`
    // （快照冻结后链中途入库的行仍可达）、契约用例 `cursor-drain-equals-full-out-of-order`。
    let shard = crate::server::cursor::shard_code(match chat_type {
        ChatType::Group => crate::store::index::GROUP_TABLE,
        ChatType::C2c => crate::store::index::C2C_TABLE,
    });
    // 快照取**已索引**上界：索引事后补齐的行行标识必然更大，因此落第二段；
    // 取库文件上界会把它们冻在第一段里又被键过滤掉。
    let snapshot: Vec<(u64, i64)> = conv
        .msgs
        .iter()
        .map(|m| m.rowid)
        .max()
        .map(|rowid| vec![(shard, rowid)])
        .unwrap_or_default();

    // 窗口下界随游标走（它表达「这条链从哪儿开始」）；`end` 每页活跃、不进游标。
    let fresh_chain = cursor_progress.is_none();
    let excluded_since = cursor_progress
        .as_ref()
        .map_or(since.unwrap_or(i64::MIN), |p| p.excluded_since());
    let progress = cursor_progress
        .unwrap_or_else(|| crate::server::cursor::CursorProgress::start(snapshot.clone(), since));

    // 第一段候选：窗口内 + 快照内 + 键在位置之后，按时间键升序。
    // 第一段**不推进**快照——推进会把「已被页切掉、但键更小」的新行跨过去。
    let phase1 = |bound_end: Option<i64>| -> Vec<usize> {
        let mut rows: Vec<usize> = (0..conv.msgs.len())
            .filter(|&i| {
                let m = &conv.msgs[i];
                m.ts > excluded_since
                    && bound_end.is_none_or(|e| m.ts <= e)
                    && progress.admits1(
                        shard,
                        crate::server::cursor::TimeKey { ts: m.ts, rowid: m.rowid },
                    )
            })
            .collect();
        rows.sort_by_key(|&i| (conv.msgs[i].ts, conv.msgs[i].rowid));
        rows
    };
    // 第二段候选：行标识在快照之后，**不看时间**——这一段就是「链开始后入库的行
    // 无论时间戳都能经游标拿到」的载体。按 (分片, 行标识) 升序，页因此是各分片
    // 行标识前缀的并，水位推进不会跨过未交付的行。
    let phase2 = || -> Vec<usize> {
        let mut rows: Vec<usize> = (0..conv.msgs.len())
            .filter(|&i| progress.admits2(shard, conv.msgs[i].rowid))
            .collect();
        rows.sort_by_key(|&i| (shard, conv.msgs[i].rowid));
        rows
    };
    // 页边界：第一段**不切秒组**（`nextSince` 要能无损续页，等键行也必须同页）；
    // 第二段按行标识逐个推进，没有秒组可切。
    let page_end_for = |rows: &[usize], split_second: bool| -> usize {
        if !split_second {
            return rows.len().min(limit);
        }
        let mut end = 0usize;
        let mut prev_ts: Option<i64> = None;
        while end < rows.len() {
            let ts = conv.msgs[rows[end]].ts;
            if prev_ts.is_some_and(|p| p != ts) && end >= limit {
                break;
            }
            prev_ts = Some(ts);
            end += 1;
        }
        end
    };
    let delivered_bounds = |page: &[usize]| -> Vec<(u64, i64)> {
        page.iter().map(|&i| (shard, conv.msgs[i].rowid)).collect()
    };

    let mut page: Vec<usize> = Vec::new();
    let next_progress;
    let mut has_more = false;
    // `offset` 只在**无游标**的翻页里生效：游标链的进度由游标承载，混用两者会让
    // 「已交付」出现两个互不同步的来源。
    let mut start = 0usize;
    if progress.phase() == 2 {
        let rows = phase2();
        let end = page_end_for(&rows, false);
        page = rows[..end].to_vec();
        has_more = end < rows.len();
        next_progress = progress.advance_phase2(delivered_bounds(&page));
    } else {
        let rows = phase1(end);
        let skip = if fresh_chain { offset.min(rows.len()) } else { 0 };
        let rows = &rows[skip..];
        if !rows.is_empty() {
            let end = page_end_for(rows, true);
            page = rows[..end].to_vec();
            has_more = end < rows.len();
            start = skip;
            // 页末行的时间键就是本页的最大键：页不切秒组，排序键与页推进一致。
            let last = &conv.msgs[*page.last().expect("非空页必有末行")];
            next_progress = progress.with_position(crate::server::cursor::TimeKey {
                ts: last.ts,
                rowid: last.rowid,
            });
        } else if fresh_chain {
            // 无游标的空页 = **空页结算**：`end` 界内的一切都已确立。位置时间取
            // `watermark`（`end` 缺省时是当前时刻），时间更大的行键更大、仍然可达
            // ——「被 `end` 挡住的行没有丢」正落在这里。
            let max_rowid = conv
                .msgs
                .iter()
                .filter(|m| m.ts <= watermark)
                .map(|m| m.rowid)
                .max()
                .unwrap_or(0);
            next_progress = crate::server::cursor::CursorProgress::settled(
                watermark,
                max_rowid,
                snapshot.clone(),
                since,
            );
        } else if !phase1(None).is_empty() {
            // 被 `end` 挡住不算耗尽：那些行键大于位置，客户端抬高 `end` 后照样交付。
            // 在这里切段会把它们永久留在第一段之外（第二段只认行标识大于快照）。
            next_progress = progress.clone();
        } else {
            // 第一段确实耗尽 ⇒ 切第二段，本页直接交付第二段的行。不返回空页：
            // 「排空即停」的客户端会把空页当成链结束，第二段的行就永远拿不到。
            let rows = phase2();
            let end = page_end_for(&rows, false);
            page = rows[..end].to_vec();
            has_more = end < rows.len();
            next_progress = progress.into_phase2().advance_phase2(delivered_bounds(&page));
        }
    }
    // 第一段耗尽、但第二段已有行 ⇒ 仍要报「还有更多」：否则「排空即停」的客户端
    // 永远看不到第二段（链开始后入库的行），而它正是游标承诺的一部分。判据是
    // 「本请求已把第一段（`end` 视作无上界）消费完」——只看 `phase1(None)` 是否为空
    // 会把「本页正好交付完窗口内的行」误判成没耗尽：那时它恰好等于本页那几行。
    // 被 `end` 挡住不算耗尽：那些行属于第一段，客户端抬高 `end` 才该继续。
    if !has_more && next_progress.phase() == 1 && phase1(None).len() <= start + page.len() {
        has_more = !phase2().is_empty();
    }

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

    // 第二段按行标识排序，页末行的时间会回退——按它续拉会把已交付的行重放一遍。
    // 因此第二段一律给出 `watermark`；第一段沿用「页末时间戳 / 排空取 watermark」。
    let next_since = if next_progress.phase() == 2 || !has_more {
        watermark
    } else {
        page.last().map(|&i| conv.msgs[i].ts).unwrap_or(watermark)
    };
    // `offset` 只在无游标、且时间戳无法推进的退化页里承载进度；游标模式恒为 0。
    let next_offset = if fresh_chain && has_more && next_since <= since.unwrap_or(i64::MIN) {
        start.saturating_add(page.len())
    } else {
        0
    };
    let next_cursor = cursor_ctx.encode(&next_progress).map_err(|e| e.into_api_error())?;
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
            next_since,
            // `i64::MIN` 是「无下界」的内部哨兵，0 是归一化之前签出的旧游标里才可能
            // 出现的值；两者都回显 `null`，其余原样回显。哨兵不外泄（契约 0.13.0）。
            since: match excluded_since {
                i64::MIN | 0 => None,
                bound => Some(bound),
            },
            watermark,
            // Both cursors are meant to be echoed back verbatim, so they must
            // not skip the same rows twice. `nextSince` is exclusive and the
            // page ends on a complete ts group, so re-filtering with it drops
            // exactly the rows already served — leaving the next unseen row at
            // offset 0. `nextOffset` therefore only carries weight in the
            // degenerate case where the timestamp could not advance at all,
            // and never on a cursor chain（进度由游标承载）。
            next_offset,
        },
    })
    .map_err(|e| ApiError::internal(format!("序列化失败: {e}")))?;
    Ok(Json(body))
}
