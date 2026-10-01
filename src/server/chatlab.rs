//! **ChatLab 形状的唯一序列化出处。**
//!
//! ChatLab 面有三个消费者：消息面、Pull 面、批量导出。它们**必须给出同一个形状** —— 否则
//! 「同一份数据、两个面、字段不一样」会在下游变成一堆按来源分叉的解析代码，而那种分叉只有在
//! 某一面改字段时才会暴露。
//!
//! 这个模块负责**字段怎么填**；`server/dto.rs` 负责**字段叫什么**。分开是因为后者能给 OpenAPI 用
//! （schema 由类型生成），而前者要读 store。
//!
//! **没抽的一层：消息字段。** 两个面拿到的输入类型不同（成形的 `MessageOut` 与原始的
//! `MessageRecord`），字段名与取值路径都不一样；硬凑一个公共签名要么得先统一上游类型（那是另一件
//! 事），要么得在公共函数里分派两种输入（那只是把重复挪了个地方）。**成员、表头、平台常量这三样
//! 是逐字重复的，所以只抽这三样。**

use crate::server::dto::{ChatlabHeader, ChatlabMember, ChatlabMeta, MediaBrief};

/// 生成器标识。两个面共用同一个值：下游按它区分「谁产出的」。
pub(crate) const GENERATOR: &str = "qqflow-server";

/// ChatLab 格式版本。
///
/// 它是**格式**的版本，不是本服务的版本 —— 升级本服务不会让它变。
pub(crate) const FORMAT_VERSION: &str = "0.0.2";

/// 平台标识。
pub(crate) const PLATFORM: &str = "qq";

pub(crate) fn header() -> ChatlabHeader {
    ChatlabHeader {
        exported_at: chrono::Utc::now().timestamp(),
        generator: GENERATOR.to_string(),
        version: FORMAT_VERSION.to_string(),
    }
}

/// 会话元数据。
///
/// `name` 由调用方给：两个面拿它的时机不同（Pull 面按会话类型解析显示名，混合面在更早的地方已经
/// 算好）。但**填进哪里、`platform` 与 `type` 取什么**是同一件事。
///
/// `type` 用 `ChatType::as_str()`（`group` / `private`）—— 它与发现面（`/chatlab/sessions`）的
/// `type` 是同一套取值，也属于对外契约（`api::Index::conversations` 按它排序）。
pub(crate) fn meta(
    chat_type: crate::parser::types::ChatType,
    group_id: String,
    name: String,
    owner_id: String,
) -> ChatlabMeta {
    ChatlabMeta {
        group_id,
        name,
        owner_id,
        platform: PLATFORM.to_string(),
        r#type: chat_type.as_str().to_string(),
    }
}

/// 成员项。
///
/// 两个面**逐字相同**，所以抽在这里。`avatar` 恒为空串：QQ 没有头像来源，而 ChatLab 0.0.2 把它列为
/// 可选 —— 空串是诚实的答案，编一个假 URL 不是。
pub(crate) fn member(uid: &str, account_name: String, group_nickname: String) -> ChatlabMember {
    ChatlabMember {
        account_name,
        avatar: String::new(),
        group_nickname,
        platform_id: uid.to_string(),
    }
}

/// 一条消息的媒体元数据（拉取面与消息面**同形**）；无媒体、或类型归不到 image/voice/video 时
/// 返回 `None`（调用方据此**省略整个 media 键**）。
///
/// 它是**元数据**，不是「字节可取」的承诺：`fileName` 只有在导出确实写出了本地副本、且名字
/// 由内容摘要派生之后才是可取句柄 —— 那一步由调用方在导出批次之后回填（见
/// `handlers::chatlab_messages`）。`md5` 取不到时省略该键：未导出不等于没有摘要。
pub(crate) fn media_brief(
    kind: Option<&str>,
    media: Option<&crate::parser::types::MediaInfo>,
) -> Option<MediaBrief> {
    let media = media?;
    let kind = kind.filter(|k| !k.is_empty())?;
    Some(MediaBrief {
        file_name: media.file_name.clone().unwrap_or_default(),
        md5: media.md5.clone().filter(|s| !s.is_empty()),
        r#type: kind.to_string(),
    })
}

/// 一页里的发送者去重（`members` 的集合，首次出现序）。
///
/// **两个面刻意用同一个规则**：只取本页出现过的发送者，而不是整个会话 —— 后者让 `members` 无界，
/// 且每次请求多一趟全量扫描。顺序是**首次出现序**（两个面都这么发），所以这里不改它。
pub(crate) fn dedup_senders(uids: &[String]) -> Vec<String> {
    let mut seen: Vec<String> = Vec::new();
    for uid in uids {
        if !uid.is_empty() && !seen.contains(uid) {
            seen.push(uid.clone());
        }
    }
    seen
}
