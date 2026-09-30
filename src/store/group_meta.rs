//! 群元数据加载器：读同族库 `group_info.db`，填群名册与群名片。
//!
//! 与消息库**分属不同文件**，所以要单独打开一次（同样带 QQ 自有的头偏移与 SQLCipher）。
//! 三张表：
//!
//! - `group_member3`：`60001` 群号 → `1000` 成员 uid，另带 `64003` 群名片、`20002` 群昵称、`1002` QQ 号；
//! - `group_list`：`60001` 群号 → `60007` 群名；
//! - `group_detail_info_ver1`：`60001` 群号 → `60002` **群主 uid**。
//!
//! 群主列的判据（真库探针实测，见 `tests/real_db_groundtruth.rs` 的
//! `probe_group_owner_candidates`）：`[60002]` 共 29 行、23 个不同值——**不是**「本人」
//! （那会是 1 个值）；每个群在**自己的名册内恰有一个**命中（29/29 群，命中分布 `(1, 29)`）
//! ——这是群主的形状。两个落选候选也留痕：`group_list` 无群主列（`60026` 全 NULL、
//! `60267` 与名册零命中）；`group_member3.[64002]` 是管理员形状（29 行、仅 11 群有），
//! 不是「每群恰一」，不能当群主用。
//!
//! **群昵称是「每个群各自一份」的** —— 同一个人在不同群的昵称不同。所以它跟名片放在一起
//! （按会话隔离），**不进全局的 uid 昵称表**：那会把某一个群的称呼泄漏到私聊、联系人与推送里。
//! 这与消息行里的 `40093` 昵称是同一个道理。
//!
//! **缺失时全部为空**，且**不是错误**：库可能不存在（旧版本）、可能打不开（不同的键）、也可能
//! 没有这些表。调用方据此「启动照常、只是没有群名片与人数」。

use std::collections::HashMap;
use std::path::Path;

use super::{conv_key, Store};

/// 一次加载的结果。
#[derive(Debug, Default)]
pub struct GroupMeta {
    /// 群号 → 群名。
    pub group_names: HashMap<String, String>,
    /// 群号 → 成员 uid（**顺序稳定**：按 uid 排序，便于比较与快照）。
    pub roster: HashMap<String, Vec<String>>,
    /// 群号 → (成员 uid → 群名片)。空名片不入表。
    pub cards: HashMap<String, HashMap<String, String>>,
    /// 成员 uid → QQ 号（`1002`）。
    pub member_qq: HashMap<String, String>,
    /// 群号 → 群主 uid（源 `group_detail_info_ver1.[60002]`）。缺表/缺库时为空，
    /// 调用方据此让 `isOwner` 全为 `false` —— 降级是常态，不是错误。
    pub owners: HashMap<String, String>,
}

impl GroupMeta {
    /// 名册人数 —— `/chatlab/sessions` 的 `memberCount` 用它。
    ///
    /// **这是「我们知道的成员数」，不是「群的确切人数」**：`group_member3` 是本地缓存，可能少于
    /// 真实值。这个区别要留在文档里 —— 下游拿它做展示预估可以，拿它做「群里一共几个人」的断言
    /// 不行。
    pub fn member_count(&self, group: &str) -> Option<usize> {
        self.roster.get(group).map(|r| r.len())
    }
}

/// 读 `group_info.db`。**任何一步失败都退化为空**，不返回错误 —— 见模块头的说明。
pub fn load_group_meta(nt_db_dir: &Path, key: &str) -> GroupMeta {
    let mut out = GroupMeta::default();
    let path = nt_db_dir.join("group_info.db");
    if !path.is_file() {
        return out;
    }
    // 与消息库同一条打开路径（偏移 VFS ＋ 只读活连接）。
    let mut reader = crate::db::live::LiveReader::new(path, key.to_string());
    if reader.open().is_err() {
        tracing::debug!("[group-meta] group_info.db 打不开，群名片与人数退化为空");
        return out;
    }
    let Ok(conn) = reader.acquire() else { return out };

    // 群名。
    if let Ok(mut stmt) = conn.prepare("SELECT [60001], [60007] FROM group_list")
        && let Ok(rows) = stmt.query_map([], |r| {
            Ok((
                r.get::<_, i64>(0)?,
                r.get::<_, Option<String>>(1)?.unwrap_or_default(),
            ))
        })
    {
        for (id, name) in rows.flatten() {
            if !name.is_empty() {
                out.group_names.insert(id.to_string(), name);
            }
        }
    }

    // 名册 ＋ 群名片。
    if let Ok(mut stmt) = conn.prepare(
        "SELECT [60001], [1000], [64003] FROM group_member3 \
         WHERE [1000] IS NOT NULL AND [1000] <> ''",
    ) && let Ok(rows) = stmt.query_map([], |r| {
        Ok((
            r.get::<_, i64>(0)?,
            r.get::<_, String>(1)?,
            r.get::<_, Option<String>>(2)?.unwrap_or_default(),
        ))
    }) {
        for (group, uid, card) in rows.flatten() {
            let g = group.to_string();
            out.roster.entry(g.clone()).or_default().push(uid.clone());
            if !card.is_empty() {
                out.cards.entry(g).or_default().insert(uid, card);
            }
        }
    }
    // 顺序稳定：不排序的话同一份库两次加载的顺序可能不同，快照与比较都会随机失败。
    for uids in out.roster.values_mut() {
        uids.sort();
    }

    // 成员 QQ 号（`1002`）—— 只用于补全联系人，不参与会话归属。
    if let Ok(mut stmt) = conn.prepare(
        "SELECT [1000], [1002] FROM group_member3 WHERE [1002] IS NOT NULL AND [1002] <> 0",
    ) && let Ok(rows) = stmt.query_map([], |r| {
        Ok((r.get::<_, String>(0)?, r.get::<_, i64>(1)?))
    }) {
        for (uid, qq) in rows.flatten() {
            if !uid.is_empty() {
                out.member_qq.insert(uid, qq.to_string());
            }
        }
    }

    // 群主（`group_detail_info_ver1.[60002]`）。判据与两个落选候选见模块头注释。
    if let Ok(mut stmt) = conn.prepare(
        "SELECT [60001], [60002] FROM group_detail_info_ver1 \
         WHERE [60002] IS NOT NULL AND [60002] <> ''",
    ) && let Ok(rows) = stmt.query_map([], |r| {
        Ok((r.get::<_, i64>(0)?, r.get::<_, String>(1)?))
    }) {
        for (group, uid) in rows.flatten() {
            out.owners.insert(group.to_string(), uid);
        }
    }

    tracing::debug!(
        "[group-meta] 群 {} 个；有名册的 {} 个；名片 {} 条；成员 QQ {} 条；有群主的 {} 个",
        out.group_names.len(),
        out.roster.len(),
        out.cards.values().map(|m| m.len()).sum::<usize>(),
        out.member_qq.len(),
        out.owners.len()
    );
    out
}


/// 把加载结果灌进 store。
///
/// 与 [`load_group_meta`] 分开：读库不需要 `&mut Store`，灌进去才需要。分成两步之后，
/// 「读失败」与「灌错地方」是两件可以分别测的事。
pub fn apply_group_meta(store: &mut Store, meta: GroupMeta) {
    for (group, name) in meta.group_names {
        // 群名进全局表：它在所有群里是同一个（群名属于群，不属于「某人在这里叫什么」）。
        store.names.group_name.insert(group, name);
    }
    for (group, uids) in meta.roster {
        store.chatroom_roster.insert(group, uids);
    }
    for (group, cards) in meta.cards {
        let key = conv_key(crate::parser::types::ChatType::Group, &group);
        store.group_cards.entry(key).or_default().extend(cards);
    }
    for (uid, qq) in meta.member_qq {
        store.names.uid_qq.entry(uid).or_insert(qq);
    }
    for (group, owner) in meta.owners {
        store.chatroom_owner.insert(group, owner);
    }
}
