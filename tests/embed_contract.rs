//! 嵌入者契约：**只用 `api::*`，不起 HTTP**。
//!
//! 这条测试的写法是刻意的：它把 `use` 限制在承诺面里。一旦它需要 `store::` / `db::` / `server::`
//! 里的任何东西，编译就会失败 —— 那正是「承诺面不够用」的信号，而不是测试的问题。
//!
//! 换句话说：**这条测试是承诺面的验收器**。它绿，说明嵌入者能用公开面把事情做完。

use qqflow_server::api;
use qqflow_server::api::ChatType;

mod common;

#[test]
fn an_embedder_can_read_an_account_without_http() {
    // 与其它测试同形：临时目录按进程号隔离，跑完不清理（系统会回收）。
    let dir = std::env::temp_dir().join(format!("qqflow_embed_{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    let nt_db = dir.join("nt_db");
    // 夹具造库并把 `nt_msg.db` materialize 出来（带 QQ 自有的头偏移，与真库同形）。
    let (writer, main) = common::open_fake_source(&nt_db, 0);
    drop(writer);

    // ① 建索引 —— 嵌入者的第一个调用。
    let index = api::open(&main, common::FAKE_KEY).expect("建索引");
    assert!(!index.is_empty(), "夹具里应当有会话与消息");

    // ② 列会话。顺序稳定（按 `(chat_type, talker)` 的字符串形式），调用方能直接做 diff。
    let convs = index.conversations();
    assert_eq!(convs.len(), 3, "两个群 + 一个私聊");
    let keys: Vec<(&str, &str)> = convs
        .iter()
        .map(|c| (c.chat_type.as_str(), c.talker.as_str()))
        .collect();
    let mut sorted = keys.clone();
    sorted.sort();
    assert_eq!(keys, sorted, "会话顺序稳定");

    // ③ 读消息，按 `(ts, rowid)` 升序。
    let group = convs
        .iter()
        .find(|c| c.talker == "10001")
        .expect("夹具里有群 10001");
    assert_eq!(group.chat_type, ChatType::Group);
    // 夹具往群 10001 写了 6 行，但**其中一行是「改群名」的系统消息** —— 它被消费成群名
    // （见下面的 `display_name` 断言），不进入消息列表。索引里因此是 5 条。
    assert_eq!(group.message_count, 5);
    let msgs = index.messages(ChatType::Group, "10001");
    assert_eq!(msgs.len(), 5);
    let ids: Vec<i64> = msgs.iter().map(|m| m.rowid).collect();
    let mut sorted_ids = ids.clone();
    sorted_ids.sort();
    assert_eq!(ids, sorted_ids, "消息按 (ts, rowid) 升序");

    // ④ 群名片与全局昵称**不是一回事**：名片只在它出现的那个群里显示。
    assert_eq!(index.group_card(ChatType::Group, "10001", "u_a"), "张三群名片");
    assert_eq!(index.group_card(ChatType::Group, "10001", "u_b"), "", "没设名片的就是空");
    // 私聊里没有群名片这一说。
    assert_eq!(index.group_card(ChatType::C2c, "u_12345", "u_a"), "");

    // ⑤ 展示名：群名来自「修改群名」那条系统消息。
    assert_eq!(index.display_name(ChatType::Group, "10001"), "测试群");
    assert!(
        !index.display_name(ChatType::C2c, "u_12345").is_empty(),
        "私聊有对方昵称"
    );

    // ⑥ 发送者展示名走的是与群名片不同的优先级链。
    assert_eq!(index.display_sender(ChatType::Group, "10001", "u_a"), "张三群名片");
}
