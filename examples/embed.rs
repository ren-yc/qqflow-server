//! 嵌入者示例：**不起 HTTP**，直接把一个账号读出来。
//!
//! 这个文件是承诺面的活文档 —— 它**只能用 `pub`**（example 是独立 crate，看不见 `pub(crate)`）。
//! 所以它编译得过，本身就说明一件事：**嵌入者要的东西都在公开面上**。
//!
//! ```text
//! cargo run --example embed --no-default-features -- qqflow-server.json
//! ```
//!
//! 参数是**服务用的同一个配置文件**（`{ "qq": …, "key": …, "db_path": … }`）—— 嵌入者手里通常
//! 就是它，不必再发明一种格式。
//!
//! `--no-default-features` 不是可选项：默认 feature 会把 axum 与 tokio 一起拉进来，而这个示例
//! 一行网络代码都没有。它同时也是**依赖面的证明** —— 这一行跑得起来，说明「不起 HTTP 也能用」
//! 不是一句口号。

use std::path::PathBuf;

use qqflow_server::api;
use qqflow_server::api::ChatType;

fn main() -> anyhow::Result<()> {
    let Some(cfg_path) = std::env::args().nth(1) else {
        eprintln!("用法: cargo run --example embed --no-default-features -- <配置文件.json>");
        eprintln!();
        eprintln!("配置文件就是服务用的那一个：");
        eprintln!("  {{ \"qq\": \"…\", \"key\": \"…\", \"db_path\": \"…/Tencent Files\" }}");
        std::process::exit(2);
    };
    let raw = std::fs::read_to_string(&cfg_path)?;
    let cfg: serde_json::Value = serde_json::from_str(&raw)?;

    let root = PathBuf::from(cfg["db_path"].as_str().unwrap_or_default());
    let qq = cfg["qq"].as_str().unwrap_or_default();
    let key = cfg["key"].as_str().unwrap_or_default();
    if qq.is_empty() || key.is_empty() {
        anyhow::bail!("配置里缺 qq 或 key —— 没有密钥就读不了库");
    }
    // QQ 的库在 `<db_path>/<qq>/nt_qq/nt_db/nt_msg.db`。**给 `api::open` 的是文件路径**，不是
    // 目录：这个库带 QQ 自有的明文头偏移，由自定义 VFS 处理，裸 `Connection::open` 读不了。
    let db_path = root.join(qq).join("nt_qq/nt_db/nt_msg.db");

    // ① 建索引。同步调用：真实账号是秒级到十几秒，放在哪个线程由调用方决定。
    eprintln!("[embed] 索引 {} …", db_path.display());
    let index = api::open(&db_path, key)?;
    let convs = index.conversations();
    eprintln!("[embed] 建好：{} 个会话", convs.len());

    // ② 列会话。顺序稳定（按 `(chat_type, talker)` 的字符串形式），调用方可以直接做 diff。
    for c in convs.iter().take(5) {
        let kind = match c.chat_type {
            ChatType::Group => "群",
            ChatType::C2c => "私聊",
        };
        println!("{kind}\t{}\t{} 条", index.display_name(c.chat_type, &c.talker), c.message_count);
    }

    // ③ 群名片与全局昵称是**两个东西** —— 名片只在它出现的那个群里显示。承诺面把它们分开，
    //    因为混起来会显示错人。
    if let Some(group) = convs.iter().find(|c| c.chat_type == ChatType::Group) {
        let msgs = index.messages(ChatType::Group, &group.talker);
        println!(
            "\n群 {} 最近三条：",
            index.display_name(ChatType::Group, &group.talker)
        );
        for m in msgs.iter().rev().take(3) {
            let card = index.group_card(ChatType::Group, &group.talker, &m.from_uid);
            println!(
                "  {}\t{}\t{}",
                index.display_sender(ChatType::Group, &group.talker, &m.from_uid),
                if card.is_empty() { "（无名片）" } else { &card },
                m.ts
            );
        }
    }

    Ok(())
}
