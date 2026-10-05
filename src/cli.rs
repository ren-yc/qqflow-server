//! 命令行子命令面（cli feature）。
//!
//! # 这层为什么存在
//!
//! 服务跑起来之后，用户第二类要做的事是「问一句」：有哪些会话、某个会话最近的消息、服务端
//! 现在绑的是哪个账号、要不要立刻同步一次。以前这些只能手打 HTTP 或另写脚本；本面把它们收进
//! 同一个二进制。
//!
//! # 默认走 HTTP，复用 SDK
//!
//! 查询一律经 `qqflow_client::client::Client`，**不在本 crate 里再写一份 HTTP**。失败模式很具体：
//! 自己拼请求的那一份不会被 SDK 的契约测试钉住，于是服务端改了字段名或错误语义时只有 SDK 的
//! 调用方会变红，而 CLI 会安静地读出错了的东西。
//!
//! `--embedded`（进程内直读本地库）**只开放给只读查询类**；`accounts` 与 `sync` 刻意没有这个
//! 开关：前者要回答的是「服务端此刻实际绑定了什么」，后者是**写动作** —— 进程内路径给出的不是
//! 同一个问题的答案。
//!
//! **`contacts` 在本仓也只走 HTTP**：qqflow 的嵌入面（`api::Index`）没有联系人读面，硬凑一个
//! 「嵌入版联系人」要么得把内部 `NameMaps` 变成承诺面，要么得另写一条取数路径 —— 两者都比
//! 「这个子命令只走 HTTP」更贵。这是与 weflow-server 的**能力差异**，写在这里而不是留给用户猜。
//!
//! # 兼容口径（裸跑仍＝serve）
//!
//! - 不带任何参数、或以旗标（`-` 开头）开头的写法**原样交给既有解析器**：`qqflow-server
//!   --port 6002` 一个字符都不用改，`--help`／`--version`／`--show-token` 的行为全部逐字不变。
//! - `serve` 子命令 = 裸跑；`serve` 之后的旗标同样交给老解析器。
//! - `token` 子命令 = 既有的 `--show-token`。
//! - 第一个参数既不是旗标也不是已知子命令时，**让 clap 报**「未知子命令」并以 2 退出：老解析器
//!   在这种情况下只会说「参数 bogus 缺少值」，那是把用法错误伪装成取值错误。
//!
//! # 退出码
//!
//! `0` 成功；`1` 运行期错误（连不上、被拒）；`2` 用法错误（未知子命令、缺参数）。`2` 由 clap 自己
//! 给出，本面不吞 —— 降级成 1 会让脚本分不出「我参数写错了」和「服务没起来」。
//!
//! # 密钥边界
//!
//! API token 与数据库密钥**只从环境变量或磁盘配置**取，绝不接受命令行明文参数：命令行会进
//! shell history 与进程列表。

use std::io::Write;
use std::path::PathBuf;

use anyhow::{Context, Result};
use clap::{Parser, Subcommand};
use qqflow_client::client::{Client, ContactsQuery, MessageQuery};
use serde_json::{json, Value};

use crate::api;
use crate::config::Config;

/// 顶层：一个子命令。`name` 固定成二进制名，因为 dispatch 用 parse_from 手工喂 argv。
#[derive(Parser)]
#[command(name = "qqflow-server", version, about = "本地 QQ NT 聊天记录只读服务与查询命令行")]
struct Cli {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    /// 起服务（与不带子命令时的行为完全一致）
    Serve,
    /// 打印已存的 API token 并退出（等价于 `--show-token`）
    Token,
    /// 会话列表
    Sessions(ReadArgs),
    /// 某个会话的消息（时间倒序）
    Messages(MessageArgs),
    /// 按关键词搜消息（`--keyword` 必填）
    Search(MessageArgs),
    /// 联系人列表（只走 HTTP：嵌入面没有联系人读面）
    Contacts(HttpArgs),
    /// 账号明细：绑定方、状态机值、消息数、`error` 根因
    Accounts(HttpArgs),
    /// 让服务端立刻跑一次增量同步（写动作）
    Sync(HttpArgs),
}

/// 只读查询类子命令的共用参数（带 `--embedded`）。
#[derive(clap::Args)]
struct ReadArgs {
    #[command(flatten)]
    common: Common,
    /// 进程内直读本地库，不经过 HTTP（仅只读查询类可用）
    #[arg(long)]
    embedded: bool,
}

#[derive(clap::Args)]
struct MessageArgs {
    #[command(flatten)]
    common: Common,
    /// 会话标识（群号或对端 uid）
    #[arg(long)]
    talker: Option<String>,
    /// 起始时间：unix 秒或 `YYYYMMDD`（含边界）
    #[arg(long)]
    since: Option<String>,
    /// 关键词子串
    #[arg(long)]
    keyword: Option<String>,
    /// 单页条数
    #[arg(long)]
    limit: Option<u32>,
    /// 进程内直读本地库，不经过 HTTP（仅只读查询类可用）
    #[arg(long)]
    embedded: bool,
}

/// 只有 HTTP 形态的子命令：刻意没有 `--embedded`（见模块头的边界说明）。
#[derive(clap::Args)]
struct HttpArgs {
    #[command(flatten)]
    common: Common,
}

#[derive(clap::Args)]
struct Common {
    /// 服务地址；默认取环境变量 `QQFLOW_BASE_URL`，再默认 http://127.0.0.1:5032
    #[arg(long, env = "QQFLOW_BASE_URL")]
    base_url: Option<String>,
    /// 输出机器可读 JSON（默认是人类可读的紧凑行）
    #[arg(long)]
    json: bool,
}

/// dispatch 需要的结果：要么按配置起服务，要么本次命令行已经办完、直接退出。
pub(crate) enum Entry {
    Serve(Config),
    Done,
}

/// 分流入口。返回 `Entry::Done` 表示不该起服务（含 `--help`／`--version`）。
pub(crate) fn dispatch() -> Result<Entry> {
    let argv: Vec<String> = std::env::args().skip(1).collect();
    let Some(first) = argv.first() else {
        // 裸跑：走 `config::load()` 这条今天就在用的入口，连读取环境变量的顺序都不动。
        return legacy_load();
    };
    if first.starts_with('-') {
        return legacy(&argv);
    }
    if first == "serve" {
        // serve 之后的旗标**原样**交给老解析器：`serve --port 6002` 必须与 `--port 6002` 等价。
        return legacy(&argv[1..]);
    }
    let cli = Cli::parse_from(std::iter::once("qqflow-server".to_string()).chain(argv.clone()));
    match cli.command {
        Command::Serve => legacy(&argv[1..]),
        Command::Token => match crate::config::show_token()? {
            Some(t) => {
                println!("{t}");
                Ok(Entry::Done)
            }
            None => anyhow::bail!("尚未生成 API token（先启动一次服务以生成）"),
        },
        Command::Sessions(q) => {
            let rows = if q.embedded {
                embedded_sessions()?
            } else {
                let client = http_client(&q.common)?;
                block(client.list_all_sessions(Some(10_000), None))?
                    .iter()
                    .map(|s| {
                        json!({
                            "username": s.username,
                            "displayName": s.display_name,
                            "lastTimestamp": s.last_timestamp,
                            "type": s.type_,
                            "unreadCount": s.unread_count,
                        })
                    })
                    .collect()
            };
            emit(&Value::Array(rows), q.common.json, "sessions");
            Ok(Entry::Done)
        }
        Command::Contacts(q) => {
            let client = http_client(&q.common)?;
            let page = block(client.contacts(&ContactsQuery::default()))?;
            let rows: Vec<Value> = page
                .contacts
                .iter()
                .map(|c| {
                    json!({
                        "username": c.username,
                        "displayName": c.display_name,
                        "remark": c.remark,
                        "nickname": c.nickname,
                        "alias": c.alias,
                    })
                })
                .collect();
            emit(&Value::Array(rows), q.common.json, "contacts");
            Ok(Entry::Done)
        }
        Command::Messages(m) => {
            let rows = run_messages(&m)?;
            emit(&Value::Array(rows), m.common.json, "messages");
            Ok(Entry::Done)
        }
        Command::Search(m) => {
            if m.keyword.as_deref().unwrap_or("").is_empty() {
                usage_error("search 需要 --keyword（要列全部消息请用 messages）");
            }
            let rows = run_messages(&m)?;
            emit(&Value::Array(rows), m.common.json, "messages");
            Ok(Entry::Done)
        }
        Command::Accounts(q) => {
            let client = http_client(&q.common)?;
            let rows = block(client.accounts())?
                .iter()
                .map(|a| {
                    json!({
                        "qq": a.qq,
                        "state": a.state.to_string(),
                        "messageCount": a.message_count,
                        "dbPath": a.db_path,
                        "error": a.error,
                    })
                })
                .collect();
            emit(&Value::Array(rows), q.common.json, "accounts");
            Ok(Entry::Done)
        }
        Command::Sync(q) => {
            let client = http_client(&q.common)?;
            let r = block(client.sync_now())?;
            emit(&json!({"success": r.success, "newMessages": r.new_messages, "revokeMessages": r.revoke_messages}), q.common.json, "sync");
            Ok(Entry::Done)
        }
    }
}

/// 裸跑（没有任何参数）的老入口。
fn legacy_load() -> Result<Entry> {
    match crate::config::load()? {
        Some(cfg) => Ok(Entry::Serve(cfg)),
        // --help / --version：既有解析器已经打印过，这里只需退出 0。
        None => Ok(Entry::Done),
    }
}

/// 带旗标的老写法：把参数原样交给既有解析器，行为逐字不变。
fn legacy(argv: &[String]) -> Result<Entry> {
    match crate::config::parse_args(argv.to_vec())? {
        Some(cfg) => Ok(Entry::Serve(cfg)),
        None => Ok(Entry::Done),
    }
}

/// 用法错误：交给 clap 打印并以 2 退出（见模块头的退出码约定）。
fn usage_error(msg: &str) -> ! {
    clap::Error::raw(clap::error::ErrorKind::MissingRequiredArgument, msg).exit()
}

/// `messages` 与 `search` 共用。HTTP 形态必须给 `--talker`（服务端按会话查询），
/// 进程内形态可以省略（索引里所有会话都能翻）。
fn run_messages(m: &MessageArgs) -> Result<Vec<Value>> {
    if m.embedded {
        return embedded_messages(m.talker.as_deref(), m.since.as_deref(), m.keyword.as_deref(), m.limit);
    }
    let Some(talker) = m.talker.clone() else {
        usage_error("HTTP 形态的 messages/search 需要 --talker（服务端按会话查询；要跨会话请配 --embedded）")
    };
    let client = http_client(&m.common)?;
    let q = MessageQuery {
        talker,
        keyword: m.keyword.clone(),
        start: m.since.clone(),
        limit: m.limit,
        ..Default::default()
    };
    let page = block(client.list_messages(&q))?;
    Ok(page
        .messages
        .iter()
        .map(|r| {
            json!({
                "serverId": r.server_id,
                "createTime": r.create_time,
                "senderName": r.sender_name,
                "senderUsername": r.sender_username,
                "content": r.content,
            })
        })
        .collect())
}

fn http_client(c: &Common) -> Result<Client> {
    let base = c.base_url.clone().unwrap_or_else(|| "http://127.0.0.1:5032".to_string());
    Ok(Client::new(base, env_token()?))
}

/// API token 只从环境变量取：它不经命令行传递，以免落进 shell history 与进程列表。
fn env_token() -> Result<String> {
    std::env::var("QQFLOW_TOKEN").map_err(|_| {
        anyhow::anyhow!("缺少 API token：请设环境变量 QQFLOW_TOKEN（值可用 `qqflow-server token` 取）；token 不经命令行传递，以免落进 shell history 与进程列表")
    })
}

/// SDK 的方法都是 async；子命令是「跑一次就退出」，所以用一个当前线程运行时把 future 拉完。
fn block<T, E: std::error::Error + Send + Sync + 'static>(
    f: impl std::future::Future<Output = std::result::Result<T, E>>,
) -> Result<T> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("建 tokio 运行时失败")?;
    rt.block_on(f).map_err(anyhow::Error::new)
}

// ---- --embedded：进程内直读 ------------------------------------------------
//
// 配置由环境变量给出而不是命令行参数 —— 路径本身不是秘密，但把它做成命令行参数会诱导人们
// 顺手把 key 也做成参数。配置形态是本仓的嵌入面决定的：`api::open(db_path, key)` 只吃
// 「一个 nt_msg.db 路径 + 一个密钥」。

const EMBED_CONFIG_ENV: &str = "QQFLOW_EMBED_CONFIG";

fn embedded_index() -> Result<api::Index> {
    let path = std::env::var(EMBED_CONFIG_ENV)
        .map(PathBuf::from)
        .with_context(|| format!("--embedded 需要环境变量 {EMBED_CONFIG_ENV} 指向配置文件"))?;
    let raw = std::fs::read_to_string(&path)
        .with_context(|| format!("读配置文件失败: {}", path.display()))?;
    let cfg: Value = serde_json::from_str(&raw)
        .with_context(|| format!("配置文件不是合法 JSON: {}", path.display()))?;
    let db_path = cfg.get("db_path").and_then(Value::as_str).unwrap_or_default();
    let key = cfg.get("key").and_then(Value::as_str).unwrap_or_default();
    if db_path.is_empty() || key.is_empty() {
        anyhow::bail!("配置里需要 db_path（nt_msg.db 文件）与 key");
    }
    let index = api::open(std::path::Path::new(db_path), key)?;
    Ok(index)
}

fn embedded_sessions() -> Result<Vec<Value>> {
    let index = embedded_index()?;
    Ok(index
        .conversations()
        .iter()
        .map(|c| {
            json!({
                "username": c.talker,
                "displayName": c.name,
                "chatType": c.chat_type.as_str(),
                "messageCount": c.message_count,
            })
        })
        .collect())
}

fn embedded_messages(
    talker: Option<&str>,
    since: Option<&str>,
    keyword: Option<&str>,
    limit: Option<u32>,
) -> Result<Vec<Value>> {
    let index = embedded_index()?;
    let start = match since {
        // 进程内形态只接受 unix 秒：YYYYMMDD 的换算规则（当天零点起、含整天）是服务端解析器定的，
        // 在 CLI 里另写一份就是第二套规则。
        Some(s) => s.parse::<i64>().with_context(|| format!("--since 需为 unix 秒: {s}"))?,
        None => 0,
    };
    let cap = limit.unwrap_or(200) as usize;
    let convs = index.conversations();
    let targets: Vec<(api::ChatType, String)> = match talker {
        Some(t) => convs
            .iter()
            .filter(|c| c.talker == t)
            .map(|c| (c.chat_type, c.talker.clone()))
            .collect(),
        None => convs.iter().map(|c| (c.chat_type, c.talker.clone())).collect(),
    };
    let mut out = Vec::new();
    for (chat_type, t) in targets {
        // 与会话/联系人两处同规：HTTP 形态给的是服务端索引的倒序，进程内这里也按时间倒序。
        let mut msgs = index.messages(chat_type, &t);
        msgs.sort_by_key(|m| std::cmp::Reverse(m.ts));
        for msg in msgs {
            if msg.ts < start {
                continue;
            }
            // 本仓的解析结果只保留正文（不保存原始 XML），所以关键词只对正文判命中 ——
            // 这是与 weflow-server 的又一处能力差异，登记在文档里而不是留给用户猜。
            let text = msg.parsed.content.as_str();
            if keyword.is_some_and(|kw| !text.contains(kw)) {
                continue;
            }
            out.push(json!({
                "serverId": msg.seq.to_string(),
                "createTime": msg.ts,
                "senderName": msg.from_nick,
                "senderUsername": msg.from_uid,
                "content": text,
            }));
            if out.len() >= cap {
                return Ok(out);
            }
        }
    }
    Ok(out)
}

// ---- 输出 ------------------------------------------------------------------

/// 人类可读输出按「哪一类数据」挑列：把所有字段都印出来一屏放不下。机器面永远是 --json。
fn emit(value: &Value, as_json: bool, kind: &str) {
    let stdout = std::io::stdout();
    let mut out = stdout.lock();
    if as_json {
        let text = serde_json::to_string_pretty(value).unwrap_or_else(|_| "{}".into());
        let _ = writeln!(out, "{text}");
        return;
    }
    match value {
        Value::Array(items) => {
            if items.is_empty() {
                let _ = writeln!(out, "（无结果）");
            }
            for it in items {
                let _ = writeln!(out, "{}", human_row(it, kind));
            }
        }
        other => {
            let _ = writeln!(out, "{}", human_row(other, kind));
        }
    }
}

fn human_row(v: &Value, kind: &str) -> String {
    let s = |k: &str| v.get(k).and_then(Value::as_str).unwrap_or("").to_string();
    let n = |k: &str| v.get(k).map(|x| x.to_string()).unwrap_or_default();
    match kind {
        "sessions" => format!(
            "{}\t{}\t{} 条\t{}",
            s("username"),
            s("displayName"),
            n("messageCount"),
            n("lastTimestamp")
        ),
        "contacts" => format!("{}\t{}", s("username"), s("displayName")),
        "accounts" => {
            let err = v
                .get("error")
                .and_then(Value::as_str)
                .map(|e| format!("\terror: {e}"))
                .unwrap_or_default();
            format!("{}\t{}\t{} 条\t{}{err}", s("qq"), s("state"), n("messageCount"), s("dbPath"))
        }
        "sync" => format!(
            "新增 {} 条，撤回 {} 条（success={}）",
            n("newMessages"),
            n("revokeMessages"),
            n("success")
        ),
        _ => format!("{}\t{}\t{}", n("createTime"), s("senderName"), s("content")),
    }
}
