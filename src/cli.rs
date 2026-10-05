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
use qqflow_client::client::{Client, ClientError, ContactsQuery, MessageQuery};
use serde_json::{json, Value};

use crate::api;
use crate::config::Config;
use crate::export;
use crate::parser::types::ChatType;

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
    /// 批量导出：把会话写成 ChatLab Format 的 JSONL / JSON 落盘
    Export(ExportArgs),
    /// 以 MCP（stdio）方式暴露只读查询工具，供 agent 客户端调用
    #[cfg(feature = "mcp")]
    Mcp(McpArgs),
}

/// `mcp` 的参数。刻意只有服务地址：token 只从环境变量取（与其它子命令同一口径），
/// 而 MCP 进程不碰数据库密钥 —— 那是服务端的事。
#[cfg(feature = "mcp")]
#[derive(clap::Args)]
struct McpArgs {
    /// 服务地址；默认取环境变量 QQFLOW_BASE_URL，再默认 http://127.0.0.1:5032
    #[arg(long, env = "QQFLOW_BASE_URL")]
    base_url: Option<String>,
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
    #[arg(long, value_parser = parse_since)]
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

/// `export` 的参数。
///
/// 两条硬约束的落点：`--embedded` **不提供** —— 批量导出只走 HTTP（服务端已经把密钥握在内存里，
/// CLI 只做编排与落盘；否则一个长任务会长时间持有密钥，还得把密钥带上命令行）。`--with-media` 把
/// 字节下载到导出目录下的 `media/`，而**导出物里不写任何 URL**。
#[derive(clap::Args)]
struct ExportArgs {
    /// 输出目录（必需：落盘是有意的动作，不给默认路径）
    #[arg(long)]
    out: PathBuf,
    /// 格式：jsonl（流式、内存与条数无关）或 json（每会话一个完整信封）
    #[arg(long, default_value = "jsonl", value_parser = ["jsonl", "json"])]
    format: String,
    /// 只导这些会话（可重复）
    #[arg(long)]
    session: Vec<String>,
    /// 起始时间：unix 秒或 YYYYMMDD
    #[arg(long, value_parser = parse_since)]
    since: Option<String>,
    /// 已存在的会话文件跳过（幂等续跑）
    #[arg(long)]
    resume: bool,
    /// 下载本会话用到的媒体字节到 <out>/media/，并把导出物里的 fileName 限定为确实落盘的句柄
    #[arg(long)]
    with_media: bool,
    #[command(flatten)]
    common: Common,

    /// 测试专用的语料生成入口：**不进 --help，且只在 testing feature 下编译**。
    #[cfg(feature = "testing")]
    #[arg(long, hide = true)]
    rows: Option<usize>,
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
        Command::Export(a) => {
            #[cfg(feature = "testing")]
            if let Some(rows) = a.rows {
                return export_corpus(&a.out, rows).map(|_| Entry::Done);
            }
            run_export(&a).map(|_| Entry::Done)
        }
        #[cfg(feature = "mcp")]
        Command::Mcp(a) => {
            let base = a.base_url.unwrap_or_else(|| "http://127.0.0.1:5032".to_string());
            crate::mcp::run(base, env_token()?)?;
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
// ---- export ---------------------------------------------------------------

/// Pull 面的消息项 → 导出的中立 Row。单独一个函数是因为取数面的类型属于 SDK，而「怎么写盘」只认
/// Row —— 这样导出模块能在不装 SDK 类型的情况下被测全。
fn row_from_pull(m: &qqflow_client::generated::r#gen::types::PullMessage) -> export::Row {
    export::Row {
        platform_message_id: m.platform_message_id.clone(),
        sender: m.sender.clone(),
        account_name: m.account_name.clone(),
        group_nickname: m.group_nickname.clone(),
        timestamp: m.timestamp,
        msg_type: m.type_,
        content: m.content.clone(),
        reply_to_message_id: m.reply_to_message_id.clone(),
        // 媒体只写元数据（type + fileName）：服务的媒体链接带 token，而导出文件会被拷进聊天
        // 工具、传上网盘。
        media_file_name: m.media.as_ref().map(|x| x.file_name.clone()).filter(|s| !s.is_empty()),
        media_type: m.media.as_ref().map(|x| x.type_.clone()),
    }
}


/// clap 层的 `--since` 校验：非法取值必须是**用法错误（退出码 2）**。
///
/// 为什么不能留给运行期：`--limit abc`／`--format xyz` 这类非法取值走 clap 的 value_parser、退 2，
/// 而 `--since abc` 若只在后面手工解析就会退 1 —— 同一类「用法写错了」给出两种退出码，
/// 脚本没法按码分流（`1` 是「连不上/被拒」那类可重试的运行期错误）。
fn parse_since(s: &str) -> Result<String, String> {
    to_unix(s).map(|_| s.to_string()).map_err(|e| format!("{e:#}"))
}

/// 把 --since 转成 unix 秒。接受 unix 秒或 YYYYMMDD（后者取当天 00:00，与服务端对**下界**的
/// 口径一致；上界才取整天）。
fn to_unix(s: &str) -> Result<i64> {
    let s = s.trim();
    if s.len() == 8 && s.bytes().all(|b| b.is_ascii_digit()) {
        let y: i64 = s[0..4].parse().unwrap_or(0);
        let m: u32 = s[4..6]
            .parse()
            .with_context(|| format!("--since 月份非法: {}", s))?;
        let d: u32 = s[6..8]
            .parse()
            .with_context(|| format!("--since 日非法: {}", s))?;
        let naive = chrono::NaiveDate::from_ymd_opt(y as i32, m, d)
            .with_context(|| format!("--since 不是合法日期: {}", s))?;
        return Ok(naive.and_time(chrono::NaiveTime::MIN).and_utc().timestamp());
    }
    s.parse::<i64>()
        .with_context(|| format!("--since 需为 unix 秒或 YYYYMMDD: {}", s))
}

fn run_export(a: &ExportArgs) -> Result<()> {
    let format = match a.format.as_str() {
        "jsonl" => export::Format::Jsonl,
        "json" => export::Format::Json,
        other => anyhow::bail!("--format 只支持 jsonl|json: {}", other),
    };
    let client = http_client(&a.common)?;
    // 令牌就是导出物里绝不允许出现的那串（见 export 模块头的硬约束一）。
    let secret = std::env::var("QQFLOW_TOKEN").unwrap_or_default();
    let all = block(client.list_all_sessions(Some(10_000), None))?;
    let find = |t: &str| all.iter().find(|s| s.username == t);
    // 会话类型无法从 talker 反推（QQ 的群号与 uid 都是数字串），只能用发现面的 type 码。
    let chat_type = |t: &str| {
        find(t)
            .map(|s| if s.type_ == 2 { ChatType::Group } else { ChatType::C2c })
            .unwrap_or(ChatType::C2c)
    };
    let targets: Vec<export::SessionTarget> = if a.session.is_empty() {
        all.iter()
            .map(|s| export::SessionTarget {
                talker: s.username.clone(),
                display_name: s.display_name.clone(),
                chat_type: if s.type_ == 2 { ChatType::Group } else { ChatType::C2c },
            })
            .collect()
    } else {
        a.session
            .iter()
            .map(|t| export::SessionTarget {
                talker: t.clone(),
                display_name: find(t).map(|s| s.display_name.clone()).unwrap_or_default(),
                chat_type: chat_type(t),
            })
            .collect()
    };
    let opts = export::Options {
        out_dir: a.out.clone(),
        format,
        resume: a.resume,
        secret,
    };
    let since = a.since.as_deref().map(to_unix).transpose()?;
    let media_dir = a.out.join("media");
    let mut media_total = 0usize;
    let start = std::time::Instant::now();
    let outcome = export::run(&targets, &opts, |target, on_page| {
        // Pull 面的 since 是**排他**下界，而 --since 对用户是含边界的：差一秒就会让「起点那一条」
        // 凭空消失，故这里减一。
        let talker = target.talker.clone();
        let since_pull = since.map(|s| s - 1);
        // --with-media：**先**触发导出并把字节落盘，**再**拉行 —— 服务端只有在真的写出了本地副本
        // 之后，才把 Pull 面的 fileName 回填成可取句柄。
        let downloaded = if a.with_media {
            let got = block_anyhow(download_session_media(&client, &talker, &media_dir))?;
            media_total += got.len();
            got
        } else {
            std::collections::BTreeSet::new()
        };
        // SDK 的回调要求它自己的错误类型，而这里真正会失败的是**写盘**：错误先存起来、循环后
        // 立即上抛，后续页只跳过不再写（半途而废的会话由 run 删掉，交给 --resume 重来）。
        let mut write_err: Option<anyhow::Error> = None;
        block(client.drain_session(&talker, since_pull, |msgs| {
            if write_err.is_none() {
                let mut rows: Vec<export::Row> = msgs.iter().map(row_from_pull).collect();
                if a.with_media {
                    for r in rows.iter_mut() {
                        export::retain_downloaded_media(r, &downloaded);
                    }
                }
                if let Err(e) = on_page(&rows) {
                    write_err = Some(e);
                }
            }
            Ok(())
        }))?;

        match write_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })?;
    println!(
        "[export] {} 个会话、{} 条消息、{} 个媒体 → {}（跳过 {}，索引 {}），用时 {:?}",
        outcome.written.len(),
        outcome.messages,
        media_total,
        opts.out_dir.display(),
        outcome.skipped.len(),
        outcome.index.display(),
        start.elapsed()
    );
    if !outcome.skipped.is_empty() {
        for s in &outcome.skipped {
            println!("[export] 跳过: {}", s);
        }
        // 少导了东西必须以非零码说话：静默的部分成功是这类工具最坏的失败方式。
        anyhow::bail!("{} 个会话未能导出（见上面的 跳过: 行）", outcome.skipped.len());
    }
    Ok(())
}

/// 把一个会话的媒体导出并下载到 `<out>/media/`，返回**确实落盘**的句柄集合。
///
/// 为什么先走 `/chatlab/messages?media=1`：服务端**只有在真的写出了本地副本之后**才把
/// `fileName` 回填成可取句柄（外链与平台名给不出跨会话唯一的句柄），所以「触发导出」与「拿到
/// 句柄」是同一次请求的两面。该面每请求最多导出 200 项，超出部分靠翻页续传。
///
/// 单个媒体拿不到（404）只跳过，不升级成会话级失败：外链媒体本来就没有可取句柄，把它升格会让
/// 「这个群里有一个表情包不是本地文件」变成「这个群一条都没导出」。
async fn download_session_media(
    client: &Client,
    talker: &str,
    media_dir: &std::path::Path,
) -> Result<std::collections::BTreeSet<String>> {
    let mut downloaded: std::collections::BTreeSet<String> = std::collections::BTreeSet::new();
    let mut offset = 0u64;
    loop {
        let mut q = MessageQuery::new(talker.to_string());
        q.media = true;
        q.limit = Some(200);
        q.offset = Some(offset);
        let page = client.chatlab_messages(&q).await?;
        let count = page.messages.len();
        for m in &page.messages {
            let Some(media) = &m.media else { continue };
            let name = media.file_name.clone();
            if name.is_empty() || downloaded.contains(&name) {
                continue;
            }
            match client.media_bytes_by_id(&name).await {
                Ok(bytes) => {
                    std::fs::create_dir_all(media_dir)
                        .with_context(|| format!("创建媒体目录失败: {}", media_dir.display()))?;
                    let path = media_dir.join(&name);
                    std::fs::write(&path, &bytes)
                        .with_context(|| format!("写媒体失败: {}", path.display()))?;
                    downloaded.insert(name);
                }
                // **只有 404 才算「不是可取句柄」**（外链、或服务端没写出副本）。
                // 其余错误（瞬时 5xx、传输层、鉴权）**上抛**：一并当成「不可取」会静默少下载若干
                // 媒体、而整体仍退 0 —— 交付物少了东西却没有任何信号，比直接失败更坏。
                Err(ClientError::Status { status: 404, .. }) => {
                    tracing::debug!("跳过不可取句柄 {name}: 404（外链或未落盘）");
                }
                Err(e) => return Err(e.into()),
            }
        }
        if !page.page.has_more || count == 0 {
            return Ok(downloaded);
        }
        offset += count as u64;
    }
}

/// 与 [`block`] 同形，但面向返回 `anyhow::Result` 的 future：`anyhow::Error` 不实现
/// `std::error::Error`，塞不进 `block` 的约束里。
fn block_anyhow<T>(f: impl std::future::Future<Output = Result<T>>) -> Result<T> {
    let rt = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .context("建 tokio 运行时失败")?;
    rt.block_on(f)
}

/// 测试专用的语料生成＋导出入口（`--rows N`，隐藏且只在 `testing` 下编译）。
///
/// **它为什么存在**：「大语料下内存恒定」这条断言在小语料上根本看不出来 —— 把整个会话读进内存
/// 的实现，在一百条的夹具上一样表现为常数内存。要让它显形，语料必须大到「整会话驻留」与「流式
/// 写」差出量级，所以这里按需造库。
///
/// 与用户面的 `export` 唯一的区别是取数来源：这里进程内直读夹具库（`api::open`），因为测试环境里
/// 没有服务可打；写盘路径、行形状、`--resume` 与令牌检查用的是同一套代码（`crate::export`）。
#[cfg(feature = "testing")]
fn export_corpus(out: &std::path::Path, rows: usize) -> Result<()> {
    use crate::testing::{self, BULK_GROUPS};
    let dir = std::env::temp_dir().join(format!("qqflow-export-corpus-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir)
        .with_context(|| format!("建临时目录失败: {}", dir.display()))?;
    let source = testing::build_source_with_rows(&dir, rows);
    let index = api::open(&source, testing::FAKE_KEY)?;
    let sessions: Vec<export::SessionTarget> = index
        .conversations()
        .iter()
        .map(|c| export::SessionTarget {
            talker: c.talker.clone(),
            display_name: c.name.clone(),
            chat_type: c.chat_type,
        })
        .collect();
    let opts = export::Options {
        out_dir: out.to_path_buf(),
        format: export::Format::Jsonl,
        resume: false,
        // 夹具里没有真令牌；留空表示「不检查」，而检查逻辑本身由单测直接测。
        secret: String::new(),
    };
    let started = std::time::Instant::now();
    let mut sample_idx = 0usize;
    let outcome = export::run(&sessions, &opts, |target, on_page| {
        let msgs = index.messages(target.chat_type, &target.talker);
        let rowsv: Vec<export::Row> = msgs
            .iter()
            .map(|m| export::Row {
                platform_message_id: m.seq.to_string(),
                sender: m.from_uid.clone(),
                account_name: m.from_nick.clone(),
                group_nickname: String::new(),
                timestamp: m.ts,
                msg_type: m.parsed.msg_type.chatlab_type(),
                content: m.parsed.content.clone(),
                reply_to_message_id: None,
                media_file_name: None,
                media_type: None,
            })
            .collect();
        let n = rowsv.len();
        on_page(&rowsv)?;
        // 采样点：每导完一个会话取一次峰值 RSS。
        println!("[rss] session={sample_idx} rows={n} peak_kb={:?}", peak_rss_kb());
        sample_idx += 1;
        Ok(())
    })?;
    println!(
        "[corpus] rows={rows} sessions={} written={} messages={} 用时 {:?}",
        BULK_GROUPS,
        outcome.written.len(),
        outcome.messages,
        started.elapsed()
    );
    let _ = std::fs::remove_dir_all(&dir);
    Ok(())
}

/// 进程峰值 RSS（KB）。取不到时返回 `None` —— 调用方据此跳过断言，而不是拿 0 当成「内存恒定」
/// 的证据。
#[cfg(feature = "testing")]
fn peak_rss_kb() -> Option<u64> {
    #[cfg(target_os = "linux")]
    {
        let text = std::fs::read_to_string("/proc/self/status").ok()?;
        for line in text.lines() {
            if let Some(rest) = line.strip_prefix("VmHWM:") {
                return rest.trim().trim_end_matches(" kB").trim().parse().ok();
            }
        }
        None
    }
    #[cfg(windows)]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("powershell")
            .args(["-NoProfile", "-Command", &format!("(Get-Process -Id {pid}).PeakWorkingSet64")])
            .output()
            .ok()?;
        let s = String::from_utf8_lossy(&out.stdout).trim().to_string();
        s.parse::<u64>().ok().map(|bytes| bytes / 1024)
    }
    #[cfg(target_os = "macos")]
    {
        let pid = std::process::id();
        let out = std::process::Command::new("ps")
            .args(["-o", "rss=", "-p", &pid.to_string()])
            .output()
            .ok()?;
        String::from_utf8_lossy(&out.stdout).trim().parse::<u64>().ok()
    }
    #[cfg(not(any(target_os = "linux", windows, target_os = "macos")))]
    {
        None
    }
}

