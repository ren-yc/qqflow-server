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
    Contacts(ContactsArgs),
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

/// 联系人面的参数：分页语义与其它面一致（limit/offset 直接透传）。只有 HTTP
/// 形态：联系人面没有进程内索引路径。
#[derive(clap::Args)]
struct ContactsArgs {
    #[command(flatten)]
    common: Common,
    /// 单页条数（服务端上限 10000）
    #[arg(long)]
    limit: Option<u32>,
    /// 起始偏移（翻页用）
    #[arg(long)]
    offset: Option<u64>,
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
    /// 跳过上一轮**完整交付**过的会话（幂等续跑；残缺或中断的一律重写）
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
            let page = block(client.contacts(&ContactsQuery {
                limit: q.limit,
                offset: q.offset,
                ..Default::default()
            }))?;
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
            emit(
                &json!({"total": page.total, "hasMore": page.has_more, "contacts": rows}),
                q.common.json,
                "contacts",
            );
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
        // None 是「跨全部会话」的合法语义；显式空串/纯空白不是 —— 它会变成一个
        // 永远匹配不到的会话键，静默给出空结果退 0，与 HTTP 形态的用法错误不一致。
        if m.talker.as_deref().is_some_and(|t| t.trim().is_empty()) {
            usage_error("--talker 不能是空串（要跨全部会话请省略 --talker）")
        }
        return embedded_messages(m.talker.as_deref(), m.since.as_deref(), m.keyword.as_deref(), m.limit);
    }
    let Some(talker) = m.talker.clone().filter(|t| !t.trim().is_empty()) else {
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
        // 与 HTTP 分支共用同一套解析（to_unix）：YYYYMMDD 的换算规则只允许存在一份，
        // 两份实现迟早给出不同的答案。
        Some(s) => to_unix(s)?,
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
        // 媒体先按拉取面给的原始名落行；--with-media 时 retain_downloaded_media 会按消息 id
        // 把它改写成实际落盘的内容摘要名。不带 --with-media 时 fileName 只是元数据，不是可取
        // 承诺（服务的媒体链接也刻意不写进导出物——导出文件会被拷进聊天工具、传上网盘）。
        media_file_name: m.media.as_ref().map(|x| x.file_name.clone()).filter(|s| !s.is_empty()),
        media_type: m.media.as_ref().map(|x| x.type_.clone()),
    }
}


/// 逐个核对**复用会话**产物里的媒体句柄是否在同目录 `media/` 下有字节。
///
/// 返回悬空的会话名。清单只含本轮 target 与复用命中的条目，所以复用会话都能在清单里找到
/// 自己的 `file`。句柄用子串扫描取（不解析整份 JSON）：两种形态都按 `{"fileName":"<name>"}` 无空格
/// 序列化，而整份解析会把 jsonl 的「内存与条数无关」这条承诺在这里破掉。
fn reused_dangling_handles(
    index: &std::path::Path,
    reused: &[String],
    media_dir: &std::path::Path,
) -> Result<Vec<String>> {
    let text = std::fs::read_to_string(index)
        .with_context(|| format!("读清单失败: {}", index.display()))?;
    let v: Value = serde_json::from_str(&text).context("清单不是合法 JSON")?;
    let mut dangling = Vec::new();
    for row in v.get("sessions").and_then(Value::as_array).into_iter().flatten() {
        let Some(talker) = row.get("talker").and_then(Value::as_str) else { continue };
        if !reused.iter().any(|t| t.as_str() == talker) {
            continue;
        }
        let Some(file) = row.get("file").and_then(Value::as_str) else { continue };
        let body = std::fs::read_to_string(index.with_file_name(file))
            .with_context(|| format!("读复用产物失败: {file}"))?;
        for handle in body.split("\"fileName\":\"").skip(1).filter_map(|s| s.split('"').next()) {
            if handle.is_empty() {
                continue;
            }
            if !media_dir.join(handle).exists() {
                dangling.push(talker.to_string());
                break;
            }
        }
    }
    Ok(dangling)
}

/// clap 层的 `--since` 校验：非法取值必须是**用法错误（退出码 2）**。
///
/// 为什么不能留给运行期：`--limit abc`／`--format xyz` 这类非法取值走 clap 的 value_parser、退 2，
/// 而 `--since abc` 若只在后面手工解析就会退 1 —— 同一类「用法写错了」给出两种退出码，
/// 脚本没法按码分流（`1` 是「连不上/被拒」那类可重试的运行期错误）。
fn parse_since(s: &str) -> Result<String, String> {
    // 返回 trim 后的值：验证用 trim 后的串、原串却原样入库的话，一个带前导空白的
    // `--since " 20250101"` 会以原样进查询参数，同一输入在两处得到两种行为。
    to_unix(s).map(|_| s.trim().to_string()).map_err(|e| format!("{e:#}"))
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
        // 续跑的复用判据要知这一轮在乎不在乎媒体字节（承诺按轮成立，见 export::run 的复用条件）。
        with_media: a.with_media,
    };
    let since = a.since.as_deref().map(to_unix).transpose()?;
    let media_dir = a.out.join("media");
    let mut media_total = 0usize;
    let mut media_rejected = 0usize;
    let start = std::time::Instant::now();
    let outcome = export::run(&targets, &opts, |target, on_page| {
        // Pull 面的 since 是**排他**下界，而 --since 对用户是含边界的：差一秒就会让
        // 「起点那一条」凭空消失，故这里减一。
        let talker = target.talker.clone();
        let since_pull = since.map(|s| s - 1);
        // 单遍化：取数与媒体配窗都在**同一趟翻页**里发生。运行时仍是一个会话建一次
        // （不是每页建一个），与改造前的量级相同。
        let rt = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .context("建 tokio 运行时失败")?;
        let mut next_since = since_pull;
        let mut next_offset = 0u64;
        // SDK 的回调要求它自己的错误类型，而这里真正会失败的是**写盘**：把写失败硬塞成
        // ClientError 会丢信息，所以错误先存起来、循环后立即上抛，后续页只跳过不再写
        // （半途而废的会话由 run 删掉，交给 --resume 重来）。
        let mut write_err: Option<anyhow::Error> = None;
        loop {
            let page = rt
                .block_on(client.pull_page(&talker, next_since, next_offset, None))
                .map_err(anyhow::Error::new)?;
            if page.messages.is_empty() {
                break;
            }
            if write_err.is_none() {
                // --with-media：**先**按本页时间窗触发导出并落盘，**再**写行 —— 消息面
                // 只有在真的写出本地副本之后才给得出可取句柄；行仍来自拉取面，句柄靠
                // 消息 id 对回。映射只活在这一页：上一批句柄不需要为整会话驻留内存
                // （jsonl 的「内存与条数无关」承诺因此不被媒体侧破坏）。
                let downloaded = if a.with_media {
                    let (got, rejected) = rt
                        .block_on(media_for_page(
                            &client,
                            &talker,
                            &media_dir,
                            &page.messages,
                        ))
                        .context("取本页媒体失败")?;
                    media_total += got.len();
                    media_rejected += rejected;
                    got
                } else {
                    std::collections::BTreeMap::new()
                };
                let rows: Vec<export::Row> = page
                    .messages
                    .iter()
                    .map(|m| {
                        let mut r = row_from_pull(m);
                        if a.with_media {
                            export::retain_downloaded_media(&mut r, &downloaded);
                        }
                        r
                    })
                    .collect();
                if let Err(e) = on_page(&rows) {
                    write_err = Some(e);
                }
            }
            if !page.sync.has_more {
                break;
            }
            next_since = Some(page.sync.next_since);
            next_offset = page.sync.next_offset;
        }
        match write_err {
            Some(e) => Err(e),
            None => Ok(()),
        }
    })?;
    println!(
        "[export] {} 个会话、{} 条消息、{} 个媒体 → {}（复用 {}，跳过 {}，索引 {}），用时 {:?}",
        outcome.written.len(),
        outcome.messages,
        media_total,
        opts.out_dir.display(),
        outcome.reused.len(),
        outcome.skipped.len(),
        outcome.index.display(),
        start.elapsed()
    );
    if media_rejected > 0 {
        // 计数要可见：静默拒绝与静默少下载一样糟——交付物少了东西却不能被发现。
        println!("[export] 拒绝 {media_rejected} 个非法媒体文件名（不落盘、不写进导出物）");
    }
    if a.with_media && !outcome.reused.is_empty() {
        // 复用的会话本轮一个字节都没下载，而 `--with-media` 的承诺是「导出物里出现的每个
        // fileName，字节都在 media/ 下」。上一轮之后媒体目录被清理或搬走时，句柄会悬空在交付
        // 包里而退出码仍是 0——这里按最终名逐个核对，把静默成功变成响亮失败。
        let dangling = reused_dangling_handles(&outcome.index, &outcome.reused, &media_dir)
            .unwrap_or_default();
        if !dangling.is_empty() {
            for t in &dangling {
                println!("[export] 媒体句柄悬空（复用产物缺字节）: {t}");
            }
            anyhow::bail!(
                "{} 个复用会话的媒体句柄在 media/ 下没有字节：请对这些会话去掉 --resume 重跑，或恢复媒体目录",
                dangling.len()
            );
        }
    }
    if !outcome.skipped.is_empty() {
        for s in &outcome.skipped {
            println!("[export] 跳过: {}", s);
        }
        // 少导了东西必须以非零码说话：静默的部分成功是这类工具最坏的失败方式。
        anyhow::bail!("{} 个会话未能导出（见上面的 跳过: 行）", outcome.skipped.len());
    }
    Ok(())
}

/// 取一个媒体句柄并保证它在 `<out>/media/` 下有字节，返回**实际落盘的名字**；`Ok(None)`
/// 表示这个名字本来就取不到（404：外链、或服务端没写出副本），应当跳过而不是让整轮失败。
///
/// 磁盘存在性即去重：摘要派生的名字在导出目录里内容唯一（同名即同内容），复用是安全的 ——
/// 这正是 `--resume` 要「只补下缺件」、以及 `--since` 重跑时不该把窗口内媒体重下一遍所需要的
/// 性质。回归位置：`with_media_reuses_bytes_already_on_disk`。
///
/// **只有 404 才算「不是可取句柄」**。其余错误（瞬时 5xx、传输层、鉴权）上抛：一并当成「不可取」
/// 会静默少下载若干媒体、而整体仍退 0 —— 交付物少了东西却没有任何信号，比直接失败更坏。
async fn fetch_one_handle(
    client: &Client,
    media_dir: &std::path::Path,
    name: &str,
) -> Result<Option<String>> {
    if media_dir.join(name).exists() {
        return Ok(Some(name.to_string()));
    }
    let bytes = match client.media_bytes_by_id(name).await {
        Ok(bytes) => bytes,
        Err(ClientError::Status { status: 404, .. }) => return Ok(None),
        Err(e) => return Err(e.into()),
    };
    std::fs::create_dir_all(media_dir)
        .with_context(|| format!("创建媒体目录失败: {}", media_dir.display()))?;
    let path = media_dir.join(name);
    std::fs::write(&path, &bytes).with_context(|| format!("写媒体失败: {}", path.display()))?;
    Ok(Some(name.to_string()))
}

/// 给**一页 Pull 行**配齐媒体，返回「消息 id → 确实落盘的文件名」映射与被拒的非法名字数。
///
/// **两条路按行分派**：
///
/// · 快路径 —— Pull 行自带 `mediaId` 的那些（服务端已保证「出现即可取」）。**直接按句柄取
///   字节，一次导出请求都不发**。
/// · 慢路径 —— 有 `media` 却没有可用 `mediaId` 的那些：本面不执行导出，服务端不会凭空给出
///   句柄，所以把**这些行的时间窗**交给消息面（`/chatlab/messages?media=1`）触发按需导出，
///   再从回填的可取句柄取字节。
///
/// **为什么不能整批撤掉慢路径**：撤掉就等于「媒体句柄只能靠全历史那趟预遍历拿」，而那一趟不认
/// `--since`（本仓刚修掉的正是这个）。快路径让「已经导出过」的会话（`--resume`、重复导出）近乎
/// 零成本，慢路径只在真需要导出时才付出成本；两条路都不再有全历史遍历。
/// 回归位置：`media_window_follows_the_pull_page`（慢路径窗口仍跟着页走）与
/// `media_id_from_pull_row_skips_the_export_round`（快路径零导出请求）。
///
/// 映射按**消息 id**：消息面回填的是内容摘要名、拉取面 `media.fileName` 给的是索引里的原始名，
/// 两者不必相同，只有 id 能把两边对上（`retain_downloaded_media` 按 id 改写句柄）。
///
/// 句柄与名字都来自 HTTP 响应、却要拿去拼本地路径：`../`、盘符、设备名配合 `join` 能写到导出
/// 目录之外。`media_bytes_by_id` 会做 URL 段编码，那防的是 HTTP 层、替代不了写盘前的这一道。
/// 非法名不进下载也不计数为媒体，按**去重后的名字**计数（同一个非法名在两个面上各出现一次时
/// 只该报一次）；静默少下载而整体退 0，是不可发现的失败。
async fn media_for_page(
    client: &Client,
    talker: &str,
    media_dir: &std::path::Path,
    page: &[qqflow_client::generated::r#gen::types::PullMessage],
) -> Result<(std::collections::BTreeMap<String, String>, usize)> {
    let mut by_message: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    // 折叠名 → 实际落盘名：同一份字节被多条消息引用（或仅大小写不同）时，一律指到同一个
    // 物理文件，不重下也不另开一份。折叠口径与会话名去重一致（Windows 卷大小写不敏感）。
    let mut landed: std::collections::BTreeMap<String, String> =
        std::collections::BTreeMap::new();
    let mut rejected_names: std::collections::BTreeSet<String> =
        std::collections::BTreeSet::new();
    let mut need_export:
        Vec<&qqflow_client::generated::r#gen::types::PullMessage> = Vec::new();
    // 快路径：吃拉取面已经给出的句柄。
    for m in page {
        if m.media.is_none() {
            continue;
        }
        let Some(handle) = m
            .media_id
            .as_deref()
            .filter(|s| !s.is_empty())
            .filter(|s| crate::pathsafe::safe_segment(s))
        else {
            // 没有句柄（或句柄不是安全分量）：交给慢路径，那里连 fileName 一起校验并计数。
            need_export.push(m);
            continue;
        };
        let folded = handle.to_lowercase();
        if let Some(known) = landed.get(&folded) {
            by_message.insert(m.platform_message_id.clone(), known.clone());
            continue;
        }
        match fetch_one_handle(client, media_dir, handle).await? {
            Some(name) => {
                landed.insert(folded, name.clone());
                by_message.insert(m.platform_message_id.clone(), name);
            }
            // 服务端说过「出现即可取」而我们拿到 404：句柄已过期（缓存被清理之类）。按取不到
            // 处理 —— 宁可少一个 media 字段，也不给一个指向不存在文件的句柄。
            None => {
                tracing::debug!("跳过 Pull 行句柄 {handle}: 404（服务端已不再可取）");
            }
        }
    }
    if need_export.is_empty() {
        return Ok((by_message, rejected_names.len()));
    }
    // 慢路径：只给「还没有句柄的那些行」配窗。窗口取这些行的时间跨度 —— 它正是消息面认的
    // start/end（两端闭区间的秒级戳；同秒的行必然落在同一页，所以两端不漏行）。
    let lo = need_export.iter().map(|m| m.timestamp).min().unwrap_or(0);
    let hi = need_export.iter().map(|m| m.timestamp).max().unwrap_or(0);
    let mut q = MessageQuery::new(talker.to_string());
    q.media = true;
    q.start = Some(lo.to_string());
    q.end = Some(hi.to_string());
    q.limit = Some(200);
    let mut offset = 0u64;
    loop {
        let resp = client.chatlab_messages(&q).await?;
        let count = resp.messages.len();
        for m in &resp.messages {
            let Some(media) = &m.media else { continue };
            // 已经拿到句柄的消息不再重取：同一份字节的**名字形态**在两个面上可以不同
            // （拉取面给的是可取句柄本身，消息面回填的是导出后的文件名），只按折叠名去重
            // 会漏掉这种「同一消息、两个名字」，于是同一份内容被下两遍、导出物里的引用名
            // 还会随运行顺序漂移。按消息 id 去重才与「映射按 id」这条主口径一致。
            if by_message.contains_key(&m.platform_message_id) {
                continue;
            }
            let name = media.file_name.clone();
            if name.is_empty() {
                continue;
            }
            if !crate::pathsafe::safe_segment(&name) {
                rejected_names.insert(name.to_lowercase());
                tracing::warn!("媒体文件名不是安全的单路径分量，跳过: {name}");
                continue;
            }
            let folded = name.to_lowercase();
            if let Some(known) = landed.get(&folded) {
                by_message.insert(m.platform_message_id.clone(), known.clone());
                continue;
            }
            match fetch_one_handle(client, media_dir, &name).await? {
                Some(landed_name) => {
                    landed.insert(folded, landed_name.clone());
                    by_message.insert(m.platform_message_id.clone(), landed_name);
                }
                None => {
                    tracing::debug!("跳过不可取句柄 {name}: 404（外链或未落盘）");
                }
            }
        }
        if !resp.page.has_more || count == 0 {
            break;
        }
        offset += count as u64;
        q.offset = Some(offset);
    }
    Ok((by_message, rejected_names.len()))
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
        with_media: false,
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


#[cfg(test)]
mod tests {
    use super::*;

    fn tmp_index_dir(tag: &str) -> std::path::PathBuf {
        let d = std::env::temp_dir().join(format!("{}-reverify-{tag}-{}", env!("CARGO_CRATE_NAME"), std::process::id()));
        let _ = std::fs::remove_dir_all(&d);
        std::fs::create_dir_all(d.join("media")).unwrap();
        d
    }

    /// 复用的会话产物里有句柄，而 `media/` 下没有字节 —— 必须点名报出。
    ///
    /// 这条路径是"上一轮带 --with-media 导出、之后媒体目录被清理或搬走"：本轮 --resume
    /// 复用产物（句柄还在导出物里），而本轮一个字节都没下载。承诺「出现即可取」被破坏
    /// 却静默退 0，是交付包里最难被发现的一类缺件。
    #[test]
    fn reused_dangling_handles_are_detected_and_clean_rounds_pass() {
        let d = tmp_index_dir("dangling");
        let artifact = "Reused.jsonl";
        std::fs::write(
            d.join(artifact),
            "{\"_type\":\"header\"}\n{\"_type\":\"message\",\"media\":{\"type\":\"image\",\"fileName\":\"gone.png\"}}\n",
        )
        .unwrap();
        let index = d.join("index.json");
        std::fs::write(
            &index,
            serde_json::to_string(&json!({"sessions": [{"talker": "90002", "file": artifact, "messages": 1, "withMedia": true}]}))
                .unwrap(),
        )
        .unwrap();
        let reused = vec!["90002".to_string()];

        // 字节缺失 → 点名
        let found = reused_dangling_handles(&index, &reused, &d.join("media")).unwrap();
        assert_eq!(found, vec!["90002".to_string()], "句柄没有字节时必须报出该会话");

        // 字节在 → 不报（同名的句柄确实落盘）
        std::fs::write(d.join("media").join("gone.png"), b"PNG-BYTES").unwrap();
        let ok = reused_dangling_handles(&index, &reused, &d.join("media")).unwrap();
        assert!(ok.is_empty(), "字节齐备时不该误报: {ok:?}");

        // 没有句柄的复用产物 → 不报（承诺空真成立）
        std::fs::remove_file(d.join("media").join("gone.png")).unwrap();
        std::fs::write(d.join(artifact), "{\"_type\":\"header\"}\n").unwrap();
        let none = reused_dangling_handles(&index, &reused, &d.join("media")).unwrap();
        assert!(none.is_empty(), "无句柄的产物不该被报为悬空: {none:?}");

        let _ = std::fs::remove_dir_all(&d);
    }
}
