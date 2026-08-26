//! Runtime configuration from command-line arguments (no config file).
//!
//! `--port` (5032), `--host` (127.0.0.1), `--log` (info), plus the watch
//! tuning knobs `--watch-debounce-ms` (350) and `--watch-fallback-ms`
//! (30000). Account database paths and SQLCipher keys are NOT configuration
//! — downstream clients register them at runtime via `POST /api/v1/accounts`.

use std::path::PathBuf;

use anyhow::{bail, Context, Result};

/// Full server configuration. Every field has a built-in default.
#[derive(Debug, Clone)]
pub struct Config {
    /// Listen port (WeFlow defaults to 5031; qqflow-server deliberately
    /// keeps 5032 — CLI-overridable).
    pub port: u16,
    /// Bind address. Keep 127.0.0.1 unless you know what you are doing.
    pub host: String,
    /// Log level: error | warn | info | debug.
    pub log: String,
    /// File-watch debounce (ms): how long the watcher waits for an event
    /// burst to quiet down before triggering a sync (WeFlow-aligned; with
    /// batch mode the worst-case delay is about 2x this value).
    pub watch_debounce_ms: u64,
    /// Slow fallback poll (ms): `AccountSync::changed()` (live-connection
    /// state, zero IO) as a safety net against file-watch events being
    /// silently lost (inotify / ReadDirectoryChangesW buffer overflow).
    /// 0 = disabled (not recommended: missed events would never recover).
    /// The watcher re-attach retry (every 10 s) is independent of this
    /// setting.
    pub watch_fallback_ms: u64,
    /// Media export root for `media=1` (WeFlow exportPath semantics);
    /// default `<data-dir>/api-media`.
    pub media_export_dir: Option<PathBuf>,
    /// Base URL for exported media links (`mediaUrl`), e.g.
    /// `--base-url http://192.168.1.10:5032` when serving LAN clients.
    /// Default: derived from `--host`/`--port` (0.0.0.0 / :: fall back to
    /// 127.0.0.1 — bind-all addresses are not reachable as URLs).
    pub base_url: Option<String>,
    /// Print the stored API token (from the OS credential store) and exit.
    pub show_token: bool,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 5032,
            host: "127.0.0.1".into(),
            log: "info".into(),
            watch_debounce_ms: 350,
            watch_fallback_ms: 30_000,
            media_export_dir: None,
            base_url: None,
            show_token: false,
        }
    }
}

fn help() -> String {
    "qqflow-server — 本地 QQ NT 聊天记录 HTTP API + SSE 服务\n\
     \n\
     用法: qqflow-server [选项]\n\
     \n\
     选项:\n\
       --port <u16>              监听端口（默认 5032）\n\
       --host <ip>               绑定地址（默认 127.0.0.1）\n\
       --log <level>             日志级别: error|warn|info|debug（默认 info）\n\
       --watch-debounce-ms <ms>  文件事件防抖（默认 350）\n\
       --watch-fallback-ms <ms>  慢速兜底轮询，0 关闭（默认 30000）\n\
       --media-export-dir <dir>  媒体导出根目录（默认 <data-dir>/api-media）\n\
       --base-url <url>          媒体导出链接 base URL（默认 http://<host>:<port>，\n\
                                 绑定 0.0.0.0/:: 时回退 127.0.0.1；局域网访问请显式指定）\n\
       --show-token              打印已存的 API token 并退出\n\
       -h, --help                显示本帮助\n\
     \n\
     账号与密钥不在命令行提供：启动后由客户端 POST /api/v1/accounts\n\
     传入 {qq, key, db_path} 注册账号。"
        .to_string()
}

/// Parse command-line arguments (skip the program name).
/// `Ok(None)` when `-h`/`--help` was given (help already printed; the
/// caller should exit 0).
pub fn load() -> Result<Option<Config>> {
    parse_args(std::env::args().skip(1).collect())
}

/// Parse `--flag value` pairs (separate from `load` so tests can drive it).
/// `Ok(None)` when `-h`/`--help` was given (help already printed to stdout).
pub fn parse_args(args: Vec<String>) -> Result<Option<Config>> {
    let mut cfg = Config::default();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].clone();
        if flag == "-h" || flag == "--help" {
            println!("{}", help());
            return Ok(None);
        }
        if flag == "--show-token" {
            cfg.show_token = true;
            i += 1;
            continue;
        }
        let value = args
            .get(i + 1)
            .ok_or_else(|| anyhow::anyhow!("参数 {flag} 缺少值\n{}", help()))?
            .clone();
        match flag.as_str() {
            "--port" => {
                cfg.port = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--port 需为 0-65535 的整数: {value}"))?
            }
            "--host" => cfg.host = value,
            "--log" => {
                if !matches!(value.as_str(), "error" | "warn" | "info" | "debug") {
                    bail!("--log 需为 error|warn|info|debug: {value}");
                }
                cfg.log = value;
            }
            "--watch-debounce-ms" => {
                cfg.watch_debounce_ms = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--watch-debounce-ms 需为非负整数: {value}"))?
            }
            "--watch-fallback-ms" => {
                cfg.watch_fallback_ms = value
                    .parse()
                    .map_err(|_| anyhow::anyhow!("--watch-fallback-ms 需为非负整数: {value}"))?
            }
            "--media-export-dir" => cfg.media_export_dir = Some(PathBuf::from(value)),
            "--base-url" => cfg.base_url = Some(value),
            other => bail!("未知参数: {other}\n{}", help()),
        }
        i += 2;
    }
    Ok(Some(cfg))
}

/// Resolve the platform data directory.
pub fn data_dir() -> Result<PathBuf> {
    #[cfg(target_os = "windows")]
    let base = dirs::data_local_dir().unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "linux")]
    let base = dirs::data_local_dir().unwrap_or_else(|| PathBuf::from("."));
    #[cfg(target_os = "macos")]
    let base = dirs::data_dir().unwrap_or_else(|| PathBuf::from("."));
    let d = base.join("qqflow-server");
    std::fs::create_dir_all(&d).with_context(|| format!("create data dir {}", d.display()))?;
    Ok(d)
}

const TOKEN_SERVICE: &str = "qqflow-server";
const TOKEN_USER: &str = "http-api-token";

/// Load the API token from the OS credential store (Windows 凭据管理器 /
/// macOS 钥匙串 / Linux Secret Service), or generate + store a new one.
///
/// The token value is logged **only when it is first generated** (or when
/// no credential store is available and the token is per-session). Use the
/// `--show-token` flag to retrieve it on demand.
pub fn load_or_create_token() -> Result<String> {
    let entry = keyring::Entry::new(TOKEN_SERVICE, TOKEN_USER)
        .map_err(|e| anyhow::anyhow!("凭据库初始化失败: {e}"))?;
    match entry.get_password() {
        Ok(t) if !t.is_empty() => Ok(t),
        Ok(_) | Err(keyring::Error::NoEntry) => {
            let token = new_token();
            entry
                .set_password(&token)
                .map_err(|e| anyhow::anyhow!("凭据库写入失败: {e}"))?;
            tracing::info!("[init] 生成新 API token: {token}（已存入系统凭据库）");
            Ok(token)
        }
        Err(e) => {
            // 无凭据库平台：会话级 token，随 warn 打印
            let token = new_token();
            tracing::warn!(
                "[init] 凭据库不可用 ({e})；API token 为会话级（重启后变化）: {token}"
            );
            Ok(token)
        }
    }
}

/// Read the stored token without generating one; `None` when none exists.
/// Used by `--show-token`.
pub fn show_token() -> Result<Option<String>> {
    let entry = keyring::Entry::new(TOKEN_SERVICE, TOKEN_USER)
        .map_err(|e| anyhow::anyhow!("凭据库初始化失败: {e}"))?;
    match entry.get_password() {
        Ok(t) if !t.is_empty() => Ok(Some(t)),
        Ok(_) | Err(keyring::Error::NoEntry) => Ok(None),
        Err(e) => Err(anyhow::anyhow!("凭据库读取失败: {e}")),
    }
}

fn new_token() -> String {
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

/// Constant-time string equality (avoids timing side channels for token checks).
pub fn constant_time_eq(a: &str, b: &str) -> bool {
    if a.len() != b.len() {
        return false;
    }
    a.as_bytes()
        .iter()
        .zip(b.as_bytes())
        .fold(0u8, |acc, (x, y)| acc | (x ^ y))
        == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn defaults_with_no_args() {
        let cfg = parse_args(vec![]).unwrap().expect("config");
        assert_eq!(cfg.port, 5032);
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.log, "info");
        assert_eq!(cfg.watch_debounce_ms, 350);
        assert_eq!(cfg.watch_fallback_ms, 30_000);
    }

    #[test]
    fn flags_override_defaults() {
        let cfg = parse_args(
            ["--port", "5999", "--host", "0.0.0.0", "--log", "debug", "--watch-debounce-ms", "500", "--watch-fallback-ms", "0"]
                .iter()
                .map(|s| s.to_string())
                .collect(),
        )
        .unwrap()
        .expect("config");
        assert_eq!(cfg.port, 5999);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.log, "debug");
        assert_eq!(cfg.watch_debounce_ms, 500);
        assert_eq!(cfg.watch_fallback_ms, 0);
    }

    #[test]
    fn show_token_switch_parses_without_value() {
        let cfg = parse_args(["--show-token"].iter().map(|s| s.to_string()).collect())
            .unwrap()
            .expect("config");
        assert!(cfg.show_token, "--show-token must set the flag");
        // and normal startup keeps it off
        let cfg = parse_args(vec![]).unwrap().expect("config");
        assert!(!cfg.show_token);
    }

    #[test]
    fn help_prints_and_returns_none_without_exiting() {
        let args: Vec<String> = ["--help"].iter().map(|s| s.to_string()).collect();
        assert!(parse_args(args).unwrap().is_none(), "--help must not start the server");
    }

    #[test]
    fn invalid_values_rejected() {
        let args: Vec<String> = ["--port", "abc"].iter().map(|s| s.to_string()).collect();
        assert!(parse_args(args).is_err());

        let args: Vec<String> = ["--log", "verbose"].iter().map(|s| s.to_string()).collect();
        assert!(parse_args(args).is_err());

        let args: Vec<String> = ["--nope", "1"].iter().map(|s| s.to_string()).collect();
        let err = parse_args(args).unwrap_err();
        assert!(format!("{err:#}").contains("未知参数"));

        let args: Vec<String> = ["--port"].iter().map(|s| s.to_string()).collect();
        assert!(parse_args(args).is_err(), "missing value must be an error");
    }
}
