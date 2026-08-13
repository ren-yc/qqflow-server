//! Runtime configuration from command-line arguments (no config file).
//!
//! `--port` (5031), `--host` (127.0.0.1), `--log` (info), plus the watch
//! tuning knobs `--watch-debounce-ms` (350) and `--watch-fallback-ms`
//! (30000). Account database paths and SQLCipher keys are NOT configuration
//! — downstream clients register them at runtime via `POST /api/v1/accounts`.

use std::path::{Path, PathBuf};

use anyhow::{bail, Context, Result};

/// Full server configuration. Every field has a built-in default.
#[derive(Debug, Clone)]
pub struct Config {
    /// Listen port (same as WeFlow).
    pub port: u16,
    /// Bind address. Keep 127.0.0.1 unless you know what you are doing.
    pub host: String,
    /// Log level: error | warn | info | debug.
    pub log: String,
    /// File-watch debounce (ms): how long the watcher waits for an event
    /// burst to quiet down before triggering a sync (WeFlow-aligned; with
    /// batch mode the worst-case delay is about 2x this value).
    pub watch_debounce_ms: u64,
    /// Slow fallback poll (ms): `Mirror::changed()` (zero-IO stats) as a
    /// safety net against file-watch events being silently lost (inotify /
    /// ReadDirectoryChangesW buffer overflow). 0 = disabled (not
    /// recommended: missed events would never recover). The watcher
    /// re-attach retry (every 10 s) is independent of this setting.
    pub watch_fallback_ms: u64,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 5031,
            host: "127.0.0.1".into(),
            log: "info".into(),
            watch_debounce_ms: 350,
            watch_fallback_ms: 30_000,
        }
    }
}

fn help() -> String {
    "qqflow-server — 本地 QQ NT 聊天记录 HTTP API + SSE 服务\n\
     \n\
     用法: qqflow-server [选项]\n\
     \n\
     选项:\n\
       --port <u16>              监听端口（默认 5031）\n\
       --host <ip>               绑定地址（默认 127.0.0.1）\n\
       --log <level>             日志级别: error|warn|info|debug（默认 info）\n\
       --watch-debounce-ms <ms>  文件事件防抖（默认 350）\n\
       --watch-fallback-ms <ms>  慢速兜底轮询，0 关闭（默认 30000）\n\
       -h, --help                显示本帮助\n\
     \n\
     账号与密钥不在命令行提供：启动后由客户端 POST /api/v1/accounts\n\
     传入 {qq, key, db_path} 注册账号。"
        .to_string()
}

/// Parse command-line arguments (skip the program name).
pub fn load() -> Result<Config> {
    parse_args(std::env::args().skip(1).collect())
}

/// Parse `--flag value` pairs (separate from `load` so tests can drive it).
pub fn parse_args(args: Vec<String>) -> Result<Config> {
    let mut cfg = Config::default();
    let mut i = 0;
    while i < args.len() {
        let flag = args[i].clone();
        if flag == "-h" || flag == "--help" {
            println!("{}", help());
            std::process::exit(0);
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
            other => bail!("未知参数: {other}\n{}", help()),
        }
        i += 2;
    }
    Ok(cfg)
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

/// Load the persisted token, or generate + persist a new one.
pub fn load_or_create_token(data_dir: &Path) -> Result<String> {
    let path = data_dir.join("token.txt");
    if let Ok(t) = std::fs::read_to_string(&path) {
        let t = t.trim();
        if !t.is_empty() {
            return Ok(t.to_string());
        }
    }
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    let token: String = bytes.iter().map(|b| format!("{b:02x}")).collect();
    std::fs::write(&path, &token).with_context(|| format!("write token file {}", path.display()))?;
    tracing::info!("[init] generated new API token (persisted to {})", path.display());
    Ok(token)
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
        let cfg = parse_args(vec![]).unwrap();
        assert_eq!(cfg.port, 5031);
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
        .unwrap();
        assert_eq!(cfg.port, 5999);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.log, "debug");
        assert_eq!(cfg.watch_debounce_ms, 500);
        assert_eq!(cfg.watch_fallback_ms, 0);
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
