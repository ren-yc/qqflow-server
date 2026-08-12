//! Runtime configuration: loaded exclusively from `./qqflow-server.json`
//! in the working directory (no command-line arguments).
//!
//! Field names mirror the former CLI args (snake_case); `keys` maps
//! qq -> SQLCipher key directly. Unknown fields are a parse error
//! (typo guard); a missing default config file falls back to defaults.

use std::collections::HashMap;
use std::path::{Path, PathBuf};

use anyhow::{Context, Result};
use serde::Deserialize;

/// Full server configuration. Every field has a built-in default;
/// a missing or partial config file fills only what it provides.
#[derive(Debug, Clone, Deserialize)]
#[serde(default, deny_unknown_fields)]
pub struct Config {
    /// Listen port (same as WeFlow).
    pub port: u16,
    /// Bind address. Keep 127.0.0.1 unless you know what you are doing.
    pub host: String,
    /// API access token. Auto-generated (32B hex) and persisted if omitted.
    pub token: Option<String>,
    /// Direct qq -> key mapping (16 printable ASCII bytes per key).
    pub keys: HashMap<String, String>,
    /// Optional external keys file: {"<qq>": "<key>"}. Overrides `keys`
    /// for the same qq.
    pub keys_file: Option<PathBuf>,
    /// Ask for missing keys interactively on stdin at startup.
    pub ask_key: bool,
    /// Restrict to these QQ accounts (by default all scanned accounts are used).
    pub qq: Vec<String>,
    /// Change-detection cadence: how often the poll loop stats the source
    /// WAL/main files for new messages, in milliseconds. A full sync only
    /// runs when the files changed, so this can be fast at idle.
    pub poll_interval: u64,
    /// Data directory (keys, token, mirror cache). Platform default:
    /// Windows %LOCALAPPDATA%\qqflow-server, Linux ~/.local/share/qqflow-server,
    /// macOS ~/Library/Application Support/qqflow-server.
    pub data_dir: Option<PathBuf>,
    /// Override database discovery: a Tencent Files-style root directory
    /// (<dir>/<qq>/nt_qq/nt_db/nt_msg.db) or a direct nt_msg.db file.
    pub db_path: Option<PathBuf>,
    /// Log level: error | warn | info | debug.
    pub log: String,
}

impl Default for Config {
    fn default() -> Self {
        Self {
            port: 5031,
            host: "127.0.0.1".into(),
            token: None,
            keys: HashMap::new(),
            keys_file: None,
            ask_key: false,
            qq: Vec::new(),
            poll_interval: 200,
            data_dir: None,
            db_path: None,
            log: "info".into(),
        }
    }
}

/// Load `./qqflow-server.json` from the working directory; missing file
/// falls back to defaults, invalid JSON / unknown fields are errors.
pub fn load() -> Result<Config> {
    load_from(Path::new("qqflow-server.json"))
}

/// Load a config from an explicit path (used by tests).
pub fn load_from(path: &Path) -> Result<Config> {
    let text = match std::fs::read_to_string(path) {
        Ok(t) => t,
        Err(e) if e.kind() == std::io::ErrorKind::NotFound => return Ok(Config::default()),
        Err(e) => return Err(e).with_context(|| format!("read config {}", path.display())),
    };
    let cfg: Config = serde_json::from_str(&text)
        .with_context(|| format!("parse config {} (未知字段或类型错误?)", path.display()))?;
    Ok(cfg)
}

/// Resolve the data directory (default per-platform, override via config `data_dir`).
pub fn data_dir(override_dir: Option<&Path>) -> Result<PathBuf> {
    if let Some(d) = override_dir {
        std::fs::create_dir_all(d).with_context(|| format!("create data dir {}", d.display()))?;
        return Ok(d.to_path_buf());
    }
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
pub fn load_or_create_token(data_dir: &std::path::Path, explicit: Option<&str>) -> Result<String> {
    if let Some(t) = explicit {
        if t.len() < 16 {
            anyhow::bail!("token too short (min 16 chars)");
        }
        return Ok(t.to_string());
    }
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
    use std::io::Write;

    /// Unique temp dir per test — tests run in parallel and must not share
    /// the same config file on disk.
    fn test_dir(name: &str) -> PathBuf {
        let d = std::env::temp_dir().join(format!("qqflow_cfg_{name}_{}", std::process::id()));
        std::fs::create_dir_all(&d).unwrap();
        d
    }

    fn write_config(dir: &Path, content: &str) -> PathBuf {
        let p = dir.join("qqflow-server.json");
        std::fs::File::create(&p)
            .unwrap()
            .write_all(content.as_bytes())
            .unwrap();
        p
    }

    #[test]
    fn missing_file_falls_back_to_defaults() {
        let dir = test_dir("missing");
        let cfg = load_from(&dir.join("nope.json")).unwrap();
        assert_eq!(cfg.port, 5031);
        assert_eq!(cfg.host, "127.0.0.1");
        assert_eq!(cfg.log, "info");
        assert!(cfg.keys.is_empty());
    }

    #[test]
    fn full_config_applied() {
        let dir = test_dir("full");
        let p = write_config(
            &dir,
            r#"{"port": 5999, "host": "0.0.0.0", "log": "debug", "poll_interval": 500,
                "db_path": "D:\\x", "qq": ["123456789"]}"#,
        );
        let cfg = load_from(&p).unwrap();
        assert_eq!(cfg.port, 5999);
        assert_eq!(cfg.host, "0.0.0.0");
        assert_eq!(cfg.log, "debug");
        assert_eq!(cfg.poll_interval, 500);
        assert_eq!(cfg.db_path.as_deref().unwrap(), Path::new("D:\\x"));
        assert_eq!(cfg.qq, vec!["123456789"]);
    }

    #[test]
    fn keys_object_loaded() {
        let dir = test_dir("keys");
        // Fabricated account number (not a real QQ).
        let p = write_config(&dir, r#"{"keys": {"335663881": "0123456789abcdef"}}"#);
        let cfg = load_from(&p).unwrap();
        assert_eq!(cfg.keys.get("335663881").unwrap(), "0123456789abcdef");
        assert_eq!(cfg.port, 5031, "unspecified fields keep defaults");
    }

    #[test]
    fn unknown_field_rejected() {
        let dir = test_dir("typo");
        let p = write_config(&dir, r#"{"porrt": 5999}"#);
        assert!(load_from(&p).is_err(), "typo'd field must be a hard error");
    }

    #[test]
    fn invalid_json_rejected() {
        let dir = test_dir("bad");
        let p = write_config(&dir, r#"{not json"#);
        assert!(load_from(&p).is_err());
    }
}
