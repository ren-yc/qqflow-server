//! CLI argument parsing (clap derive).

use std::path::PathBuf;

use clap::Parser;

/// Headless HTTP API + SSE service for reading local QQ NT chat records.
///
/// Keys are NOT extracted by this tool. Obtain the 16-byte key with an
/// external tool (e.g. QQBackup/qq-win-db-key) and pass it via --key,
/// a keys file, or interactive input (--ask-key).
#[derive(Parser, Debug, Clone)]
#[command(name = "qqflow-server", version, about)]
pub struct Args {
    /// Listen port (default 5031, same as WeFlow).
    #[arg(long, default_value_t = 5031)]
    pub port: u16,

    /// Bind address. Keep 127.0.0.1 unless you know what you are doing.
    #[arg(long, default_value = "127.0.0.1")]
    pub host: String,

    /// API access token. Auto-generated (32B hex) and persisted if omitted.
    #[arg(long)]
    pub token: Option<String>,

    /// SQLCipher database key(s) (16 printable ASCII bytes), one per QQ account.
    #[arg(long, num_args = 1..)]
    pub key: Vec<String>,

    /// Path to a keys JSON file: {"<qq>": "<key>"} (also reads QQFlow's
    /// qqflow_keys.json format).
    #[arg(long)]
    pub keys_file: Option<PathBuf>,

    /// Ask for missing keys interactively on stdin at startup.
    #[arg(long)]
    pub ask_key: bool,

    /// Restrict to these QQ accounts (by default all scanned accounts are used).
    #[arg(long, num_args = 1..)]
    pub qq: Vec<String>,

    /// Polling interval for new messages, in milliseconds.
    #[arg(long, default_value_t = 1500)]
    pub poll_interval: u64,

    /// Data directory (keys, token, mirror cache). Platform default:
    /// Windows %LOCALAPPDATA%\qqflow-server, Linux ~/.local/share/qqflow-server,
    /// macOS ~/Library/Application Support/qqflow-server.
    #[arg(long)]
    pub data_dir: Option<PathBuf>,

    /// Log level: error | warn | info | debug.
    #[arg(long, default_value = "info")]
    pub log: String,
}
