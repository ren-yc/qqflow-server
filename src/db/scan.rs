//! Locate per-account QQ NT chat databases (`nt_msg.db`).

use std::path::{Path, PathBuf};

use anyhow::{Context, Result};

/// QQ NT chat database file name (Windows). On Linux/macOS the layout
/// differs slightly; see `scan_accounts` for the fallback glob.
pub const NT_MSG_DB: &str = "nt_msg.db";
/// Custom protobuf header prepended to the SQLCipher database by QQ.
pub const CUSTOM_HEADER_LEN: u64 = 1024;

#[derive(Debug, Clone)]
pub struct DbInfo {
    pub qq: String,
    pub path: PathBuf,
}

fn documents_dir() -> PathBuf {
    #[cfg(target_os = "windows")]
    {
        dirs::document_dir().unwrap_or_else(|| PathBuf::from("."))
    }
    #[cfg(not(target_os = "windows"))]
    {
        PathBuf::from(".")
    }
}

/// Enumerate accounts: scan `<documents>/Tencent Files/<digits>/nt_qq/nt_db/nt_msg.db`
/// on Windows; on Linux/macOS glob `~/.config/QQ/nt_qq_*/nt_db/` (Linux) and
/// `~/Library/Application Support/QQ/nt_qq_*/nt_db/` (macOS) for files matching
/// `*msg*.db` and pick the largest as the chat database (v1 heuristic).
///
/// `override_root` (from `--db-path`) bypasses platform discovery: a directory
/// is treated as a Tencent Files-style root (`<dir>/<qq>/nt_qq/nt_db/nt_msg.db`),
/// a file is used directly as the chat database.
pub fn scan_accounts(override_root: Option<&Path>) -> Result<Vec<DbInfo>> {
    if let Some(p) = override_root {
        return scan_custom(p);
    }
    #[cfg(target_os = "windows")]
    {
        scan_windows()
    }
    #[cfg(target_os = "linux")]
    {
        scan_unix(&PathBuf::from(".").join(".config").join("QQ"))
    }
    #[cfg(target_os = "macos")]
    {
        scan_unix(
            &dirs::home_dir()
                .unwrap_or_else(|| PathBuf::from("."))
                .join("Library")
                .join("Application Support")
                .join("QQ"),
        )
    }
    #[cfg(not(any(target_os = "windows", target_os = "linux", target_os = "macos")))]
    {
        anyhow::bail!("unsupported platform")
    }
}

#[cfg(target_os = "windows")]
fn scan_windows() -> Result<Vec<DbInfo>> {
    scan_root(&documents_dir().join("Tencent Files"))
}

/// Walk a Tencent Files-style root: `<root>/<digits>/nt_qq/nt_db/nt_msg.db`.
fn scan_root(base: &Path) -> Result<Vec<DbInfo>> {
    let mut out = Vec::new();
    if !base.is_dir() {
        return Ok(out);
    }
    let entries = std::fs::read_dir(base).with_context(|| format!("read {}", base.display()))?;
    for e in entries.flatten() {
        let name = e.file_name().to_string_lossy().to_string();
        if !name.chars().all(|c| c.is_ascii_digit()) {
            continue; // skip non-account dirs
        }
        let db = e.path().join("nt_qq").join("nt_db").join(NT_MSG_DB);
        if db.is_file() {
            out.push(DbInfo { qq: name, path: db });
        }
    }
    out.sort_by(|a, b| a.qq.cmp(&b.qq));
    Ok(out)
}

/// `--db-path` override: direct file, or Tencent Files-style root directory.
fn scan_custom(path: &Path) -> Result<Vec<DbInfo>> {
    if path.is_file() {
        let qq = nearest_digit_dir(path).unwrap_or_else(|| "custom".to_string());
        return Ok(vec![DbInfo { qq, path: path.to_path_buf() }]);
    }
    if path.is_dir() {
        let out = scan_root(path)?;
        if out.is_empty() {
            anyhow::bail!("--db-path 目录下未找到 nt_msg.db: {}", path.display());
        }
        return Ok(out);
    }
    anyhow::bail!("--db-path 不存在: {}", path.display())
}

/// Best-effort account number from a direct db path: the nearest ancestor
/// directory whose name is all digits.
fn nearest_digit_dir(path: &Path) -> Option<String> {
    path.ancestors().find_map(|a| {
        let n = a.file_name()?.to_string_lossy();
        if !n.is_empty() && n.chars().all(|c| c.is_ascii_digit()) {
            Some(n.to_string())
        } else {
            None
        }
    })
}

#[cfg(any(target_os = "linux", target_os = "macos"))]
fn scan_unix(root: &Path) -> Result<Vec<DbInfo>> {
    let mut out = Vec::new();
    if !root.is_dir() {
        return Ok(out);
    }
    let entries = std::fs::read_dir(root).with_context(|| format!("read {}", root.display()))?;
    for e in entries.flatten() {
        let dir = e.path();
        let nt_db = dir.join("nt_db");
        if !nt_db.is_dir() {
            continue;
        }
        // Prefer an exact nt_msg.db, otherwise the largest *msg*.db (v1 heuristic).
        let exact = nt_db.join(NT_MSG_DB);
        let mut candidates: Vec<(u64, PathBuf)> = Vec::new();
        if let Ok(fe) = std::fs::read_dir(&nt_db) {
            for f in fe.flatten() {
                let p = f.path();
                let n = p.file_name().unwrap_or_default().to_string_lossy().to_string();
                if n.to_lowercase().contains("msg") && n.to_lowercase().ends_with(".db") {
                    let len = std::fs::metadata(&p).map(|m| m.len()).unwrap_or(0);
                    candidates.push((len, p));
                }
            }
        }
        let chosen = if exact.is_file() {
            exact
        } else {
            candidates.sort_by(|a, b| b.0.cmp(&a.0));
            candidates.into_iter().next().map(|(_, p)| p)
        };
        if let Some(p) = chosen {
            if let Some(h) = dir.file_name().and_then(|n| n.to_str()) {
                let qq = h.trim_start_matches("nt_qq_").to_string();
                out.push(DbInfo { qq: if qq.is_empty() { h.to_string() } else { qq }, path: p });
            }
        }
    }
    out.sort_by(|a, b| a.qq.cmp(&b.qq));
    Ok(out)
}
