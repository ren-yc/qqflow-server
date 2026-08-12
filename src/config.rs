//! Runtime configuration: data directory resolution, token management.

use std::path::PathBuf;

use anyhow::{Context, Result};

/// Resolve the data directory (default per-platform, override via --data-dir).
pub fn data_dir(override_dir: Option<&std::path::Path>) -> Result<PathBuf> {
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
