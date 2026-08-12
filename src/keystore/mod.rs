//! Key store: where the SQLCipher database keys come from.
//!
//! This project deliberately does NOT extract keys (no process debugging,
//! no PE analysis). Keys are obtained by the user with independent tools
//! (e.g. QQBackup/qq-win-db-key) and supplied via the config file:
//!
//! 1. config `"keys"` object (`{"<qq>": "<key>"}` in qqflow-server.json)
//! 2. config `"keys_file"`: external plain-format JSON `{"<qq>": "<key>"}`.
//!    Overrides `keys` for the same qq.
//! 3. config `"ask_key": true`: interactive stdin prompt per account
//!
//! All keys are validated (16 printable ASCII bytes) and persisted to
//! `<data-dir>/keys.json` for reuse.

use std::collections::HashMap;
use std::io::Write;
use std::path::Path;

use anyhow::{Context, Result};

#[derive(Debug, Default)]
pub struct KeyStore {
    keys: HashMap<String, String>, // qq -> 16-byte key
}

fn validate_key(key: &str) -> Result<()> {
    let b = key.as_bytes();
    if b.len() != 16 {
        anyhow::bail!("key must be exactly 16 bytes, got {}: {:?}", b.len(), key);
    }
    if !b.iter().all(|c| (32..=126).contains(c)) {
        anyhow::bail!("key must be printable ASCII (32..=126): {:?}", key);
    }
    Ok(())
}

impl KeyStore {
    /// Build the store from the config's `keys` map, an optional external
    /// keys file, and optional interactive input for the accounts.
    /// Invalid entries are skipped with a warning, never fatal.
    pub fn load(
        keys: &HashMap<String, String>,
        keys_file: Option<&Path>,
        ask: bool,
        accounts: &[String],
    ) -> Result<Self> {
        let mut this = Self::default();
        for (qq, k) in keys {
            if validate_key(k).is_ok() {
                this.keys.insert(qq.clone(), k.clone());
            } else {
                tracing::warn!("[keys] 配置文件中 QQ {qq} 的密钥无效（需 16 字节可打印 ASCII），已跳过");
            }
        }
        if let Some(p) = keys_file {
            let text = std::fs::read_to_string(p)
                .with_context(|| format!("read keys file {}", p.display()))?;
            let v: serde_json::Value = serde_json::from_str(&text)
                .with_context(|| format!("parse keys file {}", p.display()))?;
            let Some(map) = v.as_object() else {
                anyhow::bail!("keys 文件格式应为 JSON 对象: {{\"<qq>\": \"<key>\"}} ({})", p.display());
            };
            for (k, val) in map {
                if let Some(s) = val.as_str()
                    && validate_key(s).is_ok() {
                        this.keys.insert(k.clone(), s.to_string());
                    } else {
                        tracing::warn!("[keys] keys 文件中 QQ {k} 的密钥无效，已跳过");
                    }
            }
        }
        if ask {
            for qq in accounts {
                if this.get(qq).is_none() {
                    print!("请输入 QQ {qq} 的数据库密钥（16 字节 ASCII）: ");
                    std::io::stdout().flush().ok();
                    let mut line = String::new();
                    std::io::stdin().read_line(&mut line).ok();
                    let k = line.trim().to_string();
                    if validate_key(&k).is_ok() {
                        this.keys.insert(qq.clone(), k);
                    } else {
                        anyhow::bail!("invalid key entered for QQ {qq}");
                    }
                }
            }
        }
        Ok(this)
    }

    /// Key for a given QQ account (config keys and keys-file entries bind by
    /// account number).
    pub fn get(&self, qq: &str) -> Option<&str> {
        if let Some(k) = self.keys.get(qq) {
            return Some(k);
        }
        None
    }

    /// Persist keys to `<data-dir>/keys.json` (plain JSON, not obfuscated —
    /// same trust level as QQFlow's xor+base64, which is not security either).
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        if self.keys.is_empty() {
            return Ok(());
        }
        let path = data_dir.join("keys.json");
        let text = serde_json::to_string_pretty(&self.keys)?;
        std::fs::write(&path, text).with_context(|| format!("write {}", path.display()))?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn key_validation() {
        assert!(validate_key("1234567890abcdef").is_ok());
        assert!(validate_key("123").is_err());
        assert!(validate_key("1234567890123456").is_ok()); // 16 chars
        assert!(validate_key("123456789012345\u{1}").is_err()); // non-ascii
    }
}
