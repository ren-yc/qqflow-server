//! Key store: where the SQLCipher database keys come from.
//!
//! This project deliberately does NOT extract keys (no process debugging,
//! no PE analysis). Keys are obtained by the user with independent tools
//! (e.g. QQBackup/qq-win-db-key) and supplied via:
//!
//! 1. `--key <16-byte-ascii>` CLI args
//! 2. `--keys-file` JSON: `{"<qq>": "<key>"}` (also accepts QQFlow's
//!    `qqflow_keys.json` shape)
//! 3. `--ask-key`: interactive stdin prompt per account
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
    /// Build the store from CLI keys, an optional keys file, and optional
    /// interactive input for the given accounts.
    pub fn load(
        cli_keys: &[String],
        keys_file: Option<&Path>,
        ask: bool,
        accounts: &[String],
    ) -> Result<Self> {
        let mut this = Self::default();
        for (i, k) in cli_keys.iter().enumerate() {
            validate_key(k).with_context(|| format!("invalid --key[{}]", i))?;
            // Without an explicit account binding, keys are ordered by the
            // accounts list; see `resolve_for`.
            this.keys.insert(format!("__arg{i}"), k.clone());
        }
        if let Some(p) = keys_file {
            let text = std::fs::read_to_string(p)
                .with_context(|| format!("read keys file {}", p.display()))?;
            let v: serde_json::Value = serde_json::from_str(&text)
                .with_context(|| format!("parse keys file {}", p.display()))?;
            if let Some(map) = v.as_object() {
                for (k, val) in map {
                    if let Some(s) = val.as_str()
                        && validate_key(s).is_ok() {
                            this.keys.insert(k.clone(), s.to_string());
                        }
                }
            } else if let Some(arr) = v.as_array() {
                for e in arr {
                    let qq = e.get("qq").and_then(|x| x.as_str()).unwrap_or("");
                    let key = e.get("key").and_then(|x| x.as_str()).unwrap_or("");
                    if !qq.is_empty() && validate_key(key).is_ok() {
                        this.keys.insert(qq.to_string(), key.to_string());
                    }
                }
            }
        }
        if ask {
            for qq in accounts {
                if this.get(qq).is_none() && this.arg_keys().is_empty() {
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

    fn arg_keys(&self) -> Vec<String> {
        let mut v: Vec<String> = self
            .keys
            .iter()
            .filter(|(k, _)| k.starts_with("__arg"))
            .map(|(_, v)| v.clone())
            .collect();
        v.sort();
        v
    }

    /// Key for a given QQ account. CLI keys (--key) bind positionally to the
    /// accounts list; file keys bind by account number.
    pub fn get(&self, qq: &str) -> Option<&str> {
        if let Some(k) = self.keys.get(qq) {
            return Some(k);
        }
        None
    }

    /// Bind positional CLI keys to accounts: account i gets cli_key i
    /// (when --key count == accounts count).
    pub fn bind_positional(&mut self, accounts: &[String]) {
        let args = self.arg_keys();
        if args.len() == accounts.len() {
            for (i, qq) in accounts.iter().enumerate() {
                self.keys.entry(qq.clone()).or_insert_with(|| args[i].to_string());
            }
        }
    }

    /// Persist keys to `<data-dir>/keys.json` (plain JSON, not obfuscated —
    /// same trust level as QQFlow's xor+base64, which is not security either).
    pub fn save(&self, data_dir: &Path) -> Result<()> {
        let plain: HashMap<&String, &String> = self
            .keys
            .iter()
            .filter(|(k, _)| !k.starts_with("__arg"))
            .collect();
        if plain.is_empty() {
            return Ok(());
        }
        let path = data_dir.join("keys.json");
        let text = serde_json::to_string_pretty(&plain)?;
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
