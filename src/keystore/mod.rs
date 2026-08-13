//! In-memory key table: the SQLCipher database keys supplied at runtime by
//! downstream clients via `POST /api/v1/accounts` (`{qq, key, db_path}`).
//!
//! This project deliberately does NOT extract keys (no process debugging,
//! no PE analysis). Keys are obtained by the user with independent tools
//! (e.g. QQBackup/qq-win-db-key) and handed to the server per account.
//! Keys live in memory only — they are never persisted.

use std::collections::HashMap;

use anyhow::Result;

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
    /// Key for a given QQ account (registered by a client at runtime).
    pub fn get(&self, qq: &str) -> Option<&str> {
        self.keys.get(qq).map(|s| s.as_str())
    }

    /// Validate and insert; returns false when the key is malformed
    /// (must be exactly 16 printable ASCII bytes).
    pub fn insert_validated(&mut self, qq: &str, key: &str) -> bool {
        if validate_key(key).is_ok() {
            self.keys.insert(qq.to_string(), key.to_string());
            true
        } else {
            false
        }
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

    #[test]
    fn insert_validated_only_accepts_valid_keys() {
        let mut ks = KeyStore::default();
        assert!(ks.insert_validated("123", "1234567890abcdef"));
        assert_eq!(ks.get("123"), Some("1234567890abcdef"));
        assert!(!ks.insert_validated("456", "short"), "malformed key rejected");
        assert_eq!(ks.get("456"), None, "rejected key not stored");
    }
}
