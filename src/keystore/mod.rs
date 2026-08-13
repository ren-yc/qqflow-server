//! SQLCipher key validation for the keys supplied at runtime by downstream
//! clients via `POST /api/v1/accounts` (`{qq, key, db_path}`).
//!
//! This project deliberately does NOT extract keys (no process debugging,
//! no PE analysis). Keys are obtained by the user with independent tools
//! (e.g. QQBackup/qq-win-db-key) and handed to the server per account.
//! Keys live in memory only — never persisted (a validated key travels
//! straight into the per-account `AccountSync`).

use anyhow::Result;

/// Validate a client-supplied SQLCipher key: exactly 16 printable-ASCII
/// bytes (the only shape QQ keys come in).
pub fn validate_key(key: &str) -> Result<()> {
    let b = key.as_bytes();
    if b.len() != 16 {
        anyhow::bail!("key must be exactly 16 bytes, got {}: {:?}", b.len(), key);
    }
    if !b.iter().all(|c| (32..=126).contains(c)) {
        anyhow::bail!("key must be printable ASCII (32..=126): {:?}", key);
    }
    Ok(())
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
