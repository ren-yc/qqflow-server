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
///
/// Errors name **length and category only — never the key itself**. A
/// validation failure travels on the error chain (`{e:#}`) into HTTP
/// responses and `warn!` logs; echoing the value here would smuggle a
/// full key past the red line in `src/logging.rs` the moment any caller
/// formats the error. Mirror of weflow's `parse_db_key`, which reports
/// expected/actual length but not the hex.
pub fn validate_key(key: &str) -> Result<()> {
    let b = key.as_bytes();
    if b.len() != 16 {
        anyhow::bail!("key must be exactly 16 bytes, got {}", b.len());
    }
    if !b.iter().all(|c| (32..=126).contains(c)) {
        anyhow::bail!("key must be printable ASCII (32..=126)");
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

    /// 报错禁 echo 密钥本身：一旦有人把 `{:?}` 加回来，这条即红。
    /// 判据锚定「键里可打印的那段」而不是整串——非可打印分支若回显，Debug 会把
    /// 控制字符转义成 `\u{1}`，整串 `contains` 反而抓不到；取可打印前缀才两态都钉得住。
    #[test]
    fn validation_errors_never_echo_the_key() {
        // 15 字节：长度错误分支
        let e = validate_key("1234567890abcde").unwrap_err().to_string();
        assert!(!e.contains("1234567890abcde"), "报错含原始键值: {e}");
        // 16 字节含控制字符：可打印性错误分支（回显时 Debug 输出带这段可打印前缀）
        let e = validate_key("123456789012345\u{1}").unwrap_err().to_string();
        assert!(!e.contains("123456789012345"), "报错含原始键值: {e}");
    }
}
