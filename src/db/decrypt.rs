//! Open a header-stripped QQ NT database with SQLCipher.
//!
//! PRAGMA order and parameters follow the QQBackup ecosystem's verified
//! sequence for NTQQ databases (page size BEFORE key, kdf_iter=4000,
//! HMAC-SHA1 with PBKDF2-HMAC-SHA512 KDF, AES-256-CBC); on verification
//! failure we retry with HMAC-SHA512 (QQFlow's fallback).

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::Connection;

pub const QQ_PAGE_SIZE: i64 = 4096;
pub const QQ_KDF_ITER: i64 = 4000;

fn escape_key(key: &str) -> String {
    key.replace('\'', "''")
}

fn pragma_suite(key: &str, hmac: &str) -> String {
    format!(
        "PRAGMA cipher_page_size = {QQ_PAGE_SIZE};\n\
         PRAGMA key = '{}';\n\
         PRAGMA kdf_iter = {QQ_KDF_ITER};\n\
         PRAGMA cipher_hmac_algorithm = {hmac};\n\
         PRAGMA cipher_default_kdf_algorithm = PBKDF2_HMAC_SHA512;\n\
         PRAGMA cipher = 'aes-256-cbc';\n",
        escape_key(key)
    )
}

/// Open `path` (a header-stripped SQLCipher database) with the given key.
/// Verifies by reading sqlite_master; retries with HMAC-SHA512 on failure.
pub fn open_decrypted(path: &Path, key: &str) -> Result<Connection> {
    for hmac in ["HMAC_SHA1", "HMAC_SHA512"] {
        let conn = Connection::open(path)
            .with_context(|| format!("open {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_millis(3000))?;
        if let Err(e) = conn.execute_batch(&pragma_suite(key, hmac)) {
            tracing::debug!("pragma suite ({hmac}) failed: {e}");
            continue;
        }
        match verify(&conn) {
            Ok(true) => return Ok(conn),
            Ok(false) => {
                tracing::debug!("key verification failed (hmac={hmac})");
            }
            Err(e) => {
                tracing::debug!("sqlite_master query failed (hmac={hmac}): {e}");
            }
        }
    }
    anyhow::bail!(
        "数据库解密失败：请确认密钥正确（16 字节 ASCII，可先用 qq-win-db-key 重新提取）"
    )
}

/// Verify the key by reading sqlite_master (only possible with a valid key).
pub fn verify(conn: &Connection) -> Result<bool> {
    let n: i64 = conn.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))?;
    Ok(n >= 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_key_sql() {
        assert_eq!(escape_key("a'b"), "a''b");
        assert_eq!(escape_key("plain16bytestr"), "plain16bytestr");
    }
}
