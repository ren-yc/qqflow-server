//! Open a QQ NT database with SQLCipher — either a header-stripped copy
//! (`open_decrypted`, mirror path) or the LIVE source file in place
//! (`open_live`, through the `qqflow-offset` VFS that virtually strips the
//! 1024-byte custom header — see `crate::db::vfs`).
//!
//! PRAGMA order and parameters follow the QQBackup ecosystem's verified
//! sequence for NTQQ databases (page size BEFORE key, kdf_iter=4000,
//! HMAC-SHA1 with PBKDF2-HMAC-SHA512 KDF, AES-256-CBC); on verification
//! failure we retry with HMAC-SHA512 (QQFlow's fallback).
//!
//! Note: `PRAGMA cipher_plaintext_header_size` is deliberately NOT used —
//! it cannot handle QQ's layout (see `vfs.rs` docs for the two failure
//! modes). The offset VFS is the mechanism for live reads.

use std::path::Path;

use anyhow::{Context, Result};
use rusqlite::{Connection, OpenFlags};

use super::scan::CUSTOM_HEADER_LEN;
use super::vfs;

pub const QQ_PAGE_SIZE: i64 = 4096;
pub const QQ_KDF_ITER: i64 = 4000;

/// 密钥轮换/参数过期的统一诊断文案。
///
/// 注册受理后的后台解密失败会把它带进账号 `error` 状态与 `warn!` 日志；运维不必在
/// 「解密失败」与「密钥可能过期」之间猜。本仓没有独立的图片键（与 weflow 的差异），
/// 所以提示只要求核对库密钥与 `db_path`。
pub const KEY_RECOVERY_HINT: &str =
    "疑似密钥已轮换、参数过期或密钥缺失：请重新注册并核对 db_path 与库密钥";

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

/// `sqlite_master` 读取失败时，哪些文本仍属「密钥不匹配」家族。
///
/// SQLCipher 对错钥最常见的报错就是 `file is not a database`；把它当 I/O 错误会让真正的
/// 密钥失配少了恢复提示。反过来，权限/磁盘错误不能被误诊成密钥问题。
fn is_key_like_open_error(message: &str) -> bool {
    let lower = message.to_lowercase();
    ["not a database", "hmac", "encrypted", "cipher"]
        .iter()
        .any(|needle| lower.contains(needle))
}

/// 打开失败的诊断句：密钥失配与非密钥（I/O/权限/损坏）必须分开，否则会把排查方向带偏。
fn open_failure_message(key_mismatch: bool, last_error: Option<&str>) -> String {
    if key_mismatch {
        return format!(
            "数据库解密失败：{KEY_RECOVERY_HINT}（16 字节 ASCII，可先用 qq-win-db-key 重新提取）"
        );
    }
    if let Some(e) = last_error {
        return format!(
            "数据库打开/校验失败：读取 sqlite_master 失败（{e}）；这通常是文件权限、损坏或并发占用，不是密钥不匹配"
        );
    }
    format!("数据库解密失败：{KEY_RECOVERY_HINT}（16 字节 ASCII，可先用 qq-win-db-key 重新提取）")
}

/// Shared open core: PRAGMA suite with HMAC-SHA1 → HMAC-SHA512 retry and
/// sqlite_master verification.
fn open_checked(path: &Path, key: &str, flags: OpenFlags, vfs_name: Option<&str>) -> Result<Connection> {
    let mut key_mismatch = false;
    let mut last_error: Option<String> = None;
    for hmac in ["HMAC_SHA1", "HMAC_SHA512"] {
        let conn = match vfs_name {
            Some(name) => Connection::open_with_flags_and_vfs(path, flags, name),
            None => Connection::open_with_flags(path, flags),
        }
        .with_context(|| format!("open {}", path.display()))?;
        conn.busy_timeout(std::time::Duration::from_millis(3000))?;
        if let Err(e) = conn.execute_batch(&pragma_suite(key, hmac)) {
            last_error = Some(format!("pragma suite ({hmac}): {e}"));
            tracing::debug!("pragma suite ({hmac}) failed: {e}");
            continue;
        }
        match verify(&conn) {
            Ok(true) => return Ok(conn),
            Ok(false) => {
                key_mismatch = true;
                tracing::debug!("key verification failed (hmac={hmac})");
            }
            Err(e) => {
                if is_key_like_open_error(&format!("{e}")) {
                    key_mismatch = true;
                } else {
                    last_error = Some(format!("sqlite_master ({hmac}): {e}"));
                }
                tracing::debug!("sqlite_master query failed (hmac={hmac}): {e}");
            }
        }
    }
    anyhow::bail!(
        "{}",
        open_failure_message(key_mismatch, last_error.as_deref())
    )
}

/// Open `path` (a header-stripped SQLCipher database) with the given key.
/// Verifies by reading sqlite_master; retries with HMAC-SHA512 on failure.
pub fn open_decrypted(path: &Path, key: &str) -> Result<Connection> {
    // `Connection::open` creates missing files by default — a missing or
    // header-less mirror main.db must fail loudly instead of "verifying" as
    // a freshly created empty database.
    let meta = std::fs::metadata(path)
        .with_context(|| format!("镜像数据库不存在: {}", path.display()))?;
    if meta.len() == 0 {
        anyhow::bail!(
            "镜像数据库为空: {}（源库小于 1024 字节自定义头或镜像损坏）",
            path.display()
        );
    }
    open_checked(
        path,
        key,
        OpenFlags::SQLITE_OPEN_READ_WRITE | OpenFlags::SQLITE_OPEN_CREATE,
        None,
    )
}

/// Open any NT database read-only with an explicit header choice.
/// `offset=true` virtually strips the 1024-byte custom header via the
/// `qqflow-offset` VFS (for files that carry QQ's plaintext header — the
/// source `nt_msg.db` and possibly sibling databases like `group_info.db`);
/// `offset=false` opens the file as-is (headerless SQLCipher). Never copies
/// and never creates files; usable while the QQ client holds the database.
/// Key verification (sqlite_master) is part of `open_checked` — a wrong
/// header choice simply fails verification.
pub fn open_live_mode(path: &Path, key: &str, offset: bool) -> Result<Connection> {
    if offset {
        vfs::ensure_installed()?;
    }
    let meta = std::fs::metadata(path)
        .with_context(|| format!("源数据库不存在: {}", path.display()))?;
    if offset && meta.len() <= CUSTOM_HEADER_LEN {
        anyhow::bail!(
            "源数据库过小: {} ({} 字节 <= 自定义头 {CUSTOM_HEADER_LEN} 字节)，不是有效的 nt_msg.db",
            path.display(),
            meta.len()
        );
    }
    open_checked(
        path,
        key,
        OpenFlags::SQLITE_OPEN_READ_ONLY,
        offset.then_some(vfs::VFS_NAME),
    )
}

/// Open the LIVE source `nt_msg.db` (with its 1024-byte custom header)
/// read-only and in place, via the `qqflow-offset` VFS.
pub fn open_live(path: &Path, key: &str) -> Result<Connection> {
    open_live_mode(path, key, true)
}

/// Verify the key by reading sqlite_master (only possible with a valid key).
/// Requires at least one table: an empty database would trivially "verify".
pub fn verify(conn: &Connection) -> Result<bool> {
    let n: i64 = conn.query_row("SELECT count(*) FROM sqlite_master", [], |r| r.get(0))?;
    Ok(n > 0)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn escape_key_sql() {
        assert_eq!(escape_key("a'b"), "a''b");
        assert_eq!(escape_key("plain16bytestr"), "plain16bytestr");
    }

    #[test]
    fn empty_and_missing_database_rejected() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_empty_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let empty = dir.join("empty.db");
        std::fs::write(&empty, b"").unwrap();
        let err = open_decrypted(&empty, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("为空"), "got: {err:#}");

        let missing = dir.join("missing.db");
        let err = open_decrypted(&missing, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("不存在"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_live_rejects_missing_and_tiny_source() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_live_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let missing = dir.join("nt_msg.db");
        let err = open_live(&missing, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("不存在"), "got: {err:#}");

        let tiny = dir.join("nt_msg.db");
        std::fs::write(&tiny, vec![0u8; 512]).unwrap();
        let err = open_live(&tiny, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("过小"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn garbage_file_reports_key_error() {
        let dir = std::env::temp_dir().join(format!("qqflow_decrypt_garbage_{}", std::process::id()));
        std::fs::create_dir_all(&dir).unwrap();
        let junk = dir.join("junk.db");
        std::fs::write(&junk, vec![0xAAu8; 8192]).unwrap();
        let err = open_decrypted(&junk, "0123456789abcdef").unwrap_err();
        assert!(format!("{err:#}").contains("密钥"), "got: {err:#}");
        assert!(
            format!("{err:#}").contains("疑似密钥已轮换"),
            "错误要给出恢复动作，而不只是「解密失败」: {err:#}"
        );
        assert!(format!("{err:#}").contains("重新注册"), "got: {err:#}");
        let _ = std::fs::remove_dir_all(&dir);
    }

    #[test]
    fn open_failure_message_distinguishes_io_from_key_mismatch() {
        let io = open_failure_message(false, Some("disk I/O error"));
        assert!(io.contains("不是密钥不匹配"), "I/O 失败不能被误诊: {io}");
        assert!(!io.contains("疑似密钥已轮换"), "I/O 失败不能提密钥轮换: {io}");
        let key = open_failure_message(true, None);
        assert!(key.contains("疑似密钥已轮换"), "密钥失配要给恢复动作: {key}");
        assert!(is_key_like_open_error("file is not a database"));
        assert!(!is_key_like_open_error("disk I/O error"));
        assert!(!is_key_like_open_error("permission denied"));
    }
}
