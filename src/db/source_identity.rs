//! 数据源身份指纹：回答「这个账号绑定的那一个库文件，还是不是原来那个」。
//!
//! QQ NT 的账号只有**一个**消息库：`<documents>/Tencent Files/<qq>/nt_qq/nt_db/nt_msg.db`，
//! 文件以 `CUSTOM_HEADER_LEN`（1024 字节）的 QQ 自有明文头开头，SQLCipher 的页 1
//! （含每库独立随机的 16 字节 salt）从该偏移之后开始 —— 所以 salt 位于文件偏移
//! 1024 起的 16 字节，而不是文件开头。
//!
//! 只取**身份类**线索：路径、文件创建时间、salt。内容类线索——文件大小、mtime、
//! 页计数——**刻意不进指纹**：我方自己的读取与 QQ 的追加写入都会改变它们，一旦
//! 纳入，每来一条新消息都会让游标失效，游标就等于永远不可用。
//!
//! 代价是一条必须写进对外文档的盲区：**同一库文件的备份还原检测不到**
//! （salt 与创建时间都可能原样不变）。要覆盖它就必须引入 USN 加写入归因，
//! 那属于另一个量级的复杂度，不能靠「尽力检测」四个字含糊过去。

use sha2::{Digest, Sha256};
use std::io::Read;
use std::path::Path;
use std::time::SystemTime;

/// salt 长度：SQLCipher 页 1 的明文前缀。
const SALT_LEN: usize = 16;

/// 单个库文件的身份。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct DbIdentity {
    /// 文件创建时间（取不到时为 `None`）。换库时通常跟着变，还原同一文件时不变。
    pub created: Option<SystemTime>,
    pub salt: [u8; SALT_LEN],
}

/// 单账号数据源的指纹。
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub struct SourceFingerprint {
    pub digest: [u8; 32],
}

fn read_salt(path: &Path) -> std::io::Result<[u8; SALT_LEN]> {
    use std::io::Seek;
    let mut salt = [0u8; SALT_LEN];
    let mut file = std::fs::File::open(path)?;
    // QQ 在 SQLCipher 库前面垫了自己的明文头，页 1 的 salt 从这个偏移开始。
    file.seek(std::io::SeekFrom::Start(crate::db::scan::CUSTOM_HEADER_LEN))?;
    let mut filled = 0;
    while filled < SALT_LEN {
        match file.read(&mut salt[filled..])? {
            0 => {
                if filled < SALT_LEN {
                    // 读到 EOF 还不够一个 salt：不是身份，是坏/半截文件。
                    // 宁可报错（采样方按「本轮不推进」处理），也不能拿零填充
                    // 的半截字节冒充身份——那会让两个不同的坏文件编出同一条线。
                    return Err(std::io::Error::new(
                        std::io::ErrorKind::UnexpectedEof,
                        format!("{}: salt 区不足 {SALT_LEN} 字节", path.display()),
                    ));
                }
                break;
            }
            n => filled += n,
        }
    }
    Ok(salt)
}

fn identity(path: &Path) -> std::io::Result<DbIdentity> {
    let meta = std::fs::metadata(path)?;
    Ok(DbIdentity { created: meta.created().ok(), salt: read_salt(path)? })
}

/// 该账号消息库的指纹。
///
/// 只包含身份类线索，因此**追加写入不会改变它**；换库（删除后重建、从备份还原
/// 成另一份文件）会改变它。
pub fn fingerprint(db_path: &Path) -> std::io::Result<SourceFingerprint> {
    let id = identity(db_path)?;
    let mut hasher = Sha256::new();
    // 路径只表达「是哪个文件」，不是内容；用原生字节避免 lossy 转换带来的歧义。
    hasher.update(db_path.as_os_str().as_encoded_bytes());
    hasher.update([0u8]);
    match id.created.and_then(|t| t.duration_since(std::time::UNIX_EPOCH).ok()) {
        Some(d) => {
            hasher.update([1u8]);
            hasher.update(d.as_nanos().to_le_bytes());
        }
        None => hasher.update([0u8]),
    }
    hasher.update(id.salt);
    let mut digest = [0u8; 32];
    digest.copy_from_slice(&hasher.finalize());
    Ok(SourceFingerprint { digest })
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::path::PathBuf;

    fn scratch(name: &str) -> PathBuf {
        let dir = std::env::temp_dir().join(format!(
            "qqflow-srcid-{name}-{}-{:?}",
            std::process::id(),
            std::thread::current().id()
        ));
        let _ = std::fs::remove_dir_all(&dir);
        std::fs::create_dir_all(&dir).expect("建临时目录");
        dir
    }

    /// 造一个 QQ 形状的库：1024 字节明文头 + SQLCipher salt 位置上的 16 字节 +
    /// 其余负载。负载长度变化（模拟追加写入）不影响指纹判据。
    fn write_db(path: &Path, salt: [u8; SALT_LEN], payload: &[u8]) {
        std::fs::create_dir_all(path.parent().expect("有父目录")).expect("建库目录");
        let mut bytes = vec![0u8; crate::db::scan::CUSTOM_HEADER_LEN as usize];
        bytes.extend_from_slice(&salt);
        bytes.extend_from_slice(payload);
        std::fs::write(path, bytes).expect("写库");
    }

    #[test]
    fn appends_do_not_change_the_fingerprint() {
        let dir = scratch("append");
        let db = dir.join("nt_db/nt_msg.db");
        write_db(&db, [7u8; SALT_LEN], &[0u8; 64]);
        let before = fingerprint(&db).expect("指纹");

        // 追加（大小变化）不能让指纹变化，否则每次入库都会让游标失效。
        let mut bytes = std::fs::read(&db).expect("读库");
        bytes.extend_from_slice(&[1u8; 512]);
        std::fs::write(&db, &bytes).expect("追加写");
        let after = fingerprint(&db).expect("指纹");
        assert_eq!(before, after, "追加不是身份变化");
    }

    #[test]
    fn replacing_the_database_changes_the_fingerprint() {
        let dir = scratch("replace");
        let db = dir.join("nt_db/nt_msg.db");
        write_db(&db, [7u8; SALT_LEN], &[0u8; 64]);
        let before = fingerprint(&db).expect("指纹");

        // 换库：新文件的 salt 不同（真实替换里创建时间也常常不同）。
        write_db(&db, [9u8; SALT_LEN], &[0u8; 64]);
        let after = fingerprint(&db).expect("指纹");
        assert_ne!(before, after, "换了库就不是同一个数据源");
    }

    #[test]
    fn the_1024_byte_header_is_not_part_of_the_salt() {
        let dir = scratch("header");
        let db = dir.join("nt_db/nt_msg.db");
        // 两个文件 salt 相同、明文头不同：指纹必须相同——判据读的是偏移 1024
        // 起的 16 字节，不是文件开头。
        let mut head_a = vec![0xAAu8; crate::db::scan::CUSTOM_HEADER_LEN as usize];
        head_a.extend_from_slice(&[3u8; 16]);
        head_a.extend_from_slice(&[0u8; 32]);
        std::fs::create_dir_all(db.parent().unwrap()).unwrap();
        std::fs::write(&db, &head_a).unwrap();
        let a = fingerprint(&db).expect("指纹");
        let mut head_b = vec![0x55u8; crate::db::scan::CUSTOM_HEADER_LEN as usize];
        head_b.extend_from_slice(&[3u8; 16]);
        head_b.extend_from_slice(&[0u8; 32]);
        std::fs::write(&db, &head_b).unwrap();
        let b = fingerprint(&db).expect("指纹");
        let _ = &dir;
        assert_eq!(a, b, "明文头内容不是身份线索，salt 才是");
    }

    #[test]
    fn a_missing_or_tiny_file_is_a_sampling_error_not_a_fingerprint() {
        let dir = scratch("missing");
        let missing = dir.join("nt_db/nt_msg.db");
        assert!(fingerprint(&missing).is_err(), "库不存在必须报错，不能编出指纹");

        // 小于「头 + salt」的文件同样读不出 salt：宁可不推进世代（见 sync 的
        // sample_source），也不能拿半截字节冒充身份。
        std::fs::create_dir_all(dir.join("nt_db")).expect("建库目录");
        let tiny = dir.join("nt_db/small.db");
        std::fs::write(&tiny, vec![0u8; 512]).unwrap();
        assert!(fingerprint(&tiny).is_err(), "读不出完整 salt 的文件必须报错");
    }
}
