//! 不透明进度游标 v2。
//!
//! 一条游标必须能自证四件事，否则「拿别人的游标续拉」会静默串数据：它是**本进程**
//! 编的（内存索引的行标识没有跨进程意义）、属于**这个账号**、属于**这个会话**，
//! 且编它时的**注册代次**与**数据源世代**都还是当前这一代。
//!
//! 载荷（点分，编码前全是 ASCII 安全字符）：
//! `v2.<nonce>.<account8>.<registration>.<generation>.<session8>.<rowid>`，
//! 整段再做 URL-safe base64（无填充）。**不能用标准字母表**：`+` 在 query 里会被
//! 解成空格，客户端原样回传时就变成另一条游标。
//!
//! 会话摘要绑的是**解析后的** `(chat_type, talker)`（`conv_key` 的形态），不是用户
//! 传进来的裸 `id`——`find_conversation` 会对另一种会话类型回退，两个不同的裸 id
//! 落到同一个会话时必须编出同一条游标线，互相续拉才算「同一个会话」而不是误判跨会话。
//!
//! 失效语义见 [`CursorError`]：语法坏 400；v1 旧游标、跨进程、跨账号、跨会话、
//! 注册代次或数据源世代不符一律 409 + `reason: cursor_reset_required`。**不静默
//! 退回时间模式**——那会让客户端以为续上了，实际上从头重放。

use crate::server::error::ApiError;

/// 游标总长上限：正常载荷不到 120 字节，超过这个数一定是垃圾或攻击。
const MAX_LEN: usize = 512;
const VERSION: &str = "v2";
/// 机器可读的失效原因：客户端据此**从头重拉**，而不是重试同一条游标。
pub const REASON_CURSOR_RESET: &str = "cursor_reset_required";

#[derive(Debug, PartialEq, Eq)]
pub enum CursorError {
    /// 语法坏：解不出来、字段数不对、数字解析失败。这是**请求错误**（400）。
    Malformed,
    /// 语法没问题，但这条游标对当前请求已经失效（409）；字符串是给人看的原因。
    Reset(&'static str),
}

impl CursorError {
    pub fn into_api_error(self) -> ApiError {
        match self {
            CursorError::Malformed => {
                ApiError::bad_request("cursor 无法解析：期望服务端签发的原样游标")
            }
            CursorError::Reset(why) => ApiError::conflict_with_reason(REASON_CURSOR_RESET, why),
        }
    }
}

/// 编/解一条游标所需的「这一代」上下文。
#[derive(Clone, Copy)]
pub struct CursorContext {
    pub nonce: &'static str,
    pub account: [u8; 8],
    pub registration: u64,
    pub generation: u32,
    pub session: [u8; 8],
}

fn digest8(seed: &[u8]) -> [u8; 8] {
    use sha2::Digest;
    let mut out = [0u8; 8];
    out.copy_from_slice(&sha2::Sha256::digest(seed)[..8]);
    out
}

/// URL-safe base64（无填充），无依赖实现：标准字母表的 `+`/`/` 在 query 与路径里
/// 都会被改写，游标必须一个都不出现。与服务端其余部分一样只依赖 std。
fn b64_url_no_pad(data: &[u8]) -> String {
    const TABLE: &[u8; 64] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789-_";
    let mut out = String::with_capacity(data.len().div_ceil(3) * 4);
    for chunk in data.chunks(3) {
        let b = [chunk[0], *chunk.get(1).unwrap_or(&0), *chunk.get(2).unwrap_or(&0)];
        let n = ((b[0] as u32) << 16) | ((b[1] as u32) << 8) | b[2] as u32;
        out.push(TABLE[(n >> 18) as usize & 63] as char);
        out.push(TABLE[(n >> 12) as usize & 63] as char);
        out.push(if chunk.len() > 1 { TABLE[(n >> 6) as usize & 63] as char } else { '\0' });
        out.push(if chunk.len() > 2 { TABLE[n as usize & 63] as char } else { '\0' });
    }
    out.trim_end_matches('\0').to_string()
}

/// 上面的逆运算。语法坏（非字母表字符、长度不成 4 的余数、末尾残组）返回 `None`。
fn b64_url_decode(text: &str) -> Option<Vec<u8>> {
    const REV: fn(u8) -> Option<u32> = |c| match c {
        b'A'..=b'Z' => Some((c - b'A') as u32),
        b'a'..=b'z' => Some((c - b'a' + 26) as u32),
        b'0'..=b'9' => Some((c - b'0' + 52) as u32),
        b'-' => Some(62),
        b'_' => Some(63),
        _ => None,
    };
    let bytes: Vec<u8> = text.bytes().collect();
    if bytes.is_empty() {
        return None;
    }
    let mut out = Vec::with_capacity(bytes.len() * 3 / 4);
    for chunk in bytes.chunks(4) {
        if chunk.len() == 1 {
            return None;
        }
        let mut n: u32 = 0;
        for (i, c) in chunk.iter().enumerate() {
            n |= REV(*c)? << (18 - 6 * i);
        }
        out.push((n >> 16) as u8);
        if chunk.len() > 2 {
            out.push((n >> 8) as u8);
        }
        if chunk.len() > 3 {
            out.push(n as u8);
        }
    }
    Some(out)
}

fn hex8(bytes: &[u8; 8]) -> String {
    let mut s = String::with_capacity(16);
    for b in bytes {
        s.push_str(&format!("{b:02x}"));
    }
    s
}

fn hex_from(s: &str) -> Option<[u8; 8]> {
    if s.len() != 16 || !s.bytes().all(|b| b.is_ascii_hexdigit()) {
        return None;
    }
    let mut out = [0u8; 8];
    for (i, slot) in out.iter_mut().enumerate() {
        let hi = (s.as_bytes()[2 * i] as char).to_digit(16)?;
        let lo = (s.as_bytes()[2 * i + 1] as char).to_digit(16)?;
        *slot = ((hi << 4) | lo) as u8;
    }
    Some(out)
}

impl CursorContext {
    /// 按「当前进程 + 当前账号 + 当前会话 + 当前代次」构造上下文。
    ///
    /// `session_id` 是**解析后的会话键**（`conv_key` 的返回值）——两个裸 id 落到
    /// 同一个会话时必须编出同一条线。
    pub fn for_session(
        qq: &str,
        session_id: &str,
        registration: u64,
        generation: u32,
    ) -> Self {
        Self {
            nonce: crate::server::process_nonce(),
            account: digest8(qq.as_bytes()),
            registration,
            generation,
            session: digest8(session_id.as_bytes()),
        }
    }

    pub fn encode(&self, local_id: i64) -> String {
        let payload = format!(
            "{VERSION}.{}.{}.{}.{}.{}.{}",
            self.nonce,
            hex8(&self.account),
            self.registration,
            self.generation,
            hex8(&self.session),
            local_id,
        );
        b64_url_no_pad(payload.as_bytes())
    }

    pub fn decode(&self, cursor: &str) -> Result<i64, CursorError> {
        if cursor.is_empty() || cursor.len() > MAX_LEN {
            return Err(CursorError::Malformed);
        }
        let raw = b64_url_decode(cursor).ok_or(CursorError::Malformed)?;
        let text = String::from_utf8(raw).map_err(|_| CursorError::Malformed)?;
        let mut parts = text.split('.');
        let version = parts.next().ok_or(CursorError::Malformed)?;
        if version == "v1" {
            // v1 的载荷没有账号/会话/世代绑定，语义已经变了，只能重拉——但它的
            // 语法是本服务签发过的旧形态，所以是「重拉」信号（409），不是垃圾输入。
            return Err(CursorError::Reset("这条游标来自更早的版本，无法继续使用"));
        }
        if version != VERSION {
            // 既不认识的版本也不是我们发过的旧版本：这是垃圾输入，不是「重拉」信号。
            return Err(CursorError::Malformed);
        }
        let nonce = parts.next().ok_or(CursorError::Malformed)?;
        let account = parts.next().ok_or(CursorError::Malformed)?;
        let registration = parts.next().ok_or(CursorError::Malformed)?;
        let generation = parts.next().ok_or(CursorError::Malformed)?;
        let session = parts.next().ok_or(CursorError::Malformed)?;
        let local_id = parts.next().ok_or(CursorError::Malformed)?;
        if parts.next().is_some() {
            return Err(CursorError::Malformed);
        }
        if nonce != self.nonce {
            return Err(CursorError::Reset("这条游标由另一个服务进程签发，重启后必须从头拉取"));
        }
        if hex_from(account) != Some(self.account) {
            return Err(CursorError::Reset("这条游标属于另一个账号"));
        }
        if hex_from(session) != Some(self.session) {
            return Err(CursorError::Reset("这条游标属于另一个会话"));
        }
        let registration: u64 = registration.parse().map_err(|_| CursorError::Malformed)?;
        if registration != self.registration {
            return Err(CursorError::Reset("账号注册代次已变化（发生过注销或重新注册）"));
        }
        let generation: u32 = generation.parse().map_err(|_| CursorError::Malformed)?;
        if generation != self.generation {
            return Err(CursorError::Reset("数据源已更换（库文件身份变化）"));
        }
        local_id.parse().map_err(|_| CursorError::Malformed)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn ctx() -> CursorContext {
        CursorContext { nonce: "0123456789abcdef", account: [1; 8], registration: 3, generation: 7, session: [2; 8] }
    }

    #[test]
    fn round_trip_and_url_safety() {
        let encoded = ctx().encode(42);
        assert_eq!(ctx().decode(&encoded), Ok(42));
        // 标准字母表的 `+` / `/` 在 query 里会被改写，URL-safe 必须一个都不出现。
        assert!(!encoded.contains('+') && !encoded.contains('/'), "游标必须 URL-safe：{encoded}");
        assert!(!encoded.contains('='), "不留 padding，省一次转义：{encoded}");
    }

    #[test]
    fn malformed_cursor_is_a_bad_request() {
        for bad in ["", "!!!", "AAAA", "djI", "djIu", "djIuYS5i"] {
            assert_eq!(ctx().decode(bad), Err(CursorError::Malformed), "{bad:?}");
        }
    }

    #[test]
    fn every_identity_mismatch_is_a_reset_not_a_silent_restart() {
        let encode = |payload: &str| b64_url_no_pad(payload.as_bytes());
        let c = ctx();
        let cases = [
            (encode("v1.0.5"), "v1 旧版本"),
            (encode(&format!("v2.{}.{}.{}.{}.{}.1", "ffffffffffffffff", hex8(&[9u8; 8]), 3, 7, hex8(&[2u8; 8]))), "跨进程"),
            (encode(&format!("v2.{}.{}.{}.{}.{}.1", c.nonce, hex8(&[9u8; 8]), 3, 7, hex8(&[2u8; 8]))), "跨账号"),
            (encode(&format!("v2.{}.{}.{}.{}.{}.1", c.nonce, hex8(&c.account), 3, 7, hex8(&[9u8; 8]))), "跨会话"),
            (encode(&format!("v2.{}.{}.{}.{}.{}.1", c.nonce, hex8(&c.account), 4, 7, hex8(&[2u8; 8]))), "注册代次"),
            (encode(&format!("v2.{}.{}.{}.{}.{}.1", c.nonce, hex8(&c.account), 3, 8, hex8(&[2u8; 8]))), "数据源世代"),
        ];
        for (cursor, what) in cases {
            assert!(
                matches!(c.decode(&cursor), Err(CursorError::Reset(_))),
                "{what} 必须判为需要重拉，而不是沿用或 400"
            );
        }
    }
}
