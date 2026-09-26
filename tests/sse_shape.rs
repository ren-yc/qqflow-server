//! SSE 载荷的**键集**护栏。
//!
//! 为什么单独有它：SSE 是流式接口，golden 快照的模型（一次请求一次响应）覆盖不到它。
//! 此前测试只断言了 `ev.event`，键集变了不会有人知道 —— 而键集是契约。
//!
//! 这里断言的是**类型序列化后的形状**，不需要起服务；推送链路本身（事件能否到达订阅方）
//! 由 `fs_watch_e2e` 与 `api_smoke` 的推送用例负责。

use qqflow_server::parser::types::{ChatType, MediaInfo};
use qqflow_server::sync::events::Event;

fn keys_of(v: &serde_json::Value) -> Vec<String> {
    let mut k: Vec<String> = v.as_object().expect("必须是对象").keys().cloned().collect();
    k.sort();
    k
}

/// 文本消息：可选键**不出现**（不是 `null`）—— 客户端据「键在不在」判断有没有。
#[test]
fn message_new_text_payload_keys_are_pinned() {
    let ev = Event::message_new(
        ChatType::C2c,
        "u_a".into(),
        None,
        113737825845249,
        Some("张三".into()),
        "你好".into(),
        26_481,
        None,
        None,
    );
    let v = serde_json::to_value(&ev).unwrap();
    assert_eq!(
        keys_of(&v),
        ["content", "event", "rawid", "sessionId", "sessionType", "sourceName", "timestamp"],
        "文本 message.new 的键集是契约：{v}"
    );
    assert_eq!(v["rawid"], "113737825845249", "rawid 是**字符串**（大整数不能过 JS 精度）");
}

/// 带媒体的消息：`media` 出现，且它是**过滤过的视图** —— 绝不能带上原始缓存路径，
/// 那是本机文件系统信息，推给订阅方既是泄露也无用。
#[test]
fn message_new_media_payload_carries_no_local_path() {
    let ev = Event::message_new(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        113737825910786,
        Some("李四".into()),
        "[image]".into(),
        26_481,
        // 故意塞进本机路径与密钥：`PushMedia` 的 `From` 会把它们丢掉，下面的断言验证
        // 这件事真的发生了。类型层面 `PushMedia` 就没有这两个字段，所以这是第二道锁。
        Some(MediaInfo {
            uuid: Some("R020-test".into()),
            md5: Some("aabbccddeeff00112233445566778899".into()),
            file_name: Some("aabb.png".into()),
            size: Some(1234),
            width: Some(640),
            height: Some(480),
            urls: vec!["https://example.invalid/x".into()],
            key: Some("SECRET-CDN-KEY".into()),
            local_path: Some("D:\\secret\\nt_data\\x.dat".into()),
        }),
        Some("aabbccddeeff00112233445566778899".into()),
    );
    let v = serde_json::to_value(&ev).unwrap();
    let media = v["media"].as_object().expect("media 必须出现");
    let mut mkeys: Vec<&str> = media.keys().map(String::as_str).collect();
    mkeys.sort_unstable();
    assert_eq!(
        mkeys,
        ["fileName", "height", "md5", "size", "urls", "uuid", "width"],
        "SSE 的 media 形状是契约：{media:?}"
    );
    // 原始缓存路径不在 `MediaInfo` 里，因此也不会出现在载荷里 —— 这条断言是第二道锁：
    // 查**整帧原文**，藏在某个值里的路径同样是泄露。
    let raw = serde_json::to_string(&ev).unwrap();
    assert!(
        !raw.contains("nt_data"),
        "推送载荷里出现了本机目录名（local_path 没被丢掉）：{raw}"
    );
    assert!(
        !raw.contains("SECRET-CDN-KEY"),
        "推送载荷里出现了 CDN 密钥（key 没被丢掉）：{raw}"
    );
}

/// 撤销事件：**没有 `media`** —— 撤销的是消息，不是媒体。
#[test]
fn message_revoke_payload_has_no_media() {
    let ev = Event::message_revoke(
        ChatType::Group,
        "10001".into(),
        Some("项目群".into()),
        113737825910787,
        Some("李四".into()),
        "撤回了一条消息".into(),
        26_482,
    );
    let v = serde_json::to_value(&ev).unwrap();
    // 传了 `group_name` 就有 `groupName` —— 它是**条件键**，与「值为 null」是两回事。
    assert_eq!(
        keys_of(&v),
        [
            "content",
            "event",
            "groupName",
            "rawid",
            "sessionId",
            "sessionType",
            "sourceName",
            "timestamp"
        ],
        "message.revoke 的键集是契约：{v}"
    );
    assert!(v.get("media").is_none(), "撤销事件不该有 media：{v}");
}
