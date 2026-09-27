//! qqflow-server: headless HTTP API + SSE service for reading local QQ NT
//! chat records (SQLCipher-decrypted nt_msg.db).
//!
//! Interface follows the WeFlow HTTP API contract (see weflow-api.md):
//! same paths, parameters, response envelopes and SSE event fields.
//!
//! Scope: decryption layer + data reading + service encapsulation only.
//! Key extraction is intentionally NOT implemented — keys come from external
//! tools (e.g. QQBackup/qq-win-db-key) via CLI / keys file / interactive input.

// 实现面：默认 **`pub(crate)`**（边界由编译器强制，外部不可达）；
// `--features testing` 下转 `pub` —— 集成测试在独立 crate 里，只能看见 `pub`。
//
// 承诺面是 [`api`]，见它的模块文档。
macro_rules! internal {
    ($($m:ident),* $(,)?) => {
        $(
            // `testing` 那一支**豁免文档门**：它把实现面转成 `pub` 只是为了让集成测试够得着，
            // 不是对外承诺 —— 要求它逐项写 rustdoc 是把成本放错了地方。
            #[cfg(feature = "testing")]
            #[allow(missing_docs)]
            pub mod $m;
            #[cfg(not(feature = "testing"))]
            pub(crate) mod $m;
        )*
    };
}

// 核心：只读数据访问与解析。不依赖 tokio，也不依赖 axum。
internal!(config, db, keystore, logging, parser, pathsafe, store);

// 可选面：关掉即从依赖树里消失。
#[cfg(feature = "sync")]
internal!(sync);
#[cfg(feature = "server")]
internal!(server);

/// CLI 入口：起服务直到结束。
///
/// **二进制走这里，而不是直接用 `config`/`logging`。** `src/main.rs` 是**独立 crate**，只能看见
/// `pub` —— 而实现面默认是 `pub(crate)`。所以「连自家二进制也得走承诺面」不是麻烦，正是这条
/// 边界在起作用：它保证嵌入者能做的事，二进制没有多一分。
///
/// 本仓库的 `server::serve()` 自己载配置、自己初始化日志（与 weflow 那边把这两步放在入口不同），
/// 所以这里是薄薄一层。
#[cfg(feature = "server")]
pub async fn run_cli() -> anyhow::Result<()> {
    server::serve().await
}
