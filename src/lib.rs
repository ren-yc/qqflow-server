//! # 两条使用路径
//!
//! - **起服务**：`cargo run`（或 `cargo install qqflow-server`）—— 默认 feature 就是它。
//! - **当库用**：`default-features = false`，然后按需开 feature。**必须显式关掉默认 feature**，
//!   否则会连带拉进 axum 与 tokio。嵌入者从 [`api`] 入手。
//!
//! # 文档门
//!
//! `#![deny(missing_docs)]` 开着。它**天然只作用在承诺面**：[`api`] —— 因为其余模块在默认构建下
//! 是 `pub(crate)`，而 missing_docs 只看得到 `pub`。于是「补 rustdoc」从愿望变成了可验证的门。

#![deny(missing_docs)]

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

/// 嵌入者承诺面 —— 本 crate **唯一**的对外承诺。
///
/// 它始终可用（不随任何 feature 开关），因为「读自己的聊天记录」是最小可用面。
pub mod api;

// 核心：只读数据访问与解析。不依赖 tokio，也不依赖 axum。
internal!(config, db, keystore, logging, parser, pathsafe, store);

// 可选面：关掉即从依赖树里消失。
#[cfg(feature = "sync")]
internal!(sync);
#[cfg(feature = "server")]
internal!(server);

/// 造库/造密钥夹具 —— **不是承诺面**，只随 `testing` feature 编译。
///
/// 落点为什么在库里而不是 `tests/common`：需要它的有两个调用方，而它们互相看不见
/// 对方的代码 —— 集成测试是独立 crate（链库），**根包二进制同样是独立 crate**，批量
/// 导出的夹具生成入口（CLI 的 `--rows`）够不着只在 `tests/` 下存在的模块。一份造库器、
/// 两个调用方、一个 feature 门。
///
/// 只有造库部分是库内单元；axum `oneshot` 那几个 HTTP 助手留在测试侧（它们要
/// `axum` 与 `tower`，后者是 dev-dependency）。
///
/// 豁免文档门：它随 `testing` 编译，语义等同上面 `internal!` 的 testing 分支 ——
/// 把实现面转 `pub` 只为让本仓自己的二进制与集成测试够得着，不是对外承诺。
#[cfg(feature = "testing")]
#[allow(missing_docs)]
pub mod testing;

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
