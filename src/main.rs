//! 服务二进制。
//!
//! **全部走 `pub` 门面** —— 它是独立 crate，看不见 `pub(crate)` 的实现面。这不是限制：它保证
//! 「嵌入者能做的事」与「二进制能做的事」是同一个集合。

use qqflow_server::run_cli;

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    if let Err(e) = rt.block_on(run_cli()) {
        eprintln!("[fatal] {e:#}");
        std::process::exit(1);
    }
}
