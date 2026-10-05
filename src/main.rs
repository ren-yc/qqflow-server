//! 服务二进制。
//!
//! **全部走 `pub` 门面** —— 它是独立 crate，看不见 `pub(crate)` 的实现面。这不是限制：它保证
//! 「嵌入者能做的事」与「二进制能做的事」是同一个集合。

use qqflow_server::run_cli;

fn main() {
    // `run_cli` 自己做子命令分流，并在需要起服务时建运行时。二进制这一层只负责报错与退出码。
    if let Err(e) = run_cli() {
        eprintln!("[fatal] {e:#}");
        std::process::exit(1);
    }
}
