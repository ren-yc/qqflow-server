//! 命令行子命令面的行为钉子：用真实二进制跑，断言**退出码**与**兼容口径**。
//!
//! 为什么不用单元测试代替：退出码是进程边界上的行为（clap 的 Error::exit、process::exit），
//! 在进程内测不到；而这些码正是脚本据以分支的东西 —— 用 1 冒充 2，脚本就分不出「我参数写错了」
//! 和「服务没起来」。
//!
//! 全部用 CARGO_BIN_EXE_qqflow-server 拿到 cargo 刚构建出来的二进制，不假设 target 目录结构。

use std::process::Command;

struct Out {
    code: i32,
    stdout: String,
    stderr: String,
}

/// 跑一次二进制。第三个参数是**额外**环境变量。
///
/// 为什么不用 std::env::set_var：那是进程级全局，而同一个测试二进制里的用例是并行跑的 ——
/// 一个用例设的 token 会被另一个用例读到。基座里这几个开关一律清掉，免得开发者本机的值把测试
/// 变成非确定性。
fn run(args: &[&str], extra: &[(&str, &str)]) -> Out {
    let mut c = Command::new(env!("CARGO_BIN_EXE_qqflow-server"));
    c.env_remove("QQFLOW_BASE_URL");
    c.env_remove("QQFLOW_TOKEN");
    c.env_remove("QQFLOW_TOKEN_SERVICE");
    c.env_remove("QQFLOW_EMBED_CONFIG");
    // `RUST_LOG` 必须一并清掉：`logging::install` 里 EnvFilter 的取值是「环境变量优先」，
    // 开发者本机设了它，`--log` 的判据（级别、debug 行存在性）就会随环境漂移。
    c.env_remove("RUST_LOG");
    for (k, v) in extra {
        c.env(k, v);
    }
    let r = c.args(args).output().expect("spawn qqflow-server");
    Out {
        code: r.status.code().unwrap_or(-1),
        stdout: String::from_utf8_lossy(&r.stdout).to_string(),
        stderr: String::from_utf8_lossy(&r.stderr).to_string(),
    }
}

fn bare(args: &[&str]) -> Out {
    run(args, &[])
}

/// 兼容口径：**老写法一个字符都不用改**。
#[test]
fn legacy_flags_still_handled_by_the_old_parser() {
    let o = bare(&["--version"]);
    assert_eq!(o.code, 0, "--version 应退出 0；stderr: {}", o.stderr);
    let expect = format!("qqflow-server {}", env!("CARGO_PKG_VERSION"));
    assert_eq!(
        o.stdout.trim(),
        expect,
        "--version 必须由既有解析器处理（走 clap 会换掉文案），实际 stdout: {:?}",
        o.stdout
    );

    let h = bare(&["--help"]);
    assert_eq!(h.code, 0, "--help 应退出 0");
    assert!(
        h.stdout.contains("用法: qqflow-server [选项]") && h.stdout.contains("--watch-debounce-ms"),
        "--help 应仍是既有解析器那份（含全部旗标说明），实际: {:?}",
        h.stdout
    );
}

/// serve 子命令 = 裸跑：后面的旗标仍由老解析器处理。
#[test]
fn serve_subcommand_is_the_same_as_bare_run() {
    let o = bare(&["serve", "--version"]);
    assert_eq!(o.code, 0, "serve 后面的旗标必须交给既有解析器；stderr: {}", o.stderr);
    assert_eq!(
        o.stdout.trim(),
        format!("qqflow-server {}", env!("CARGO_PKG_VERSION")),
        "serve --version 应与 --version 给出同一行文案，实际: {:?}",
        o.stdout
    );
}

/// 用法错误必须是 2，而且不能伪装成取值错误。
#[test]
fn unknown_subcommand_is_a_usage_error_exit_2() {
    let o = bare(&["bogus"]);
    assert_eq!(o.code, 2, "未知子命令应为用法错误；stderr: {}", o.stderr);
    assert!(o.stderr.contains("bogus"), "报错要点名那个词，实际: {}", o.stderr);
}

/// search 的 --keyword 必填：缺了就是用法错误（2），不是运行期错误（1）。
#[test]
fn search_requires_keyword_as_usage_error() {
    let o = bare(&["search"]);
    assert_eq!(o.code, 2, "search 缺 --keyword 应为 2；stderr: {}", o.stderr);
    assert!(o.stderr.contains("keyword"), "报错要点名 --keyword，实际: {}", o.stderr);
}

/// HTTP 形态的 messages 必须点名 --talker（服务端按会话查询）。
#[test]
fn messages_over_http_requires_talker() {
    let o = run(&["messages"], &[("QQFLOW_TOKEN", "probe-token-not-used")]);
    assert_eq!(o.code, 2, "缺 --talker 是用法错误，应为 2；stderr: {}", o.stderr);
    assert!(o.stderr.contains("talker"), "报错要点名 --talker，实际: {}", o.stderr);
}

/// HTTP 形态在「环境变量未设且凭据库无记录」时是运行期错误（1），并且要指路。
///
/// 测试用独立的凭据库命名空间，免得开发者本机真实存着的 token 把这条断言变成看环境运气
/// （testing feature 才编译该覆盖）。
#[test]
fn missing_token_fails_before_touching_the_network() {
    // 指向一个**保证没有监听**的端口：若实现是「先连、连不上再报鉴权」，错误文案会退化成连接失败，
    // 下面那条「不得出现连接失败措辞」的断言就会抓住它 —— 只断言「退 1 ＋ 提到变量名」抓不住。
    let o = run(
        &["sessions"],
        &[
            ("QQFLOW_BASE_URL", "http://127.0.0.1:1"),
            (
                "QQFLOW_TOKEN_SERVICE",
                "qqflow-server-test-missing-token",
            ),
        ],
    );
    assert_eq!(o.code, 1, "缺 token 是运行期错误；stderr: {}", o.stderr);
    assert!(o.stderr.contains("QQFLOW_TOKEN"), "报错要给出补救（环境变量名），实际: {}", o.stderr);
    assert!(o.stderr.contains("凭据库"), "报错要点名回落来源，实际: {}", o.stderr);
    let lower = o.stderr.to_lowercase();
    for wording in ["connection refused", "tcp connect", "error sending request", "connect error"] {
        assert!(!lower.contains(wording), "缺 token 不该走到网络（出现「{wording}」）: {}", o.stderr);
    }
}

/// --embedded 只开放给只读查询类：accounts／sync／contacts 没有这个开关。
///
/// contacts 在这一列是因为 qqflow 的嵌入面没有联系人读面 —— 硬凑一个比「只走 HTTP」更贵
/// （理由见 src/cli.rs 的模块头）。
#[test]
fn embedded_is_refused_on_write_and_account_subcommands() {
    for sub in ["accounts", "sync", "contacts"] {
        let o = bare(&[sub, "--embedded"]);
        assert_eq!(o.code, 2, "{sub} 不该接受 --embedded；stderr: {}", o.stderr);
        assert!(o.stderr.contains("embedded"), "{sub} 的报错应指向 --embedded，实际: {}", o.stderr);
    }
}

/// --embedded 缺配置文件时：运行期错误（1）＋ 点名环境变量。
#[test]
fn embedded_without_config_is_a_runtime_error() {
    let o = bare(&["sessions", "--embedded"]);
    assert_eq!(o.code, 1, "缺配置是运行期错误；stderr: {}", o.stderr);
    assert!(o.stderr.contains("QQFLOW_EMBED_CONFIG"), "报错要给出补救（环境变量名），实际: {}", o.stderr);
}
/// 隐藏参数 `--rows` 是**测试专用**：不进 `--help`。
///
/// 为什么这条值得钉：参数一旦出现在帮助里，就会有用户拿它去「导出三十万条试试」，而那造出来的是
/// 假账号语料。另一半（发布二进制里根本没有这个参数）由 `#[cfg(feature = "testing")]` 在编译期
/// 保证：默认 feature 的整轮 clippy/test 编译的就是不含它的版本。
#[test]
fn export_help_does_not_advertise_the_rows_param() {
    let o = bare(&["export", "--help"]);
    assert_eq!(o.code, 0, "export --help 应成功；stderr: {}", o.stderr);
    assert!(o.stdout.contains("--out"), "帮助里应有常规参数: {}", o.stdout);
    assert!(!o.stdout.contains("--rows"), "--rows 不该出现在帮助里: {}", o.stdout);
}

/// `--out` 是必需参数：不给就是用法错误（2），而不是把导出物写到某个默认目录。
///
/// 为什么刻意不给默认路径：落盘是有意的动作，猜一个目录会把几百个文件写到用户没打算放的地方，
/// 而那种事情发生时导出已经跑完了。
#[test]
fn export_requires_out_dir_as_usage_error() {
    let o = bare(&["export"]);
    assert_eq!(o.code, 2, "export 缺 --out 应为用法错误；stderr: {}", o.stderr);
    assert!(o.stderr.contains("out"), "报错要点名 --out，实际: {}", o.stderr);
}

/// 非法的 `--since` 是**用法错误（退出码 2）**，不是运行期错误（1）。
///
/// 为什么这条要紧：`--limit abc` 与 `--format xyz` 这类非法取值走 clap 的 value_parser、退 2；
/// 而 `--since` 此前只在后面手工解析、退 1。同一类「用法写错了」给出两种退出码，脚本就没法
/// 按码分流 —— `1` 在这套契约里是「连不上／被拒」那类可重试的运行期错误。
#[test]
fn invalid_since_is_a_usage_error_exit_2() {
    for args in [
        vec!["export", "--out", "unused-out-dir", "--since", "20240230"],
        vec!["messages", "--talker", "x", "--since", "abc"],
    ] {
        let o = bare(&args);
        assert_eq!(o.code, 2, "{args:?} 应为用法错误；stderr: {}", o.stderr);
        assert!(o.stderr.contains("since"), "报错要点名 --since，实际: {}", o.stderr);
    }
}

/// `--log` / `--log-format` 是子命令面上的 global 旗标：非法取值必须是**用法错误（2）**，
/// 与 `--since`／`--format` 同一退出码契约 —— 脚本按码分流时「参数写错了」不能混进
/// 可重试的运行期错误（1）。回归位置即本测试名。
///
/// 断言取的是 clap 报错里的**旗标本体**（`'--log <LOG>'` 与 `'--log-format <LOG_FORMAT>'`，
/// 实测文本）而不是 `--log` 子串：后者的报错串是前者的超集，两例都会过，等于没钉住是哪根旗标。
#[test]
fn invalid_log_flags_are_usage_errors_exit_2() {
    for (args, want) in [
        (vec!["token", "--log", "verbose"], "'--log <LOG>'"),
        (
            vec!["token", "--log-format", "yaml"],
            "'--log-format <LOG_FORMAT>'",
        ),
    ] {
        let o = bare(&args);
        assert_eq!(o.code, 2, "{args:?} 应为用法错误；stderr: {}", o.stderr);
        assert!(
            o.stderr.contains(want),
            "报错要点名旗标 {want}（而不是别的同族旗标），实际: {}",
            o.stderr
        );
    }
}

/// `serve` 之后的旗标**仍走老解析器**：非法 `--log` 值的报错是老解析器那句（中文 bail），
/// 而不是 clap 的用法错误格式。这条钉的是「serve = 裸跑」兼容口径没被 global 旗标劫走
/// （dispatch 在构造 Cli 之前就把 serve 短路进 legacy —— 结构上保住了 serve 的 info 默认）。
/// 退出码 1 是老路径的既有形态（`--port abc` 等同形），不随本批改变。
#[test]
fn serve_log_flag_still_goes_through_the_old_parser() {
    let o = bare(&["serve", "--log", "verbose"]);
    assert_eq!(o.code, 1, "老解析器的取值错误是运行期报错形态；stderr: {}", o.stderr);
    assert!(
        o.stderr.contains("--log 需为 error|warn|info|debug"),
        "报错应是老解析器那句，实际: {}",
        o.stderr
    );
}

/// **serve 的日志走 stderr**（0.10.0 起与子命令合并为一条通道）：`2>` 一处收齐服务日志与
/// `[init]`/`[fatal]`——此前 `[fatal]` 走 stderr、普通日志走 stdout，`2>` 采集拿不到启动
/// 上下文。**这是破坏性变更**：`qqflow-server > 日志文件` 类部署会拿到空 stdout，需改用
/// `2>`（迁移说明在 CHANGELOG 0.10.0 与 quickstart「起服务」）。**子命令**的日志本来就走
/// stderr（stdout 是命令输出，判据在
/// `tests/cli_e2e.rs::log_json_format_emits_parseable_lines`）。两入口的去向理由写在
/// `src/logging.rs` 模块头。
///
/// 判据刻意取「`[init] 服务启动` 行在 stderr、且**这一行**不在 stdout」：它在 listener 绑定
/// 成功后无条件打印，钉的正是「启动日志去了哪」。不写成宽断言「stdout 不含任何行」：
/// sqlcipher 的 C 层错误行本来就进 stderr，而 stdout 现在应保持安静——断言只钉启动行不在
/// stdout，不把未来合法的 stdout 命令输出一并禁掉。本测试起一个**真实 serve**：与任何真实
/// 启动一样，它可能在本机凭据库首次生成 API token
/// （无头 Linux runner 上则走「凭据库不可用 ⇒ 会话级 token」的既有降级分支）——两种形态下
/// 启动行都会打印，断言与凭据库状态无关。
/// 有 15s 死线：serve 日志若退回 stdout，这里红而不是挂死。
#[test]
fn serve_logs_go_to_stderr() {
    /// 杀子进程并在**任何**退出路径（含 panic 展开）上执行。
    ///
    /// 为什么必须有它：判据测试会 spawn 一个真实 serve 子进程，断言失败（panic）路径不会
    /// 走到函数尾部的 `child.kill()`；泄漏的服务进程继续持有管道写端，整个 harness 跟着
    /// 挂死（变异驱动只能靠外部杀进程才能往下跑）。守卫把「红」变成「快速且干净的红」。
    struct KillOnDrop(std::process::Child);
    impl Drop for KillOnDrop {
        fn drop(&mut self) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }

    let port = 25632; // 本文件独占的固定测试端口（与 cli_e2e 的动态端口不冲突）
    let child = Command::new(env!("CARGO_BIN_EXE_qqflow-server"))
        .args(["serve", "--port", &port.to_string(), "--log", "info"])
        .env_remove("QQFLOW_BASE_URL")
        .env_remove("QQFLOW_TOKEN")
        .env_remove("QQFLOW_EMBED_CONFIG")
        .env_remove("RUST_LOG")
        .stdout(std::process::Stdio::piped())
        .stderr(std::process::Stdio::piped())
        .spawn()
        .expect("spawn serve");
    let mut guard = KillOnDrop(child);
    let stdout = guard.0.stdout.take().expect("piped stdout");
    let stderr = guard.0.stderr.take().expect("piped stderr");

    // 后台线程收集 stdout 行（管道满会阻塞子进程，必须持续读）。
    let (tx, rx) = std::sync::mpsc::channel::<String>();
    std::thread::spawn(move || {
        use std::io::BufRead;
        for line in std::io::BufReader::new(stdout).lines() {
            let Ok(line) = line else { break };
            let _ = tx.send(line);
        }
    });
    // stderr 也持续读掉（不读满管道会挂起子进程），留样本供断言。
    let (etx, erx) = std::sync::mpsc::channel::<String>();
    let ereader = std::thread::spawn(move || {
        use std::io::BufRead;
        let mut collected = Vec::new();
        for line in std::io::BufReader::new(stderr).lines() {
            let Ok(line) = line else { break };
            let _ = etx.send(line.clone());
            collected.push(line);
        }
        collected
    });

    let start = std::time::Instant::now();
    let deadline = std::time::Duration::from_secs(15);
    let mut stderr_lines: Vec<String> = Vec::new();
    let init_line = loop {
        match erx.recv_timeout(std::time::Duration::from_millis(200)) {
            Ok(line) => {
                stderr_lines.push(line.clone());
                if line.contains("[init] 服务启动") {
                    break line;
                }
            }
            Err(std::sync::mpsc::RecvTimeoutError::Timeout) => {}
            // stderr 管道关闭 ⇒ serve 已经退出（多半是端口被占或启动即失败）。
            // 这里必须报出真实成因：往下走会拿空串去断言「启动行要报监听地址」，
            // 那条文案会把「没起来」误报成「日志去错了通道」。
            Err(std::sync::mpsc::RecvTimeoutError::Disconnected) => panic!(
                "serve 的 stderr 提前关闭：进程没跑起来（端口 {port} 被占或启动失败？）；已收 stderr: {stderr_lines:?}"
            ),
        }
        if start.elapsed() > deadline {
            panic!(
                "15s 内 stderr 没出现 [init] 服务启动 行（serve 日志退回 stdout 就是这种红）；已收 stderr: {stderr_lines:?}"
            );
        }
    };
    assert!(init_line.contains(&format!("127.0.0.1:{port}")), "启动行要报监听地址: {init_line}");

    // stdout 里不得有那行「服务启动」（`> 日志文件` 部署拿到空 stdout 正是迁移要点）。
    // 多给 1s 宽限收在途行。
    let mut stdout_lines: Vec<String> = Vec::new();
    let grace = std::time::Instant::now();
    while grace.elapsed() < std::time::Duration::from_secs(1) {
        if let Ok(l) = rx.recv_timeout(std::time::Duration::from_millis(100)) {
            stdout_lines.push(l);
        }
    }
    assert!(
        stdout_lines.iter().all(|l| !l.contains("[init] 服务启动")),
        "serve 的启动日志不得出现在 stdout（0.10.0 起走 stderr）: {stdout_lines:?}"
    );

    // 走到这里就是绿：guard 的 Drop 负责杀进程与回收；reader 线程因管道关闭自然结束。
    drop(guard);
    let _ = ereader.join();
}
