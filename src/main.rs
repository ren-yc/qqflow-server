use qqflow_server::run;

fn main() {
    let rt = tokio::runtime::Runtime::new().expect("create tokio runtime");
    if let Err(e) = rt.block_on(run()) {
        eprintln!("[fatal] {e:#}");
        std::process::exit(1);
    }
}
