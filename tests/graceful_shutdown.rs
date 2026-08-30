//! Graceful shutdown. Before this existed, `run_with` spawned the server in a
//! detached task and returned as soon as Ctrl+C arrived: the process exited
//! while requests were still in flight, so responses were truncated and SSE
//! clients saw a dropped socket instead of a clean end of stream.
//!
//! A real `CTRL_C_EVENT` cannot be delivered to another process from a test on
//! Windows, so these drive `run_with_shutdown` with a channel instead. What
//! that still covers is the whole composition: signal -> log -> `shutdown`
//! broadcast -> axum drain -> bounded grace period -> return.

use std::time::Duration;

use qqflow_server::config::Config;

/// Cross-test serialization for the (qqflow-server, http-api-token) keyring
/// entry. The two graceful-shutdown tests both start a real server in-process
/// and therefore both call `load_or_create_token()`, which writes through
/// `keyring`. On Windows the credential store is not atomic across concurrent
/// `set_password` calls — a second concurrent writer can land on the keyring
/// with a token that the first server's `state.token` does not match, and the
/// second test then reads the wrong value out of keyring and the SSE handshake
/// answers 401. The earlier `clear_keyring_token()` helper only fixed the
/// stale-token-leak case (one race); this mutex fixes the remaining
/// two-writers-race case (the other race).
///
/// The guard is held across `spawn` + `wait_until_up` — by the time the next
/// test acquires it, the previous test's server has finished
/// `load_or_create_token()` and bound the listening port, so the keyring value
/// is stable. `tokio::sync::Mutex` rather than `std::sync::Mutex` so the guard
/// can be held across `.await`.
async fn keyring_guard() -> tokio::sync::MutexGuard<'static, ()> {
    static GUARD: std::sync::OnceLock<tokio::sync::Mutex<()>> = std::sync::OnceLock::new();
    GUARD
        .get_or_init(|| tokio::sync::Mutex::new(()))
        .lock()
        .await
}

/// Reserve a free port by binding and immediately releasing it. The window
/// between release and re-bind is a race in principle, but on a loopback test
/// port it is far more reliable than hardcoding a number that may be in use.
fn free_port() -> u16 {
    let l = std::net::TcpListener::bind("127.0.0.1:0").expect("bind probe");
    let port = l.local_addr().unwrap().port();
    drop(l);
    port
}

fn test_cfg(dir: &std::path::Path, port: u16) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port,
        log: "info".into(),
        watch_debounce_ms: 20,
        watch_fallback_ms: 0,
        media_export_dir: Some(dir.join("media")),
        base_url: None,
        show_token: false,
    }
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("qqflow-shutdown-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Drop any pre-existing API token so `load_or_create_token()` deterministically
/// walks the `NoEntry` -> `set_password` path on the very next call. Without
/// this, a stale token from a previous run (or, on Windows, a credential that
/// the parallel `shutdown_signal_stops_the_server` test left behind) can leak
/// into the second test: the server's `state.token` ends up being a freshly
/// generated in-memory value while `show_token()` reads back the stale one,
/// and the SSE handshake then answers 401. `delete_credential` is best-effort:
/// returning `NoEntry` (or any other error) just means there is nothing to
/// clean up, which is exactly the state we want.
///
/// The service/user strings MUST stay in sync with `TOKEN_SERVICE` / `TOKEN_USER`
/// in `src/config.rs`; if those constants ever change, this helper has to be
/// updated alongside them.
fn clear_keyring_token() {
    let service = "qqflow-server";
    let user = "http-api-token";
    if let Ok(entry) = keyring::Entry::new(service, user) {
        // `NoEntry` is the success case for a clean runner — anything else
        // (e.g. Windows ACL issues, platform failures) is also fine for our
        // purposes: we are not asserting the credential store is writable, we
        // are only trying to make sure whatever was there is gone.
        let _ = entry.delete_credential();
    }
}

/// Wait until the port accepts connections, so shutdown races a live listener
/// rather than an unbound socket.
async fn wait_until_up(port: u16) -> bool {
    for _ in 0..100 {
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            return true;
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
    false
}

/// The signal must actually stop the server, and it must do so well inside the
/// grace period when nothing is holding a connection open.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_signal_stops_the_server() {
    clear_keyring_token();
    // Hold the guard across the server spawn + `wait_until_up` so the other
    // graceful-shutdown test cannot observe an intermediate keyring state
    // while our server is still calling `set_password`. See `keyring_guard`.
    let _guard = keyring_guard().await;
    let dir = tmp_dir("basic");
    let port = free_port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let cfg = test_cfg(&dir, port);
    let server = tokio::spawn(async move {
        qqflow_server::server::run_with_shutdown(cfg, async move {
            let _ = rx.await;
        })
        .await
    });

    assert!(wait_until_up(port).await, "server never came up on port {port}");
    // Dropping the guard here is what serializes the two tests: by the time
    // the next test acquires it, our server has finished `load_or_create_token`
    // and bound the listening port, so the keyring value is stable.
    drop(_guard);

    let started = std::time::Instant::now();
    tx.send(()).expect("shutdown trigger delivered");
    let result = tokio::time::timeout(Duration::from_secs(10), server)
        .await
        .expect("server must stop after the shutdown signal")
        .expect("server task must not panic");
    result.expect("run_with_shutdown returned an error");

    // With no connection held open, axum drains immediately: this must NOT
    // take the full grace period.
    assert!(
        started.elapsed() < Duration::from_secs(3),
        "idle shutdown should be prompt, took {:?}",
        started.elapsed()
    );

    let _ = std::fs::remove_dir_all(&dir);
}

/// An open SSE stream must not hold shutdown hostage. `with_graceful_shutdown`
/// waits for every in-flight connection, and an SSE response never ends on its
/// own — so without both the `shutdown` broadcast (which closes the stream from
/// the handler side) and the bounded grace period, Ctrl+C would hang for as
/// long as a client stayed subscribed.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_ends_a_live_sse_stream_within_the_grace_period() {
    clear_keyring_token();
    // Hold the guard across the server spawn + `wait_until_up` so the other
    // graceful-shutdown test cannot observe an intermediate keyring state
    // while our server is still calling `set_password`. See `keyring_guard`.
    let _guard = keyring_guard().await;
    let dir = tmp_dir("sse");
    let port = free_port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    let cfg = test_cfg(&dir, port);
    let server = tokio::spawn(async move {
        qqflow_server::server::run_with_shutdown(cfg, async move {
            let _ = rx.await;
        })
        .await
    });

    // The token is minted inside run_with_shutdown from the credential store,
    // so read it the same way a client would be told to. By the time
    // `wait_until_up` returns true, the server has finished
    // `load_or_create_token()` and the keyring value is stable, so any
    // `show_token()` call from here on is guaranteed to match
    // `state.token`.
    let mut token = None;
    if wait_until_up(port).await {
        token = qqflow_server::config::show_token().ok().flatten();
    }
    let token = token.expect("server up and token readable");
    // Release the guard: the rest of this test only talks to the server it
    // just spun up, and we want the other test to be free to spawn its own
    // server as soon as it gets scheduled.
    drop(_guard);

    // Hold an SSE stream open with a raw socket: no client library, and the
    // response body is deliberately never drained to completion.
    let mut sse = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .expect("SSE connect");
    {
        use tokio::io::AsyncWriteExt;
        let req = format!(
            "GET /api/v1/push/messages?access_token={token} HTTP/1.1\r\n\
             Host: 127.0.0.1:{port}\r\nAccept: text/event-stream\r\n\r\n"
        );
        sse.write_all(req.as_bytes()).await.expect("send SSE request");
        sse.flush().await.unwrap();
    }
    // Read enough to be sure the stream is established (headers + `ready`).
    {
        use tokio::io::AsyncReadExt;
        let mut buf = [0u8; 1024];
        let n = tokio::time::timeout(Duration::from_secs(5), sse.read(&mut buf))
            .await
            .expect("SSE response arrived")
            .expect("SSE read");
        let head = String::from_utf8_lossy(&buf[..n]);
        assert!(head.contains("200"), "SSE handshake: {head}");
        assert!(head.contains("text/event-stream"), "SSE content-type: {head}");
    }

    let started = std::time::Instant::now();
    tx.send(()).expect("shutdown trigger delivered");
    let result = tokio::time::timeout(Duration::from_secs(15), server)
        .await
        .expect("a live SSE stream must not block shutdown past the timeout")
        .expect("server task must not panic");
    result.expect("run_with_shutdown returned an error");
    let elapsed = started.elapsed();

    // Must be well under SHUTDOWN_GRACE (3s), not merely under some generous
    // ceiling: landing AT the grace period means the stream never closed
    // itself and the timer force-exited instead — which is the bug this test
    // exists to catch.
    assert!(
        elapsed < Duration::from_millis(1500),
        "the shutdown broadcast must close the SSE stream, not the grace timer; \
         took {elapsed:?} (grace period is 3s)"
    );
    println!("[shutdown] live SSE stream released in {elapsed:?}");

    let _ = std::fs::remove_dir_all(&dir);
}
