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
async fn credential_guard() -> tokio::sync::MutexGuard<'static, ()> {
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

fn test_cfg(dir: &std::path::Path, port: u16, token: String) -> Config {
    Config {
        host: "127.0.0.1".into(),
        port,
        log: "info".into(),
        watch_debounce_ms: 20,
        watch_fallback_ms: 0,
        media_export_dir: Some(dir.join("media")),
        base_url: None,
        show_token: false,
        token: Some(token),
    }
}

fn tmp_dir(tag: &str) -> std::path::PathBuf {
    let dir = std::env::temp_dir().join(format!("qqflow-shutdown-{tag}-{}", std::process::id()));
    let _ = std::fs::remove_dir_all(&dir);
    std::fs::create_dir_all(&dir).unwrap();
    dir
}

/// Mint the token this test run hands to its server and probe with. The
/// server takes it through `Config::token` (no OS credential store involved),
/// so the readiness predicate no longer depends on the store being readable.
/// Why that matters: the store is an environment dependency, not a logic one —
/// on a headless CI runner the keyring daemon can be absent or half-started,
/// and the failure mode was exactly the nasty one: the server fell back to a
/// session token while the probe kept reading "no token" and the wait timed
/// out, failing an unrelated shutdown assertion (regression:
/// shutdown_ends_a_live_sse_stream_within_the_grace_period on CI). Both tests
/// still run with distinct tokens; the credential guard keeps the two in-flight
/// servers from sharing any state.
fn mint_token() -> String {
    let mut bytes = [0u8; 32];
    rand::RngCore::fill_bytes(&mut rand::thread_rng(), &mut bytes);
    bytes.iter().map(|b| format!("{b:02x}")).collect()
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

/// A server that has passed all three readiness stages.
struct ServerProbe {
    token: String,
}

/// Which stage the predicate last got stuck on. The three have different
/// remedies, and collapsing them into one "server up and token readable"
/// expect (what this file used to do) is exactly what made the flake
/// undiagnosable: a 401 from a credential-store race, an unreadable keyring
/// entry and a port that never bound all produced the same panic line.
enum UpFailure {
    Port {
        port: u16,
        waited: Duration,
    },
    Auth {
        port: u16,
        status: u16,
        body: String,
        waited: Duration,
    },
}

impl UpFailure {
    fn remedy(&self) -> &'static str {
        match self {
            Self::Port { .. } => {
                "nothing is listening on that port: check the server task did not exit early, and that another process is not holding the port"
            }

            Self::Auth { .. } => {
                "a token was read but the server rejected it: the stored token and the running server's token disagree (a credential-store write race between parallel tests); delete the stored entry and re-run"
            }
        }
    }
}

impl std::fmt::Display for UpFailure {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let (stage, detail) = match self {
            Self::Port { port, waited } => (
                "port",
                format!("no TCP connection on 127.0.0.1:{port} within {waited:?}"),
            ),

            Self::Auth { port, status, body, waited } => (
                "auth",
                format!("127.0.0.1:{port} answered {status} to an authenticated request within {waited:?} ({body})"),
            ),
        };
        write!(
            f,
            "server never became usable — stuck at stage '{stage}': {detail}\n  remedy: {}",
            self.remedy()
        )
    }
}

/// Wait until the server is *usable*, in two stages:
///
/// 1. the port accepts a TCP connection;
/// 2. the token this test handed to the server authenticates a real request.
///
/// The old stage 2 read the token back through the OS credential store. That
/// made readiness depend on the store being readable — an environment
/// property the test neither controls nor cares about (the server may even be
/// legitimately running on a session-token fallback). The token now travels
/// through `Config::token`, so the authenticated round trip below is against
/// exactly the value the server holds.
async fn wait_until_up(port: u16, token: &str) -> Result<ServerProbe, UpFailure> {
    let started = std::time::Instant::now();
    let deadline = started + Duration::from_secs(10);
    loop {
        let mut stuck = UpFailure::Port {
            port,
            waited: started.elapsed(),
        };
        if tokio::net::TcpStream::connect(("127.0.0.1", port)).await.is_ok() {
            match probe_auth(port, token).await {
                Ok(()) => return Ok(ServerProbe { token: token.to_string() }),
                Err((status, body)) => {
                    stuck = UpFailure::Auth {
                        port,
                        status,
                        body,
                        waited: started.elapsed(),
                    }
                }
            }
        }
        if std::time::Instant::now() >= deadline {
            return Err(stuck);
        }
        tokio::time::sleep(Duration::from_millis(50)).await;
    }
}

/// One authenticated request over a raw socket (no client library, same
/// approach as the SSE handshake below). `/api/v1/accounts` is the probe:
/// it is token-protected and answers 200 with an empty list when no account
/// is registered, so it tests the credential path without depending on an
/// account being bound.
async fn probe_auth(port: u16, token: &str) -> Result<(), (u16, String)> {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let mut sock = tokio::net::TcpStream::connect(("127.0.0.1", port))
        .await
        .map_err(|e| (0, e.to_string()))?;
    // 逐行写、续行符后不留缩进：多出来的前导空格会让请求行/头部非法，
    // 服务端回 400 而不是 401——那会把「凭据不对」误报成「请求写坏了」。
    let req = format!(
        "GET /api/v1/accounts HTTP/1.1\r\nHost: 127.0.0.1:{port}\r\nAuthorization: Bearer {token}\r\nConnection: close\r\n\r\n"
    );
    sock.write_all(req.as_bytes())
        .await
        .map_err(|e| (0, e.to_string()))?;
    sock.flush().await.map_err(|e| (0, e.to_string()))?;
    let mut buf = Vec::new();
    let _ = tokio::time::timeout(Duration::from_secs(5), sock.read_to_end(&mut buf)).await;
    let text = String::from_utf8_lossy(&buf);
    let status = text
        .split_whitespace()
        .nth(1)
        .and_then(|s| s.parse::<u16>().ok())
        .unwrap_or(0);
    if (200..300).contains(&status) {
        Ok(())
    } else {
        Err((status, text.lines().next().unwrap_or("").to_string()))
    }
}

/// The signal must actually stop the server, and it must do so well inside the
/// grace period when nothing is holding a connection open.
#[tokio::test(flavor = "multi_thread")]
async fn shutdown_signal_stops_the_server() {
    clear_keyring_token();
    // Hold the guard across the server spawn + `wait_until_up` so the other
    // graceful-shutdown test cannot observe an intermediate keyring state
    // while our server is still calling `set_password`. See `credential_guard`.
    let _guard = credential_guard().await;
    let dir = tmp_dir("basic");
    let port = free_port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // The token travels through `Config::token`: the server never touches the
    // OS credential store, and the probe authenticates with the same value.
    let token = mint_token();
    let cfg = test_cfg(&dir, port, token.clone());
    let server = tokio::spawn(async move {
        qqflow_server::server::run_with_shutdown(cfg, async move {
            let _ = rx.await;
        })
        .await
    });

    // Wait until the server is serving authenticated traffic — and so a failure
    // reports which stage stalled (port / auth) instead of one generic
    // "never came up" line.
    let probe = wait_until_up(port, &token).await.unwrap_or_else(|e| panic!("{e}"));
    assert!(!probe.token.is_empty(), "probe token must not be empty");
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
    // while our server is still calling `set_password`. See `credential_guard`.
    let _guard = credential_guard().await;
    let dir = tmp_dir("sse");
    let port = free_port();
    let (tx, rx) = tokio::sync::oneshot::channel::<()>();

    // The token travels through `Config::token`, and the predicate hands it
    // back only after it has authenticated a real request, so the handshake
    // below cannot use a token the server will reject.
    let token = mint_token();
    let cfg = test_cfg(&dir, port, token.clone());
    let server = tokio::spawn(async move {
        qqflow_server::server::run_with_shutdown(cfg, async move {
            let _ = rx.await;
        })
        .await
    });

    let token = wait_until_up(port, &token)
        .await
        .unwrap_or_else(|e| panic!("{e}"))
        .token;
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
