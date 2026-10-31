# qqflow-client

Typed Rust client for [qqflow-server](https://github.com/ren-yc/qqflow-server), a read-only HTTP/SSE service over local QQ NT databases.

## Install

```
cargo add qqflow-client
```

## Minimal example

```rust
use std::time::Duration;
use qqflow_client::client::{Client, ClientError};

#[tokio::main]
async fn main() -> Result<(), ClientError> {
    let client = Client::new("http://127.0.0.1:5032", "<token>");
    client.wait_ready("<qq>", Duration::from_secs(30)).await?;
    let sessions = client.list_all_sessions(None, None).await?;
    println!("{} sessions", sessions.len());
    Ok(())
}
```

Requires `tokio` with `rt-multi-thread` and `macros`; the SDK uses tokio for timeouts and retries.

## Generated and handwritten layers

Types and request plumbing are generated; the behavior layer is handwritten. Never edit them by hand — run `cargo run -p qqflow-regen --` instead.

## Scope

The server is strictly read-only: accounts, sessions, messages, contacts, group members, media export and an SSE push stream.

This client mirrors that surface one-to-one.

## Links

- [Server repository](https://github.com/ren-yc/qqflow-server)
- [API reference](https://github.com/ren-yc/qqflow-server/blob/master/docs/qqflow-server-api.md)
- [CHANGELOG](https://github.com/ren-yc/qqflow-server/blob/master/CHANGELOG.md)
- [License](https://github.com/ren-yc/qqflow-server/blob/master/LICENSE) — MIT
