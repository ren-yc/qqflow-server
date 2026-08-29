//! qqflow-server: headless HTTP API + SSE service for reading local QQ NT
//! chat records (SQLCipher-decrypted nt_msg.db).
//!
//! Interface follows the WeFlow HTTP API contract (see weflow-api.md):
//! same paths, parameters, response envelopes and SSE event fields.
//!
//! Scope: decryption layer + data reading + service encapsulation only.
//! Key extraction is intentionally NOT implemented — keys come from external
//! tools (e.g. QQBackup/qq-win-db-key) via CLI / keys file / interactive input.

pub mod config;
pub mod db;
pub mod keystore;
pub mod logging;
pub mod parser;
pub mod pathsafe;
pub mod sync;
pub mod server;
pub mod store;

/// Run the server to completion (used by main.rs and integration tests).
pub async fn run() -> anyhow::Result<()> {
    server::serve().await
}
