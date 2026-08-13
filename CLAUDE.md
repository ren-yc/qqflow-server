# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Headless HTTP API + SSE service that reads **local QQ NT chat records** (`nt_msg.db`, SQLCipher-encrypted, 1024-byte custom header, WAL mode). This is an **independent project**; other projects (WeFlow HTTP API, QQBackup tools, etc.) are used only as functional references — e.g. the HTTP interface follows the WeFlow HTTP API contract (paths, params, response envelopes, SSE field names). Docs and 调研 notes are in Chinese; code comments are bilingual.

**Deliberately out of scope:** key extraction (keys come from external tools like `QQBackup/qq-win-db-key` and are supplied at runtime by clients via `POST /api/v1/accounts`), media export, SNS endpoints.

## Commands

All cargo invocations on Windows must go through the wrapper — the vendored OpenSSL build (openssl-src, via `rusqlite`'s `bundled-sqlcipher-vendored-openssl`) calls cl/link directly and needs the vcvars environment plus native Windows Perl. The wrapper locates `vcvars64.bat` via vswhere (`QQFLOW_VCVARS` env var overrides), prepends Strawberry Perl/nasm to PATH, and **rejects Git's MSYS perl** (it mangles paths in openssl Configure). Linux/macOS use `scripts/build.sh` (system perl + gcc/clang, no vcvars). Both pass through cargo args:

```powershell
powershell -File scripts\build.ps1 build          # debug build (Windows)
powershell -File scripts\build.ps1 test           # all tests (integration + unit)
powershell -File scripts\build.ps1 test roundtrip # single test by name filter
powershell -File scripts\build.ps1 run
powershell -File scripts\build.ps1 clippy
```

```bash
bash scripts/build.sh test                        # same on Linux/macOS
```

The toolchain is pinned by `rust-toolchain.toml` (rustc 1.97.1, version-locked for reproducible clippy lints); rustup auto-installs it on first use.

Run against real data — **no config file**; run parameters come from CLI flags, all optional with the defaults shown:

```powershell
.\qqflow-server.exe                                  # defaults: 127.0.0.1:5031, log info
.\qqflow-server.exe --port 5999 --host 0.0.0.0 --log debug
.\qqflow-server.exe --help
```

CLI flags: `--port` (5031) / `--host` (127.0.0.1) / `--log` (error|warn|info|debug, default info) / `--watch-debounce-ms` (350) / `--watch-fallback-ms` (30000, 0 disables the slow fallback poll; the 10 s watcher re-attach is independent). Unknown flags are fatal.

Accounts are **client-driven**: startup only scans platform paths for discovery and lists the accounts as `awaiting_key` in `/health` (zero-account startup is valid). A downstream client registers an account via `POST /api/v1/accounts` with `{qq, key, db_path}`:

- `db_path` (optional) is a direct `nt_msg.db` file or a Tencent Files-style root (`<dir>/<qq>/nt_qq/nt_db/nt_msg.db`); omitting it reuses the scanned path.
- Keys are validated (16 printable-ASCII bytes) and kept **in memory only** — never persisted. A wrong key puts the account in `error` state (recoverable by re-registering); the process never exits over key problems.

Default `127.0.0.1:5031` (same port as WeFlow). API token is auto-generated (32B hex) and persisted to `<data-dir>/token.txt`; the startup log prints the file path, not the token value. Data dir: `%LOCALAPPDATA%\qqflow-server` on Windows, `~/.local/share/qqflow-server` on Linux, `~/Library/Application Support/qqflow-server` on macOS.

## Architecture

### Data pipeline

```
nt_msg.db (QQ, SQLCipher + 1024B header + WAL)
  → db::mirror::Mirror   copies per-account into <data-dir>/mirror/<qq>/
                         (header stripped from main.db; WAL copied verbatim)
  → db::decrypt::open_decrypted
                         SQLCipher PRAGMA suite (page_size=4096, kdf_iter=4000,
                         HMAC-SHA1, PBKDF2-SHA512, aes-256-cbc); retries with
                         HMAC-SHA512; verifies via sqlite_master
  → store::index::build_index / read_new + apply_records
                         full-table scan → in-memory Store; incremental rowid
                         reads + apply afterwards (two-phase, see below)
  → server (axum) + sync engine (watch task + tokio broadcast)
```

### The in-memory index (core design decision)

`nt_msg.db` message columns have no useful indexes — SQL filtering means a 30–60 s full table scan per query on a real (~190 MB) database. So the server scans both tables **once per account registration** into a `HashMap`-based `Store` and keeps it incrementally updated: the sync engine appends rows with `rowid > watermark` and the same `Store` is the single source of truth for both HTTP queries and SSE events. All query logic (`store::query`) works against this in-memory structure, never SQL.

- Table shapes (numeric column names are QQ-version-dependent, treat as fragile):
  - `group_msg_table`: `"40021"` group id, `"40001"` seq, `"40020"` sender uid, `"40093"` nickname, `"40800"` message blob
  - `c2c_msg_table`: `"40020"` peer uid, `"40001"` seq, `"40093"` nickname, `"40800"` blob
- Message timestamp is packed in the high 32 bits of seq: `seq_to_time(seq) = seq >> 32` (`parser::types`).
- Conversation map key: `g:<groupId>` / `c:<peerUid>` (`store::conv_key`). `classify_talker` disambiguates: all-digit → group, `u_`-prefixed → c2c.
- Conversations carry a `dirty` flag set by appends; `build_index` sorts each conversation once via `Conversation::ensure_sorted`, while query paths (`query_messages`, chatlab pull) sort their own index snapshots by `(ts, rowid)` per query.

### Poller / real-time path

File-system-event-driven (WeFlow-style): one watch task per account (`sync::watch::spawn`, tokio) watches the source `nt_db` directory via notify — ReadDirectoryChangesW on Windows, inotify on Linux, FSEvents on macOS — filters to `nt_msg.db`/`-wal`/`-shm`, debounces bursts (`--watch-debounce-ms`, default 350 ms), then runs the full sync (`AccountSync::poll_once`). A slow fallback poll (`--watch-fallback-ms`, default 30 s) re-checks `Mirror::changed()` — two metadata stats, no IO — as insurance against silently dropped watch events, and re-attaches a dead watcher (directory deleted/recreated). The full sync:
1. `Mirror::sync()` — re-copies the source WAL (cheap; if the source main file's size/mtime changed, SQLite checkpointed and the whole mirror is rebuilt).
2. Reopens the decrypted connection, then reads per table with `index::read_new` (`rowid > watermark`, pure read — a failure in either table leaves the store untouched).
3. Applies both tables under a single store write-lock (`index::apply_records` + watermark write-back) and emits `message.new` / `message.revoke` events on a tokio broadcast channel (capacity 1024); recall messages are detected by the parser (`MsgType::Recall`).

Idle periods cost only the stat; a failed sync sets a retry flag so the next tick retries even with unchanged stats.

**Manual sync**: the same per-account `AccountSync` (mirror behind `Arc<Mutex>`) is shared with `GET|POST /api/v1/sync`, which runs `SyncEngine::sync_all()` on demand and returns the newly appended records (newest first) — for client init / manual refresh. Concurrent poll/sync passes serialize on the mirror mutex and the store write lock.

SSE clients (`GET/POST /api/v1/push/messages`) get a `sync` event on connect carrying current rowid watermarks (a qqflow-server extension), then live events; a fresh `sync` is also broadcast when an account's index build completes (clients connected during indexing start with a `(0,0)` baseline and are re-baselined by it), and broadcast lag re-syncs the client the same way. KeepAlive ping every 15 s. SSE has no ready gate — it serves 200 even while indexing.

### Startup sequence (`server::run_with`)

Parse CLI args → resolve data dir + token → `db::scan::scan_accounts` (platform-gated path discovery) → bind listener **early** so `/health` reports "starting" → list scanned accounts as `awaiting_key` (no build at startup) → wait for client registrations (`POST /api/v1/accounts`) → per account `server::init_account` (`spawn_blocking` mirror + decrypt + index; `install_index` broadcasts the SSE baseline; `AccountSync` registration + watch task) → recompute the global ready flag → wait for Ctrl-C → signal shutdown watch, remove mirror dir.

### Heuristic message parser (`parser`)

Message BLOBs are protobuf-ish with no stable schema, so text extraction is heuristic by design (inherited from QQFlow behavior): scan for runs of common Han characters (U+4E00–U+9FA5, which avoids protobuf varint codepoint garbage), ≥ 2 chars with > 60% common ratio, plus an ASCII fallback; media recognized by byte signatures (`.jpg/.png/.gif/gchatpic`, `.amr/.silk/.ptt`, `shortvideo/.mp4`); recall/system by characteristic phrases ("你猜猜撤回了什么", "拍了拍", "撤回了一条", "修改群名"). An iteration budget (`n*50`) bounds worst-case cost. This is intentionally tolerant of QQ version churn — expect degraded output, not crashes.

### Concurrency

- `parking_lot::RwLock<Store>` shared via `Arc` — single lock for the whole store (sync engine writes, handlers read).
- notify watcher threads bridge into the tokio watch task via an unbounded channel (`sync::watch`); watch/fallback/manual sync passes serialize on the mirror mutex and the store write lock.
- tokio `broadcast` for SSE events; `watch` channel for shutdown; CPU-bound decrypt/scan work in `spawn_blocking`.
- `AppState` (in `store`) holds: store, broadcast sender, per-account readiness (`server::AccountState`, `awaiting_key`/`indexing`/`ready`/`error`), a global `ready` AtomicBool, the token, and the `AccountRegistry` (scanned/registered `DbInfo`s, in-memory `KeyStore`, mirror root, watch config, shutdown receiver).
- Auth: Bearer header / `?access_token=` (recommended for SSE) / POST JSON body, constant-time comparison (`config::constant_time_eq`). `/health` and `POST /api/v1/accounts` are the only non-readiness-gated endpoints (accounts is the bootstrap path).

## Known issues

- **c2c (private-chat) messages were silently dropped** — FIXED: `store/index.rs` now uses per-table column mapping (group 6 cols / c2c 5 cols, peer = sender). Guarded by the `fake_db_indexes_c2c_rows` regression test in `tests/real_db_groundtruth.rs`.

## Version-fragility notes

- Numeric column names (`"40021"`, `"40800"`, …), table layouts, and the uid→QQ mapping table all vary with QQ versions; code degrades gracefully (best-effort queries, heuristic parsing).
- `store::mapping::load_uid_map` is currently **dead code** — defined for future UID→QQ-number resolution but never called.
- `MessageOut::is_send` is hardcoded `0` — v1 limitation: message direction is not reliably derivable from the available columns.

## Tests

- `tests/sqlcipher_roundtrip.rs` — self-built SQLCipher test database with QQ's exact PRAGMA parameters + fake 1024-byte header + WAL. Proves: decryption round-trip, WAL-only writes visible through the mirror (real-time polling path), checkpoint-triggered mirror rebuild, wrong-key failure. **Never touches real QQ data.**
- `tests/api_smoke.rs` — HTTP layer contract tests via `tower::ServiceExt::oneshot` (no network, no real DB); builds a fake `AppState` with seeded conversations.
- `tests/fs_watch_e2e.rs` — file-system event → sync → SSE broadcast e2e (fake DB).
- `tests/real_db_groundtruth.rs` — fake-DB regression tests + client-registration e2e (wrong key → `error` → corrected key → `ready`); the `real_db_groundtruth` probe (`#[ignore]`) runs ground-truth queries against a real QQ DB via `QQFLOW_TEST_DB_ROOT` / `QQFLOW_TEST_DB_KEY`.
- `tests/downstream_client.rs` — downstream-client GET/POST simulation against a real QQ DB, including client-driven registration (`#[ignore]`; inputs resolve from the gitignored repo-root `qqflow-server.json` (`qq`/`key`/`db_path`) first, then `QQFLOW_TEST_QQ` / `QQFLOW_TEST_DB_KEY` / `QQFLOW_TEST_DB_ROOT` env vars).
- Unit tests live inline in modules (`parser`, `keystore`, `decrypt`, `mirror`).
- Note for `sqlcipher_roundtrip`: the mirror's reader connection must be dropped before the mirror is rebuilt underneath it.

## External references

- WeFlow API contract: `weflow-api.md` in the local `campus-info-hub-py` project (`src/sources/weflow/`)
- Research notes: local Claude plan document `https-github-com-yfgug-qqflow-1-github-concurrent-lovelace.md` (plans directory)
