# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Headless HTTP API + SSE service that reads **local QQ NT chat records** (`nt_msg.db`, SQLCipher-encrypted, 1024-byte custom header, WAL mode). This is an **independent project**; other projects (WeFlow HTTP API, QQBackup tools, etc.) are used only as functional references — e.g. the HTTP interface follows the WeFlow HTTP API contract (paths, params, response envelopes, SSE field names). Docs and 调研 notes are in Chinese; code comments are bilingual.

**Deliberately out of scope:** key extraction (keys come from external tools like `QQBackup/qq-win-db-key`), media export, SNS endpoints.

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

Run against real data — **config-only, no CLI arguments**; configuration comes exclusively from `./qqflow-server.json` in the working directory (a missing file falls back to defaults):

```powershell
.\qqflow-server.exe
```

Config fields (snake_case, all optional, serde `deny_unknown_fields` — unknown fields or type errors are fatal): `port` / `host` / `token` / `keys` / `keys_file` / `ask_key` / `qq` / `watch_debounce_ms` / `watch_fallback_ms` / `data_dir` / `db_path` / `log`. See the README for an example.

- `db_path` overrides database discovery: a Tencent Files-style root directory (`<dir>/<qq>/nt_qq/nt_db/nt_msg.db`) or a direct `nt_msg.db` file (account name taken from the nearest all-digit ancestor dir, fallback `custom`).
- Keys (16 printable-ASCII bytes per account): `"keys"` object (`{"<qq>": "<key>"}`), optional `"keys_file"` external file (overrides `keys` for the same qq), or `"ask_key": true` interactive stdin. Invalid entries are skipped with a warning; persisted to `<data-dir>/keys.json` (write-only — not auto-loaded).

Default `127.0.0.1:5031` (same port as WeFlow). API token is auto-generated (32B hex) and persisted to `<data-dir>/token.txt`, printed on first start. Data dir: `%LOCALAPPDATA%\qqflow-server` on Windows, `~/.local/share/qqflow-server` on Linux, `~/Library/Application Support/qqflow-server` on macOS.

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
  → store::index::build_index / append_new
                         full-table scan → in-memory Store; incremental rowid
                         appends afterwards
  → server (axum) + sync engine (watch task + tokio broadcast)
```

### The in-memory index (core design decision)

`nt_msg.db` message columns have no useful indexes — SQL filtering means a 30–60 s full table scan per query on a real (~190 MB) database. So the server scans both tables **once at startup** into a `HashMap`-based `Store` and keeps it incrementally updated: the sync engine appends rows with `rowid > watermark` and the same `Store` is the single source of truth for both HTTP queries and SSE events. All query logic (`store::query`) works against this in-memory structure, never SQL.

- Table shapes (numeric column names are QQ-version-dependent, treat as fragile):
  - `group_msg_table`: `"40021"` group id, `"40001"` seq, `"40020"` sender uid, `"40093"` nickname, `"40800"` message blob
  - `c2c_msg_table`: `"40020"` peer uid, `"40001"` seq, `"40093"` nickname, `"40800"` blob
- Message timestamp is packed in the high 32 bits of seq: `seq_to_time(seq) = seq >> 16` (`parser::types`).
- Conversation map key: `g:<groupId>` / `c:<peerUid>` (`store::conv_key`). `classify_talker` disambiguates: all-digit → group, `u_`-prefixed → c2c.
- Conversations have a `dirty` flag; append-only changes trigger a lazy re-sort by `(ts, rowid)` on next query (`Conversation::ensure_sorted`).

### Poller / real-time path

File-system-event-driven (WeFlow-style): one watch task per account (`sync::watch::spawn`, tokio) watches the source `nt_db` directory via notify — ReadDirectoryChangesW on Windows, inotify on Linux, FSEvents on macOS — filters to `nt_msg.db`/`-wal`/`-shm`, debounces bursts (`watch_debounce_ms`, default 350 ms), then runs the full sync (`AccountSync::poll_once`). A slow fallback poll (`watch_fallback_ms`, default 30 s) re-checks `Mirror::changed()` — two metadata stats, no IO — as insurance against silently dropped watch events, and re-attaches a dead watcher (directory deleted/recreated). The full sync:
1. `Mirror::sync()` — re-copies the source WAL (cheap; if the source main file's size/mtime changed, SQLite checkpointed and the whole mirror is rebuilt).
2. Reopens the decrypted connection, then `index::append_new` per table for `rowid > watermark`.
3. Emits `message.new` / `message.revoke` events on a tokio broadcast channel (capacity 1024); recall messages are detected by the parser (`MsgType::Recall`).

Idle periods cost only the stat; a failed sync sets a retry flag so the next tick retries even with unchanged stats.

**Manual sync**: the same per-account `AccountSync` (mirror behind `Arc<Mutex>`) is shared with `GET|POST /api/v1/sync`, which runs `SyncEngine::sync_all()` on demand and returns the newly appended records (newest first) — for client init / manual refresh. Concurrent poll/sync passes serialize on the mirror mutex and the store write lock.

SSE clients (`GET/POST /api/v1/push/messages`) get a `sync` event on connect carrying current rowid watermarks (a qqflow-server extension), then live events; broadcast lag re-syncs the client with a fresh `sync`. KeepAlive ping every 15 s.

### Startup sequence (`server::run_with`)

Parse args → resolve data dir + token → `db::scan::scan_accounts` (platform-gated path discovery) → load keys (`KeyStore`, validated + persisted to `<data-dir>/keys.json`) → bind listener **early** so `/health` reports "starting" during index build → per-account `spawn_blocking` index build (CPU-bound decrypt + full scan) → start watch tasks → set ready flag → wait for Ctrl-C → signal shutdown watch, abort watch tasks, delete mirror dir.

### Heuristic message parser (`parser`)

Message BLOBs are protobuf-ish with no stable schema, so text extraction is heuristic by design (inherited from QQFlow behavior): scan for runs of common Han characters (U+4E00–U+9FA5, which avoids protobuf varint codepoint garbage), ≥ 2 chars with > 60% common ratio, plus an ASCII fallback; media recognized by byte signatures (`.jpg/.png/.gif/gchatpic`, `.amr/.silk/.ptt`, `shortvideo/.mp4`); recall/system by characteristic phrases ("你猜猜撤回了什么", "拍了拍", "撤回了一条", "修改群名"). An iteration budget (`n*50`) bounds worst-case cost. This is intentionally tolerant of QQ version churn — expect degraded output, not crashes.

### Concurrency

- `parking_lot::RwLock<Store>` shared via `Arc` — single lock for the whole store (sync engine writes, handlers read).
- notify watcher threads bridge into the tokio watch task via an unbounded channel (`sync::watch`); watch/fallback/manual sync passes serialize on the mirror mutex and the store write lock.
- tokio `broadcast` for SSE events; `watch` channel for shutdown; CPU-bound decrypt/scan work in `spawn_blocking`.
- `AppState` (in `store`) holds: store, broadcast sender, per-account readiness (`server::AccountState`, "indexing"/"ready"/"error"), a global `ready` AtomicBool, and the token.
- Auth: Bearer header / `?access_token=` (recommended for SSE) / POST JSON body, constant-time comparison (`config::constant_time_eq`).

## Known issues

- **c2c (private-chat) messages were silently dropped** — FIXED: `store/index.rs` now uses per-table column mapping (group 6 cols / c2c 5 cols, peer = sender). Guarded by the `fake_db_indexes_c2c_rows` regression test in `tests/real_db_groundtruth.rs`.

## Version-fragility notes

- Numeric column names (`"40021"`, `"40800"`, …), table layouts, and the uid→QQ mapping table all vary with QQ versions; code degrades gracefully (best-effort queries, heuristic parsing).
- `store::mapping::load_uid_map` is currently **dead code** — defined for future UID→QQ-number resolution but never called.
- `MessageOut::is_send` is hardcoded `0` — v1 limitation: message direction is not reliably derivable from the available columns.

## Tests

- `tests/sqlcipher_roundtrip.rs` — self-built SQLCipher test database with QQ's exact PRAGMA parameters + fake 1024-byte header + WAL. Proves: decryption round-trip, WAL-only writes visible through the mirror (real-time polling path), checkpoint-triggered mirror rebuild, wrong-key failure. **Never touches real QQ data.**
- `tests/api_smoke.rs` — HTTP layer contract tests via `tower::ServiceExt::oneshot` (no network, no real DB); builds a fake `AppState` with seeded conversations.
- Unit tests live inline in modules (`parser`, `keystore`, `decrypt`, `mirror`).
- Note for `sqlcipher_roundtrip`: the mirror's reader connection must be dropped before the mirror is rebuilt underneath it.

## External references

- WeFlow API contract: `weflow-api.md` in the local `campus-info-hub-py` project (`src/sources/weflow/`)
- Research notes: local Claude plan document `https-github-com-yfgug-qqflow-1-github-concurrent-lovelace.md` (plans directory)
