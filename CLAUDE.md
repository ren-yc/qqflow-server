# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Overview

Headless HTTP API + SSE service that reads **local QQ NT chat records** (`nt_msg.db`, SQLCipher-encrypted, 1024-byte custom header, WAL mode). This is an **independent project**; other projects (WeFlow HTTP API, QQBackup tools, etc.) are used only as functional references — e.g. the HTTP interface follows the WeFlow HTTP API contract (paths, params, response envelopes, SSE field names). Docs and 调研 notes are in Chinese; code comments are bilingual.

**Deliberately out of scope:** key extraction (keys come from external tools like `QQBackup/qq-win-db-key` and are supplied at runtime by clients via `POST /api/v1/accounts`), SNS endpoints (QQ NT has no moments data). Media: the server serves bytes read-only from QQ's own local cache (`GET /api/v1/media/{id}`) AND supports the WeFlow-shaped on-demand export (`media=1` on `/api/v1/messages` copies the page's media into `--media-export-dir`, default `<data-dir>/api-media`, served via `/api/v1/media/{talker}/{mediaType}/{file}`).

## Privacy (隐私检查)

**每次修改源代码或文档后必须检查隐私数据**：项目 hook（`.claude/settings.json` → `scripts/check-privacy.sh`）会在每次文件编辑后自动扫描，也可手动运行 `bash scripts/check-privacy.sh`。检查内容：gitignored 本地配置 `qqflow-server.json` 中的**真实 QQ 号与 SQLCipher 密钥**、本机路径（`D:\AppData`、`C:\Users\*`）与本机用户名是否泄露进仓库文件；任何命中必须修复后才能继续。

**规则：**
- 严禁把 `qqflow-server.json` 的真实值（qq / key / db_path）写入任何仓库文件——它们只存在于内存（`keystore`）与 gitignored 的本地配置
- 代码、测试、文档一律使用虚构数据或占位符（`FAKE_QQ=335663881`、`FAKE_KEY=0123456789abcdef`、`<QQ号>`、`<16字节密钥>`）
- 测试中的文件路径用虚构路径（如 `C:\SomeUser\nt_qq\nt_db\nt_msg.db`），绝不复制本机真实路径
- 真实 token 只存在于 `<data-dir>/系统凭据库（--show-token 获取）`（仓库外），启动日志只打印路径不打印值

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
.\qqflow-server.exe                                  # defaults: 127.0.0.1:5032, log info
.\qqflow-server.exe --port 5999 --host 0.0.0.0 --log debug
.\qqflow-server.exe --help
```

CLI flags: `--port` (5032) / `--host` (127.0.0.1) / `--log` (error|warn|info|debug, default info) / `--watch-debounce-ms` (350) / `--watch-fallback-ms` (30000, 0 disables the slow fallback poll; the 10 s watcher re-attach is independent) / `--media-export-dir` (media export root for `media=1`, default `<data-dir>/api-media`) / `--base-url` (base URL for exported `mediaUrl` links, default `http://<host>:<port>`; bind-all hosts `0.0.0.0`/`::` are not reachable as URLs and fall back to `127.0.0.1` with a warning — LAN clients must pass `--base-url http://<reachable-addr>:<port>` explicitly; IPv6 hosts are bracketed). Unknown flags are fatal. Note: WeFlow's default port is 5031; qqflow-server deliberately keeps 5032 (CLI-overridable).

Accounts are **client-driven**: startup only scans platform paths for discovery and lists the accounts as `awaiting_key` in `/health` (zero-account startup is valid). A downstream client registers an account via `POST /api/v1/accounts` with `{qq, key, db_path}`:

- `db_path` (optional) is a direct `nt_msg.db` file or a Tencent Files-style root (`<dir>/<qq>/nt_qq/nt_db/nt_msg.db`); omitting it reuses the scanned path.
- Keys are validated (16 printable-ASCII bytes) and kept **in memory only** — never persisted. A wrong key puts the account in `error` state (recoverable by re-registering); the process never exits over key problems.
- The reply carries three fields on purpose (`status`/`db_path` borrowed from weflow-server's registration contract): `state` = this request's outcome (`accepted`/`invalid_key`/`invalid_db_path`/`unknown_qq`/`already_ready`/`in_progress`), `status` = the account's `AccountStatus` (same enum `/health` reports, omitted when the account has no state entry yet), `db_path` = the **resolved** database file (omitted when unresolvable). Two contract subtleties: the reject branches don't mutate state, so their `status` is the account's unchanged one (wrong-key re-registration of a failed account → `state=invalid_key` + `status=error`), and `status="indexing"` only means the key's FORMAT passed — the decrypt verification happens later in `init_account`, so clients still converge via `/health`. The `accepted` reply is built BEFORE the `tokio::spawn` (reading the state back afterwards would race the background build); the idempotent branches echo the registry path of the running account, not this request's (ignored) `db_path`.

Default `127.0.0.1:5032` (same port as WeFlow). API token is auto-generated (32B hex) and persisted to `<data-dir>/系统凭据库（--show-token 获取）`; the startup log prints the file path, not the token value. Data dir: `%LOCALAPPDATA%\qqflow-server` on Windows, `~/.local/share/qqflow-server` on Linux, `~/Library/Application Support/qqflow-server` on macOS.

## Architecture

### Data pipeline

```
nt_msg.db (QQ, SQLCipher + 1024B plaintext custom header + WAL)
  → db::vfs::VFS_NAME ("qqflow-offset")
                         custom SQLite VFS: reads the LIVE source file with
                         every offset +1024 (header virtually stripped);
                         WAL/-shm pass through unshifted (no prefix there)
  → db::decrypt::open_live  READONLY long-lived connection to the source
                         SQLCipher PRAGMA suite (page_size=4096, kdf_iter=4000,
                         HMAC-SHA1, PBKDF2-SHA512, aes-256-cbc); retries with
                         HMAC-SHA512; verifies via sqlite_master
  → parser::extract_message (structured 40800 wire decode first —
                         spec-confirmed MsgBody layout per QQDecrypt/
                         nt_msg_db_util db_docs; heuristic fallback)
  → store::index::build_index / read_new + apply_records
                         full-table scan → in-memory Store; incremental rowid
                         reads + apply afterwards (two-phase, see below)
  → server (axum) + sync engine (watch task + tokio broadcast)
```

WeFlow-style zero-copy: no mirror, no copies ever (the old `db::mirror` copied
the whole ~1.2 GB main file on every SQLite checkpoint and the WAL on every
poll). `cipher_plaintext_header_size` cannot do the offset — the btree magic
check compares the decoded page-1 buffer while the codec copies the plaintext
header in verbatim, and the KDF salt is always read at offset 0 — hence the
VFS.

### The in-memory index (core design decision)

`nt_msg.db` message columns have no useful indexes — SQL filtering means a 30–60 s full table scan per query on a real (~190 MB) database. So the server scans both tables **once per account registration** into a `HashMap`-based `Store` and keeps it incrementally updated: the sync engine appends rows with `rowid > watermark` and the same `Store` is the single source of truth for both HTTP queries and SSE events. All query logic (`store::query`) works against this in-memory structure, never SQL.

- Table shapes (numeric column names are QQ-version-dependent, treat as fragile):
  - `group_msg_table`: `"40021"` group id, `"40001"` seq, `"40020"` sender uid, `"40093"` nickname, `"40800"` message blob
  - `c2c_msg_table`: `"40020"` peer uid, `"40001"` seq, `"40093"` nickname, `"40800"` blob
  - `"40001"` 是 `INTEGER PRIMARY KEY`（rowid 别名，rowid == 40001 值）；SQLite 因此把裸 `SELECT rowid` 的结果列命名为 `"40001"`，扫描/增量 SQL 显式 `rowid AS "rowid"` 固定列名（`map_row` 按名读取依赖它；缺失该别名曾致真库索引静默为 0）
- Spec-derived optional columns (QQDecrypt/nt_msg_db_util analysis, ground-truth probed per table by `store::index::probe_cols` — absent columns degrade): `"40013"` message direction (0 other / 1,2 self / 3 system / unknown bitmasks observed → `direction_to_is_send`: 1|2→1 else 0), `"40050"` unix send time (preferred over `seq >> 32` per-row when non-zero — probe found ~29% of rows disagree by >2 s), `"40090"` sender group card (ground-truth confirmed per-sender card; **scope = per-conversation only**: rides in `MessageRecord.card` → `Store.group_cards` (conv_key → uid → card) and displays via `Store::display_sender` in group context (SSE `source_name`, chatlab members/accountName, group-members `groupNickname`) — it NEVER enters the global `uid_names`/`display_uid`/contacts (would leak the group card into c2c chats; 40093 is often empty on card rows, so without `profile_info.db` the global name degrades to the uid while the in-group display still shows the card).
- Message timestamp: `"40050"` when present/non-zero, else packed high 32 bits of seq: `seq_to_time(seq) = seq >> 32` (`parser::types`).
- Conversation map key: `g:<groupId>` / `c:<peerUid>` (`store::conv_key`). `classify_talker` disambiguates: all-digit → group, `u_`-prefixed → c2c.
- Conversations carry a `dirty` flag set by appends; `build_index` sorts each conversation once via `Conversation::ensure_sorted`, while query paths (`query_messages`, chatlab pull) sort their own index snapshots by `(ts, rowid)` per query.
- `Store.media: HashMap<md5-or-uuid, MediaEntry{local_path, file_name}>` maps structured media metadata to fetchable local cache files (only entries with a local path; built at index time from `parsed.media`, keyed by `MediaInfo::key()` — 45424 md5 hex **normalized to lowercase** (real 45424 is uppercase while the file-name fallback lowercases; one image must never register under two keys), else the "MD5.ext" file-name stem, else 45503 uuid). **First-wins with stale-path refresh**: QQ clears its cache — when a later row's 45812 path resolves while the registered one no longer does, the entry is replaced (else the same image re-sent would 404 forever). **Cache-index fallback（文件系统扫描兜底，真机探针仲裁）**: 真机实测 "45812" 磁盘存在率仅 ~0.3%（QQ 清理缓存后存量路径大面积失效 → 下游 mediaId/mediaUrl 几乎全空）；注册建索引时 `store::media::scan_cache_index` 一次性只读遍历 `nt_data` 的媒体目录（白名单 `Pic/Ptt/Video/Emoji/File`，跳过符号链接，深度 ≤6、条目 ≤300k——真机 8.8k 文件/2.2k 目录约 230 ms）生成 `Store.media_fallback`（`CacheIndex`: 小写 stem → 路径 + alnum 归一 stem → 路径）。`index::register_media`（apply_record 调用）在 45812 缺失/失效时按层序查兜底表：①存储 key 本身（md5 小写精确 stem；非 md5 形 key 走 uuid alnum 归一且**必须含字母**——纯数字 uuid 是表情包 id 从不作文件名）②file_name 的 md5 形 stem（45424 与磁盘文件自身 md5 可能不一致，注册仍用原 key，mediaId 稳定）③file_name 非 md5 stem（最低优先）；同 stem 多候选按 种类扩展名族 > 45405 size 精确 > 最新 mtime > 路径字典序 去歧义，元数据已失效的文件直接丢弃。真机 3,722 条无活路径行救回 2,332 条（**62.7%**：md5 2,186 / file_name-md5 146 / uuid 0；来源目录 Pic 610/Emoji 1,717/Video 4/Ptt 1；精确 stem 匹配天然不命中 `<md5>_198/_720` 缩略图，实测 0 误配），其余 ~37% 为 QQ 已物理清理、无解。watch 轮询对兜底表**只做纯内存查询**（零新增文件 IO）；表快照在手动同步时重建（`AccountSync::refresh_media_fallback`，先重建再 `poll_once` 保证新行可救，且对已应用而未注册的 key 用 `reapply_media_registration` 二次补注册——覆盖"快照之后新到实时消息"的过期窗口）。`Store.media_root` (`nt_qq/nt_data`, supplied to `build_index`) resolves relative `45812` paths. Served by `GET|POST /api/v1/media/{id}` (auth + ready gates; streamed read-only; 404 when QQ cleared the cache; Content-Type from `file_name` fallback to the resolved file's extension). `MessageOut.media_id` is only filled when the key IS registered in `store.media` (no local path → omitted, never a guaranteed 404; the `media` metadata object still rides along).
- **WeFlow media export** (`store::media_export`, WeFlow exportPath semantics): `media=1` (alias `meiti`, per-kind switches `image/tupian`, `voice/vioce`, `video`, `emoji`) on `/api/v1/messages` copies the page's media into `<exportRoot>/<talker>/<images|voices|videos>/<file>` (copy not hardlink — QQ clears its cache; idempotent same-size skip — sound because the destination name embeds the content key) and fills `mediaFileName`/`mediaUrl`/`mediaLocalPath`; envelope `media: {enabled, exportPath=<root>, count=<exported>}`. **Export source resolution falls back to the registered `store.media` entry when the row's own "45812" is absent** — cache-index-fallback rescues export too, so `media=1` and `/api/v1/media/{id}` agree on one source per mediaId (the handler passes a `store.media` snapshot into the blocking export closure). Without the param the capability envelope (`exportPath=""`) is kept for compat. **Export file names are key-derived** (`<md5|uuid>.<source ext>`, e.g. `9f2a1c...4d.png`) — QQ's original `fileName` is arbitrary and two different files can share it; key-derived names make same-content idempotency correct and cross-page exports stable (raw QQ names kept only when no key exists). **The export loop runs on the blocking pool** (`spawn_blocking` in the messages handler) — real file IO must never stall the tokio workers (concurrent exports would starve SSE keep-alives). `GET|POST /api/v1/media/{talker}/{mediaType}/{file}` serves exported files (mediaType whitelist + segment checks + canonicalize prefix check). `exportRoot`/`baseUrl` live on `AppState` (`--media-export-dir`, default `<data-dir>/api-media`; `baseUrl` = `--base-url` or derived `http://{host}:{port}` — bind-all `0.0.0.0`/`::` fall back to `127.0.0.1` with a warning, IPv6 bracketed). Known deviations from WeFlow (documented): emoji switch is inert (QQ emoji carry display text, no files); voice exports raw `.silk`/`.amr` (no transcoding to wav); URLs use our own host:port (5032); file names are key-derived instead of QQ's originals.

### Name maps (`store::names`)

`Store.names` (`NameMaps`) carries uid→昵称/备注/QQ 号 and 群号→群名 maps loaded from QQ's mapping sources. **Ground-truth-confirmed layout (2026-08, ground-truth 探针仲裁)**: 昵称的权威来源是兄弟文件 `profile_info.db` 的 `profile_info_v2/v6.20002`（混合字符昵称如 "Alice Smith" 会低于中文比例阈值，故 20002 字段 id 提示绕过阈值）；QQ 号来自 `nt_msg.db` 的 `nt_uid_mapping_table.1002`（该表只有 uid→QQ，48902=uid/1002=QQ）+ profile 合并；群名来自兄弟文件 `group_info.db` 的 `group_list`/`group_detail_info_ver1`（60001=群号/60007=群名，60026=群备注）；**备注列已确认：`profile_info_v2/v6.20009` = 用户备注**（QQDecrypt 字段 id + 真库 ground-truth 确认，真库实测存在联系人备注数据）。`uid_remark`/`group_remark` 按列名/字段 id 提示（`20009`/`remark`/`20003`/`60026`）自动启用——**20009 是字段 id 提示、绕过 CJK 门槛**（拉丁备注如 "Bob Johnson"/"CppLover" 低于中文比例阈值，与 `classify_nick` 对 20002 的处理同理）；其余提示仍带 CJK 门槛（`20003` 在部分版本是时间戳列，靠门槛排除）。兄弟文件同密钥 SQLCipher、带 1024B 头（`open_live_mode(path, key, offset)` 先带头后无头双试；`group_info.db`/`profile_info.db` 均确认带头）。加载器是**值驱动探测**（`PRAGMA table_info` + 采样列统计：`u_` 比例/已知 uid 重叠定 uid 键列、5–12 位全数字定 QQ 列、已知字段 id + 列名提示定昵称/备注列、与已知群号重叠度/60001 提示定群 id 列、60007 提示定群名列），**备注/昵称分类仅认提示不认中文比例兜底**（加好友验证问题、入群问题、AI 助手介绍全是中文——误判雷区，ground-truth 确认）；名字漂移的表（如 `group_msg_table` 也含 "group"）须通过**改名消息一致性校验**才能成为群名来源。**加载时机：注册建索引时 + 手动同步（`POST /api/v1/sync` → `AccountSync::refresh_names`）；watch 轮询从不重读**（保持零文件 IO）。契约：任何失败 → 空映射 + debug 日志，绝不 panic。显示优先级（`Store::display_name`/`display_uid`，纯查找不改 `conv.name`）：私聊会话 备注(20009)>会话名（首行 40093）>档案昵称>UID；群会话 群备注(60026)>改名消息群名>群信息库群名>群号（未改名群的 conv.name 是群号占位、不参与显示，群信息库群名仍生效）；发送者 备注(20009)>消息昵称（末行 40093）>档案昵称>UID（群聊发送者显示由本群 40090 群名片优先覆盖，见上）。

### Poller / real-time path

File-system-event-driven (WeFlow-style): one watch task per account (`sync::watch::spawn`, tokio) watches the source `nt_db` directory via notify — ReadDirectoryChangesW on Windows, inotify on Linux, FSEvents on macOS — filters to `nt_msg.db`/`-wal`/`-shm` (兄弟文件如 `group_info.db`/`nt_uid_mapping.db` 刻意不 watch——名称映射刷新搭手动同步，见上), debounces bursts (`--watch-debounce-ms`, default 350 ms), then runs the sync (`AccountSync::poll_once`). A slow fallback poll (`--watch-fallback-ms`, default 30 s) re-checks `AccountSync::changed()` — retry flag, connection state, plus one WAL metadata stat (no data IO) — as insurance against silently dropped watch events, and re-attaches a dead watcher (directory deleted/recreated, e.g. QQ reinstall). No drop/reopen machinery: a failing read simply retries on the next trigger (WeFlow's forceReopen pattern deliberately dropped). The sync pass is **zero file IO** (the sole exception: the media-entry liveness probe in `register_media` — one `canonicalize` per genuinely new media path plus a few candidate stats per fallback lookup, bounded by the candidate count; re-sends with an unchanged registered path skip the probes entirely, and the cache-index fallback itself is consulted as pure in-memory map lookups — `Store.media_fallback` is only ever built on the registration/manual-sync paths):
1. `LiveReader::acquire()` — the long-lived read-only connection to the LIVE source (via the offset VFS); reopens automatically when closed (`db::live`).
2. Reads per table with `index::read_new` (`rowid > watermark`, pure read — a failure in either table leaves the store untouched).
3. Applies both tables under a single store write-lock (`index::apply_records` borrows `&[MessageRecord]` and clones per row — the caller iterates the records again afterwards to build SSE events with post-apply media registration; cheap small strings on the incremental path, the full-scan path stays move-based — plus watermark write-back) and emits `message.new` / `message.revoke` events on a tokio broadcast channel (capacity 1024); recall messages are detected by the parser (`MsgType::Recall`). Response rows are shaped in the same lock (`MessageOut` with the mediaId fetchability filter, shared with the messages query).

Idle periods cost nothing; a failed sync sets a retry flag so the next tick retries; SQLite handles checkpoints transparently (the reader shares QQ's wal-index via `-shm`).

**Manual sync**: the same per-account `AccountSync` (live reader behind `Arc<Mutex>`) is shared with `GET|POST /api/v1/sync`, which runs `SyncEngine::sync_all()` on demand and returns the newly appended records (newest first) — for client init / manual refresh. `sync_all` 顺带刷新 name maps（`refresh_names`）——这是除重新注册外名称映射唯一的刷新点；媒体兜底快照同样只在这里刷新（`refresh_media_fallback` 在 `poll_once` **之前**重建 `Store.media_fallback` 并跑 `reapply_media_registration`，让本轮新行与之前被过期快照漏掉的行都能救回）。Concurrent poll/sync passes serialize on the reader mutex and the store write lock.

SSE clients (`GET/POST /api/v1/push/messages`) get a `sync` event on connect carrying current rowid watermarks (a qqflow-server extension), then live events (`message.new` events carry an optional `media` object for image/voice/video messages — a path-free `PushMedia` serialization view (`sync::events`): md5/uuid/fileName/size/dims/CDN urls, **never the raw "45812" `localPath`** — a mostly-dead machine-local path that would leak host layout downstream; fetching goes via the sibling `mediaId`, present only when the store registered a live path — the same `store::query::fetchable_media_id` rule as the REST `messages.mediaId` — so a pushed id is always servable by `GET /api/v1/media/{id}`); a fresh `sync` is also broadcast when an account's index build completes (clients connected during indexing start with a `(0,0)` baseline and are re-baselined by it), and broadcast lag re-syncs the client the same way. KeepAlive ping every 15 s. SSE has no ready gate — it serves 200 even while indexing. `AccountSync::poll_locked` applies records BEFORE building events, so a row rescued by the cache-index fallback advertises its own fetchable `mediaId` in its own event (`apply_records` therefore borrows `&[MessageRecord]` and clones per row on the incremental path — cheap small strings; the heavy full-scan path `scan_table` stays move-based).

### Startup sequence (`server::run_with`)

Parse CLI args → resolve data dir + token → `db::scan::scan_accounts` (platform-gated path discovery) → bind listener **early** so `/health` reports "starting" → list scanned accounts as `awaiting_key` (no build at startup) → wait for client registrations (`POST /api/v1/accounts`) → per account `server::init_account` (`spawn_blocking` live open + key verify + index; `install_index` broadcasts the SSE baseline; `AccountSync` registration + watch task) → recompute the global ready flag → wait for Ctrl-C → signal shutdown watch.

### Message parser (`parser`)

Structured-first hybrid. `parser::proto` is a hand-rolled two-level protobuf wire reader (no prost dependency) for the spec-confirmed `MsgBody{repeated MsgContent content=40800}` layout (45002 content types: 1 text/2 image/4 voice/5 video/6 qqface; text body `45101`, emoji text `47602`; media fields `45503` uuid, `45402` name, `45406` raw md5, `45424` md5 hex, `45405` size, `45411/45412` dims, `45802-45804` CDN urls, `45812` local cache path). `extract_message` = `extract_structured` (wins only on a real 40800 field + known content types: text from 45101, exact media metadata for image/voice/video; first media segment wins — v1 multi-image limitation) else the unchanged heuristic `extract_text` (inherited from QQFlow): runs of common Han characters (U+4E00–U+9FA5), ≥ 2 chars with > 60% common ratio, ASCII fallback; media by byte signatures (`.jpg/.png/.gif/gchatpic`, `.amr/.silk/.ptt`, `shortvideo/.mp4`); recall/system by characteristic phrases. An iteration budget (`n*50`) bounds worst-case cost. Tolerant of QQ version churn — expect degraded output, not crashes. Note: heuristic media signatures can false-positive on plain text mentioning ".jpg" — the structured pass fixes that only for real structured blobs.

### Concurrency

- `parking_lot::RwLock<Store>` shared via `Arc` — single lock for the whole store (sync engine writes, handlers read).
- notify watcher threads bridge into the tokio watch task via an unbounded channel (`sync::watch`); watch/fallback/manual sync passes serialize on the reader mutex and the store write lock.
- tokio `broadcast` for SSE events; `watch` channel for shutdown; CPU-bound decrypt/scan work in `spawn_blocking`; `rusqlite::Connection` is `Send + !Sync` — owned by `LiveReader` behind `Arc<Mutex<LiveReader>>`, used only inside the lock.
- `AppState` (in `store`) holds: store, broadcast sender, per-account readiness (`server::AccountState` with the `AccountStatus` enum, `awaiting_key`/`indexing`/`ready`/`error`), a global `ready` AtomicBool, the token, and the `AccountRegistry` (scanned/registered `DbInfo`s, watch config, shutdown receiver).
- Auth: Bearer header / `?access_token=` (recommended for SSE) / POST JSON body, constant-time comparison (`config::constant_time_eq`). `/health` and `POST /api/v1/accounts` are the only non-readiness-gated endpoints (accounts is the bootstrap path).

## Known issues

- **c2c (private-chat) messages were silently dropped** — FIXED: `store/index.rs` now uses per-table column mapping (group 6 cols / c2c 5 cols, peer = sender). Guarded by the `fake_db_indexes_c2c_rows` regression test in `tests/real_db_groundtruth.rs`.
- **运行中替换源库不自动感知**（已按设计取舍）：QQ 更新 / 迁移数据目录把 `nt_msg.db` 替换掉时，运行中的服务不会自动重开句柄——watcher 存活时读旧文件、watermark 永不前进；检测与重开机制（WeFlow forceReopen 模式）已舍去，**重启服务（重新注册）可恢复**。Windows 上源文件被服务器持有（无 FILE_SHARE_DELETE），替换本身也会失败直到服务退出。
- **媒体兜底快照的过期窗口**（已知取舍）：`Store.media_fallback` 只在注册建索引与手动同步时重建；快照建成后新到的实时消息若同样无 45812，其媒体文件尚未进入快照，该行的兜底注册会推迟到下一次手动同步（`refresh_media_fallback` → `reapply_media_registration` 对已应用但未注册的 key 补注册）或重新注册。watch 轮询刻意不重扫以保持零文件 IO。另外 file_name-md5 层救回的行，其 mediaId（45424）与磁盘文件的真实 md5 可能不一致——同一条消息内自洽（导出/服务都走该注册路径），但 mediaId 不再严格等于所服务内容的 md5。

## Version-fragility notes

- Numeric column names (`"40021"`, `"40800"`, …), table layouts, and the uid→QQ mapping table all vary with QQ versions; code degrades gracefully (best-effort queries, heuristic parsing).
- uid→昵称/备注/QQ、群号→群名的映射表列结构无稳定文档：`nt_uid_mapping_table`/`profile_info.db` 的列与 `group_info.db` 的存在性/布局/是否带头均由 `store::names` 在加载时值驱动探测（缺表缺列 → 空映射，显示回落消息昵称/群号）；ground-truth 探针（`tests/real_db_groundtruth.rs` 的 `probe_columns` + 兄弟文件探测 + `load_names` 端到端验证）输出真实布局用于仲裁。
- `MessageOut::is_send` derives from the `"40013"` direction column (`direction_to_is_send`: 0/3/unknown → 0, 1/2 → 1); degrades to 0 when the QQ version lacks the column.
- `"40001"` 的 `INTEGER PRIMARY KEY` 声明（rowid 别名）随版本稳定存在，但 SQLite 对 `SELECT rowid` 的结果列名会随该声明变化（真库实测命名为 `"40001"`）；索引 SQL 显式 `rowid AS "rowid"`，`store::index` 的 `rowid_alias_columns_still_index` 单测用同构 fixture 守护该命名。

## Tests

- `tests/sqlcipher_roundtrip.rs` — self-built SQLCipher test database with QQ's exact PRAGMA parameters + fake 1024-byte header + WAL. Proves: **direct live read of the header-prefixed file through the offset VFS** (the arbitration test), WAL-only writes visible to the still-open reader, checkpoint survived by the same reader without reopening, cold reopen, wrong-key failure. **Never touches real QQ data.** (The fake writer's WAL and wal-index are hard-linked to the `nt_msg.*` names the reader opens — production shares QQ's live files the same way.)
- `tests/api_smoke.rs` — HTTP layer contract tests via `tower::ServiceExt::oneshot` (no network, no real DB); builds a fake `AppState` with seeded conversations.
- `tests/fs_watch_e2e.rs` — file-system event → sync → SSE broadcast e2e (fake DB with a persistent writer).
- `tests/real_db_groundtruth.rs` — fake-DB regression tests + client-registration e2e (wrong key → `error` → corrected key → `ready`) + `fake_db_names_loaded`（假库 + 带头 `group_info.db` 验证名称映射加载）+ `fake_db_index_media_metadata` / `fake_db_media_endpoint_serves_bytes`（结构化图片行全链路：store.media 注册、is_send/40050/群名片、端点 200/404/401）+ `fake_db_media_fallback_registers_and_serves`（死 45812 + md5 命名缓存文件 → 兜底注册 + 端点返回缓存字节）/ `fake_db_media_fallback_first_wins_and_no_match`（活 45812 不被兜底覆盖；无文件 → 不注册、mediaId 缺省）；the `real_db_groundtruth` probe (`#[ignore]`) runs ground-truth queries against a REAL QQ DB via the live reader (`QQFLOW_TEST_DB_ROOT` / `QQFLOW_TEST_DB_KEY`) — it arbitrates the offset VFS against the real on-disk layout, its `probe_columns` + sibling-file sections arbitrate the name-map sources, and its spec-column sections arbitrate `40013` 分布（实测含位掩码 32761）、`40050` vs `seq>>32`（实测 ~29% 行差 >2s）、`40090`（逐发送者群名片确认）、真实 40800 图片段解码与 `45812` 磁盘存在率.
- `tests/downstream_client.rs` — downstream-client GET/POST simulation against a real QQ DB, including client-driven registration (`#[ignore]`; inputs resolve from the gitignored repo-root `qqflow-server.json` (`qq`/`key`/`db_path`) first, then `QQFLOW_TEST_QQ` / `QQFLOW_TEST_DB_KEY` / `QQFLOW_TEST_DB_ROOT` env vars). Asserts the real contract: `media.enabled=true`、`isSend ∈ {0,1}`（来自 40013）、`media`/`mediaId` 随图片消息出现，并打印每页 `mediaId coverage in page: <withId>/<mediaRows>`（缓存兜底的命中率证据）. contacts `remark` 断言为形状级（真库值未知）.
- Unit tests live inline in modules (`parser`, `keystore`, `decrypt`, `vfs`, `live`, `store/names` 探测分类与列漂移退化, `store` 显示优先级).

## External references

- WeFlow API contract: `weflow-api.md` in the local `campus-info-hub-py` project (`src/sources/weflow/`)
- Research notes: local Claude plan document `https-github-com-yfgug-qqflow-1-github-concurrent-lovelace.md` (plans directory)
