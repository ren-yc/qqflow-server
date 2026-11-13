//! Sync engine: file-system-event-driven push + passive manual sync.
//!
//! New messages are detected by watching the source `nt_db` directory with
//! notify (see `watch`): ReadDirectoryChangesW / inotify / FSEvents events
//! are debounced and trigger a sync pass (incremental append + SSE
//! broadcast). The pass is a zero-file-IO indexed query against the
//! long-lived read-only connection to the LIVE source database (WeFlow
//! style — no mirror, no copies). A slow fallback poll guards against
//! silently dropped watch events and re-attaches a dead watcher.
//!
//! The same per-account `AccountSync` is shared with the manual sync
//! endpoint (`POST /api/v1/sync`): callers trigger `poll_once` on demand
//! and receive the newly appended records. QQ writes new messages into
//! `nt_msg.db-wal`; recall messages ("你猜猜撤回了什么") are detected by
//! the parser and emitted as `message.revoke`.

pub mod events;
pub mod history;
pub mod watch;

use std::path::PathBuf;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicU32, Ordering};

use anyhow::Result;
use parking_lot::{Mutex, RwLock};

use crate::db::live::LiveReader;
use crate::parser::types::{ChatType, MsgType};
use crate::store::index;
use crate::store::query::MessageOut;
use crate::store::Store;
use crate::sync::history::EventBus;

pub use events::Event;

/// One account's sync machinery: the live reader (mutex-guarded so the
/// manual sync endpoint and the poll loop can share it) plus everything
/// needed to run a sync pass.
pub struct AccountSync {
    /// Owning account — the key `SyncEngine` unregisters by.
    pub qq: String,
    pub reader: Arc<Mutex<LiveReader>>,
    pub store: Arc<RwLock<Store>>,
    /// 内部事件总线（重放历史 ＋ 广播通道）。服务层的 SSE 直接订阅它 —— **这不是承诺面**：
    /// 它要求调用方用 tokio 的 `broadcast` 并处理 `RecvError::Lagged`，而嵌入者不该被绑到这两件
    /// 事上（见 [`crate::api::Sync::drain_events`]）。
    ///
    /// 发布只走 `publish`：**历史由生产者在广播前单点写入**。订阅端各自 append 会让事件 id
    /// 随在线连接数跳号、把同一条塞进缓冲多次，而零订阅者时广播根本没人接 —— 那段事件就永远
    /// 不在重放窗口里（回归：`sse_history_is_recorded_once_without_subscribers`）。
    pub(crate) bus: EventBus,
    /// Set when a sync failed; the poll loop then retries even though the
    /// reader state is unchanged.
    retry: AtomicBool,
    /// Set by deregistration. A pass already inside `poll_once` cannot be
    /// cancelled — `spawn_blocking` work runs to completion even after its
    /// awaiting task is aborted — so the flag is what stops it from writing
    /// into a store that no longer belongs to this account.
    stopped: AtomicBool,
    /// 一次注册的身份复查闭包（由 init_account 注入，持注册令牌）：最终写入
    /// 点在 **Store 写锁内**调用它——`sync_all` 只在账号处理开始前查一次
    /// stopped，注销发生在扫描完成与应用之间的窗口由这里封住。旧句柄在注销
    /// 后仍可被调用（Arc 存活），没有这层复查时旧账号的名字表/媒体回退会把
    /// 新注册的 store 覆盖掉。None = 未注入（嵌入者直接构造的句柄），复查
    /// 退化为只看 stopped。
    registration_guard: Mutex<Option<Box<dyn Fn() -> bool + Send>>>,
    /// Source main db path — derives the `-wal` path the fallback stats.
    ///
    /// 只读暴露给定向导出：发布前的身份复查要在**导出自己**的阻塞任务里再采一次
    /// 指纹（轮询采样在整个导出期间被 `reader`/`store` 的锁挡住，世代计数必然
    /// 不动），采样的输入就是这条路径。
    pub db_path: PathBuf,
    /// `nt_db` directory — sibling group-info databases live here (name maps).
    db_dir: PathBuf,
    /// SQLCipher key — sibling databases share it (name maps).
    key: String,
    /// Last observed (mtime-millis, size) of the source WAL; the fallback
    /// poll compares against it to detect changes the watcher missed.
    last_wal: Mutex<Option<(u64, u64)>>,
    /// 数据源世代：库文件**身份**（路径、创建时间、salt）变化时递增。
    ///
    /// 游标 v2 把它编进载荷：身份换了（删库重建、还原成另一份文件），旧游标
    /// 的行标识在新文件上毫无意义，续拉等于按旧表的位置读新表。与指纹基线
    /// `source_digest` 配对，每轮同步前采样一次。
    ///
    /// 共享句柄经 [`AccountSync::generation_handle`] 交给 HTTP 层：读路径不能
    /// 去拿 reader/store 的锁（见 `poll_locked` 的临界区），交出去的必须是 `Arc`。
    generation: Arc<AtomicU32>,
    /// 上一次采样到的指纹摘要（基线）。`None` = 尚未采样成功过。
    source_digest: Mutex<Option<[u8; 32]>>,
}

impl AccountSync {
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        qq: String,
        reader: Arc<Mutex<LiveReader>>,
        store: Arc<RwLock<Store>>,
        bus: EventBus,
        db_path: PathBuf,
        db_dir: PathBuf,
        key: String,
    ) -> Self {
        Self {
            qq,
            reader,
            store,
            bus,
            retry: AtomicBool::new(false),
            stopped: AtomicBool::new(false),
            registration_guard: Mutex::new(None),
            db_path,
            db_dir,
            key,
            last_wal: Mutex::new(None),
            generation: Arc::new(AtomicU32::new(0)),
            source_digest: Mutex::new(None),
        }
    }

    /// 注入注册身份复查闭包（生产路径由 init_account 在公开句柄前调用）。
    /// 闭包返回 false = 该句柄所属的注册已失效。
    pub fn set_registration_guard(&self, guard: Box<dyn Fn() -> bool + Send>) {
        *self.registration_guard.lock() = Some(guard);
    }

    /// 这个句柄还允许写共享 Store 吗？stopped（注销停机）与注册令牌复查
    /// （旧注册借新注册名义写回）任一失败即否。
    fn may_write_store(&self) -> bool {
        !self.is_stopped()
            && self
                .registration_guard
                .lock()
                .as_ref()
                .is_none_or(|g| g())
    }

    /// 数据源世代的共享句柄，给 HTTP 层编游标用。
    ///
    /// 与 `reader`/`store` 的锁同理：读世代是一次原子加载，HTTP 侧不应为此排队
    /// 等一个可能正持锁数秒的同步轮次——交出去的必须是 `Arc` 的克隆。
    pub fn generation_handle(&self) -> Arc<AtomicU32> {
        self.generation.clone()
    }

    /// 采一次数据源指纹；身份类线索变化时推进世代并返回 `true`。
    ///
    /// 只比对库路径、创建时间与 SQLCipher salt（页 1 明文前缀，在 QQ 的 1024
    /// 字节自定义头之后）——**追加写入不会改变它们**，所以正常的新消息入库不会
    /// 让游标失效。采样失败时**不推进**世代：一次读失败（文件被 QQ 短暂占用、
    /// 权限抖动）不能把所有在用游标判死，下一轮重试即可；失败本身记警告，不静默吞掉。
    fn sample_source(&self) -> bool {
        match crate::db::source_identity::fingerprint(&self.db_path) {
            Ok(fp) => {
                let mut baseline = self.source_digest.lock();
                match *baseline {
                    None => {
                        *baseline = Some(fp.digest);
                        false
                    }
                    Some(previous) if previous != fp.digest => {
                        *baseline = Some(fp.digest);
                        self.generation.fetch_add(1, Ordering::SeqCst);
                        tracing::warn!("数据源身份发生变化：旧游标与进行中的导出将失效");
                        true
                    }
                    Some(_) => false,
                }
            }
            Err(err) => {
                tracing::warn!(error = %err, "数据源指纹采样失败，本轮不推进世代");
                false
            }
        }
    }

    /// Retire this account's sync: no further pass may write to the store.
    pub fn stop(&self) {
        self.stopped.store(true, Ordering::SeqCst);
    }

    pub fn is_stopped(&self) -> bool {
        self.stopped.load(Ordering::SeqCst)
    }

    /// Re-read name maps (备注/群名) from nt_msg.db + sibling databases.
    /// Registration and manual sync only — never on watch ticks, keeping
    /// the poll pass zero-file-IO. Best-effort: a failure leaves the
    /// previous maps in place.
    pub fn refresh_names(&self) {
        let mut reader = self.reader.lock();
        let Ok(conn) = reader.acquire() else {
            tracing::debug!("[names] refresh skipped: live connection unavailable");
            return;
        };
        let known = {
            let guard = self.store.read();
            crate::store::names::KnownKeys::from_store(&guard)
        };
        let maps = crate::store::names::load_names(conn, &self.db_dir, &self.key, &known);
        let mut guard = self.store.write();
        // 最终写入在写锁内复查注册身份：耗时读取在锁外做完之后，注销与重注册
        // 都可能已经发生——旧账号的名字表不能落进（可能已属于）新账号的 store。
        if !self.may_write_store() {
            tracing::debug!("[names] refresh discarded: registration no longer current");
            return;
        }
        guard.names = maps;
    }

    /// Rebuild the media cache-index fallback snapshot from nt_data, then
    /// re-apply media registration for keys the previous (stale) snapshot
    /// missed — rows applied between the snapshot and now get their second
    /// chance here. Manual sync only (real file IO — the caller runs it on
    /// the blocking pool); watch ticks never call this and only consult
    /// the snapshot through pure map lookups.
    pub fn refresh_media_fallback(&self) {
        let root = {
            let guard = self.store.read();
            guard
                .media_root
                .clone()
                .or_else(|| crate::store::media::media_root_of(&self.db_dir))
        };
        let Some(root) = root else {
            return;
        };
        let index = crate::store::media::scan_cache_index(&root);
        let mut guard = self.store.write();
        // 复查必须包住 reapply_media_registration 整段：它按旧账号的缓存索引
        // 改写**当前** store 已有行的媒体登记——只复查 media_fallback 赋值时，
        // 注销后的旧账号仍能借 reapply 把新账号的行改写掉。
        if !self.may_write_store() {
            tracing::debug!("[media-fallback] refresh discarded: registration no longer current");
            return;
        }
        guard.media_fallback = index;
        crate::store::index::reapply_media_registration(&mut guard);
    }

    /// 建索引完成后、对外可用之前的一次基线采样（见 `init_account` 的调用注释）。
    /// 失败只告警：下一轮 `poll_once` 会重试，基线缺失不判死任何游标。
    pub fn sample_source_baseline(&self) {
        self.sample_source();
    }

    /// Cheap change detection (no data IO — at most one metadata stat):
    /// true right after a failed sync (retry flag), while the live
    /// connection is closed (QQ not running yet — reopen on the next
    /// poll), or when the source WAL changed since the last check
    /// (insurance against silently dropped watch events: QQ appends new
    /// rows into the WAL).
    pub fn changed(&self) -> bool {
        if self.retry.swap(false, Ordering::SeqCst) || !self.reader.lock().is_open() {
            return true;
        }
        let Some(snap) = self.wal_snapshot() else {
            return false; // no WAL yet — the watcher stays the only trigger
        };
        let mut last = self.last_wal.lock();
        if *last != Some(snap) {
            *last = Some(snap);
            return true;
        }
        false
    }

    /// (mtime-millis, size) of the source WAL, falling back to the main
    /// file when the WAL is absent (fully checkpointed, QQ closed).
    /// Metadata only — never reads page data.
    fn wal_snapshot(&self) -> Option<(u64, u64)> {
        let mut wal = self.db_path.as_os_str().to_owned();
        wal.push("-wal");
        [PathBuf::from(wal), self.db_path.clone()]
            .iter()
            .find_map(|p| {
                let m = std::fs::metadata(p).ok()?;
                let ms = m.modified().ok()?;
                let d = ms.duration_since(std::time::UNIX_EPOCH).ok()?;
                Some((d.as_millis() as u64, m.len()))
            })
    }

    /// One sync pass: read rows above the watermark from the LIVE source,
    /// append them, broadcast SSE events, and return the appended messages
    /// (already shaped as `MessageOut`, with the mediaId fetchability
    /// filter applied). Safe to call concurrently — the reader mutex and
    /// the store write lock serialize overlapping passes (the second one
    /// finds nothing new).
    pub fn poll_once(&self) -> Result<Vec<MessageOut>> {
        // 每轮同步前采一次数据源指纹：换了库就在这里推进世代。放在读阶段之前
        // ——本轮读出的行如果来自新文件，游标也必须从这一代开始算。
        self.sample_source();
        let mut reader = self.reader.lock();
        let result = self.poll_locked(&mut reader);
        if result.is_err() {
            // The read phase left the store untouched — retry the same rows
            // on the next trigger instead of duplicating them.
            self.retry.store(true, Ordering::SeqCst);
        }
        result
    }

    fn poll_locked(&self, reader: &mut LiveReader) -> Result<Vec<MessageOut>> {
        let conn = reader.acquire()?;

        // Read phase: parse rows above the watermark without touching the
        // store. If either table's read fails, nothing has been applied —
        // the retry re-reads the same rows instead of duplicating them.
        let (wm_g, wm_c) = {
            let guard = self.store.read();
            (guard.watermark_group, guard.watermark_c2c)
        };
        let (new_wm_g, new_g) = index::read_new(conn, ChatType::Group, wm_g)?;
        let (new_wm_c, new_c) = index::read_new(conn, ChatType::C2c, wm_c)?;

        // Apply phase: one write-lock critical section — apply both tables
        // FIRST so this batch's own media registrations are visible to the
        // SSE mediaId filter below (a row that rescues its media through the
        // cache-index fallback must advertise the same fetchable id in its
        // own event), then build the SSE events and response rows (the
        // records are borrowed — see apply_records), advance watermarks.
        let outs: Vec<MessageOut> = {
            let mut guard = self.store.write();
            // Deregistration check, deliberately the first thing inside the
            // critical section rather than at `poll_once`'s entry: the read
            // phase above holds no store lock, so a pass that started before
            // the deregistration would otherwise reach this point and write
            // this account's rows (and watermarks) into the cleared store —
            // resurrecting data the operator just removed. Checking under the
            // same lock deregistration takes makes the two mutually exclusive.
            if !self.may_write_store() {
                tracing::debug!(qq = %self.qq, "sync pass discarded: account deregistered");
                return Ok(Vec::new());
            }
            // Media root may predate the account (sync can run before the
            // index build in edge paths); resolve it lazily once.
            if guard.media_root.is_none() {
                guard.media_root = crate::store::media::media_root_of(&self.db_dir);
            }
            index::apply_records(&mut guard, &new_g);
            index::apply_records(&mut guard, &new_c);
            guard.watermark_group = new_wm_g;
            guard.watermark_c2c = new_wm_c;
            // 只有这一轮确实动过索引才更新时刻：空转的一轮不该让 updatedAt 看起来更新了。
            if !new_g.is_empty() || !new_c.is_empty() {
                guard.mark_index_built();
            }
            let events: Vec<Event> = new_g
                .iter()
                .chain(&new_c)
                .map(|r| {
                    // Display names resolve through the name maps — the
                    // remark (私聊) / group-info name (群聊) wins when known;
                    // in a group the sender's per-conversation card (40090)
                    // wins over the global name (never leaks across chats).
                    let group_name = Some(guard.display_name(r.chat_type, &r.talker));
                    let source_name = Some(guard.display_sender(r.chat_type, &r.talker, &r.from_uid));
                    if r.parsed.msg_type == MsgType::Recall {
                        Event::message_revoke(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                        )
                    } else {
                        // mediaId filtered by the same rule as the REST rows
                        // (registered live path only) — the media object is
                        // the path-free PushMedia view.
                        let media_id = r
                            .parsed
                            .media
                            .as_ref()
                            .and_then(|m| crate::store::query::fetchable_media_id(&guard, m));
                        Event::message_new(
                            r.chat_type,
                            r.talker.clone(),
                            group_name,
                            r.rowid,
                            source_name,
                            r.parsed.content.clone(),
                            r.ts,
                            r.parsed.media.clone(),
                            media_id,
                        )
                    }
                })
                .collect();
            // Response rows: shaped under the lock so the store-dependent
            // fields (mediaId fetchability, senderName) see this batch's own
            // registrations — same single shaping function as the messages
            // query, so the two paths cannot drift.
            let outs: Vec<MessageOut> = new_g
                .iter()
                .chain(&new_c)
                .map(|r| crate::store::query::shape_record(&guard, r))
                .collect();
            // Keep events before a deregistration reset: clearing the store and
            // resetting history cannot overtake this publication.
            for ev in events {
                self.bus.publish(ev);
            }
            outs
        };
        Ok(outs)
    }
}

/// Registry of per-account sync engines, shared with the HTTP layer for
/// the manual-sync endpoint. Accounts are registered as their indexes
/// finish building.
pub struct SyncEngine {
    registry: Mutex<SyncRegistry>,
}

#[derive(Default)]
struct SyncRegistry {
    accounts: Vec<Arc<AccountSync>>,
    watchers: std::collections::HashMap<String, tokio::task::JoinHandle<Result<()>>>,
}

impl SyncEngine {
    pub fn new() -> Self {
        Self { registry: Mutex::new(SyncRegistry::default()) }
    }

    /// Validate and publish under the same registry lock used by removal.
    /// Server callers also hold the lifecycle gate while changing the token.
    pub fn register_if_current(&self, account: Arc<AccountSync>, token_ok: &dyn Fn() -> bool) -> bool {
        let mut registry = self.registry.lock();
        if !token_ok() || account.is_stopped()
            || registry.accounts.iter().any(|a| a.qq == account.qq)
        {
            return false;
        }
        registry.accounts.push(account);
        true
    }

    /// Attach only to the exact registered handle. A rejected task must be
    /// aborted explicitly: dropping a JoinHandle would detach it permanently.
    pub fn attach_watcher_if_current(
        &self,
        account: &Arc<AccountSync>,
        handle: tokio::task::JoinHandle<Result<()>>,
        token_ok: &dyn Fn() -> bool,
    ) -> bool {
        let mut registry = self.registry.lock();
        if !token_ok() || account.is_stopped()
            || !registry.accounts.iter().any(|a| Arc::ptr_eq(a, account))
        {
            handle.abort();
            return false;
        }
        if let Some(old) = registry.watchers.insert(account.qq.clone(), handle) {
            old.abort();
        }
        true
    }

    /// Remove the current account and watcher together. Callers stop the
    /// returned handle before clearing the index under its write lock.
    pub fn unregister(
        &self,
        qq: &str,
    ) -> (Option<Arc<AccountSync>>, Option<tokio::task::JoinHandle<Result<()>>>) {
        let mut registry = self.registry.lock();
        let account = registry.accounts.iter().position(|a| a.qq == qq)
            .map(|i| registry.accounts.remove(i));
        let watcher = registry.watchers.remove(qq);
        (account, watcher)
    }

    /// A stale initialization may retire itself, never a later registration
    /// of the same account. The identity check and removal are indivisible.
    pub fn unregister_if(&self, account: &Arc<AccountSync>) -> bool {
        let mut registry = self.registry.lock();
        let Some(i) = registry.accounts.iter().position(|a| Arc::ptr_eq(a, account)) else {
            return false;
        };
        registry.accounts.remove(i).stop();
        if let Some(watcher) = registry.watchers.remove(&account.qq) {
            watcher.abort();
        }
        true
    }

    pub fn snapshot(&self) -> Vec<Arc<AccountSync>> {
        self.registry.lock().accounts.clone()
    }

    /// Manual sync: run a full pass on every registered account and return
    /// `(newMessages, revokeMessages)` — how many rows the pass appended,
    /// split the same way WeFlow splits them (a recall is not counted as a
    /// new message). Name maps (备注/群名) and the media cache-index fallback
    /// ride the manual sync — the one place a client-visible refresh happens.
    /// The fallback refreshes BEFORE the poll so newly-read rows can already
    /// register through it.
    ///
    /// The rows themselves are deliberately not returned: the endpoint is a
    /// trigger, and clients read the appended rows back through
    /// `/api/v1/messages` (or receive them on the SSE stream) rather than
    /// from a second, differently-shaped copy.
    pub fn sync_all(&self) -> (usize, usize) {
        let (mut new_count, mut revoke_count) = (0usize, 0usize);
        for account in self.snapshot() {
            // A deregistration concurrent with this call may already have
            // stopped the account after `snapshot` cloned it; `poll_once`
            // would bail out anyway, but `refresh_media_fallback` writes to
            // the store without that check, so skip the whole account.
            if account.is_stopped() {
                continue;
            }
            account.refresh_media_fallback();
            match account.poll_once() {
                Ok(messages) => {
                    for m in &messages {
                        if MsgType::from_code(m.local_type) == MsgType::Recall {
                            revoke_count += 1;
                        } else {
                            new_count += 1;
                        }
                    }
                    account.refresh_names();
                }
                Err(e) => tracing::warn!("manual sync error: {e:#}"),
            }
        }
        (new_count, revoke_count)
    }
}

impl Default for SyncEngine {
    fn default() -> Self {
        Self::new()
    }
}
