//! File-system-event-driven sync trigger (cross-platform).
//!
//! Watches the source `nt_db` directory (the parent of `nt_msg.db`) with
//! notify — ReadDirectoryChangesW on Windows, inotify on Linux, FSEvents
//! on macOS — debounces event bursts (WeFlow-style), and runs a full sync
//! (`AccountSync::poll_once`, the exact same path as the manual
//! `POST /api/v1/sync` endpoint) so new messages flow to SSE subscribers.
//!
//! Reliability: file-watch backends can silently drop events on buffer
//! overflow, so a slow fallback poll (default 30 s) re-checks the source
//! files with zero-IO stats and also re-attaches a dead watcher (directory
//! deleted/recreated by a QQ reinstall).

use std::ffi::OsStr;
use std::path::{Path, PathBuf};
use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use notify::{RecursiveMode, RecommendedWatcher};
use notify_debouncer_mini::{
    new_debouncer_opt, Config as DebounceConfig, DebounceEventResult, Debouncer,
};
use tokio::sync::{mpsc, watch};

use super::AccountSync;

/// Watch behavior for one account.
#[derive(Debug, Clone)]
pub struct WatchConfig {
    /// How long the watcher waits for an event burst to quiet down before
    /// triggering a sync (WeFlow-aligned; batch mode worst case ~2x this).
    pub debounce: Duration,
    /// Slow fallback poll interval; `None` disables it (not recommended —
    /// silently dropped watch events would never recover).
    pub fallback: Option<Duration>,
}

impl Default for WatchConfig {
    /// CLI defaults without the fallback poll (test fixtures; production
    /// derives this from `config::Config`, which enables the fallback).
    fn default() -> Self {
        Self { debounce: Duration::from_millis(350), fallback: None }
    }
}

/// Source files whose changes trigger a sync (everything else in `nt_db`,
/// e.g. `nt_uid_mapping.db`, is ignored).
const WATCH_FILES: [&str; 3] = ["nt_msg.db", "nt_msg.db-wal", "nt_msg.db-shm"];

/// Re-attach retry interval when the slow fallback poll is disabled
/// (`watch_fallback_ms: 0`).
const REATTACH_INTERVAL: Duration = Duration::from_secs(10);

fn is_relevant(name: &OsStr) -> bool {
    WATCH_FILES.iter().any(|w| name.eq_ignore_ascii_case(OsStr::new(w)))
}

/// Notify thread -> watch task messages (closure sends synchronously).
enum WatchMsg {
    Changed(PathBuf),
    BackendError,
}

/// Run the watch loop for one account until `shutdown` turns true.
pub async fn spawn(
    account: Arc<AccountSync>,
    watch_dir: PathBuf,
    cfg: WatchConfig,
    mut shutdown: watch::Receiver<bool>,
) -> Result<()> {
    let (tx, mut rx) = mpsc::unbounded_channel::<WatchMsg>();
    let mut watcher = rebuild_watcher(&tx, &watch_dir, cfg.debounce);
    // One interval drives both jobs: the watcher re-attach (always) and the
    // slow fallback sync (only when `fallback` is configured). With
    // `watch_fallback_ms: 0` the re-attach must still run, or one backend
    // error would permanently kill event-driven sync.
    let mut iv = tokio::time::interval(cfg.fallback.unwrap_or(REATTACH_INTERVAL));
    iv.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

    loop {
        tokio::select! {
            biased;
            _ = shutdown.changed() => { drop(watcher); break; }
            msg = rx.recv() => {
                match msg {
                    Some(WatchMsg::Changed(p)) => {
                        // Coalesce bursts: drain remaining signals and run
                        // one sync — poll_once reads the current state, so
                        // intermediate signals can be dropped safely.
                        while let Ok(rest) = rx.try_recv() {
                            if matches!(rest, WatchMsg::BackendError) {
                                watcher = None;
                            }
                        }
                        tracing::debug!(?p, "watch event -> sync");
                        sync_once(account.clone()).await;
                    }
                    Some(WatchMsg::BackendError) => {
                        tracing::warn!("watcher backend error; re-attaching on next tick");
                        watcher = None;
                    }
                    None => {} // channel closed (watcher dropped) -> next tick rebuilds
                }
            }
            _ = iv.tick() => {
                if watcher.is_none() {
                    watcher = rebuild_watcher(&tx, &watch_dir, cfg.debounce);
                }
                if cfg.fallback.is_some() && account.changed() {
                    sync_once(account.clone()).await;
                }
            }
        }
    }
    Ok(())
}

async fn sync_once(account: Arc<AccountSync>) {
    // poll_once does blocking IO (WAL copy + SQLCipher open) — run it on
    // the blocking pool; the mirror mutex + store write lock serialize
    // overlapping passes.
    match tokio::task::spawn_blocking(move || account.poll_once()).await {
        Ok(Ok(_)) => {}
        Ok(Err(e)) => tracing::warn!("watch sync error: {e:#}"),
        Err(e) => tracing::error!("sync task panicked: {e}"),
    }
}

/// Create the debouncer and attach the watch; `None` on failure (the
/// fallback tick retries). Dropping the `Debouncer` stops its threads.
fn rebuild_watcher(
    tx: &mpsc::UnboundedSender<WatchMsg>,
    watch_dir: &Path,
    debounce: Duration,
) -> Option<Debouncer<RecommendedWatcher>> {
    let handler_tx = tx.clone();
    let handler = move |res: DebounceEventResult| match res {
        Ok(events) => {
            for e in events {
                if is_relevant(e.path.file_name().unwrap_or_default()) {
                    let _ = handler_tx.send(WatchMsg::Changed(e.path));
                }
            }
        }
        Err(_) => {
            let _ = handler_tx.send(WatchMsg::BackendError);
        }
    };
    let mut debouncer = match new_debouncer_opt(
        DebounceConfig::default().with_timeout(debounce).with_batch_mode(true),
        handler,
    ) {
        Ok(d) => d,
        Err(e) => {
            tracing::warn!("create watcher failed: {e}");
            return None;
        }
    };
    if let Err(e) = debouncer.watcher().watch(watch_dir, RecursiveMode::NonRecursive) {
        tracing::warn!(
            "watch {} failed: {e}（目录可能不存在，兜底轮询将重试）",
            watch_dir.display()
        );
        return None;
    }
    tracing::info!("watching {} (debounce {:?})", watch_dir.display(), debounce);
    Some(debouncer)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn filename_filter() {
        assert!(is_relevant(OsStr::new("nt_msg.db")));
        assert!(is_relevant(OsStr::new("nt_msg.db-wal")));
        assert!(is_relevant(OsStr::new("nt_msg.db-shm")));
        assert!(is_relevant(OsStr::new("NT_MSG.DB-WAL"))); // case-insensitive
        assert!(!is_relevant(OsStr::new("nt_uid_mapping.db")));
        assert!(!is_relevant(OsStr::new("nt_msg_log.db")));
        assert!(!is_relevant(OsStr::new("db_storage")));
        assert!(!is_relevant(OsStr::new("")));
    }
}
