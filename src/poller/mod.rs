//! Poll loop: watches the source WAL for new rows and pushes SSE events.
//!
//! QQ writes new messages into nt_msg.db-wal (WAL mode); the mirror sync
//! copies the WAL cheaply, and rows with rowid > watermark are appended to
//! the in-memory store. Recall messages ("你猜猜撤回了什么") are detected
//! by the parser and emitted as `message.revoke`.

pub mod events;

use std::sync::Arc;
use std::time::Duration;

use anyhow::Result;
use parking_lot::RwLock;
use tokio::sync::broadcast;

use crate::db::decrypt;
use crate::db::mirror::Mirror;
use crate::parser::types::{ChatType, MsgType};
use crate::store::index;
use crate::store::Store;

pub use events::Event;

/// Spawn one poll task per account. Runs until `shutdown` turns true.
pub async fn spawn(
    mut mirror: Mirror,
    key: String,
    store: Arc<RwLock<Store>>,
    tx: broadcast::Sender<Event>,
    interval: Duration,
    mut shutdown: tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    tokio::task::spawn_blocking(move || poll_loop(&mut mirror, &key, &store, &tx, interval, &mut shutdown))
        .await
        .map_err(|e| anyhow::anyhow!("poll task panicked: {e}"))?
}

fn poll_loop(
    mirror: &mut Mirror,
    key: &str,
    store: &Arc<RwLock<Store>>,
    tx: &broadcast::Sender<Event>,
    interval: Duration,
    shutdown: &mut tokio::sync::watch::Receiver<bool>,
) -> Result<()> {
    loop {
        if *shutdown.borrow() {
            break;
        }
        let started = std::time::Instant::now();
        if let Err(e) = poll_once(mirror, key, store, tx) {
            tracing::warn!("poll error: {e:#}");
        }
        let wait = interval.saturating_sub(started.elapsed());
        std::thread::sleep(wait);
    }
    Ok(())
}

fn poll_once(
    mirror: &mut Mirror,
    key: &str,
    store: &Arc<RwLock<Store>>,
    tx: &broadcast::Sender<Event>,
) -> Result<()> {
    // Refresh WAL copy; rebuild the whole mirror on source checkpoint.
    mirror.sync()?;
    let conn = decrypt::open_decrypted(&mirror.main_path, key)?;

    let mut guard = store.write();
    let wm_g = guard.watermark_group;
    let wm_c = guard.watermark_c2c;

    let (new_wm_g, new_g) = index::append_new(&conn, ChatType::Group, &mut guard, wm_g)?;
    let (new_wm_c, new_c) = index::append_new(&conn, ChatType::C2c, &mut guard, wm_c)?;

    for r in new_g.into_iter().chain(new_c) {
        let group_name = guard
            .conversation(r.chat_type, &r.talker)
            .map(|c| c.name.clone());
        let ev = if r.parsed.msg_type == MsgType::Recall {
            Event::message_revoke(
                r.chat_type,
                r.talker.clone(),
                group_name,
                r.rowid,
                Some(r.from_nick.clone()),
                r.parsed.content.clone(),
                r.ts,
            )
        } else {
            Event::message_new(
                r.chat_type,
                r.talker.clone(),
                group_name,
                r.rowid,
                Some(r.from_nick.clone()),
                r.parsed.content.clone(),
                r.ts,
            )
        };
        let _ = tx.send(ev);
    }

    guard.watermark_group = new_wm_g;
    guard.watermark_c2c = new_wm_c;
    Ok(())
}
