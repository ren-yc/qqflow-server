//! Lifecycle regressions built from encrypted fixture databases.

use super::*;
use crate::testing::{FAKE_KEY, open_fake_source};

struct Fixture(std::path::PathBuf);

impl Fixture {
    fn new() -> Self {
        static SEQ: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        Self(std::env::temp_dir().join(format!(
            "qqflow_lifecycle_{}_{}", std::process::id(), SEQ.fetch_add(1, Ordering::SeqCst),
        )))
    }

    fn prepare(&self, state: &Arc<AppState>, qq: &str) -> (RegistrationToken, Arc<sync::AccountSync>) {
        static SOURCE: std::sync::atomic::AtomicU64 = std::sync::atomic::AtomicU64::new(0);
        let dir = self.0.join(SOURCE.fetch_add(1, Ordering::SeqCst).to_string()).join(qq).join("nt_db");
        let (_writer, path) = open_fake_source(&dir, 0);
        let info = DbInfo { qq: qq.into(), path: path.clone() };
        let BindOutcome::Bound(token) = begin_indexing_with_source(state, qq, Some(info)) else {
            panic!("fixture must acquire the account binding");
        };
        let mut reader = LiveReader::new(path.clone(), FAKE_KEY.into());
        reader.open().unwrap();
        let st = index::build_with(reader.acquire().unwrap(), &dir, FAKE_KEY, None).unwrap();
        assert!(install_index(&state.init.lifecycle, &state.store, &state.bus, st, || {
            !token.is_cancelled(&state.init)
        }));
        let account = Arc::new(sync::AccountSync::new(
            qq.into(), Arc::new(Mutex::new(reader)), state.store.clone(), state.bus.clone(),
            path, dir, FAKE_KEY.into(),
        ));
        (token, account)
    }
}

impl Drop for Fixture {
    fn drop(&mut self) {
        let _ = std::fs::remove_dir_all(&self.0);
    }
}

fn idle_watcher() -> tokio::task::JoinHandle<Result<()>> {
    tokio::spawn(std::future::pending())
}

fn rebuild_fixture(state: &AppState, account: &sync::AccountSync) -> Store {
    let source = state.init.find_db(&account.qq).unwrap();
    let mut reader = account.reader.lock();
    index::build_with(reader.acquire().unwrap(), source.path.parent().unwrap(), FAKE_KEY, None).unwrap()
}

#[tokio::test]
async fn stale_cleanup_and_watcher_cannot_remove_new_registration() {
    let fixture = Fixture::new();
    let state = tests::state_with_accounts(&[]);
    let (token, old) = fixture.prepare(&state, "10001");
    assert!(activate_account(&state, token, old.clone(), 8, idle_watcher));
    deregister_account(&state, "10001", false);
    let (fresh, current) = fixture.prepare(&state, "10001");
    assert!(activate_account(&state, fresh, current.clone(), 8, idle_watcher));

    assert!(!state.sync.unregister_if(&old));
    let (dropped_tx, dropped_rx) = tokio::sync::oneshot::channel::<()>();
    let (started_tx, started_rx) = tokio::sync::oneshot::channel();
    let rejected = tokio::spawn(async move {
        let _drop_signal = dropped_tx;
        started_tx.send(()).unwrap();
        std::future::pending::<Result<()>>().await
    });
    started_rx.await.unwrap();
    // Even a mistakenly permissive predicate must not attach an old handle.
    assert!(!state.sync.attach_watcher_if_current(&old, rejected, &|| true));
    assert!(tokio::time::timeout(std::time::Duration::from_secs(5), dropped_rx)
        .await.expect("rejected watcher must be cancelled").is_err());
    assert!(Arc::ptr_eq(&state.sync.snapshot()[0], &current));
    assert!(!current.is_stopped());
    assert!(state.ready.load(Ordering::SeqCst));
    deregister_account(&state, "10001", false);
}

#[tokio::test]
async fn queued_initialization_cannot_replace_a_new_account_index() {
    let fixture = Fixture::new();
    for replacement in ["10001", "10002"] {
        let state = tests::state_with_accounts(&[]);
        let (old_token, old) = fixture.prepare(&state, "10001");
        let old_info = state.init.find_db("10001").unwrap();
        deregister_account(&state, "10001", false);
        let (fresh, current) = fixture.prepare(&state, replacement);
        assert!(activate_account(&state, fresh, current.clone(), 8, idle_watcher));
        let history_len = state.bus.history().lock().replay_since(0).len();
        init_account(&state, old_info, FAKE_KEY.into(), old_token).await;
        assert_eq!(state.bus.history().lock().replay_since(0).len(), history_len);
        assert!(Arc::ptr_eq(&state.sync.snapshot()[0], &current));
        assert!(state.ready.load(Ordering::SeqCst));
        // The same rejected task cannot later publish its handle or spawn work.
        assert!(!activate_account(&state, old_token, old, 8, || panic!("stale watcher spawned")));
        assert_eq!(bound_account(&state.accounts.read()).unwrap().qq, replacement);
        deregister_account(&state, replacement, false);
    }
}

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn lifecycle_commit_excludes_deregister_through_watcher_and_ready() {
    let fixture = Fixture::new();
    let state = tests::state_with_accounts(&[]);
    let (token, account) = fixture.prepare(&state, "10001");
    let (entered_tx, entered_rx) = std::sync::mpsc::channel();
    let (attempt_tx, attempt_rx) = std::sync::mpsc::channel();
    let state_for_remove = state.clone();
    let remover = std::thread::spawn(move || {
        entered_rx.recv().unwrap();
        attempt_tx.send(()).unwrap();
        deregister_account(&state_for_remove, "10001", false)
    });
    assert!(activate_account(&state, token, account.clone(), 8, || {
        entered_tx.send(()).unwrap();
        attempt_rx.recv().unwrap();
        // This hook is after handle registration and before watcher/ready.
        // The remover cannot enter the lifecycle critical section here.
        assert!(state.init.lifecycle.try_lock().is_none());
        assert!(state.sync.snapshot().iter().any(|a| Arc::ptr_eq(a, &account)));
        idle_watcher()
    }));
    assert!(matches!(remover.join().unwrap(), DeregisterOutcome::Deregistered { .. }));
    assert!(account.is_stopped());
    assert!(state.sync.snapshot().is_empty());
    assert!(state.store.read().convs.is_empty());
    assert!(!state.ready.load(Ordering::SeqCst));
    let history = state.bus.history();
    let events = history.lock().replay_since(0);
    assert_eq!(events.len(), 1, "only the reset baseline survives");
    assert_eq!(events[0].1.last_rowid_group, Some(0));
}

#[test]
fn cancelled_build_cannot_install_after_deregister() {
    let fixture = Fixture::new();
    let state = tests::state_with_accounts(&[]);
    let (token, _account) = fixture.prepare(&state, "10001");
    let built = rebuild_fixture(&state, &_account);
    deregister_account(&state, "10001", false);
    assert!(!install_index(&state.init.lifecycle, &state.store, &state.bus, built, || {
        assert!(state.init.lifecycle.try_lock().is_none());
        assert!(state.store.try_read().is_none(), "validation must hold the store write lock");
        !token.is_cancelled(&state.init)
    }));
    assert!(state.store.read().convs.is_empty());
    let events = state.bus.history().lock().replay_since(0);
    assert_eq!(events.len(), 1, "stale installation must not publish after reset");
    assert_eq!(events[0].1.last_rowid_group, Some(0));
}

#[test]
fn install_index_rebaselines_subscribers() {
    let fixture = Fixture::new();
    let state = tests::state_with_accounts(&[]);
    let (token, _account) = fixture.prepare(&state, "10001");
    let built = rebuild_fixture(&state, &_account);
    let expected = (built.watermark_group, built.watermark_c2c);
    assert!(expected.0 > 0 && expected.1 > 0);
    let mut events = state.bus.subscribe();
    assert!(install_index(&state.init.lifecycle, &state.store, &state.bus, built, || {
        !token.is_cancelled(&state.init)
    }));
    let baseline = events.try_recv().unwrap().event;
    assert_eq!(baseline.event, "sync");
    assert_eq!((baseline.last_rowid_group, baseline.last_rowid_c2c), (Some(expected.0), Some(expected.1)));
}

#[test]
fn source_path_only_changes_for_winning_binding() {
    let fixture = Fixture::new();
    let state = tests::state_with_accounts(&[]);
    let (_token, _account) = fixture.prepare(&state, "10001");
    let original = state.init.find_db("10001").unwrap().path;
    let ignored = DbInfo { qq: "10001".into(), path: fixture.0.join("ignored.db") };
    assert!(matches!(begin_indexing_with_source(&state, "10001", Some(ignored)), BindOutcome::SameQq(_)));
    assert_eq!(state.init.find_db("10001").unwrap().path, original);
    let other = DbInfo { qq: "10002".into(), path: fixture.0.join("other.db") };
    assert!(matches!(begin_indexing_with_source(&state, "10002", Some(other)), BindOutcome::Occupied { .. }));
    assert!(state.init.find_db("10002").is_none());
}
