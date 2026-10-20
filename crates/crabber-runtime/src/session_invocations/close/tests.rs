//! Local supervisor observation tests; no native owner is fabricated.
use super::super::{Invocation, Request, SessionInvocationStatus, SessionInvocations};
use std::sync::{
    Arc, Mutex,
    atomic::{AtomicBool, Ordering},
};
use tokio::sync::Semaphore;
use tokio_util::sync::CancellationToken;

fn retained(manager: &SessionInvocations) -> Arc<Invocation> {
    let owner = Arc::new(Invocation {
        session: crabber_core::SessionId::new(),
        request: Request::new(
            "local-close-fixture".into(),
            crabber_core::InputFingerprint::new("a".repeat(64)).unwrap(),
        ),
        cancel: CancellationToken::new(),
        cancel_observation: Mutex::new(None),
        status: Mutex::new(SessionInvocationStatus::Active),
        resources: crabber_extension::ToolResourceOwner::new(),
        join: tokio::sync::Mutex::new(None),
        observed: AtomicBool::new(false),
        join_failed: AtomicBool::new(false),
    });
    manager.owners.lock().unwrap().insert(
        (owner.session.clone(), owner.request.id.clone()),
        owner.clone(),
    );
    owner
}
#[tokio::test]
async fn cut_signals_every_original_before_observing_and_preserves_pending() {
    let manager = SessionInvocations::default();
    let release = Arc::new(Semaphore::new(0));
    for _ in 0..2 {
        let owner = retained(&manager);
        let original = owner.clone();
        let release = release.clone();
        *owner.join.lock().await = Some(tokio::spawn(async move {
            original.cancel.cancelled().await;
            release.acquire().await.unwrap().forget();
            original.resources.close();
            original.status(SessionInvocationStatus::Joined);
        }));
    }
    let cut = manager.begin_close();
    assert!(manager.closed.load(Ordering::Acquire));
    cut.signal_all();
    assert!(cut.owners.iter().all(|owner| owner.cancel.is_cancelled()));
    let pending = cut.observe_until(tokio::time::Instant::now()).await;
    assert_eq!(pending.len(), 2);
    assert!(pending.iter().all(|report| !report.local_joined));
    release.add_permits(2);
    let observed = cut
        .observe_until(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
        .await;
    assert!(
        observed
            .iter()
            .all(|report| report.local_joined && report.status == SessionInvocationStatus::Joined)
    );
    manager.reap_finished().await;
    assert_eq!(manager.snapshot_close(), []);
}
#[tokio::test]
async fn failed_original_remains_sticky_after_normal_reap_before_close() {
    let manager = SessionInvocations::default();
    let owner = retained(&manager);
    *owner.join.lock().await = Some(tokio::spawn(async { panic!("actual supervisor failure") }));
    tokio::time::timeout(std::time::Duration::from_secs(1), async {
        while !owner.join.lock().await.as_ref().unwrap().is_finished() {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    manager.reap_finished().await;
    let cut = manager.begin_close();
    let reports = cut.snapshot();
    assert!(reports[0].local_joined);
    assert_eq!(reports[0].status, SessionInvocationStatus::Uncertain);
    manager.reap_finished().await;
    assert_eq!(manager.snapshot_close(), reports);
}

#[tokio::test]
async fn actual_registry_identity_is_detection_only_and_never_crosses_registries() {
    let manager = SessionInvocations::default();
    let unrelated = SessionInvocations::default();
    let owner = retained(&manager);
    assert!(!manager.is_current_owner());
    super::scope(owner, async {
        assert!(manager.is_current_owner());
        assert!(!unrelated.is_current_owner());
        let bare = manager.clone();
        assert!(
            !tokio::spawn(async move { bare.is_current_owner() })
                .await
                .unwrap()
        );
    })
    .await;
    assert!(!manager.is_current_owner());
}

struct JoinPayload {
    owner: std::sync::Weak<Invocation>,
    disposed_unlocked: Arc<AtomicBool>,
}
impl Drop for JoinPayload {
    fn drop(&mut self) {
        if let Some(owner) = self.owner.upgrade() {
            let unlocked = owner.join.try_lock().is_ok();
            let report = owner.close_report();
            self.disposed_unlocked.store(
                unlocked
                    && report.local_joined
                    && report.status == SessionInvocationStatus::Uncertain,
                Ordering::Release,
            );
        }
    }
}
#[tokio::test]
async fn every_join_observer_publishes_failure_before_unlocked_payload_disposal() {
    for mode in 0..3 {
        let manager = SessionInvocations::default();
        let owner = retained(&manager);
        owner.status(SessionInvocationStatus::Joined);
        owner.resources.close();
        let disposed = Arc::new(AtomicBool::new(false));
        let payload = JoinPayload {
            owner: Arc::downgrade(&owner),
            disposed_unlocked: disposed.clone(),
        };
        *owner.join.lock().await =
            Some(tokio::spawn(async move { std::panic::panic_any(payload) }));
        tokio::time::timeout(std::time::Duration::from_secs(1), async {
            while !owner.join.lock().await.as_ref().unwrap().is_finished() {
                tokio::task::yield_now().await;
            }
        })
        .await
        .unwrap();
        match mode {
            0 => manager.reap_finished().await,
            1 => {
                manager
                    .drain(&owner.session, false, std::time::Duration::from_secs(1))
                    .await;
            }
            _ => {
                manager
                    .begin_close()
                    .observe_until(tokio::time::Instant::now() + std::time::Duration::from_secs(1))
                    .await;
            }
        }
        assert!(disposed.load(Ordering::Acquire));
        let reports = manager.snapshot_close();
        assert_eq!(reports.len(), 1);
        assert!(reports[0].local_joined);
        assert_eq!(reports[0].status, SessionInvocationStatus::Uncertain);
        manager.reap_finished().await;
        assert_eq!(manager.snapshot_close(), reports);
    }
}
