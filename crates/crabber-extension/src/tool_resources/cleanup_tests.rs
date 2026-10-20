use super::{ResourceFailure, ToolResourceOwner};
use std::{
    sync::{
        Arc,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};

#[tokio::test]
async fn phase_cleanup_retains_join_after_output_handle_abort_and_timeout() {
    let owner = ToolResourceOwner::new();
    let tracker = owner.cleanup_tracker();
    let (release, wait) = tokio::sync::oneshot::channel();
    let handle = tracker.spawn(async move {
        wait.await.unwrap();
        42
    });
    handle.abort();
    let report = owner.drain(Duration::ZERO).await;
    assert!(report.sealed);
    assert_eq!(report.pending, 1);
    assert_eq!(report.failures, [] as [ResourceFailure; 0]);
    assert!(tracker.is_closing());
    release.send(()).unwrap();
    let report = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, 0);
    assert_eq!(report.joined, 1);
    assert_eq!(report.failures, [] as [ResourceFailure; 0]);
    assert_eq!(tracker.pending(), 0);
}

#[tokio::test]
async fn phase_cleanup_panic_remains_a_resource_failure() {
    let owner = ToolResourceOwner::new();
    let handle = owner
        .cleanup_tracker()
        .spawn(async { panic!("cleanup failed") });
    let report = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, vec![ResourceFailure::Panicked]);
    assert!(handle.await.unwrap_err().is_panic());
    assert_eq!(owner.drain(Duration::ZERO).await, report);
}

#[tokio::test]
async fn retained_tracker_cannot_start_cleanup_after_phase_sealing() {
    let owner = ToolResourceOwner::new();
    let tracker = owner.cleanup_tracker();
    owner.close();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    let rejected = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        tracker.spawn(async move {
            flag.store(true, Ordering::SeqCst);
        })
    }));
    assert!(rejected.is_err());
    tokio::task::yield_now().await;
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(tracker.pending(), 0);
    let report = owner.drain(Duration::ZERO).await;
    assert_eq!(report.joined, 0);
    assert_eq!(report.pending, 0);
}

#[tokio::test]
async fn phase_cleanup_output_and_close_signal_preserve_callback_contract() {
    let owner = ToolResourceOwner::new();
    let tracker = owner.cleanup_tracker();
    let close = tracker.closing();
    let output = tracker.spawn(async move {
        close.await;
        "closed"
    });
    let report = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(report.failures, [] as [ResourceFailure; 0]);
    assert_eq!(report.pending, 0);
    assert_eq!(output.await.unwrap(), "closed");
}

#[tokio::test]
async fn checked_phase_cleanup_retains_declared_failure() {
    let owner = ToolResourceOwner::new();
    let tracker = owner.cleanup_tracker();
    tracker
        .spawn_checked(async { Err(ResourceFailure::CleanupFailed) })
        .unwrap();
    let report = owner.drain(Duration::from_secs(1)).await;
    assert_eq!(report.pending, 0);
    assert_eq!(report.failures, vec![ResourceFailure::CleanupFailed]);
    assert_eq!(tracker.pending(), 0);
    assert_eq!(owner.drain(Duration::ZERO).await, report);
}

#[tokio::test]
async fn ordinary_tracker_rejects_checked_cleanup_without_polling() {
    let tracker = crate::CleanupOwner::new().tracker();
    let ran = Arc::new(AtomicBool::new(false));
    let flag = ran.clone();
    assert_eq!(
        tracker.spawn_checked(async move {
            flag.store(true, Ordering::SeqCst);
            Ok(())
        }),
        Err(super::ResourceRegistrationError::Unsupported)
    );
    tokio::task::yield_now().await;
    assert!(!ran.load(Ordering::SeqCst));
    assert_eq!(tracker.pending(), 0);
}
