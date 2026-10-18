//! Genuine actual-supervisor join and retained-capacity boundaries.
use super::*;
use std::time::Duration;
use tokio::sync::Notify;

#[derive(Default)]
pub(super) struct Gate {
    entered: Notify,
    release: Notify,
}
impl Gate {
    pub(super) async fn wait(&self) {
        self.entered.notify_one();
        self.release.notified().await;
    }
}
fn lease(owner: &PendingDriverTasks) -> Arc<CapacityLease> {
    Arc::new(CapacityLease {
        _permit: owner.capacity().unwrap(),
    })
}
async fn report(owner: &PendingDriverTasks) -> PendingSupervisionReport {
    owner.observe_supervision(Duration::from_secs(5)).await
}
#[tokio::test]
async fn actual_epilogue_join_and_cached_legacy_output_share_one_capacity_lease() {
    let owner = PendingDriverTasks::default();
    let gate = Arc::new(Gate::default());
    *owner.inner.epilogue.lock().unwrap() = Some(gate.clone());
    let mut task = super::super::core::Task::<()>::register_body(
        &owner,
        super::super::core::Family::Initial,
        None,
        |_, _| async { Ok(TaskOutput::Stopped) },
    )
    .unwrap();
    task.start().unwrap();
    let observed = task
        .observation(super::super::core::Family::Initial)
        .await
        .unwrap();
    gate.entered.notified().await;
    let pending = owner.observe_supervision(Duration::ZERO).await;
    let while_waiting = owner.available_capacity();
    // Canceling a report never cancels the original supervisor observation.
    let cancelled = tokio::time::timeout(Duration::from_millis(10), report(&owner))
        .await
        .is_err();
    gate.release.notify_one();
    let joined = report(&owner).await;
    let cached = owner.available_capacity();
    drop(observed);
    assert_eq!(pending.pending_supervisors(), 1);
    assert!(!pending.is_complete());
    assert_eq!(while_waiting, 63);
    assert!(cancelled);
    assert!(joined.is_complete());
    assert_eq!(cached, 63);
    assert_eq!(owner.available_capacity(), 64);
}

struct PanicDrop;
impl Drop for PanicDrop {
    fn drop(&mut self) {
        panic!("literal supervisor value Drop");
    }
}
#[tokio::test]
async fn actual_supervisor_drop_panic_is_sticky_and_unit_wait_cannot_claim_clean() {
    let owner = PendingDriverTasks::default();
    let (send, receive) = oneshot::channel();
    drop(receive);
    owner.retain(
        tokio::spawn(async { Ok(TaskOutput::Business(PanicDrop)) }),
        Control::new(super::super::core::Family::Initial),
        send,
        owner.capacity().unwrap(),
        None,
    );
    let failed = report(&owner).await;
    let failed_again = owner.observe_supervision(Duration::ZERO).await;
    let unit_wait =
        tokio::time::timeout(Duration::from_millis(10), owner.wait_for_supervision()).await;
    assert_eq!(failed.failed_supervisors(), 1);
    assert!(!failed.is_complete());
    assert_eq!(failed, failed_again);
    assert!(unit_wait.is_err());
    assert_eq!(owner.available_capacity(), 63);
}

#[tokio::test]
async fn actual_aborted_supervisor_is_failed_not_successfully_joined() {
    let owner = PendingDriverTasks::default();
    let supervisor = tokio::spawn(std::future::pending::<()>());
    let abort = supervisor.abort_handle();
    owner.track(supervisor, lease(&owner));
    abort.abort();
    let failed = report(&owner).await;
    assert_eq!(failed.failed_supervisors(), 1);
    assert!(!failed.is_complete());
    assert_eq!(owner.available_capacity(), 63);
}

#[tokio::test]
async fn sticky_supervisor_failures_are_bounded_by_the_original_64_permits() {
    let owner = PendingDriverTasks::default();
    for _ in 0..MAX_PENDING {
        owner.track(
            tokio::spawn(async {
                panic!("actual supervisor failure");
            }),
            lease(&owner),
        );
    }
    let failed = report(&owner).await;
    assert_eq!(failed.pending_supervisors(), 0);
    assert_eq!(failed.failed_supervisors(), MAX_PENDING);
    assert_eq!(owner.available_capacity(), 0);
    assert!(matches!(owner.capacity(), Err(Error::Capacity)));
    assert_eq!(owner.observe_supervision(Duration::ZERO).await, failed);
}

#[tokio::test]
async fn zero_overflow_and_cancelled_reports_preserve_the_same_pending_supervisor() {
    let owner = PendingDriverTasks::default();
    let (release, ready) = oneshot::channel();
    owner.track(
        tokio::spawn(async {
            let _ = ready.await;
        }),
        lease(&owner),
    );
    let zero = owner.observe_supervision(Duration::ZERO).await;
    let overflow = owner.observe_supervision(Duration::MAX).await;
    let cancelled = tokio::time::timeout(Duration::from_millis(10), report(&owner))
        .await
        .is_err();
    release.send(()).unwrap();
    let completed = report(&owner).await;
    assert_eq!(zero.pending_supervisors(), 1);
    assert_eq!(overflow, zero);
    assert!(cancelled);
    assert!(completed.is_complete());
    assert_eq!(owner.available_capacity(), 64);
}

struct BlockingDrop {
    gate: Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>,
    entered: std::sync::mpsc::Sender<()>,
}
impl Drop for BlockingDrop {
    fn drop(&mut self) {
        let _ = self.entered.send(());
        let (lock, changed) = &*self.gate;
        let mut released = lock.lock().unwrap();
        while !*released {
            released = changed.wait(released).unwrap();
        }
    }
}
struct Release(Arc<(std::sync::Mutex<bool>, std::sync::Condvar)>);
impl Drop for Release {
    fn drop(&mut self) {
        let (lock, changed) = &*self.0;
        *lock.lock().unwrap() = true;
        changed.notify_all();
    }
}
#[test]
fn literal_original_value_drop_blocks_actual_supervisor_join_and_keeps_capacity() {
    let owner = PendingDriverTasks::default();
    let current = owner.clone();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Release(gate.clone());
    let (entered, arriving) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let (send, receive) = oneshot::channel();
                drop(receive);
                current.retain(
                    tokio::spawn(async move {
                        Ok(TaskOutput::Business(BlockingDrop { gate, entered }))
                    }),
                    Control::new(super::super::core::Family::Initial),
                    send,
                    current.capacity().unwrap(),
                    None,
                );
                report(&current).await
            })
    });
    let arrived = arriving.recv_timeout(Duration::from_secs(5)).is_ok();
    // A separate runtime can inspect while the original runtime is inside Drop.
    let pending = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(owner.observe_supervision(Duration::ZERO));
    let capacity = owner.available_capacity();
    drop(release);
    let completed = worker.join().unwrap();
    assert!(arrived);
    assert!(completed.is_complete());
    assert_eq!(pending.pending_supervisors(), 1);
    assert!(!pending.is_complete());
    assert_eq!(capacity, 63);
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn unexpected_observer_abort_retains_original_handle_and_sticky_failure() {
    let owner = PendingDriverTasks::default();
    let (release, ready) = oneshot::channel();
    let supervisor = tokio::spawn(async {
        let _ = ready.await;
    });
    let id = uuid::Uuid::new_v4();
    let entry = Arc::new(Entry {
        failed: AtomicBool::new(false),
        _capacity: lease(&owner),
        unobserved: Mutex::default(),
    });
    owner.inner.tasks.lock().unwrap().insert(id, entry.clone());
    let observer = tokio::spawn(
        observation::Observer::new(owner.inner.clone(), id, entry.clone(), supervisor).run(),
    );
    observer.abort();
    let _ = observer.await;
    let failed = report(&owner).await;
    let original = entry.unobserved.lock().unwrap().take().unwrap();
    release.send(()).unwrap();
    original.await.unwrap();
    assert_eq!(failed.failed_supervisors(), 1);
    assert!(!failed.is_complete());
    assert_eq!(owner.available_capacity(), 63);
    assert_eq!(owner.observe_supervision(Duration::ZERO).await, failed);
}

#[test]
fn join_error_is_sticky_before_untrusted_panic_payload_disposal() {
    let owner = PendingDriverTasks::default();
    let current = owner.clone();
    let gate = Arc::new((std::sync::Mutex::new(false), std::sync::Condvar::new()));
    let release = Release(gate.clone());
    let (entered, arriving) = std::sync::mpsc::channel();
    let worker = std::thread::spawn(move || {
        tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap()
            .block_on(async {
                let payload = BlockingDrop { gate, entered };
                current.track(
                    tokio::spawn(async move {
                        std::panic::panic_any(payload);
                    }),
                    lease(&current),
                );
                report(&current).await
            })
    });
    let arrived = arriving.recv_timeout(Duration::from_secs(5)).is_ok();
    let failed = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
        .block_on(owner.observe_supervision(Duration::ZERO));
    let capacity = owner.available_capacity();
    drop(release);
    let after_disposal = worker.join().unwrap();
    assert!(arrived);
    assert_eq!(failed.failed_supervisors(), 1);
    assert!(!failed.is_complete());
    assert_eq!(capacity, 63);
    assert_eq!(after_disposal, failed);
    assert_eq!(owner.available_capacity(), 63);
}
