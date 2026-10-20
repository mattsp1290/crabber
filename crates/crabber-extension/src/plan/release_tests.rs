use super::*;
use std::sync::atomic::{AtomicBool, Ordering};

fn plan(release: impl FnOnce() + Send + Sync + 'static) -> RunPlan {
    let mut plan = StaticPlanProvider::new(Vec::new(), Vec::new()).plan;
    plan.lease = Some(Arc::new(PlanLease(Some(Box::new(release)))));
    plan
}

#[tokio::test]
async fn acquisition_release_waits_for_last_clone_and_underlying_callback() {
    let callback_finished = Arc::new(AtomicBool::new(false));
    let flag = callback_finished.clone();
    let (plan, observer) = plan(move || flag.store(true, Ordering::SeqCst)).observe_release();
    let clone = plan.clone();
    let second_observer = observer.clone();
    let waiter = tokio::spawn(async move {
        second_observer.released().await;
        assert!(callback_finished.load(Ordering::SeqCst));
    });
    plan.release();
    assert!(!observer.is_released());
    clone.release();
    waiter.await.unwrap();
    assert!(observer.is_released());
    // Completed observations also support waiters arriving after disposal.
    observer.released().await;
}

#[test]
fn release_observer_neither_pins_nor_certifies_shared_lease() {
    let finished = Arc::new(AtomicBool::new(false));
    let flag = finished.clone();
    let plan = plan(move || flag.store(true, Ordering::SeqCst));
    let previously_existing_clone = plan.clone();
    let (plan, observer) = plan.observe_release();
    plan.release();
    assert!(observer.is_released());
    assert!(!finished.load(Ordering::SeqCst));
    previously_existing_clone.release();
    assert!(finished.load(Ordering::SeqCst));
}

#[test]
fn release_callback_panic_does_not_acknowledge_disposal() {
    let (plan, observer) = plan(|| panic!("failed lease release")).observe_release();
    assert!(std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| plan.release())).is_err());
    assert!(!observer.is_released());
}

#[test]
fn nested_observations_release_in_order_without_pinning_each_other() {
    let (plan, inner) = plan(|| {}).observe_release();
    let inner_at_release = inner.clone();
    let original = plan.lease.clone();
    let mut plan = plan;
    plan.lease = Some(Arc::new(PlanLease(Some(Box::new(move || {
        drop(original);
        assert!(inner_at_release.is_released());
    })))));
    let (plan, outer) = plan.observe_release();
    assert!(!inner.is_released());
    assert!(!outer.is_released());
    plan.release();
    assert!(inner.is_released());
    assert!(outer.is_released());
}
