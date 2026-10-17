use super::*;
use std::sync::atomic::{AtomicUsize, Ordering};
#[test]
fn scope_covers_unpolled_constructor_capture_and_original_poll_disposal() {
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            assert!(
                ProviderReleaseHolds::capture_current()
                    .check_admission_limit(0)
                    .is_err()
            );
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (signal, observer) = super::super::release::completion();
    let holds = ProviderReleaseHolds::default().with_original(signal.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Probe(drops.clone());
    let future = holds.scope(move || {
        assert!(
            ProviderReleaseHolds::capture_current()
                .check_admission_limit(0)
                .is_err()
        );
        async move { drop(probe) }
    });
    drop(future);
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    let future = holds.scope(|| async {
        assert!(
            ProviderReleaseHolds::capture_current()
                .check_admission_limit(0)
                .is_err()
        );
        Probe(drops.clone())
    });
    let probe = futures::executor::block_on(future);
    holds.sync_scope(|| drop(probe));
    assert_eq!(drops.load(Ordering::SeqCst), 2);
    assert!(
        ProviderReleaseHolds::capture_current()
            .check_admission_limit(0)
            .is_ok()
    );
    drop(signal);
    assert!(!observer.is_released());
    drop(holds);
    assert!(observer.is_released());
}
#[test]
fn full_capture_survives_admission_refusal_and_checked_overflow() {
    let (signal, observer) = super::super::release::completion();
    let base = ProviderReleaseHolds {
        head: Some(Arc::new(Frame {
            token: Some(signal.clone()),
            outer: None,
            depth: Some(usize::MAX),
        })),
    };
    let holds = base.with_original(signal.clone());
    assert!(holds.check_admission_limit(usize::MAX).is_err());
    let captured = holds.sync_scope(ProviderReleaseHolds::capture_current);
    drop(holds);
    drop(signal);
    assert!(!observer.is_released());
    drop(captured);
    assert!(observer.is_released());
}
#[test]
fn nested_token_panic_retains_outer_original_signal() {
    struct Fails;
    impl Drop for Fails {
        fn drop(&mut self) {
            panic!("actual lease disposal failed");
        }
    }
    let (signal, observer) = super::super::release::completion();
    let holds = ProviderReleaseHolds::default()
        .with_original(signal.clone())
        .with_original(Arc::new(Fails));
    drop(signal);
    assert!(catch_unwind(AssertUnwindSafe(|| drop(holds))).is_err());
    assert!(!observer.is_released());
}
#[test]
fn deep_unique_frames_are_reclaimed_without_recursive_stack_growth() {
    struct Token(Arc<AtomicUsize>);
    impl Drop for Token {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let drops = Arc::new(AtomicUsize::new(0));
    let seen = drops.clone();
    std::thread::Builder::new()
        .stack_size(64 * 1024)
        .spawn(move || {
            let mut holds = ProviderReleaseHolds::default();
            for _ in 0..100_000 {
                holds = holds.with_original(Arc::new(Token(seen.clone())));
            }
            assert!(holds.check_admission_limit(64).is_err());
            drop(holds);
        })
        .unwrap()
        .join()
        .unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 100_000);
}
#[test]
fn reentrant_context_failure_retains_original_future_and_tokens() {
    struct Probe(Arc<AtomicUsize>);
    impl Drop for Probe {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (signal, observer) = super::super::release::completion();
    let holds = ProviderReleaseHolds::default().with_original(signal.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Probe(drops.clone());
    let future = holds.scope(move || async move { drop(probe) });
    let result = CURRENT.with(|slot| {
        let _borrow = slot.borrow_mut();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
    });
    assert!(result.is_err());
    drop(signal);
    drop(holds);
    assert_eq!(
        drops.load(Ordering::SeqCst),
        0,
        "unavailable scope must not dispose unframed original work"
    );
    assert!(!observer.is_released());
}

#[test]
fn original_future_moves_between_threads_without_losing_its_frame() {
    struct Pending(Arc<AtomicUsize>);
    impl Future for Pending {
        type Output = ();
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<()> {
            assert!(
                ProviderReleaseHolds::capture_current()
                    .check_admission_limit(0)
                    .is_err()
            );
            Poll::Pending
        }
    }
    impl Drop for Pending {
        fn drop(&mut self) {
            assert!(
                ProviderReleaseHolds::capture_current()
                    .check_admission_limit(0)
                    .is_err()
            );
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }
    let (signal, observer) = super::super::release::completion();
    let holds = ProviderReleaseHolds::default().with_original(signal.clone());
    let drops = Arc::new(AtomicUsize::new(0));
    let probe = Pending(drops.clone());
    let mut future = Box::pin(holds.scope(move || {
        assert!(
            ProviderReleaseHolds::capture_current()
                .check_admission_limit(0)
                .is_err()
        );
        probe
    }));
    let future = std::thread::spawn(move || {
        let mut context = Context::from_waker(futures::task::noop_waker_ref());
        assert!(future.as_mut().poll(&mut context).is_pending());
        future
    })
    .join()
    .unwrap();
    std::thread::spawn(move || drop(future)).join().unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);
    drop(holds);
    drop(signal);
    assert!(observer.is_released());
}
