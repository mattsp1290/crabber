//! Extra holds delay existing reference ACKs through the SAME original join.
mod release_holds {
    mod acquisition;
    mod creation;
    pub(super) mod fixture;
}
use crabber_providers::{ProviderReleaseHolds, Streamer};
use release_holds::fixture::*;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn opaque_stream_holds_both_wrapper_orders_until_original_join() {
    for outer_lease in [false, true] {
        for panic in [false, true] {
            let seen = Seen::new(panic);
            let released = Arc::new(AtomicUsize::new(0));
            let bound = binding(Arc::new(Backend {
                seen: seen.clone(),
                original: None,
            }));
            let (bound, observer) = if outer_lease {
                let (bound, observer) = bound.observe_release();
                (
                    bound.retain_service_lease(Arc::new(Lease(released.clone()))),
                    observer,
                )
            } else {
                bound
                    .retain_service_lease(Arc::new(Lease(released.clone())))
                    .observe_release()
            };
            let stream = bound.streamer().stream(request()).await.unwrap();
            drop(bound);
            drop(stream);
            seen.entered.notified().await;
            let pending_before = !observer.is_released() && released.load(Ordering::SeqCst) == 0;
            seen.release();
            seen.returned.notified().await;
            let pending_unobserved =
                !observer.is_released() && released.load(Ordering::SeqCst) == 0;
            seen.observe.add_permits(1);
            seen.finish().await;
            assert!(pending_before && pending_unobserved);
            assert_eq!(observer.is_released(), !panic);
            assert_eq!(released.load(Ordering::SeqCst), usize::from(!panic));
        }
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn never_called_streamer_drop_captures_full_over_limit_frame() {
    let seen = Seen::new(false);
    let mut bound = binding(Arc::new(Backend {
        seen: seen.clone(),
        original: Some(Original(seen.clone())),
    }));
    let mut observers = vec![];
    for _ in 0..80 {
        let (next, observer) = bound.observe_release();
        bound = next;
        observers.push(observer);
    }
    drop(bound);
    seen.entered.notified().await;
    let pending = observers.iter().all(|observer| !observer.is_released());
    seen.release();
    seen.returned.notified().await;
    let unobserved = observers.iter().all(|observer| !observer.is_released());
    seen.observe.add_permits(1);
    seen.finish().await;
    assert!(pending && unobserved);
    assert!(
        observers
            .iter()
            .all(crabber_providers::ProviderReleaseObserver::is_released)
    );
    assert!(seen.over_limit.load(Ordering::SeqCst));
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn default_and_hostile_early_drop_cannot_release_before_outer_owners() {
    struct Early {
        seen: Arc<Seen>,
        capture: bool,
    }
    #[async_trait::async_trait]
    impl Streamer for Early {
        async fn stream(
            &self,
            _: crabber_providers::ModelRequest,
        ) -> Result<crabber_providers::DeltaStream, crabber_providers::ProviderError> {
            if self.capture {
                drop(ProviderReleaseHolds::capture_current());
            }
            Ok(Box::pin(Original(self.seen.clone())))
        }
    }
    for capture in [false, true] {
        let seen = Seen::new(false);
        let (bound, observer) = binding(Arc::new(Early {
            seen: seen.clone(),
            capture,
        }))
        .observe_release();
        let stream = bound.streamer().stream(request()).await.unwrap();
        drop(bound);
        let (release, ready) = std::sync::mpsc::channel();
        let original_drop = tokio::task::spawn_blocking(move || {
            ready.recv().unwrap();
            drop(stream);
        });
        release.send(()).unwrap();
        seen.entered.notified().await;
        let pending = !observer.is_released();
        seen.release();
        original_drop.await.unwrap();
        assert!(pending);
        assert!(observer.is_released());
    }
    assert!(
        ProviderReleaseHolds::capture_current()
            .check_admission_limit(0)
            .is_ok()
    );
}
