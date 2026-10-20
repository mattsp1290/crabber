//! The original stream-creation future can outlive its returned facade future.
use super::fixture::*;
use crabber_providers::{DeltaStream, ModelRequest, ProviderError, ProviderReleaseHolds, Streamer};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
struct Creation {
    original: Option<Original>,
    seen: Arc<Seen>,
    holds: ProviderReleaseHolds,
}
impl Future for Creation {
    type Output = Result<DeltaStream, ProviderError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for Creation {
    fn drop(&mut self) {
        retain_original(
            &self.seen,
            self.original.take().unwrap(),
            std::mem::take(&mut self.holds),
        );
    }
}
struct Backend(Arc<Seen>);
#[async_trait::async_trait]
impl Streamer for Backend {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let holds = ProviderReleaseHolds::capture_current();
        assert!(holds.check_admission_limit(0).is_err());
        Creation {
            original: Some(Original(self.0.clone())),
            seen: self.0.clone(),
            holds,
        }
        .await
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn pending_original_stream_creation_keeps_signal_and_lease_until_observed_join() {
    for panic in [false, true] {
        let seen = Seen::new(panic);
        let released = Arc::new(AtomicUsize::new(0));
        let (bound, observer) = binding(Arc::new(Backend(seen.clone())))
            .retain_service_lease(Arc::new(Lease(released.clone())))
            .observe_release();
        let streamer = bound.streamer();
        let mut future = Box::pin(streamer.stream(request()));
        assert!(futures::poll!(future.as_mut()).is_pending());
        drop(future);
        drop(streamer);
        drop(bound);
        seen.entered.notified().await;
        let pending = !observer.is_released() && released.load(Ordering::SeqCst) == 0;
        seen.release();
        seen.returned.notified().await;
        let unobserved = !observer.is_released() && released.load(Ordering::SeqCst) == 0;
        seen.observe.add_permits(1);
        seen.finish().await;
        assert!(pending && unobserved);
        assert_eq!(observer.is_released(), !panic);
        assert_eq!(released.load(Ordering::SeqCst), usize::from(!panic));
    }
}
