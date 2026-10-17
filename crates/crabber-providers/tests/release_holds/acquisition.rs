use super::fixture::*;
use crabber_providers::{
    BoundProvider, ProviderBindingError, ProviderReleaseHolds, ProviderResourceContext,
    ProviderResourceFailure, ProviderResourceHost, ProviderResourceRegistrationError,
    ProviderResourceTask,
};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
struct Host(Mutex<Vec<tokio::task::JoinHandle<Result<(), ProviderResourceFailure>>>>);
impl ProviderResourceHost for Host {
    fn register(
        &self,
        work: ProviderResourceTask,
    ) -> Result<(), ProviderResourceRegistrationError> {
        self.0.lock().unwrap().push(tokio::spawn(work));
        Ok(())
    }
    fn closing(&self) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
struct FutureFacade {
    original: Option<Original>,
    seen: Arc<Seen>,
    creation: ProviderReleaseHolds,
}
impl Future for FutureFacade {
    type Output = Result<BoundProvider, ProviderBindingError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for FutureFacade {
    fn drop(&mut self) {
        retain_original(
            &self.seen,
            self.original.take().unwrap(),
            std::mem::take(&mut self.creation),
        );
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn binding_future_drop_keeps_original_ticket_pending_through_actual_join() {
    let host = Arc::new(Host(Mutex::new(vec![])));
    let context = ProviderResourceContext::from_host(
        crabber_core::SessionContext::new(
            crabber_core::SessionId::new(),
            "workspace",
            "/workspace",
        ),
        host.clone(),
    );
    let seen = Seen::new(false);
    let mut acquisition = Box::pin(context.acquire_binding(|| {
        assert!(
            ProviderReleaseHolds::capture_current()
                .check_admission_limit(0)
                .is_err()
        );
        FutureFacade {
            original: Some(Original(seen.clone())),
            seen: seen.clone(),
            creation: ProviderReleaseHolds::capture_current(),
        }
    }));
    assert!(futures::poll!(acquisition.as_mut()).is_pending());
    drop(acquisition);
    seen.entered.notified().await;
    let mut cleanup = host.0.lock().unwrap().pop().unwrap();
    let before = futures::poll!(&mut cleanup).is_pending();
    seen.release();
    seen.returned.notified().await;
    let unobserved = futures::poll!(&mut cleanup).is_pending();
    seen.observe.add_permits(1);
    seen.finish().await;
    assert_eq!(cleanup.await.unwrap(), Ok(()));
    assert!(before && unobserved);
}
