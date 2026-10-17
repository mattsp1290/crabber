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

#[tokio::test]
async fn completed_public_acquisition_future_keeps_baseline_ready_timing() {
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
    let bound = binding(Arc::new(Backend {
        seen,
        original: None,
    }));
    let mut acquisition =
        Box::pin(context.acquire_binding(move || futures::future::ready(Ok(bound))));
    let bound = match futures::poll!(acquisition.as_mut()) {
        Poll::Ready(result) => result.unwrap(),
        Poll::Pending => panic!("immediate original acquisition did not complete"),
    };
    drop(bound);
    let mut cleanup = host.0.lock().unwrap().pop().unwrap();
    let observed = tokio::time::timeout(std::time::Duration::from_secs(1), &mut cleanup).await;
    let before_future_drop = observed.is_ok();
    drop(acquisition);
    let result = match observed {
        Ok(result) => result,
        Err(_) => cleanup.await,
    };
    assert_eq!(result.unwrap(), Ok(()));
    assert!(
        before_future_drop,
        "completed public future must not extend its original disposal ticket"
    );
}

#[tokio::test]
async fn acquisition_secondary_panic_payload_disposes_inside_original_frame() {
    struct Payload(
        Arc<std::sync::atomic::AtomicBool>,
        Arc<std::sync::atomic::AtomicUsize>,
    );
    impl Drop for Payload {
        fn drop(&mut self) {
            self.1.fetch_add(1, std::sync::atomic::Ordering::SeqCst);
            self.0.store(
                ProviderReleaseHolds::capture_current()
                    .check_admission_limit(0)
                    .is_err(),
                std::sync::atomic::Ordering::SeqCst,
            );
        }
    }
    struct Fails(
        Arc<std::sync::atomic::AtomicBool>,
        Arc<std::sync::atomic::AtomicUsize>,
    );
    impl Future for Fails {
        type Output = Result<BoundProvider, ProviderBindingError>;
        fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
            panic!("original acquisition poll failed");
        }
    }
    impl Drop for Fails {
        fn drop(&mut self) {
            std::panic::panic_any(Payload(self.0.clone(), self.1.clone()));
        }
    }
    let host = Arc::new(Host(Mutex::new(vec![])));
    let context = ProviderResourceContext::from_host(
        crabber_core::SessionContext::new(
            crabber_core::SessionId::new(),
            "workspace",
            "/workspace",
        ),
        host.clone(),
    );
    let seen = Arc::new(std::sync::atomic::AtomicBool::new(false));
    let drops = Arc::new(std::sync::atomic::AtomicUsize::new(0));
    let mut acquisition = Box::pin(context.acquire_binding(|| Fails(seen.clone(), drops.clone())));
    let mut cx = Context::from_waker(futures::task::noop_waker_ref());
    let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
        acquisition.as_mut().poll(&mut cx)
    }));
    drop(acquisition);
    // The original future has actually been disposed. Only the deliberately
    // uncertain diagnostic cleanup waiter remains; it cannot certify success.
    let mut cleanup = host.0.lock().unwrap().pop().unwrap();
    let pending = futures::poll!(&mut cleanup).is_pending();
    cleanup.abort();
    assert!(cleanup.await.unwrap_err().is_cancelled());
    assert!(polled.is_err() && pending);
    assert_eq!(drops.load(std::sync::atomic::Ordering::SeqCst), 1);
    assert!(
        seen.load(std::sync::atomic::Ordering::SeqCst),
        "actual secondary payload Drop lost the original frame"
    );
}
