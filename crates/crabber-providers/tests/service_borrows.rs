//! Reference disposal is distinct from physical service cleanup.
#[path = "service_borrows/stream_creation.rs"]
mod stream_creation;
use crabber_providers::{BoundProvider, ProviderBindingError};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
struct PendingFactory {
    disposed: Arc<AtomicBool>,
    panic: bool,
}
impl Future for PendingFactory {
    type Output = Result<BoundProvider, ProviderBindingError>;
    fn poll(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Self::Output> {
        Poll::Pending
    }
}
impl Drop for PendingFactory {
    fn drop(&mut self) {
        self.disposed.store(true, Ordering::SeqCst);
        assert!(!self.panic, "actual factory destructor failed");
    }
}
struct Lease {
    disposed: Arc<AtomicBool>,
    released: Arc<AtomicUsize>,
}
impl Drop for Lease {
    fn drop(&mut self) {
        assert!(self.disposed.load(Ordering::SeqCst));
        self.released.fetch_add(1, Ordering::SeqCst);
    }
}
#[tokio::test]
async fn actual_factory_future_disposes_before_borrow_release() {
    for panic in [false, true] {
        let disposed = Arc::new(AtomicBool::new(false));
        let released = Arc::new(AtomicUsize::new(0));
        let lease = Arc::new(Lease {
            disposed: disposed.clone(),
            released: released.clone(),
        });
        let mut acquisition =
            Box::pin(BoundProvider::acquire_service_reference(lease, move || {
                PendingFactory { disposed, panic }
            }));
        assert!(futures::poll!(acquisition.as_mut()).is_pending());
        let dropped = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(acquisition)));
        assert_eq!(dropped.is_err(), panic);
        assert_eq!(
            released.load(Ordering::SeqCst),
            usize::from(!panic),
            "failed disposal must retain the original dependency"
        );
    }
}

struct Host(
    std::sync::Mutex<
        Vec<tokio::task::JoinHandle<Result<(), crabber_providers::ProviderResourceFailure>>>,
    >,
);
impl crabber_providers::ProviderResourceHost for Host {
    fn register(
        &self,
        task: crabber_providers::ProviderResourceTask,
    ) -> Result<(), crabber_providers::ProviderResourceRegistrationError> {
        self.0.lock().unwrap().push(tokio::spawn(task));
        Ok(())
    }
    fn closing(&self) -> futures::future::BoxFuture<'static, ()> {
        Box::pin(async {})
    }
}
struct Unsupported;
impl crabber_providers::ProviderServiceSource for Unsupported {}
#[tokio::test]
async fn absent_service_host_refuses_before_constructor_and_disposes_ticket() {
    let host = Arc::new(Host(std::sync::Mutex::new(Vec::new())));
    let context = crabber_providers::ProviderResourceContext::from_host(
        crabber_core::SessionContext::new(
            crabber_core::SessionId::new(),
            "workspace",
            "/workspace",
        ),
        host.clone(),
    );
    let calls = Arc::new(AtomicUsize::new(0));
    let constructor = calls.clone();
    assert!(matches!(
        context
            .acquire_service_binding(Arc::new(Unsupported), move || async move {
                constructor.fetch_add(1, Ordering::SeqCst);
                Err(ProviderBindingError::VersionConflict)
            })
            .await,
        Err(ProviderBindingError::Service(
            crabber_providers::ProviderServiceError::Unsupported
        ))
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let tasks = std::mem::take(&mut *host.0.lock().unwrap());
    assert_eq!(tasks.len(), 1);
    for task in tasks {
        assert_eq!(
            tokio::time::timeout(std::time::Duration::from_secs(1), task)
                .await
                .unwrap()
                .unwrap(),
            Ok(())
        );
    }
}
