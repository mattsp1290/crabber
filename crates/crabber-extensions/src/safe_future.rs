//! Panic containment for host future polling and destruction.
use crabber::ExtensionError;
use std::{
    future::{Future, poll_fn},
    panic::{AssertUnwindSafe, catch_unwind},
    pin::Pin,
    task::Poll,
};

type HostFuture<T> = Pin<Box<dyn Future<Output = Result<T, ExtensionError>> + Send>>;

pub(crate) struct SafeFuture<T> {
    on_panic: fn() -> ExtensionError,
    future: Option<HostFuture<T>>,
}
impl<T> SafeFuture<T> {
    pub(crate) fn new(future: HostFuture<T>, on_panic: fn() -> ExtensionError) -> Self {
        Self {
            future: Some(future),
            on_panic,
        }
    }
    pub(crate) async fn wait(&mut self) -> Result<T, ExtensionError> {
        poll_fn(|cx| {
            let Some(future) = self.future.as_mut() else {
                return Poll::Ready(Err((self.on_panic)()));
            };
            match catch_unwind(AssertUnwindSafe(|| future.as_mut().poll(cx))) {
                Ok(poll) => poll,
                Err(_) => Poll::Ready(Err((self.on_panic)())),
            }
        })
        .await
    }
    /// Observe disposal separately when the caller owns a checked cleanup scope.
    pub(crate) fn dispose(&mut self) -> bool {
        self.future
            .take()
            .is_none_or(|future| catch_unwind(AssertUnwindSafe(|| drop(future))).is_ok())
    }
}
impl<T> Drop for SafeFuture<T> {
    fn drop(&mut self) {
        let _ = self.dispose();
    }
}
