//! A failed constructor future cannot acknowledge disposal of its references.
use std::{
    any::Any,
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::Arc,
    task::{Context, Poll},
};
type Panic = Box<dyn Any + Send>;
struct Retained<F, S: ?Sized> {
    future: Option<Pin<Box<F>>>,
    signal: Option<Arc<S>>,
}
impl<F, S: ?Sized> Retained<F, S> {
    fn dispose(&mut self) -> Result<(), Panic> {
        let future = self.future.take();
        catch_unwind(AssertUnwindSafe(|| drop(future)))
    }
}
impl<F, S: ?Sized> Drop for Retained<F, S> {
    fn drop(&mut self) {
        if let Err(payload) = self.dispose() {
            std::mem::forget(self.signal.take());
            resume_unwind(payload);
        }
    }
}
impl<F: Future, S: ?Sized> Future for Retained<F, S> {
    type Output = (F::Output, Arc<S>);
    fn poll(self: Pin<&mut Self>, context: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = catch_unwind(AssertUnwindSafe(|| {
            this.future
                .as_mut()
                .expect("live constructor")
                .as_mut()
                .poll(context)
        }));
        let result = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(result)) => result,
            Err(payload) => {
                // A nested constructor may already have retained an uncertain
                // returned value. Neither an outer wrapper nor future disposal
                // during unwind may convert that failure into a release ACK.
                std::mem::forget(this.signal.take());
                if let Err(secondary) = this.dispose()
                    && let Err(nested) = catch_unwind(AssertUnwindSafe(|| drop(secondary)))
                {
                    std::mem::forget(nested);
                }
                resume_unwind(payload);
            }
        };
        if let Err(payload) = this.dispose() {
            std::mem::forget(this.signal.take());
            std::mem::forget(result);
            resume_unwind(payload);
        }
        Poll::Ready((result, this.signal.take().expect("live release signal")))
    }
}
pub(super) async fn observe<F: Future, S: ?Sized>(
    future: F,
    signal: Arc<S>,
) -> (F::Output, Arc<S>) {
    Retained {
        future: Some(Box::pin(future)),
        signal: Some(signal),
    }
    .await
}
