//! The ticket proves disposal of acquired provider references, not physical
//! cleanup of a provider or an independently owned service generation.
use super::ProviderResourceContext;
use crate::{
    BoundProvider, ProviderBindingError, ProviderReleaseHolds, ProviderReleaseObserver,
    ProviderResourceFailure,
};
use std::{
    future::Future,
    panic::{AssertUnwindSafe, catch_unwind, resume_unwind},
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};

#[derive(Default)]
enum Outcome {
    #[default]
    Acquiring,
    NoBinding,
    Acquired(ProviderReleaseObserver),
}
struct Ticket {
    outcome: Mutex<Outcome>,
    ready: ProviderReleaseObserver,
}
impl Ticket {
    fn complete(&self, outcome: Outcome) {
        *self.outcome.lock().unwrap() = outcome;
    }
    async fn wait(&self) -> Result<(), ProviderResourceFailure> {
        self.ready.released().await;
        let observer = match &*self.outcome.lock().unwrap() {
            Outcome::Acquiring => return Err(ProviderResourceFailure::CleanupFailed),
            Outcome::NoBinding => None,
            Outcome::Acquired(observer) => Some(observer.clone()),
        };
        if let Some(observer) = observer {
            observer.released().await;
        }
        Ok(())
    }
}

type Panic = Box<dyn std::any::Any + Send>;
struct Acquiring<F> {
    future: Option<Pin<Box<F>>>,
    ticket: Arc<Ticket>,
    original: Option<Arc<super::super::release::Signal>>,
    holds: ProviderReleaseHolds,
}
impl<F> Acquiring<F> {
    fn dispose(&mut self) -> Result<(), Panic> {
        let future = self.future.take();
        self.holds
            .sync_scope(|| catch_unwind(AssertUnwindSafe(|| drop(future))))
    }
    fn uncertain(&mut self) {
        std::mem::forget(self.original.take());
    }
}
impl<F> Drop for Acquiring<F> {
    fn drop(&mut self) {
        if self.future.is_some() {
            match self.dispose() {
                Ok(()) if !std::thread::panicking() => self.ticket.complete(Outcome::NoBinding),
                Ok(()) => self.uncertain(),
                Err(payload) => {
                    self.uncertain();
                    resume_unwind(payload);
                }
            }
            drop(self.original.take());
        }
    }
}
impl<F> Future for Acquiring<F>
where
    F: Future<Output = Result<BoundProvider, ProviderBindingError>>,
{
    type Output = Result<BoundProvider, ProviderBindingError>;
    fn poll(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        let this = self.get_mut();
        let polled = this.holds.sync_scope(|| {
            catch_unwind(AssertUnwindSafe(|| {
                this.future
                    .as_mut()
                    .expect("live acquisition")
                    .as_mut()
                    .poll(cx)
            }))
        });
        let outcome = match polled {
            Ok(Poll::Pending) => return Poll::Pending,
            Ok(Poll::Ready(outcome)) => outcome,
            Err(payload) => {
                this.uncertain();
                if let Err(secondary) = this.dispose()
                    && let Err(nested) = this
                        .holds
                        .sync_scope(|| catch_unwind(AssertUnwindSafe(|| drop(secondary))))
                {
                    std::mem::forget(nested);
                }
                resume_unwind(payload);
            }
        };
        if let Err(payload) = this.dispose() {
            this.uncertain();
            std::mem::forget(outcome);
            resume_unwind(payload);
        }
        let outcome = match outcome {
            Ok(binding) => {
                let (binding, observer) = binding.observe_release();
                this.ticket.complete(Outcome::Acquired(observer));
                Ok(binding)
            }
            Err(error) => {
                this.ticket.complete(Outcome::NoBinding);
                Err(error)
            }
        };
        // The stored outcome precedes the SAME original ready-signal release.
        drop(this.original.take());
        Poll::Ready(outcome)
    }
}

impl ProviderResourceContext {
    /// Reserve retained acquisition cleanup before invoking the factory. Error
    /// or cancellation acknowledges only disposal of that factory's provider
    /// references. Independent resource/service joins remain the caller's duty.
    /// # Errors
    /// Refuses exhausted/sealed cleanup registration before invoking the factory,
    /// and preserves errors returned by the factory.
    pub async fn acquire_binding<F, W>(
        &self,
        factory: F,
    ) -> Result<BoundProvider, ProviderBindingError>
    where
        F: FnOnce() -> W,
        W: Future<Output = Result<BoundProvider, ProviderBindingError>>,
    {
        let (original, ready) = super::super::release::completion();
        let ticket = Arc::new(Ticket {
            outcome: Mutex::new(Outcome::Acquiring),
            ready,
        });
        let cleanup = ticket.clone();
        self.defer(move || async move { cleanup.wait().await })?;
        let holds = ProviderReleaseHolds::capture_with_original(original.clone());
        // Retain the ORIGINAL returned future directly. An added async await
        // wrapper could destroy it during poll unwind before our catch boundary.
        let future = match holds.sync_scope(|| catch_unwind(AssertUnwindSafe(factory))) {
            Ok(future) => future,
            Err(payload) => {
                std::mem::forget(original);
                std::mem::forget(holds);
                resume_unwind(payload);
            }
        };
        Acquiring {
            future: Some(Box::pin(future)),
            ticket,
            holds,
            original: Some(original),
        }
        .await
    }
}
