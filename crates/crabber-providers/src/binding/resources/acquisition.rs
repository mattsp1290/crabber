//! The ticket proves disposal of acquired provider references, not physical
//! cleanup of a provider or an independently owned service generation.
use super::ProviderResourceContext;
use crate::{
    BoundProvider, ProviderBindingError, ProviderReleaseObserver, ProviderResourceFailure,
};
use futures::channel::oneshot;
use std::{
    future::Future,
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
    finished: std::sync::Mutex<Option<oneshot::Sender<()>>>,
    ready: futures::future::Shared<futures::future::BoxFuture<'static, ()>>,
}
impl Default for Ticket {
    fn default() -> Self {
        use futures::FutureExt;
        let (sender, receiver) = oneshot::channel();
        Self {
            outcome: Mutex::new(Outcome::Acquiring),
            finished: Mutex::new(Some(sender)),
            ready: async move {
                if receiver.await.is_err() {
                    std::future::pending::<()>().await;
                }
            }
            .boxed()
            .shared(),
        }
    }
}
impl Ticket {
    fn complete(&self, outcome: Outcome) {
        *self.outcome.lock().unwrap() = outcome;
        if let Some(sender) = self.finished.lock().unwrap().take() {
            let _ = sender.send(());
        }
    }
    async fn wait(&self) -> Result<(), ProviderResourceFailure> {
        self.ready.clone().await;
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

struct Acquiring<F> {
    future: Option<Pin<Box<F>>>,
    ticket: Arc<Ticket>,
}
impl<F> Drop for Acquiring<F> {
    fn drop(&mut self) {
        if let Some(future) = self.future.take() {
            // Await-frame arguments may otherwise outlive a local drop guard.
            // Dispose the actual factory future before acknowledging NoBinding.
            // A panicking destructor deliberately leaves the ticket unfinished.
            drop(future);
            self.ticket.complete(Outcome::NoBinding);
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
        let outcome = std::task::ready!(
            this.future
                .as_mut()
                .expect("acquisition polled after completion")
                .as_mut()
                .poll(cx)
        );
        drop(this.future.take());
        Poll::Ready(match outcome {
            Ok(binding) => {
                let (binding, observer) = binding.observe_release();
                this.ticket.complete(Outcome::Acquired(observer));
                Ok(binding)
            }
            Err(error) => {
                this.ticket.complete(Outcome::NoBinding);
                Err(error)
            }
        })
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
        let ticket = Arc::new(Ticket::default());
        let cleanup = ticket.clone();
        self.defer(move || async move { cleanup.wait().await })?;
        Acquiring {
            future: Some(Box::pin(factory())),
            ticket,
        }
        .await
    }
}
