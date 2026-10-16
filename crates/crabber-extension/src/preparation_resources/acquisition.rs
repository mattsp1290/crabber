//! The ticket proves disposal of one acquired plan reference, not physical
//! cleanup of a provider or an independently owned service generation.
use super::PreparationResourceContext;
use crate::{ExtensionError, PlanReleaseObserver, RunPlan};
use std::{
    future::Future,
    pin::Pin,
    sync::{Arc, Mutex},
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

#[derive(Default)]
enum Outcome {
    #[default]
    Acquiring,
    NoPlan,
    Acquired(PlanReleaseObserver),
}
#[derive(Default)]
struct Ticket {
    outcome: Mutex<Outcome>,
    finished: CancellationToken,
}
impl Ticket {
    fn complete(&self, outcome: Outcome) {
        *self.outcome.lock().unwrap() = outcome;
        self.finished.cancel();
    }
    async fn wait(&self) -> Result<(), crate::ResourceFailure> {
        self.finished.cancelled().await;
        let observer = match &*self.outcome.lock().unwrap() {
            Outcome::Acquiring => return Err(crate::ResourceFailure::CleanupFailed),
            Outcome::NoPlan => None,
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
            // Dispose the actual factory future before acknowledging NoPlan.
            // A panicking destructor deliberately leaves the ticket unfinished.
            drop(future);
            self.ticket.complete(Outcome::NoPlan);
        }
    }
}
impl<F> Future for Acquiring<F>
where
    F: Future<Output = Result<RunPlan, ExtensionError>>,
{
    type Output = Result<RunPlan, ExtensionError>;
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
            Ok(plan) => {
                let (plan, observer) = plan.observe_release();
                this.ticket.complete(Outcome::Acquired(observer));
                Ok(plan)
            }
            Err(error) => {
                this.ticket.complete(Outcome::NoPlan);
                Err(error)
            }
        })
    }
}

impl PreparationResourceContext {
    /// Reserve retained acquisition cleanup before invoking the factory. Error
    /// or cancellation acknowledges only disposal of that factory's plan
    /// references. Independent resource/service joins remain the caller's duty.
    /// # Errors
    /// Refuses exhausted/sealed cleanup registration before invoking the factory,
    /// and preserves errors returned by the factory.
    pub async fn acquire_plan<F, W>(&self, factory: F) -> Result<RunPlan, ExtensionError>
    where
        F: FnOnce() -> W,
        W: Future<Output = Result<RunPlan, ExtensionError>>,
    {
        let ticket = Arc::new(Ticket::default());
        let cleanup = ticket.clone();
        self.cleanup()
            .defer(move || async move { cleanup.wait().await })?;
        Acquiring {
            future: Some(Box::pin(factory())),
            ticket,
        }
        .await
    }
}

#[cfg(test)]
#[path = "acquisition_tests.rs"]
mod tests;
