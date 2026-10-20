//! The same original resource-owning supervisor, observed externally after return.
use super::super::super::{JoinedOwnedTask, OwnedMainDisposition};
use super::{CapacityLease, Control, Observation, PendingSuccessorError, TaskOutput};
use std::sync::Arc;
use tokio::{sync::oneshot, task::JoinHandle};
pub(super) async fn run<T: Send + 'static>(
    task: JoinHandle<Result<TaskOutput<T>, PendingSuccessorError>>,
    control: Control,
    send: oneshot::Sender<Observation<T>>,
    capacity: Arc<CapacityLease>,
    protected: Option<super::super::core::RetainedOwner>,
    #[cfg(test)] epilogue: Option<Arc<super::supervision_joins::Gate>>,
) {
    let (disposition, value, error, replay) = match task.await {
        Ok(Ok(TaskOutput::Business(value))) => {
            (OwnedMainDisposition::Returned, Some(value), None, None)
        }
        Ok(Ok(TaskOutput::BusinessFailed(error))) => {
            (OwnedMainDisposition::Returned, None, Some(error), None)
        }
        Ok(Ok(TaskOutput::Stopped)) => (OwnedMainDisposition::NoDispatch, None, None, None),
        Ok(Ok(TaskOutput::Replayed(replay))) => {
            (OwnedMainDisposition::NoDispatch, None, None, Some(replay))
        }
        Ok(Err(error)) => (OwnedMainDisposition::NoDispatch, None, Some(error), None),
        Err(error) if error.is_panic() => {
            // Persist failure before safely disposing an untrusted payload.
            control.finish(OwnedMainDisposition::Panicked);
            super::super::super::discard_panic(error);
            (OwnedMainDisposition::Panicked, None, None, None)
        }
        Err(_) => (OwnedMainDisposition::Aborted, None, None, None),
    };
    let main = control.binding().map(|bound| JoinedOwnedTask {
        activation: bound.activation().clone(),
        disposition,
        value,
    });
    // The same supervisor retains original outputs/resources even if
    // the bounded observation or its caller-owned retry is dropped.
    let physical = protected.clone();
    let protected = match protected {
        Some(retained) => retained.after_join(disposition).await,
        None => None,
    };
    let pending = physical
        .as_ref()
        .is_some_and(super::super::core::RetainedOwner::resources_pending);
    let observation_capacity = physical.is_none().then_some(capacity);
    let observation = Observation {
        family: control.family(),
        main,
        disposition,
        error,
        replay,
        _capacity: observation_capacity,
        protected,
    };
    control.finish(disposition);
    // Failed send owns/disposes the actual result here, not a detached main.
    if let Err(observation) = send.send(observation) {
        drop(observation);
    }
    if pending && let Some(retained) = &physical {
        retained.join_original_resources().await;
    }
    #[cfg(test)]
    if let Some(gate) = epilogue {
        gate.wait().await;
    }
}
