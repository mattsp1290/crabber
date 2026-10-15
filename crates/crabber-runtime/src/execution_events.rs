//! Bounded observation of one worker; lag/drop never changes execution ownership.
use crate::{Observer, OperationalObservation};
use crabber_core::{EventRecord, RunId, SessionId, TraceContext};
use std::sync::{Arc, Mutex};
use tokio::sync::{broadcast, watch};

pub(crate) const EVENT_CAPACITY: usize = 256;

/// The sole event receiver for one execution worker. Lag ends this observation
/// path; reconcile from durable state rather than resuming a partial stream.
pub struct RunEvents {
    receiver: broadcast::Receiver<Arc<EventRecord>>,
    completion: watch::Receiver<bool>,
    ended: bool,
}
impl RunEvents {
    /// Receive an event, or `None` after worker completion and buffer drainage.
    /// EOF does not prove terminal persistence or cleanup; await the owned join.
    /// # Errors
    /// A lag/closure error ends this observation path permanently.
    pub async fn recv(&mut self) -> Result<Option<Arc<EventRecord>>, broadcast::error::RecvError> {
        if self.ended {
            return Ok(None);
        }
        loop {
            if *self.completion.borrow() {
                match self.receiver.try_recv() {
                    Ok(event) => return Ok(Some(event)),
                    Err(broadcast::error::TryRecvError::Lagged(count)) => {
                        self.ended = true;
                        return Err(broadcast::error::RecvError::Lagged(count));
                    }
                    Err(
                        broadcast::error::TryRecvError::Empty
                        | broadcast::error::TryRecvError::Closed,
                    ) => {
                        self.ended = true;
                        return Ok(None);
                    }
                }
            }
            tokio::select! {
                biased;
                event = self.receiver.recv() => match event {
                    Ok(event) => return Ok(Some(event)),
                    Err(error) => { self.ended = true; return Err(error); }
                },
                changed = self.completion.changed() => {
                    if changed.is_err() { self.ended = true; return Ok(None); }
                }
            }
        }
    }
}

pub(crate) struct ExecutionEventObserver {
    inner: Arc<dyn Observer>,
    sender: broadcast::Sender<Arc<EventRecord>>,
    session: SessionId,
    run: RunId,
    sealed: Mutex<bool>,
}
impl ExecutionEventObserver {
    pub(crate) fn channel(
        inner: Arc<dyn Observer>,
        session: SessionId,
        run: RunId,
        completion: watch::Receiver<bool>,
    ) -> (Arc<Self>, RunEvents) {
        let (sender, receiver) = broadcast::channel(EVENT_CAPACITY);
        (
            Arc::new(Self {
                inner,
                sender,
                session,
                run,
                sealed: Mutex::new(false),
            }),
            RunEvents {
                receiver,
                completion,
                ended: false,
            },
        )
    }
    fn publish(&self, event: &EventRecord) {
        let sealed = self
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if !*sealed && event.session_id == self.session && event.run_id == self.run {
            let _ = self.sender.send(Arc::new(event.clone()));
        }
    }
    pub(crate) fn guard(self: &Arc<Self>) -> EventSealGuard {
        EventSealGuard(self.clone())
    }
}
/// Seals even when a worker panics; retained native progress sinks cannot reopen
/// the ended observation. This guard makes no claim about external cleanup.
pub(crate) struct EventSealGuard(Arc<ExecutionEventObserver>);
impl Drop for EventSealGuard {
    fn drop(&mut self) {
        *self
            .0
            .sealed
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner) = true;
    }
}
impl Observer for ExecutionEventObserver {
    fn emit(&self, event: &EventRecord) {
        self.publish(event);
        self.inner.emit(event);
    }
    fn emit_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        self.publish(event);
        self.inner.emit_with_context(event, context);
    }
    fn emit_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.publish(event);
        self.inner.emit_in_attempt(event, context, attempt);
    }
    fn model_completed(&self, event: &EventRecord) {
        self.inner.model_completed(event);
    }
    fn model_completed_with_context(&self, event: &EventRecord, context: Option<&TraceContext>) {
        self.inner.model_completed_with_context(event, context);
    }
    fn model_completed_in_attempt(
        &self,
        event: &EventRecord,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.inner
            .model_completed_in_attempt(event, context, attempt);
    }
    fn operational_completed(&self, observation: &OperationalObservation) {
        self.inner.operational_completed(observation);
    }
    fn operational_completed_in_attempt(
        &self,
        observation: &OperationalObservation,
        context: Option<&TraceContext>,
        attempt: &RunId,
    ) {
        self.inner
            .operational_completed_in_attempt(observation, context, attempt);
    }
    fn mount_close_timed_out(&self, timeout: &crabber_extension::MountCloseTimeout) {
        self.inner.mount_close_timed_out(timeout);
    }
}
