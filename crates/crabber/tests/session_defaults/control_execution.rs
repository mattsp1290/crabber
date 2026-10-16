use async_trait::async_trait;
use crabber::{core::*, session::*};
use std::sync::{
    Arc,
    atomic::{AtomicBool, AtomicUsize, Ordering},
};
pub(super) struct ControlExecution {
    pub inner: Box<dyn ExecutionStore>,
    pub fault: Arc<AtomicUsize>,
    pub calls: Arc<AtomicUsize>,
    pub hold_pause: Arc<AtomicBool>,
    pub skip_observation: Arc<AtomicBool>,
    pub entered: Arc<tokio::sync::Notify>,
    pub release: Arc<tokio::sync::Notify>,
}
#[async_trait]
impl ExecutionStore for ControlExecution {
    async fn observe_run_controls(
        &self,
        turn: Option<&TurnId>,
    ) -> Result<RunControlBoundary, RunControlError> {
        if turn.is_none() && self.skip_observation.load(Ordering::SeqCst) {
            return Ok(RunControlBoundary::default());
        }
        self.inner.observe_run_controls(turn).await
    }

    async fn mutate_phase(
        &self,
        request: crabber::session::PhaseMutation,
        authority: Option<crabber::session::PhaseWriteAuthority>,
    ) -> Result<crabber::session::PhaseMutationOutcome, crabber::session::PhaseError> {
        self.inner.mutate_phase(request, authority).await
    }
    async fn start_tool_phase(
        &self,
        start: crabber::session::PhaseStart,
        authority: Option<crabber::session::PhaseAuthority>,
    ) -> Result<crabber::session::PhaseMutationOutcome, crabber::session::PhaseError> {
        self.inner.start_tool_phase(start, authority).await
    }
    async fn settle_tool_phase(
        &self,
        settlement: crabber::session::PhaseSettlement,
        authority: Option<crabber::session::PhaseAuthority>,
    ) -> Result<crabber::session::PhaseMutationOutcome, crabber::session::PhaseError> {
        self.inner.settle_tool_phase(settlement, authority).await
    }
    async fn suspend_for_approval(
        &self,
        batch: crabber::session::ApprovalSuspension,
        authority: crabber::session::AttemptAuthority,
    ) -> Result<crabber::session::ApprovalReceipt, crabber::session::ApprovalError> {
        self.inner.suspend_for_approval(batch, authority).await
    }

    async fn mutate_attempt(
        &self,
        request: crabber_session::AttemptMutation,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        self.inner.mutate_attempt(request, authority).await
    }
    async fn conclude_attempt(
        &self,
        conclusion: crabber_session::AttemptConclusion,
        authority: Option<crabber_session::AttemptAuthority>,
    ) -> Result<crabber_session::AttemptMutationOutcome, crabber_session::AttemptError> {
        if matches!(
            conclusion.boundary,
            crabber_session::RunBoundary::Suspended { .. }
        ) {
            if self.hold_pause.swap(false, Ordering::SeqCst) {
                self.entered.notify_one();
                self.release.notified().await;
            }
            return self.inner.conclude_attempt(conclusion, authority).await;
        }
        self.calls.fetch_add(1, Ordering::SeqCst);
        if self.fault.load(Ordering::SeqCst) == 1 {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        let result = self.inner.conclude_attempt(conclusion, authority).await;
        if self.fault.load(Ordering::SeqCst) == 2 && result.is_ok() {
            return Err(crabber_session::AttemptError::UnknownStoreOutcome);
        }
        result
    }

    async fn renew_lease(&self, until: time::OffsetDateTime) -> Result<(), StoreError> {
        self.inner.renew_lease(until).await
    }
    async fn begin_admission_execution(
        &self,
    ) -> Result<(), crabber_session::AdmissionExecutionError> {
        self.inner.begin_admission_execution().await
    }
    async fn append_message(&self, message: Message) -> Result<(), StoreError> {
        self.inner.append_message(message).await
    }
    async fn append_part(&self, part: Part) -> Result<(), StoreError> {
        self.inner.append_part(part).await
    }
    async fn append_event(&self, event: EventRecord) -> Result<(), StoreError> {
        self.inner.append_event(event).await
    }
    async fn create_tool_call(
        &self,
        call: ToolCallRecord,
        pending_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.create_tool_call(call, pending_event).await
    }
    async fn claim_tool_call(
        &self,
        id: &ToolCallId,
        running_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.claim_tool_call(id, running_event).await
    }
    async fn settle_tool_call(
        &self,
        id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner
            .settle_tool_call(id, result, result_message, terminal_event)
            .await
    }
    async fn start_epoch(&self, epoch: ContextEpoch) -> Result<(), StoreError> {
        self.inner.start_epoch(epoch).await
    }
    async fn finish_epoch(&self, id: &EpochId, summary: Message) -> Result<(), StoreError> {
        self.inner.finish_epoch(id, summary).await
    }
    async fn pause_run(
        &self,
        checkpoint: serde_json::Value,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.pause_run(checkpoint, event).await
    }
    async fn settle_run(
        &self,
        status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,
    ) -> Result<(), StoreError> {
        self.inner.settle_run(status, error, usage, event).await
    }
    async fn put_extension_state(
        &self,
        extension_id: &str,
        entries: Vec<(String, Option<String>)>,
    ) -> Result<(), StoreError> {
        self.inner.put_extension_state(extension_id, entries).await
    }
    async fn claim_inbox(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox(kind).await
    }
    async fn claim_inbox_into_history(&self, kind: InboxKind) -> Result<Vec<Message>, StoreError> {
        self.inner.claim_inbox_into_history(kind).await
    }
}
