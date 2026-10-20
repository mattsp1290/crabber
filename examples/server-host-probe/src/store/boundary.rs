//! Private response-loss fixture: every operation delegates to the SAME execution.
use crabber::{
    core::{
        ContextEpoch, EpochId, EventRecord, Message, Part, RunStatus, ToolCallId, ToolCallRecord,
        ToolResult, Usage,
    },
    session::{self as s, ExecutionStore, InboxKind, StoreError},
};
use std::sync::Mutex;
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use time::OffsetDateTime;

#[derive(Default)]
pub(crate) struct BoundaryAck {
    pub(crate) calls: AtomicUsize,
    pub(crate) committed: Mutex<Option<(s::AttemptConclusion, s::AttemptReceipt)>>,
}
pub(super) struct Execution {
    pub(super) inner: Box<dyn ExecutionStore>,
    pub(super) ack: Arc<BoundaryAck>,
}
macro_rules! execution_methods {
    ($(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)*) => {
        #[async_trait::async_trait]
        impl ExecutionStore for Execution {
            async fn conclude_attempt(
                &self,
                conclusion: s::AttemptConclusion,
                authority: Option<s::AttemptAuthority>,
            ) -> Result<s::AttemptMutationOutcome, s::AttemptError> {
                self.ack.calls.fetch_add(1, Ordering::SeqCst);
                let outcome = self.inner.conclude_attempt(conclusion.clone(), authority).await?;
                let receipt = match &outcome {
                    s::AttemptMutationOutcome::Applied { receipt, .. }
                    | s::AttemptMutationOutcome::Replayed(receipt) => receipt.clone(),
                };
                *self.ack.committed.lock().unwrap() = Some((conclusion, receipt));
                // Only a successful actual native transaction reaches response loss.
                Err(s::AttemptError::UnknownStoreOutcome)
            }
            $(async fn $name(&self, $($arg: $ty),*) -> $ret {
                self.inner.$name($($arg),*).await
            })*
        }
    };
}
execution_methods! {
    fn stage_frontend_call(_request: s::FrontendCallStaging,
        _authority: s::AttemptAuthority,) -> Result<s::FrontendStagingReceipt, s::ToolContinuationError>;
    fn suspend_tool_continuation(_batch: s::ToolContinuationSuspension,
        _authority: s::ToolSuspensionAuthority,) -> Result<s::ToolContinuationReceipt, s::ToolContinuationError>;
    fn observe_run_controls(_turn: Option<&crabber::core::TurnId>,) -> Result<s::RunControlBoundary, s::RunControlError>;
    fn start_tool_phase(_start: s::PhaseStart,
        _authority: Option<s::PhaseAuthority>,) -> Result<s::PhaseMutationOutcome, s::PhaseError>;
    fn suspend_for_approval(_batch: s::ApprovalSuspension,
        _authority: s::AttemptAuthority,) -> Result<s::ApprovalReceipt, s::ApprovalError>;
    fn settle_tool_phase(_settlement: s::PhaseSettlement,
        _authority: Option<s::PhaseAuthority>,) -> Result<s::PhaseMutationOutcome, s::PhaseError>;
    fn mutate_phase(_request: s::PhaseMutation,
        _authority: Option<s::PhaseWriteAuthority>,) -> Result<s::PhaseMutationOutcome, s::PhaseError>;
    fn mutate_attempt(_request: s::AttemptMutation,
        _authority: Option<s::AttemptAuthority>,) -> Result<s::AttemptMutationOutcome, s::AttemptError>;
    fn mutate_public_state(_mutation: s::PublicStateMutation,) -> Result<s::PublicStateReceipt, s::PublicStateError>;
    fn mutate_public_source(_mutation: s::PublicSourceMutation,
        _authority: s::PublicSourceAuthority,) -> Result<s::PublicStateReceipt, s::PublicStateError>;
    fn begin_admission_execution() -> Result<(), s::AdmissionExecutionError>;
    fn renew_lease(until: OffsetDateTime) -> Result<(), StoreError>;
    fn append_message(message: Message) -> Result<(), StoreError>;
    fn append_part(part: Part) -> Result<(), StoreError>;
    fn append_event(event: EventRecord) -> Result<(), StoreError>;
    fn create_tool_call(call: ToolCallRecord,
        pending_event: EventRecord,) -> Result<(), StoreError>;
    fn claim_tool_call(id: &ToolCallId,
        running_event: EventRecord,) -> Result<(), StoreError>;
    fn settle_tool_call(id: &ToolCallId,
        result: ToolResult,
        result_message: Message,
        terminal_event: EventRecord,) -> Result<(), StoreError>;
    fn start_epoch(epoch: ContextEpoch) -> Result<(), StoreError>;
    fn finish_epoch(id: &EpochId, summary: Message) -> Result<(), StoreError>;
    fn pause_run(checkpoint: serde_json::Value,
        event: EventRecord,) -> Result<(), StoreError>;
    fn settle_run(status: RunStatus,
        error: Option<String>,
        usage: Usage,
        event: EventRecord,) -> Result<(), StoreError>;
    fn put_extension_state(extension_id: &str,
        entries: Vec<(String, Option<String>)>,) -> Result<(), StoreError>;
    fn claim_inbox(kind: InboxKind) -> Result<Vec<Message>, StoreError>;
    fn claim_inbox_into_history(kind: InboxKind) -> Result<Vec<Message>, StoreError>;
}
