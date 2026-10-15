//! Example-owned first-thread admission. The runtime's ordinary Some(session)
//! path requires an existing session. This wrapper allows first POST creation
//! using the already validated host-selected user-message session ID, atomically
//! in `MemoryStore`'s existing admission transaction. It adds no retry receipt.
use async_trait::async_trait;
use crabber::{
    core::{
        EpochId, EventCursor, EventRecord, Message, Run, RunFence, RunId, Session, SessionId,
        ToolCallRecord,
    },
    session::{
        AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, MemoryStore, Store, StoreError,
    },
};
use std::{collections::BTreeMap, sync::Arc};

pub(crate) struct ThreadStore(pub Arc<MemoryStore>);
#[async_trait]
impl Store for ThreadStore {
    fn supports_owned_attempts(&self) -> bool {
        self.0.supports_owned_attempts()
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.0.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.0.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber::core::RunId,
        id: &crabber::core::PhaseId,
    ) -> Result<crabber::session::VersionedPhase, crabber::session::PhaseError> {
        self.0.tool_phase(run, id).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber::session::PhaseResolution,
    ) -> Result<crabber::session::PhaseResolutionOutcome, crabber::session::PhaseError> {
        self.0.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::PhaseReceipt>, crabber::session::PhaseError> {
        self.0.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber::core::SessionId,
        run: &crabber::core::RunId,
    ) -> Result<Option<crabber::session::ApprovalSnapshot>, crabber::session::ApprovalError> {
        self.0.approval_snapshot(session, run).await
    }
    async fn lookup_approval(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::ApprovalReceipt>, crabber::session::ApprovalError> {
        self.0.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber::session::ApprovalAnswer,
    ) -> Result<crabber::session::ApprovalReceipt, crabber::session::ApprovalError> {
        self.0.answer_approval(answer).await
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber::session::ApprovalContinuationClaim,
    ) -> Result<crabber::session::ApprovalClaimOutcome, crabber::session::ApprovalError> {
        self.0.claim_approval_continuation(claim).await
    }

    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber::session::VersionedAttempt>, crabber::session::AttemptError> {
        self.0.latest_execution_attempt(run).await
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber::core::SessionRevision, StoreError> {
        self.0.session_revision(session).await
    }

    async fn admit_run(&self, mut request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        if request.session_id.as_ref() != Some(&request.user_message.session_id) {
            return Err(StoreError::Validation("thread identity mismatch".into()));
        }
        request.session_id = None;
        self.0.admit_run(request).await
    }
    async fn admit_bound_run(
        &self,
        mut request: crabber::session::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        if request.request.session_id.as_ref() != Some(&request.request.user_message.session_id) {
            return Err(StoreError::Validation("thread identity mismatch".into()));
        }
        request.request.session_id = None;
        self.0.admit_bound_run(request).await
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber::session::RetainedExecutionBinding>, StoreError> {
        self.0.execution_binding(session, run).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.0.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.0.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.0.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.0.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.0.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.0.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.0.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.0.list_unfinished_tool_calls(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.0.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.0.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.0.enqueue_inbox(session, kind, message).await
    }
}
