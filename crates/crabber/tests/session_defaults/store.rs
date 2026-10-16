use async_trait::async_trait;
use crabber::{core::*, session::*};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
};
use tokio::sync::Notify;
pub(super) struct TestStore {
    pub inner: Arc<dyn Store>,
    pub summary_reads: AtomicUsize,
    pub queue_loss: AtomicUsize,
    pub unsupported: AtomicBool,
    pub hold_summary: AtomicBool,
    pub hold_admission: AtomicBool,
    pub lose_ack: AtomicBool,
    pub entered: Notify,
    pub release: Notify,
}
impl TestStore {
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            summary_reads: AtomicUsize::new(0),
            queue_loss: AtomicUsize::new(0),
            unsupported: AtomicBool::new(false),
            hold_summary: AtomicBool::new(false),
            hold_admission: AtomicBool::new(false),
            lose_ack: AtomicBool::new(false),
            entered: Notify::new(),
            release: Notify::new(),
        }
    }
}
#[async_trait]
impl Store for TestStore {
    async fn queue_preparation_snapshot(
        &self,
        id: &SessionId,
    ) -> Result<SessionSummary, StoreError> {
        let result = self.inner.queue_preparation_snapshot(id).await;
        if self.hold_summary.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        result
    }

    fn supports_future_queue(&self) -> bool {
        !self.unsupported.load(Ordering::SeqCst) && self.inner.supports_future_queue()
    }
    async fn mutate_queue(&self, request: QueueMutation) -> Result<QueueOutcome, StoreError> {
        let step = match request.action {
            QueueAction::Prepare { .. } => 1,
            QueueAction::Admit { .. } => 2,
            _ => 0,
        };
        let result = self.inner.mutate_queue(request).await?;
        if step != 0
            && self
                .queue_loss
                .compare_exchange(step, 0, Ordering::SeqCst, Ordering::SeqCst)
                .is_ok()
        {
            return Err(StoreError::Validation("queue acknowledgement lost".into()));
        }
        Ok(result)
    }
    async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<QueueReceipt>, StoreError> {
        self.inner.lookup_queue(session, id).await
    }
    async fn list_queue(&self, request: QueuePageRequest) -> Result<QueuePage, StoreError> {
        self.inner.list_queue(request).await
    }
    async fn get_queue(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<QueueSummary, StoreError> {
        self.inner.get_queue(session, id).await
    }
    async fn retained_queue(
        &self,
        session: &SessionId,
        id: &QueueId,
    ) -> Result<RetainedQueue, StoreError> {
        self.inner.retained_queue(session, id).await
    }

    fn supports_owned_attempts(&self) -> bool {
        self.inner.supports_owned_attempts()
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.inner.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.inner.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber::core::RunId,
        id: &crabber::core::PhaseId,
    ) -> Result<crabber::session::VersionedPhase, crabber::session::PhaseError> {
        self.inner.tool_phase(run, id).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber::session::PhaseResolution,
    ) -> Result<crabber::session::PhaseResolutionOutcome, crabber::session::PhaseError> {
        self.inner.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::PhaseReceipt>, crabber::session::PhaseError> {
        self.inner.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber::core::SessionId,
        run: &crabber::core::RunId,
    ) -> Result<Option<crabber::session::ApprovalSnapshot>, crabber::session::ApprovalError> {
        self.inner.approval_snapshot(session, run).await
    }
    async fn lookup_approval(
        &self,
        session: &crabber::core::SessionId,
        id: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::ApprovalReceipt>, crabber::session::ApprovalError> {
        self.inner.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber::session::ApprovalAnswer,
    ) -> Result<crabber::session::ApprovalReceipt, crabber::session::ApprovalError> {
        self.inner.answer_approval(answer).await
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crabber_session::ApprovalContinuationRecovery,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        self.inner.reclaim_approval_continuation(request).await
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber::session::ApprovalContinuationClaim,
    ) -> Result<crabber::session::ApprovalClaimOutcome, crabber::session::ApprovalError> {
        self.inner.claim_approval_continuation(claim).await
    }
    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::VersionedAttempt>, crabber_session::AttemptError> {
        self.inner.latest_execution_attempt(run).await
    }
    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber::core::SessionRevision, StoreError> {
        self.inner.session_revision(session).await
    }
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_bound_run(
        &self,
        request: crabber_session::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_bound_run(request).await
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::RetainedExecutionBinding>, StoreError> {
        self.inner.execution_binding(session, run).await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &crabber::core::AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.inner.get_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.inner.get_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        self.inner.list_messages(id, epoch).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        self.inner.list_all_messages(id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.inner.list_events(id, after, limit).await
    }
    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.inner.list_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.inner.list_unfinished_tool_calls(run).await
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crabber_session::AdmissionExecutionState>, StoreError> {
        self.inner.admission_execution_state(run).await
    }
    async fn claim_expired_run(&self, run: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        self.inner.claim_expired_run(run, owner).await
    }
    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.inner.get_extension_state(extension_id, session).await
    }
    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.inner.enqueue_inbox(session, kind, message).await
    }
    async fn manage_session(
        &self,
        request: ManageSession,
    ) -> Result<ManagementReceipt, ManagementError> {
        self.inner.manage_session(request).await
    }
    async fn session_summary(&self, id: &SessionId) -> Result<SessionSummary, ManagementError> {
        self.summary_reads.fetch_add(1, Ordering::SeqCst);
        if self.unsupported.load(Ordering::SeqCst) {
            return Err(ManagementError::Unsupported);
        }
        let result = self.inner.session_summary(id).await;
        if self.hold_summary.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        result
    }
    async fn admit_keyed_run(
        &self,
        request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        self.inner.admit_keyed_run(request).await
    }
    async fn admit_bound_keyed_run(
        &self,
        mut request: BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        if self.hold_admission.swap(false, Ordering::SeqCst) {
            self.entered.notify_one();
            self.release.notified().await;
        }
        let lose_ack = self.lose_ack.swap(false, Ordering::SeqCst);
        if lose_ack {
            request.keyed.request.lease = std::time::Duration::from_millis(20);
        }
        let outcome = self.inner.admit_bound_keyed_run(request).await?;
        if lose_ack {
            return Err(StoreError::Validation(
                "injected acknowledgement loss".into(),
            ));
        }
        Ok(outcome)
    }
    async fn claim_unstarted_admission(
        &self,
        request: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, AdmissionExecutionError> {
        self.inner.claim_unstarted_admission(request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.inner.execution(fence).await
    }
}
