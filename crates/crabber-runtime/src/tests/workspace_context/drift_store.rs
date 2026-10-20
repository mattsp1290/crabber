//! Private persisted-identity drift fixture; same original Store.
use super::*;

/// Test-only view of a memory store whose persisted session identity can be
/// altered between pause and resume, and whose keyed admission response can be lost.
pub(super) struct DriftStore {
    pub(super) inner: Arc<MemoryStore>,
    pub(super) drift: Mutex<HashMap<SessionId, (String, String)>>,
    /// Session reads that fail with the error, or find nothing when `None`.
    pub(super) broken: Mutex<HashMap<SessionId, Option<StoreError>>>,
    /// Runs whose checkpoint request loses its workspace identity on read.
    pub(super) corrupt: Mutex<HashSet<RunId>>,
    pub(super) lose_admission: AtomicBool,
}
impl DriftStore {
    pub(super) fn new(inner: Arc<MemoryStore>) -> Arc<Self> {
        Arc::new(Self {
            inner,
            drift: Mutex::new(HashMap::new()),
            broken: Mutex::new(HashMap::new()),
            corrupt: Mutex::new(HashSet::new()),
            lose_admission: AtomicBool::new(false),
        })
    }
    pub(super) fn alter(&self, session: &SessionId, workspace_id: &str, directory: &str) {
        self.drift
            .lock()
            .unwrap()
            .insert(session.clone(), (workspace_id.into(), directory.into()));
    }
    pub(super) fn restore(&self, session: &SessionId) {
        self.drift.lock().unwrap().remove(session);
    }
    /// Every read of a corrupt run sees a checkpoint request without identity.
    fn view(&self, mut run: Run) -> Run {
        if self.corrupt.lock().unwrap().contains(&run.id)
            && let Some(request) = run
                .checkpoint
                .as_mut()
                .and_then(|checkpoint| checkpoint.get_mut("request"))
                .and_then(Value::as_object_mut)
        {
            request.remove("workspace_id");
        }
        run
    }
}
#[async_trait]
impl Store for DriftStore {
    fn supports_preparation_protocol(&self) -> bool {
        true
    }
    async fn reserve_preparation(
        &self,
        request: crabber_session::session_owner::preparation::ReservePreparation,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.reserve_preparation(request).await
    }
    async fn lookup_preparation(
        &self,
        session: &crabber_core::SessionId,
        key: &crabber_session::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crabber_session::session_owner::preparation::PreparationReconciliation>,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.lookup_preparation(session, key).await
    }
    async fn complete_preparation(
        &self,
        joined: crabber_session::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crabber_session::session_owner::preparation::PreparationCompletion,
        crabber_session::session_owner::preparation::PreparationCommitError,
    > {
        let completed = self.inner.complete_preparation(joined).await?;
        if matches!(
            &completed,
            crabber_session::session_owner::preparation::PreparationCompletion::Keyed(_)
        ) && self.lose_admission.load(Ordering::SeqCst)
        {
            return Err(StoreError::Validation("unknown admission response".into()).into());
        }
        Ok(completed)
    }
    async fn mutate_session_owner(
        &self,
        request: crabber_session::session_owner::MutateSessionOwner,
        authority: Option<crabber_session::session_owner::SessionOwnerAuthority>,
    ) -> Result<
        crabber_session::session_owner::SessionOwnerOutcome,
        crabber_session::session_owner::SessionOwnerError,
    > {
        self.inner.mutate_session_owner(request, authority).await
    }

    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        self.inner.supports_owned_tool_phases()
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        self.inner.supports_pre_effect_approvals()
    }
    async fn tool_phase(
        &self,
        run: &crabber_core::RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crabber_session::VersionedPhase, crabber_session::PhaseError> {
        self.inner.tool_phase(run, id).await
    }
    async fn mark_phase_uncertain(
        &self,
        request: crabber_session::PhaseUncertainty,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.mark_phase_uncertain(request).await
    }
    async fn record_phase_cleanup(
        &self,
        request: crabber_session::PhaseCleanup,
    ) -> Result<crabber_session::PhaseControlOutcome, crabber_session::PhaseError> {
        self.inner.record_phase_cleanup(request).await
    }
    async fn list_tool_phases(
        &self,
        request: crabber_session::PhasePageRequest,
    ) -> Result<crabber_session::PhasePage, crabber_session::PhaseError> {
        self.inner.list_tool_phases(request).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crabber_session::PhaseResolution,
    ) -> Result<crabber_session::PhaseResolutionOutcome, crabber_session::PhaseError> {
        self.inner.resolve_uncertain_phase(resolution).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::PhaseReceipt>, crabber_session::PhaseError> {
        self.inner.lookup_phase_mutation(session, id).await
    }
    async fn approval_snapshot(
        &self,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
    ) -> Result<Option<crabber_session::ApprovalSnapshot>, crabber_session::ApprovalError> {
        self.inner.approval_snapshot(session, run).await
    }
    async fn original_approval_plan(
        &self,
        selected: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        target: &crabber_session::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crabber_session::ApprovalError> {
        self.inner
            .original_approval_plan(selected, run, target)
            .await
    }
    async fn lookup_approval(
        &self,
        session: &crabber_core::SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crabber_session::ApprovalReceipt>, crabber_session::ApprovalError> {
        self.inner.lookup_approval(session, id).await
    }
    async fn answer_approval(
        &self,
        answer: crabber_session::ApprovalAnswer,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.answer_approval(answer).await
    }
    async fn terminate_approval(
        &self,
        request: crabber_session::ApprovalTermination,
    ) -> Result<crabber_session::ApprovalReceipt, crabber_session::ApprovalError> {
        self.inner.terminate_approval(request).await
    }
    async fn reclaim_approval_continuation(
        &self,
        request: crabber_session::ApprovalContinuationRecovery,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
        self.inner.reclaim_approval_continuation(request).await
    }
    async fn claim_approval_continuation(
        &self,
        claim: crabber_session::ApprovalContinuationClaim,
    ) -> Result<crabber_session::ApprovalClaimOutcome, crabber_session::ApprovalError> {
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
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        self.inner.session_revision(session).await
    }

    async fn admit_bound_run(
        &self,
        request: crabber_session::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_bound_run(request).await
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crabber_session::BoundKeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        let outcome = self.inner.admit_bound_keyed_run(request).await?;
        if self.lose_admission.load(Ordering::SeqCst) {
            return Err(StoreError::Validation("unknown admission response".into()));
        }
        Ok(outcome)
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crabber_session::RetainedExecutionBinding>, StoreError> {
        self.inner.execution_binding(session, run).await
    }

    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.inner.admit_run(request).await
    }
    async fn admit_keyed_run(
        &self,
        request: crabber_session::KeyedAdmitRequest,
    ) -> Result<crabber_session::KeyedAdmitOutcome, StoreError> {
        let outcome = self.inner.admit_keyed_run(request).await?;
        if self.lose_admission.load(Ordering::SeqCst) {
            return Err(StoreError::Validation("unknown admission response".into()));
        }
        Ok(outcome)
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<
        Option<crabber_session::AdmissionExecutionRecord>,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.load_admission_execution(session, key).await
    }
    async fn inspect_admission_recovery(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crabber_session::session_owner::preparation::AdmissionRecoveryPreparation,
        crabber_session::AdmissionExecutionError,
    > {
        self.inner.inspect_admission_recovery(request).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crabber_session::ClaimUnstartedAdmissionRequest,
    ) -> Result<crabber_session::ClaimedAdmission, crabber_session::AdmissionExecutionError> {
        self.inner.claim_unstarted_admission(request).await
    }
    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        self.inner.lookup_admission(session, key).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.inner.execution(fence).await
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        if let Some(broken) = self.broken.lock().unwrap().get(id) {
            return broken.clone().map_or(Ok(None), Err);
        }
        let mut session = self.inner.get_session(id).await?;
        if let (Some(session), Some((workspace_id, directory))) =
            (session.as_mut(), self.drift.lock().unwrap().get(id))
        {
            session.workspace_id.clone_from(workspace_id);
            session.directory.clone_from(directory);
        }
        Ok(session)
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        Ok(self.inner.get_run(id).await?.map(|run| self.view(run)))
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
        let runs = self.inner.list_unfinished_runs().await?;
        Ok(runs.into_iter().map(|run| self.view(run)).collect())
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
}
