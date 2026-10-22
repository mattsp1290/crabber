//! Explicit Store adapter and reader-pool metadata operations.
use super::{
    PostgresExecution, PostgresStore, abandon, admission_execution, db, expired, lookup_receipt,
    messages, project,
};
use crate::{
    AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome, KeyedAdmitRequest,
    Store, StoreError,
};
use async_trait::async_trait;
use crabber_core::{
    AdmissionKey, AdmissionReceipt, EpochId, EventCursor, EventRecord, Message, Run, RunFence,
    RunId, Session, SessionId, ToolCallId, ToolCallRecord,
};
use std::collections::BTreeMap;

#[async_trait]
impl Store for PostgresStore {
    async fn pending_initial_consumer_native(
        &self,
        call: crate::session_owner::owned_scope::PendingInitialConsumerNativeCall<'_>,
    ) -> Result<
        crate::session_owner::owned_scope::PendingInitialConsumerNativeReply,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        self.pending_initial_consumer_native_impl(call).await
    }
    async fn pending_initial_native(
        &self,
        call: crate::session_owner::owned_scope::PendingInitialNativeCall<'_>,
    ) -> Result<
        crate::session_owner::owned_scope::PendingInitialNativeReply,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Box::pin(self.pending_initial_native_impl(call)).await
    }

    async fn delete_session(
        &self,
        request: crate::DeleteSession,
    ) -> Result<crate::DeletionOutcome, crate::DeletionError> {
        self.guarded_delete(request).await
    }
    async fn lookup_session_deletion(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::DeletionReceipt>, crate::DeletionError> {
        self.deletion_receipt(session, id).await
    }
    async fn session_disposition(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionDisposition, crate::DeletionError> {
        self.disposition(session).await
    }
    fn supports_session_owners(&self) -> bool {
        true
    }
    async fn reserve_session_owner(
        &self,
        request: crate::session_owner::ReserveSessionOwner,
        parent: Option<crate::session_owner::SessionOwnerParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.reserve_owner(request, parent)).await
    }
    fn supports_session_owner_cancellation(&self) -> bool {
        true
    }
    async fn request_session_owner_cancel(
        &self,
        request: crate::session_owner::RequestSessionOwnerCancel,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.request_owner_cancel(request)).await
    }
    async fn observe_session_owner_cancel(
        &self,
        request: crate::session_owner::ObserveSessionOwnerCancel,
        authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.observe_owner_cancel(request, authority)).await
    }
    async fn pending_session_owner_cancel(
        &self,
        authority: &crate::session_owner::SessionOwnerAuthority,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerCancellation>,
        crate::session_owner::SessionOwnerError,
    > {
        Box::pin(self.pending_owner_cancel(authority)).await
    }
    fn supports_queue_preparation_admission_inspection(&self) -> bool {
        true
    }
    async fn inspect_queue_preparation_admission(
        &self,
        original: &crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crate::QueueReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Box::pin(self.inspect_queue_admission(original)).await
    }
    fn supports_initial_preparation_admission_inspection(&self) -> bool {
        true
    }
    async fn inspect_initial_preparation_admission(
        &self,
        original: &crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationAdmissionReceipt>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        self.inspect_initial_admission(original).await
    }
    fn supports_preparation_protocol(&self) -> bool {
        true
    }
    async fn reserve_preparation(
        &self,
        request: crate::session_owner::preparation::ReservePreparation,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.reserve_preparation(request)).await
    }
    async fn lookup_preparation(
        &self,
        session: &SessionId,
        key: &crate::session_owner::preparation::PreparationKey,
    ) -> Result<
        Option<crate::session_owner::preparation::PreparationReconciliation>,
        crate::session_owner::SessionOwnerError,
    > {
        self.preparation_metadata(session, key).await
    }
    fn supports_preparation_resources(&self) -> bool {
        true
    }
    fn supports_session_service_dependencies(&self) -> bool {
        true
    }
    async fn register_session_service_dependency(
        &self,
        request: crate::session_owner::service::RegisterSessionServiceDependency,
    ) -> Result<
        crate::session_owner::service::SessionServiceDependencyOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Box::pin(self.register_service(request)).await
    }
    async fn check_session_service_dependency_access(
        &self,
        request: crate::session_owner::service::SessionServiceDependencyAccess,
    ) -> Result<(), crate::session_owner::SessionOwnerError> {
        Box::pin(self.check_service_access(request)).await
    }
    async fn retain_preparation_resources(
        &self,
        grant: crate::session_owner::preparation::PreparationGrant,
    ) -> Result<
        crate::session_owner::preparation::PreparationResourceOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        Box::pin(self.retain_preparation_resources(grant)).await
    }
    async fn complete_preparation(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crate::session_owner::preparation::PreparationCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Box::pin(self.complete_preparation(joined)).await
    }
    async fn complete_preparation_with_driver_source(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
    ) -> Result<
        crate::session_owner::preparation::PreparedDriverCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        crate::session_owner::preparation::driver_completion::validate_original(&joined)?;
        Box::pin(self.complete_preparation_captured(joined, true)).await
    }
    async fn inspect_pending_approval_preparation(
        &self,
        request: crate::session_owner::preparation::InspectPendingApprovalPreparation,
    ) -> Result<
        crate::session_owner::preparation::PendingApprovalPreparationMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        self.inspect_pending_approval(&request).await
    }
    async fn inspect_pending_tool_preparation(
        &self,
        request: crate::session_owner::preparation::InspectPendingToolPreparation,
    ) -> Result<
        crate::session_owner::preparation::PendingToolPreparationMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        self.inspect_pending_tool(&request).await
    }
    fn supports_pending_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        use crate::session_owner::driver::SuccessorExecutionDriverOrigin as Origin;
        matches!(
            origin,
            Origin::AdmissionRecovery
                | Origin::ExpiredRun
                | Origin::ApprovalClaim
                | Origin::ApprovalRecovery
                | Origin::ToolClaim
                | Origin::ToolRecovery
                | Origin::FrontendClaim
                | Origin::FrontendRecovery
        )
    }
    async fn inspect_prepared_approval_driver(
        &self,
        request: crate::session_owner::preparation::InspectPreparedApprovalDriver,
    ) -> Result<
        crate::session_owner::preparation::PreparedApprovalDriverMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        Box::pin(self.inspect_approval_driver(&request)).await
    }
    async fn inspect_prepared_tool_driver(
        &self,
        request: crate::session_owner::preparation::InspectPreparedToolDriver,
    ) -> Result<
        crate::session_owner::preparation::PreparedToolDriverMetadata,
        crate::session_owner::SessionOwnerError,
    > {
        Box::pin(self.inspect_tool_driver(&request)).await
    }

    fn supports_successor_execution_drivers(&self) -> bool {
        true
    }
    fn supports_successor_execution_driver_origin(
        &self,
        origin: crate::session_owner::driver::SuccessorExecutionDriverOrigin,
    ) -> bool {
        use crate::session_owner::driver::SuccessorExecutionDriverOrigin as Origin;
        matches!(
            origin,
            Origin::AdmissionRecovery
                | Origin::ExpiredRun
                | Origin::ApprovalClaim
                | Origin::ApprovalRecovery
                | Origin::ToolClaim
                | Origin::ToolRecovery
                | Origin::FrontendClaim
                | Origin::FrontendRecovery
        )
    }
    async fn register_successor_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterSuccessorExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::SuccessorExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.register_successor_driver(request).await
    }
    fn supports_execution_drivers(&self) -> bool {
        true
    }
    async fn register_execution_driver(
        &self,
        request: crate::session_owner::driver::RegisterExecutionDriver,
    ) -> Result<
        crate::session_owner::driver::ExecutionDriverOutcome,
        crate::session_owner::SessionOwnerError,
    > {
        self.register_driver(request).await
    }
    fn supports_owned_scopes(&self) -> bool {
        true
    }
    /// Activate only an already registered exact local gate. Fresh Applied is
    /// the sole source of a dispatch permit; replay and unknown ACK never grant.
    async fn publish_owned_scope_activation(
        &self,
        request: crate::session_owner::owned_scope::OwnedScopeActivation,
        authority: crate::session_owner::SessionOwnerAuthority,
        parent: Option<crate::session_owner::owned_scope::OwnedScopeParentAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.publish_owned_scope_activation(request, authority, parent)).await
    }
    /// Report exact observed main and sealed resource joins. Receipt replay
    /// never grants work and no lease expiry, timeout or Drop supplies proof.
    async fn join_owned_scope(
        &self,
        proof: crate::session_owner::owned_scope::JoinedOwnedScope,
        authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        Box::pin(self.join_owned_scope(proof, authority)).await
    }
    async fn mutate_session_owner(
        &self,
        request: crate::session_owner::MutateSessionOwner,
        authority: Option<crate::session_owner::SessionOwnerAuthority>,
    ) -> Result<crate::session_owner::SessionOwnerOutcome, crate::session_owner::SessionOwnerError>
    {
        self.mutate_owner(request, authority).await
    }
    async fn get_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerId,
    ) -> Result<crate::session_owner::SessionOwnerSnapshot, crate::session_owner::SessionOwnerError>
    {
        self.owner_snapshot(session, id).await
    }
    async fn lookup_session_owner(
        &self,
        session: &SessionId,
        id: &crabber_core::SessionOwnerOperationId,
    ) -> Result<
        Option<crate::session_owner::SessionOwnerReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        self.owner_receipt(session, id).await
    }

    async fn public_history_v2(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::PublicHistoryOutcome, StoreError> {
        Ok(self
            .read_snapshot_profile(request, crate::snapshot_profile::Profile::AggregateV2)
            .await?
            .into())
    }
    async fn public_control_header_v2(
        &self,
        request: crate::PublicHeaderRequestV2,
    ) -> Result<crate::PublicHeaderOutcomeV2, crate::PublicSessionError> {
        self.read_public_header_v2(request).await
    }
    async fn public_control_header(
        &self,
        request: crate::PublicHeaderRequest,
    ) -> Result<crate::PublicHeaderOutcome, crate::PublicSessionError> {
        self.read_public_header(request).await
    }

    fn supports_tool_continuations(&self) -> bool {
        true
    }
    fn supports_frontend_tools(&self) -> bool {
        true
    }
    async fn frontend_call_origin(
        &self,
        session: &SessionId,
        run: &RunId,
        call: &ToolCallId,
    ) -> Result<Option<crate::FrontendCallOrigin>, crate::ToolContinuationError> {
        self.read_frontend_origin(session, run, call).await
    }
    async fn lookup_frontend_staging(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::FrontendStagingReceipt>, crate::ToolContinuationError> {
        self.frontend_staging_receipt(session, id).await
    }
    async fn tool_continuation_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::ToolContinuationSnapshot>, crate::ToolContinuationError> {
        self.read_tools(session, run).await
    }
    async fn original_tool_continuation_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ToolContinuationError> {
        self.read_original_tool_plan(session, run, target).await
    }
    async fn lookup_tool_continuation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ToolContinuationReceipt>, crate::ToolContinuationError> {
        self.tool_receipt(session, id).await
    }
    async fn answer_tool_interaction(
        &self,
        request: crate::ToolInteractionAnswer,
    ) -> Result<crate::ToolContinuationReceipt, crate::ToolContinuationError> {
        self.answer_tools(request).await
    }
    async fn claim_tool_continuation(
        &self,
        request: crate::ToolContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.claim_tools(request).await
    }
    async fn claim_frontend_continuation(
        &self,
        request: crate::FrontendContinuationClaim,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.claim_frontend_tools(request).await
    }
    async fn recover_tool_continuation(
        &self,
        request: crate::ToolContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.recover_tools(request).await
    }
    async fn recover_frontend_continuation(
        &self,
        request: crate::FrontendContinuationRecovery,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError> {
        self.recover_frontend_tools(request).await
    }

    async fn settle_waiting_cancellation(
        &self,
        request: crate::WaitingCancellation,
    ) -> Result<crate::RunControlSummary, crate::RunControlError> {
        self.control_settle_waiting(request).await
    }

    async fn run_control_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<crate::RunControlBinding, crate::RunControlError> {
        self.control_binding(session, run).await
    }

    fn supports_run_controls(&self) -> bool {
        true
    }
    async fn control_run(
        &self,
        request: crate::RunControlRequest,
    ) -> Result<crate::RunControlOutcome, crate::RunControlError> {
        Box::pin(self.control_accept(request)).await
    }
    async fn lookup_run_control(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::RunControlReceipt>, crate::RunControlError> {
        self.control_lookup(session, id).await
    }
    async fn run_controls(
        &self,
        target: &crate::RunControlTarget,
    ) -> Result<crate::RunControlPage, crate::RunControlError> {
        self.control_page(target).await
    }

    async fn terminate_approval(
        &self,
        request: crate::ApprovalTermination,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.terminate_approval_inner(&request).await
    }
    async fn mark_attempt_uncertain(
        &self,
        request: crate::AttemptUncertainty,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(crate::attempt_control::Control::Uncertain(request))
            .await
    }
    async fn retire_attempt(
        &self,
        request: crate::AttemptRetirement,
    ) -> Result<crate::AttemptControlOutcome, crate::AttemptError> {
        self.host_attempt(crate::attempt_control::Control::Retire(request))
            .await
    }

    async fn queue_preparation_snapshot(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionSummary, StoreError> {
        self.queue_session_snapshot(session).await
    }
    fn supports_future_queue(&self) -> bool {
        true
    }
    fn supports_owned_queue_preparation_inspection(&self) -> bool {
        true
    }
    async fn inspect_owned_queue_preparation(
        &self,
        context: &crabber_core::SessionContext,
        authority: &crate::QueueAuthority,
        expected_revision: crabber_core::SessionRevision,
    ) -> Result<crate::RetainedQueue, StoreError> {
        self.queue_owned_inspection(context, authority, expected_revision)
            .await
    }
    async fn mutate_queue(
        &self,
        request: crate::QueueMutation,
    ) -> Result<crate::QueueOutcome, StoreError> {
        Box::pin(self.queue_mutate(request)).await
    }
    async fn lookup_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::QueueReceipt>, StoreError> {
        self.queue_lookup(session, id).await
    }
    async fn list_queue(
        &self,
        request: crate::QueuePageRequest,
    ) -> Result<crate::QueuePage, StoreError> {
        self.queue_list(request).await
    }
    async fn get_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::QueueId,
    ) -> Result<crate::QueueSummary, StoreError> {
        self.queue_get(session, id).await
    }
    async fn retained_queue(
        &self,
        session: &SessionId,
        id: &crabber_core::QueueId,
    ) -> Result<crate::RetainedQueue, StoreError> {
        self.queue_private(session, id).await
    }

    async fn reclaim_approval_continuation(
        &self,
        request: crate::ApprovalContinuationRecovery,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.recover_approval(&request).await
    }
    async fn fork_session(
        &self,
        request: crate::ForkSession,
    ) -> Result<crate::ForkOutcome, crate::ForkError> {
        self.fork(request).await
    }
    async fn lookup_session_fork(
        &self,
        target: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ForkReceipt>, crate::ForkError> {
        self.fork_receipt(target, id).await
    }
    async fn manage_session(
        &self,
        request: crate::ManageSession,
    ) -> Result<crate::ManagementReceipt, crate::ManagementError> {
        self.manage(request).await
    }
    async fn lookup_session_management(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ManagementReceipt>, crate::ManagementError> {
        self.management_receipt(session, id).await
    }
    async fn public_run_state(
        &self,
        session: &SessionId,
        max_bytes: usize,
    ) -> Result<crate::PublicRunOutcome, StoreError> {
        self.read_public_run(session, max_bytes).await
    }
    async fn session_summary(
        &self,
        session: &SessionId,
    ) -> Result<crate::SessionSummary, crate::ManagementError> {
        self.read_summary(session).await
    }
    async fn list_sessions(
        &self,
        request: crate::ListSessions,
    ) -> Result<crate::SessionListPage, crate::ManagementError> {
        self.summaries(request).await
    }

    async fn claim_approval_continuation(
        &self,
        claim: crate::ApprovalContinuationClaim,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError> {
        self.claim_approval(&claim).await
    }

    async fn answer_approval(
        &self,
        answer: crate::ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, crate::ApprovalError> {
        self.answer_approval_inner(&answer).await
    }

    async fn approval_snapshot(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::ApprovalSnapshot>, crate::ApprovalError> {
        self.read_approval(session, run).await
    }
    async fn original_approval_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<crabber_core::InputFingerprint, crate::ApprovalError> {
        self.read_original_approval_plan(session, run, target).await
    }
    async fn lookup_approval(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::ApprovalReceipt>, crate::ApprovalError> {
        self.approval_receipt(session, id).await
    }
    fn supports_owned_attempts(&self) -> bool {
        true
    }
    fn supports_owned_tool_phases(&self) -> bool {
        true
    }
    fn supports_pre_effect_approvals(&self) -> bool {
        true
    }

    async fn mark_phase_uncertain(
        &self,
        request: crate::PhaseUncertainty,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Uncertainty(&request),
        )
        .await
    }
    async fn record_phase_cleanup(
        &self,
        request: crate::PhaseCleanup,
    ) -> Result<crate::PhaseControlOutcome, crate::PhaseError> {
        request.fingerprint()?;
        self.host_phase(
            &request.run_id,
            request.mutation(),
            crate::phase::PhaseEffect::Cleanup(&request),
        )
        .await
    }
    async fn list_tool_phases(
        &self,
        request: crate::PhasePageRequest,
    ) -> Result<crate::PhasePage, crate::PhaseError> {
        self.phase_page(&request).await
    }
    async fn resolve_uncertain_phase(
        &self,
        resolution: crate::PhaseResolution,
    ) -> Result<crate::PhaseResolutionOutcome, crate::PhaseError> {
        self.resolve_phase(&resolution).await
    }

    async fn tool_phase(
        &self,
        run: &RunId,
        id: &crabber_core::PhaseId,
    ) -> Result<crate::VersionedPhase, crate::PhaseError> {
        self.read_phase(run, id).await
    }
    async fn lookup_phase_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PhaseReceipt>, crate::PhaseError> {
        self.phase_receipt(session, id).await
    }

    async fn execution_attempt(
        &self,
        run: &RunId,
        id: &crabber_core::AttemptId,
    ) -> Result<crate::VersionedAttempt, crate::AttemptError> {
        self.read_attempt(run, Some(id))
            .await?
            .ok_or(crate::AttemptError::NotFound)
    }
    async fn latest_execution_attempt(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::VersionedAttempt>, crate::AttemptError> {
        self.read_attempt(run, None).await
    }
    async fn lookup_attempt_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::AttemptReceipt>, crate::AttemptError> {
        self.attempt_receipt(session, id).await
    }

    async fn public_continuation_source(
        &self,
        fence: &RunFence,
        authority: &crate::AttemptAuthority,
    ) -> Result<crabber_core::continuation_source::ResumeSnapshot, crate::PublicStateError> {
        self.read_continuation_source(fence, authority).await
    }
    async fn public_state(
        &self,
        session: &SessionId,
    ) -> Result<crate::PublicStateSnapshot, crate::PublicStateError> {
        self.read_public_state(session).await
    }
    async fn lookup_public_source_publication(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicSourcePublication>, crate::PublicStateError> {
        self.read_public_source_publication(session, id).await
    }
    async fn mutate_bound_public_source(
        &self,
        mutation: crate::PublicSourceMutation,
        authority: crate::PublicSourceAuthority,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        self.write_public_source(mutation, authority, None).await
    }
    async fn mutate_invocation_public_source(
        &self,
        mutation: crate::PublicSourceMutation,
        authority: crate::session_owner::SessionOwnerAuthority,
    ) -> Result<crate::PublicStateReceipt, crate::PublicStateError> {
        self.write_public_source(
            mutation,
            crate::PublicSourceAuthority::Invocation(authority),
            None,
        )
        .await
    }
    async fn lookup_public_state_mutation(
        &self,
        session: &SessionId,
        id: &crabber_core::ControlId,
    ) -> Result<Option<crate::PublicStateReceipt>, crate::PublicStateError> {
        self.public_receipt(session, id).await
    }

    async fn session_revision(
        &self,
        session: &SessionId,
    ) -> Result<crabber_core::SessionRevision, StoreError> {
        self.read_session_revision(session).await
    }
    async fn admit_run(&self, request: AdmitRequest) -> Result<AdmitOutcome, StoreError> {
        self.admit_with_binding(request, None).await
    }

    async fn admit_keyed_run(
        &self,
        request: KeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        Box::pin(self.admit_keyed_with_binding(request, None)).await
    }
    async fn admit_bound_run(
        &self,
        request: crate::BoundAdmitRequest,
    ) -> Result<AdmitOutcome, StoreError> {
        self.admit_with_binding(request.request, Some(request.binding))
            .await
    }
    async fn admit_bound_keyed_run(
        &self,
        request: crate::BoundKeyedAdmitRequest,
    ) -> Result<KeyedAdmitOutcome, StoreError> {
        Box::pin(self.admit_keyed_with_binding(request.keyed, Some(request.binding))).await
    }
    async fn execution_binding(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<crate::RetainedExecutionBinding>, StoreError> {
        self.read_execution_binding(session, run).await
    }

    async fn lookup_admission(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionReceipt>, StoreError> {
        lookup_receipt(&mut *self.pool.acquire().await.map_err(db)?, session, key).await
    }
    async fn load_admission_execution(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<crate::AdmissionExecutionRecord>, crate::AdmissionExecutionError> {
        self.read_admission_execution(session, key).await
    }
    async fn lookup_expired_run_preparation(
        &self,
        session: &SessionId,
        run: &RunId,
        issuance: u64,
    ) -> Result<
        Option<crate::session_owner::preparation::ExpiredRunReceipt>,
        crate::session_owner::SessionOwnerError,
    > {
        self.lookup_expired_source(session, run, issuance).await
    }
    async fn inspect_expired_run_preparation(
        &self,
        request: crate::session_owner::preparation::InspectExpiredRunPreparation,
    ) -> Result<
        crate::session_owner::preparation::ExpiredRunPreparation,
        crate::session_owner::SessionOwnerError,
    > {
        expired::inspect(self, request).await
    }
    async fn inspect_admission_recovery(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<
        crate::session_owner::preparation::AdmissionRecoveryPreparation,
        crate::AdmissionExecutionError,
    > {
        admission_execution::inspect(self, request).await
    }
    async fn claim_unstarted_admission(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError> {
        admission_execution::claim(self, request).await
    }
    async fn abandon_run(
        &self,
        request: crabber_core::AbandonRequest,
    ) -> Result<crabber_core::AbandonOutcome, crabber_core::AbandonError> {
        abandon::abandon(self, request).await
    }
    async fn execution(&self, fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError> {
        self.raw_execution_access(&fence).await?;
        Ok(Box::new(PostgresExecution {
            store: self.clone(),
            fence,
        }))
    }
    async fn get_session(&self, id: &SessionId) -> Result<Option<Session>, StoreError> {
        self.read_session(id).await
    }
    async fn get_run(&self, id: &RunId) -> Result<Option<Run>, StoreError> {
        self.raw_run(id).await
    }
    async fn list_messages(
        &self,
        id: &SessionId,
        epoch: Option<EpochId>,
    ) -> Result<Vec<Message>, StoreError> {
        project(&mut *self.pool.acquire().await.map_err(db)?, id, epoch).await
    }
    async fn snapshot(
        &self,
        request: crate::SnapshotRequest,
    ) -> Result<crate::SnapshotOutcome, StoreError> {
        self.read_snapshot(request).await
    }
    async fn list_all_messages(&self, id: &SessionId) -> Result<Vec<Message>, StoreError> {
        messages(&mut *self.pool.acquire().await.map_err(db)?, id).await
    }
    async fn list_events(
        &self,
        id: &SessionId,
        after: Option<EventCursor>,
        limit: usize,
    ) -> Result<Vec<EventRecord>, StoreError> {
        self.read_events(id, after, limit).await
    }
    async fn list_workspace_unfinished_runs(
        &self,
        request: crate::WorkspaceRunsRequest,
    ) -> Result<Vec<Run>, StoreError> {
        self.raw_workspace_unfinished_runs(&request.workspace_id)
            .await
    }

    async fn list_queue_heads(
        &self,
        request: crate::QueueHeadsRequest,
    ) -> Result<crate::QueueHeadsPage, StoreError> {
        self.queue_heads(request).await
    }

    async fn list_unfinished_runs(&self) -> Result<Vec<Run>, StoreError> {
        self.raw_unfinished_runs().await
    }
    async fn list_unfinished_tool_calls(
        &self,
        run: &RunId,
    ) -> Result<Vec<ToolCallRecord>, StoreError> {
        self.read_unfinished_calls(run).await
    }
    async fn admission_execution_state(
        &self,
        run: &RunId,
    ) -> Result<Option<crate::AdmissionExecutionState>, StoreError> {
        self.read_admission_execution_state(run).await
    }

    async fn claim_expired_run(&self, id: &RunId, owner: &str) -> Result<RunFence, StoreError> {
        Box::pin(self.claim_legacy_expired_with_proof(
            id,
            owner,
            &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
        ))
        .await
    }

    async fn get_extension_state(
        &self,
        extension_id: &str,
        session: &SessionId,
    ) -> Result<BTreeMap<String, String>, StoreError> {
        self.read_extension_state(extension_id, session).await
    }

    async fn enqueue_inbox(
        &self,
        session: &SessionId,
        kind: InboxKind,
        message: Message,
    ) -> Result<(), StoreError> {
        self.enqueue_inbox_message(session, kind, message).await
    }
}
