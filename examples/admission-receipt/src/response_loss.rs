//! Example-only response loss after the actual native admission compound commits.
//! Every capability and opaque transport delegates to the same inner Store.
use crabber::{core as c, session as s};
use s::Store;
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

pub(super) const LOST_REPLY: &str = "injected unknown admission response";
pub(super) struct LostAdmissionReply {
    pub(super) inner: Arc<dyn Store>,
    pub(super) committed: AtomicUsize,
}

// As in the public AuditedStore probe, this explicit list keeps native capability
// adoption reviewable. It does not reconstruct opaque transports or transactions.
macro_rules! delegate {
    (async { $(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)* }
     sync { $(fn $sync:ident($($sarg:ident: $sty:ty),* $(,)?) -> $sret:ty;)* }
     overrides { $($overrides:item)* }) => {
        #[async_trait::async_trait]
        impl Store for LostAdmissionReply {
            $(async fn $name(&self, $($arg: $ty),*) -> $ret {
                self.inner.$name($($arg),*).await
            })*
            $(fn $sync(&self, $($sarg: $sty),*) -> $sret {
                self.inner.$sync($($sarg),*)
            })*
            $($overrides)*
        }
    };
}
delegate! {
    async {
        fn public_history_v2( request: s::SnapshotRequest, ) -> Result<s::PublicHistoryOutcome, s::StoreError>;
        fn public_session_snapshot_v2( request: s::PublicSessionRequestV2, ) -> Result<s::PublicSessionOutcomeV2, s::PublicSessionError>;
        fn public_control_header_v2( request: s::PublicHeaderRequestV2, ) -> Result<s::PublicHeaderOutcomeV2, s::PublicSessionError>;
        fn public_control_header( request: s::PublicHeaderRequest, ) -> Result<s::PublicHeaderOutcome, s::PublicSessionError>;
        fn public_session_snapshot( request: s::PublicSessionRequest, ) -> Result<s::PublicSessionOutcome, s::PublicSessionError>;
        fn run_control_binding( session: &c::SessionId, run: &c::RunId, ) -> Result<s::RunControlBinding, s::RunControlError>;
        fn settle_waiting_cancellation( request: s::WaitingCancellation, ) -> Result<s::RunControlSummary, s::RunControlError>;
        fn control_run( request: s::RunControlRequest, ) -> Result<s::RunControlOutcome, s::RunControlError>;
        fn lookup_run_control( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::RunControlReceipt>, s::RunControlError>;
        fn run_controls( target: &s::RunControlTarget, ) -> Result<s::RunControlPage, s::RunControlError>;
        fn mark_attempt_uncertain( request: s::AttemptUncertainty, ) -> Result<s::AttemptControlOutcome, s::AttemptError>;
        fn retire_attempt( request: s::AttemptRetirement, ) -> Result<s::AttemptControlOutcome, s::AttemptError>;
        fn inspect_owned_queue_preparation( context: &c::SessionContext, authority: &s::QueueAuthority, expected_revision: c::SessionRevision, ) -> Result<s::RetainedQueue, s::StoreError>;
        fn queue_preparation_snapshot( session: &c::SessionId, ) -> Result<s::SessionSummary, s::StoreError>;
        fn mutate_queue( request: s::QueueMutation, ) -> Result<s::QueueOutcome, s::StoreError>;
        fn lookup_queue( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::QueueReceipt>, s::StoreError>;
        fn list_queue( request: s::QueuePageRequest, ) -> Result<s::QueuePage, s::StoreError>;
        fn get_queue( session: &c::SessionId, id: &c::QueueId, ) -> Result<s::QueueSummary, s::StoreError>;
        fn retained_queue( session: &c::SessionId, id: &c::QueueId, ) -> Result<s::RetainedQueue, s::StoreError>;
        fn reserve_session_owner( request: s::session_owner::ReserveSessionOwner, parent: Option<s::session_owner::SessionOwnerParentAuthority>, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn inspect_initial_preparation_admission( original: &s::session_owner::preparation::JoinedPreparation, ) -> Result< Option<s::session_owner::preparation::PreparationAdmissionReceipt>, s::session_owner::preparation::PreparationCommitError, >;
        fn inspect_queue_preparation_admission( original: &s::session_owner::preparation::JoinedPreparation, ) -> Result< Option<s::QueueReceipt>, s::session_owner::preparation::PreparationCommitError, >;
        fn reserve_preparation( request: s::session_owner::preparation::ReservePreparation, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn lookup_preparation( session: &c::SessionId, key: &s::session_owner::preparation::PreparationKey, ) -> Result< Option<s::session_owner::preparation::PreparationReconciliation>, s::session_owner::SessionOwnerError, >;
        fn register_execution_driver( request: s::session_owner::driver::RegisterExecutionDriver, ) -> Result< s::session_owner::driver::ExecutionDriverOutcome, s::session_owner::SessionOwnerError, >;
        fn register_successor_execution_driver( request: s::session_owner::driver::RegisterSuccessorExecutionDriver, ) -> Result< s::session_owner::driver::SuccessorExecutionDriverOutcome, s::session_owner::SessionOwnerError, >;
        fn register_session_service_dependency( request: s::session_owner::service::RegisterSessionServiceDependency, ) -> Result< s::session_owner::service::SessionServiceDependencyOutcome, s::session_owner::SessionOwnerError, >;
        fn check_session_service_dependency_access( request: s::session_owner::service::SessionServiceDependencyAccess, ) -> Result<(), s::session_owner::SessionOwnerError>;
        fn retain_preparation_resources( grant: s::session_owner::preparation::PreparationGrant, ) -> Result< s::session_owner::preparation::PreparationResourceOutcome, s::session_owner::SessionOwnerError, >;
        fn pending_initial_native( call: s::session_owner::owned_scope::PendingInitialNativeCall<'_>, ) -> Result< s::session_owner::owned_scope::PendingInitialNativeReply, s::session_owner::preparation::PreparationCommitError, >;
        fn pending_initial_consumer_native( call: s::session_owner::owned_scope::PendingInitialConsumerNativeCall<'_>, ) -> Result< s::session_owner::owned_scope::PendingInitialConsumerNativeReply, s::session_owner::preparation::PreparationCommitError, >;
        fn inspect_prepared_approval_driver( request: s::session_owner::preparation::InspectPreparedApprovalDriver, ) -> Result< s::session_owner::preparation::PreparedApprovalDriverMetadata, s::session_owner::SessionOwnerError, >;
        fn inspect_prepared_tool_driver( request: s::session_owner::preparation::InspectPreparedToolDriver, ) -> Result< s::session_owner::preparation::PreparedToolDriverMetadata, s::session_owner::SessionOwnerError, >;
        fn inspect_pending_approval_preparation( request: s::session_owner::preparation::InspectPendingApprovalPreparation, ) -> Result< s::session_owner::preparation::PendingApprovalPreparationMetadata, s::session_owner::SessionOwnerError, >;
        fn inspect_pending_tool_preparation( request: s::session_owner::preparation::InspectPendingToolPreparation, ) -> Result< s::session_owner::preparation::PendingToolPreparationMetadata, s::session_owner::SessionOwnerError, >;
        fn request_session_owner_cancel( request: s::session_owner::RequestSessionOwnerCancel, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn observe_session_owner_cancel( request: s::session_owner::ObserveSessionOwnerCancel, authority: s::session_owner::SessionOwnerAuthority, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn pending_session_owner_cancel( authority: &s::session_owner::SessionOwnerAuthority, ) -> Result< Option<s::session_owner::SessionOwnerCancellation>, s::session_owner::SessionOwnerError, >;
        fn activate_owned_scope( request: s::session_owner::owned_scope::OwnedScopeActivation, authority: s::session_owner::SessionOwnerAuthority, parent: Option<s::session_owner::owned_scope::OwnedScopeParentAuthority>, ) -> Result< s::session_owner::owned_scope::OwnedScopeActivationOutcome, s::session_owner::SessionOwnerError, >;
        fn publish_owned_scope_activation( request: s::session_owner::owned_scope::OwnedScopeActivation, authority: s::session_owner::SessionOwnerAuthority, parent: Option<s::session_owner::owned_scope::OwnedScopeParentAuthority>, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn join_owned_scope( proof: s::session_owner::owned_scope::JoinedOwnedScope, authority: s::session_owner::SessionOwnerAuthority, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn mutate_session_owner( request: s::session_owner::MutateSessionOwner, authority: Option<s::session_owner::SessionOwnerAuthority>, ) -> Result<s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError>;
        fn get_session_owner( session: &c::SessionId, id: &c::SessionOwnerId, ) -> Result<s::session_owner::SessionOwnerSnapshot, s::session_owner::SessionOwnerError>;
        fn lookup_session_owner( session: &c::SessionId, id: &c::SessionOwnerOperationId, ) -> Result< Option<s::session_owner::SessionOwnerReceipt>, s::session_owner::SessionOwnerError, >;
        fn delete_session( request: s::DeleteSession, ) -> Result<s::DeletionOutcome, s::DeletionError>;
        fn lookup_session_deletion( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::DeletionReceipt>, s::DeletionError>;
        fn session_disposition( session: &c::SessionId, ) -> Result<s::SessionDisposition, s::DeletionError>;
        fn fork_session( request: s::ForkSession, ) -> Result<s::ForkOutcome, s::ForkError>;
        fn lookup_session_fork( target: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::ForkReceipt>, s::ForkError>;
        fn manage_session( request: s::ManageSession, ) -> Result<s::ManagementReceipt, s::ManagementError>;
        fn lookup_session_management( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::ManagementReceipt>, s::ManagementError>;
        fn session_summary( session: &c::SessionId, ) -> Result<s::SessionSummary, s::ManagementError>;
        fn public_run_state( session: &c::SessionId, max_bytes: usize, ) -> Result<s::PublicRunOutcome, s::StoreError>;
        fn list_sessions( request: s::ListSessions, ) -> Result<s::SessionListPage, s::ManagementError>;
        fn approval_snapshot( session: &c::SessionId, run: &c::RunId, ) -> Result<Option<s::ApprovalSnapshot>, s::ApprovalError>;
        fn original_approval_plan( session: &c::SessionId, run: &c::RunId, target: &s::ContinuationTarget, ) -> Result<c::InputFingerprint, s::ApprovalError>;
        fn lookup_approval( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::ApprovalReceipt>, s::ApprovalError>;
        fn answer_approval( answer: s::ApprovalAnswer, ) -> Result<s::ApprovalReceipt, s::ApprovalError>;
        fn reclaim_approval_continuation( request: s::ApprovalContinuationRecovery, ) -> Result<s::ApprovalClaimOutcome, s::ApprovalError>;
        fn terminate_approval( termination: s::ApprovalTermination, ) -> Result<s::ApprovalReceipt, s::ApprovalError>;
        fn claim_approval_continuation( claim: s::ApprovalContinuationClaim, ) -> Result<s::ApprovalClaimOutcome, s::ApprovalError>;
        fn frontend_call_origin( session: &c::SessionId, run: &c::RunId, call: &c::ToolCallId, ) -> Result<Option<s::FrontendCallOrigin>, s::ToolContinuationError>;
        fn lookup_frontend_staging( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::FrontendStagingReceipt>, s::ToolContinuationError>;
        fn claim_frontend_continuation( claim: s::FrontendContinuationClaim, ) -> Result<s::ToolContinuationClaimOutcome, s::ToolContinuationError>;
        fn recover_frontend_continuation( request: s::FrontendContinuationRecovery, ) -> Result<s::ToolContinuationClaimOutcome, s::ToolContinuationError>;
        fn tool_continuation_snapshot( session: &c::SessionId, run: &c::RunId, ) -> Result<Option<s::ToolContinuationSnapshot>, s::ToolContinuationError>;
        fn original_tool_continuation_plan( session: &c::SessionId, run: &c::RunId, target: &s::ContinuationTarget, ) -> Result<c::InputFingerprint, s::ToolContinuationError>;
        fn lookup_tool_continuation( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::ToolContinuationReceipt>, s::ToolContinuationError>;
        fn answer_tool_interaction( answer: s::ToolInteractionAnswer, ) -> Result<s::ToolContinuationReceipt, s::ToolContinuationError>;
        fn recover_tool_continuation( request: s::ToolContinuationRecovery, ) -> Result<s::ToolContinuationClaimOutcome, s::ToolContinuationError>;
        fn claim_tool_continuation( claim: s::ToolContinuationClaim, ) -> Result<s::ToolContinuationClaimOutcome, s::ToolContinuationError>;
        fn admit_bound_run( request: s::BoundAdmitRequest, ) -> Result<s::AdmitOutcome, s::StoreError>;
        fn execution_binding( session: &c::SessionId, run: &c::RunId, ) -> Result<Option<s::RetainedExecutionBinding>, s::StoreError>;
        fn tool_phase( run: &c::RunId, id: &c::PhaseId, ) -> Result<s::VersionedPhase, s::PhaseError>;
        fn mark_phase_uncertain( request: s::PhaseUncertainty, ) -> Result<s::PhaseControlOutcome, s::PhaseError>;
        fn record_phase_cleanup( request: s::PhaseCleanup, ) -> Result<s::PhaseControlOutcome, s::PhaseError>;
        fn list_tool_phases( request: s::PhasePageRequest, ) -> Result<s::PhasePage, s::PhaseError>;
        fn resolve_uncertain_phase( resolution: s::PhaseResolution, ) -> Result<s::PhaseResolutionOutcome, s::PhaseError>;
        fn lookup_phase_mutation( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::PhaseReceipt>, s::PhaseError>;
        fn execution_attempt( run: &c::RunId, id: &c::AttemptId, ) -> Result<s::VersionedAttempt, s::AttemptError>;
        fn latest_execution_attempt( run: &c::RunId, ) -> Result<Option<s::VersionedAttempt>, s::AttemptError>;
        fn lookup_attempt_mutation( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::AttemptReceipt>, s::AttemptError>;
        fn public_state( session: &c::SessionId, ) -> Result<s::PublicStateSnapshot, s::PublicStateError>;
        fn mutate_bound_public_source( mutation: s::PublicSourceMutation, authority: s::PublicSourceAuthority, ) -> Result<s::PublicStateReceipt, s::PublicStateError>;
        fn mutate_invocation_public_source( mutation: s::PublicSourceMutation, authority: s::session_owner::SessionOwnerAuthority, ) -> Result<s::PublicStateReceipt, s::PublicStateError>;
        fn public_continuation_source( fence: &c::RunFence, authority: &s::AttemptAuthority, ) -> Result<c::continuation_source::ResumeSnapshot, s::PublicStateError>;
        fn lookup_public_source_publication( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::PublicSourcePublication>, s::PublicStateError>;
        fn lookup_public_state_mutation( session: &c::SessionId, id: &c::ControlId, ) -> Result<Option<s::PublicStateReceipt>, s::PublicStateError>;
        fn session_revision( session: &c::SessionId, ) -> Result<c::SessionRevision, s::StoreError>;
        fn admit_run( request: s::AdmitRequest) -> Result<s::AdmitOutcome, s::StoreError>;
        fn lookup_admission( session: &c::SessionId, key: &c::AdmissionKey, ) -> Result<Option<c::AdmissionReceipt>, s::StoreError>;
        fn load_admission_execution( session: &c::SessionId, key: &c::AdmissionKey, ) -> Result<Option<s::AdmissionExecutionRecord>, s::AdmissionExecutionError>;
        fn admission_execution_state( run: &c::RunId, ) -> Result<Option<s::AdmissionExecutionState>, s::StoreError>;
        fn lookup_expired_run_preparation( session: &c::SessionId, run: &c::RunId, issuance: u64, ) -> Result< Option<s::session_owner::preparation::ExpiredRunReceipt>, s::session_owner::SessionOwnerError, >;
        fn inspect_expired_run_preparation( request: s::session_owner::preparation::InspectExpiredRunPreparation, ) -> Result< s::session_owner::preparation::ExpiredRunPreparation, s::session_owner::SessionOwnerError, >;
        fn inspect_admission_recovery( request: s::ClaimUnstartedAdmissionRequest, ) -> Result< s::session_owner::preparation::AdmissionRecoveryPreparation, s::AdmissionExecutionError, >;
        fn claim_unstarted_admission( request: s::ClaimUnstartedAdmissionRequest, ) -> Result<s::ClaimedAdmission, s::AdmissionExecutionError>;
        fn abandon_run( request: c::AbandonRequest, ) -> Result<c::AbandonOutcome, c::AbandonError>;
        fn execution( fence: c::RunFence) -> Result<Box<dyn s::ExecutionStore>, s::StoreError>;
        fn get_session( id: &c::SessionId) -> Result<Option<c::Session>, s::StoreError>;
        fn get_run( id: &c::RunId) -> Result<Option<c::Run>, s::StoreError>;
        fn snapshot( request: s::SnapshotRequest, ) -> Result<s::SnapshotOutcome, s::StoreError>;
        fn public_history( request: s::SnapshotRequest, ) -> Result<s::PublicHistoryOutcome, s::StoreError>;
        fn list_messages( id: &c::SessionId, epoch: Option<c::EpochId>, ) -> Result<Vec<c::Message>, s::StoreError>;
        fn list_all_messages( id: &c::SessionId) -> Result<Vec<c::Message>, s::StoreError>;
        fn list_events( id: &c::SessionId, after: Option<c::EventCursor>, limit: usize, ) -> Result<Vec<c::EventRecord>, s::StoreError>;
        fn list_unfinished_runs() -> Result<Vec<c::Run>, s::StoreError>;
        fn list_unfinished_tool_calls( run: &c::RunId, ) -> Result<Vec<c::ToolCallRecord>, s::StoreError>;
        fn claim_expired_run( run: &c::RunId, owner: &str) -> Result<c::RunFence, s::StoreError>;
        fn get_extension_state( extension_id: &str, session: &c::SessionId, ) -> Result<BTreeMap<String, String>, s::StoreError>;
        fn enqueue_inbox( session: &c::SessionId, kind: s::InboxKind, message: c::Message, ) -> Result<(), s::StoreError>;
    }
    sync {
        fn supports_run_controls() -> bool;
        fn supports_future_queue() -> bool;
        fn supports_owned_queue_preparation_inspection() -> bool;
        fn supports_session_owners() -> bool;
        fn supports_preparation_protocol() -> bool;
        fn supports_initial_preparation_admission_inspection() -> bool;
        fn supports_queue_preparation_admission_inspection() -> bool;
        fn supports_execution_drivers() -> bool;
        fn supports_successor_execution_drivers() -> bool;
        fn supports_successor_execution_driver_origin( origin: s::session_owner::driver::SuccessorExecutionDriverOrigin, ) -> bool;
        fn supports_preparation_resources() -> bool;
        fn supports_session_service_dependencies() -> bool;
        fn supports_pending_execution_driver_origin( origin: s::session_owner::driver::SuccessorExecutionDriverOrigin, ) -> bool;
        fn supports_session_owner_cancellation() -> bool;
        fn supports_owned_scopes() -> bool;
        fn supports_owned_attempts() -> bool;
        fn supports_owned_tool_phases() -> bool;
        fn supports_frontend_tools() -> bool;
        fn supports_tool_continuations() -> bool;
        fn supports_pre_effect_approvals() -> bool;
    }

    overrides {
        async fn complete_preparation(
            &self, mut original: s::session_owner::preparation::JoinedPreparation,
        ) -> Result<s::session_owner::preparation::PreparationCompletion,
                    s::session_owner::preparation::PreparationCommitError> {
            original.set_admission_lease(std::time::Duration::from_secs(2));
            let completed = self.inner.complete_preparation(original).await?;
            if keyed_started(&completed) {
                self.committed.fetch_add(1, Ordering::SeqCst);
                return Err(s::StoreError::Validation(LOST_REPLY.into()).into());
            }
            Ok(completed)
        }
        async fn complete_preparation_with_driver_source(
            &self, mut original: s::session_owner::preparation::JoinedPreparation,
        ) -> Result<s::session_owner::preparation::PreparedDriverCompletion,
                    s::session_owner::preparation::PreparationCommitError> {
            original.set_admission_lease(std::time::Duration::from_secs(2));
            let completed = self.inner.complete_preparation_with_driver_source(original).await?;
            if keyed_started(&completed.completion) {
                self.committed.fetch_add(1, Ordering::SeqCst);
                return Err(s::StoreError::Validation(LOST_REPLY.into()).into());
            }
            Ok(completed)
        }
        async fn admit_keyed_run(&self, mut request: s::KeyedAdmitRequest)
            -> Result<s::KeyedAdmitOutcome, s::StoreError> {
            request.request.lease = std::time::Duration::from_secs(2);
            let completed = self.inner.admit_keyed_run(request).await?;
            self.lose_started(completed)
        }
        async fn admit_bound_keyed_run(&self, mut request: s::BoundKeyedAdmitRequest)
            -> Result<s::KeyedAdmitOutcome, s::StoreError> {
            request.keyed.request.lease = std::time::Duration::from_secs(2);
            let completed = self.inner.admit_bound_keyed_run(request).await?;
            self.lose_started(completed)
        }
    }
}
fn keyed_started(completed: &s::session_owner::preparation::PreparationCompletion) -> bool {
    matches!(
        completed,
        s::session_owner::preparation::PreparationCompletion::Keyed(
            s::KeyedAdmitOutcome::Started { .. }
        )
    )
}
impl LostAdmissionReply {
    fn lose_started(
        &self,
        completed: s::KeyedAdmitOutcome,
    ) -> Result<s::KeyedAdmitOutcome, s::StoreError> {
        if matches!(completed, s::KeyedAdmitOutcome::Started { .. }) {
            self.committed.fetch_add(1, Ordering::SeqCst);
            return Err(s::StoreError::Validation(LOST_REPLY.into()));
        }
        Ok(completed)
    }
}
