//! Public adapter: every advertised native operation stays in the same Store.
//! No private fields, SQL, authority serialization or split transactions.
use crabber::{
    core as c,
    core::{
        AdmissionKey, AdmissionReceipt, EpochId, EventCursor, EventRecord, Message, Run, RunFence,
        RunId, Session, SessionId, ToolCallId, ToolCallRecord,
    },
    session as s,
    session::{
        AdmitOutcome, AdmitRequest, ExecutionStore, InboxKind, KeyedAdmitOutcome,
        KeyedAdmitRequest, Store, StoreError,
    },
};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

/// A custom host adapter forwarding the complete public transactional contract.
/// The counter observes calls; it is never execution truth or an ownership cache.
pub struct AuditedStore {
    inner: Arc<dyn Store>,
    calls: Arc<AtomicUsize>,
}
impl AuditedStore {
    #[must_use]
    pub fn new(inner: Arc<dyn Store>) -> Self {
        Self {
            inner,
            calls: Arc::default(),
        }
    }
    #[must_use]
    pub fn calls(&self) -> usize {
        self.calls.load(Ordering::Relaxed)
    }
}
// Keeping the delegation list explicit makes added Store capabilities reviewable.
// Each call uses the same inner transaction implementation and opaque grants.
macro_rules! delegate {
    (async { $(fn $name:ident($($arg:ident: $ty:ty),* $(,)?) -> $ret:ty;)* }
     sync { $(fn $sync:ident($($sarg:ident: $sty:ty),* $(,)?) -> $sret:ty;)* }) => {
        #[async_trait::async_trait]
        impl Store for AuditedStore {
            $(async fn $name(&self, $($arg: $ty),*) -> $ret {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.inner.$name($($arg),*).await
            })*
            $(fn $sync(&self, $($sarg: $sty),*) -> $sret {
                self.calls.fetch_add(1, Ordering::Relaxed);
                self.inner.$sync($($sarg),*)
            })*
        }
    };
}
delegate! {
    async {
        fn public_history_v2(_request: s::SnapshotRequest, ) -> Result<s::PublicHistoryOutcome, StoreError>;
        fn public_session_snapshot_v2(request: s::PublicSessionRequestV2, ) -> Result<s::PublicSessionOutcomeV2, s::PublicSessionError>;
        fn public_control_header_v2(_request: s::PublicHeaderRequestV2, ) -> Result<s::PublicHeaderOutcomeV2, s::PublicSessionError>;
        fn public_control_header(_request: s::PublicHeaderRequest, ) -> Result<s::PublicHeaderOutcome, s::PublicSessionError>;
        fn public_session_snapshot(request: s::PublicSessionRequest, ) -> Result<s::PublicSessionOutcome, s::PublicSessionError>;
        fn run_control_binding(_session: &SessionId, _run: &RunId, ) -> Result<s::RunControlBinding, s::RunControlError>;
        fn settle_waiting_cancellation(_request: s::WaitingCancellation, ) -> Result<s::RunControlSummary, s::RunControlError>;
        fn control_run(_request: s::RunControlRequest, ) -> Result<s::RunControlOutcome, s::RunControlError>;
        fn lookup_run_control(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::RunControlReceipt>, s::RunControlError>;
        fn run_controls(_target: &s::RunControlTarget, ) -> Result<s::RunControlPage, s::RunControlError>;
        fn mark_attempt_uncertain(_request: s::AttemptUncertainty, ) -> Result<s::AttemptControlOutcome, s::AttemptError>;
        fn retire_attempt(_request: s::AttemptRetirement, ) -> Result<s::AttemptControlOutcome, s::AttemptError>;
        fn queue_preparation_snapshot(_session: &SessionId, ) -> Result<s::SessionSummary, StoreError>;
        fn mutate_queue(_request: s::QueueMutation, ) -> Result<s::QueueOutcome, StoreError>;
        fn lookup_queue(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::QueueReceipt>, StoreError>;
        fn list_queue(_request: s::QueuePageRequest, ) -> Result<s::QueuePage, StoreError>;
        fn get_queue(_session: &SessionId, _id: &c::QueueId, ) -> Result<s::QueueSummary, StoreError>;
        fn retained_queue(_session: &SessionId, _id: &c::QueueId, ) -> Result<s::RetainedQueue, StoreError>;
        fn reserve_session_owner(_request: s::session_owner::ReserveSessionOwner, _parent: Option<s::session_owner::SessionOwnerParentAuthority>, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn reserve_preparation(_request: s::session_owner::preparation::ReservePreparation, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn lookup_preparation(_session: &SessionId, _key: &s::session_owner::preparation::PreparationKey, ) -> Result< Option<s::session_owner::preparation::PreparationReconciliation>, s::session_owner::SessionOwnerError, >;
        fn retain_preparation_resources(_grant: s::session_owner::preparation::PreparationGrant, ) -> Result< s::session_owner::preparation::PreparationResourceOutcome, s::session_owner::SessionOwnerError, >;
        fn register_session_service_dependency(request: s::session_owner::service::RegisterSessionServiceDependency) -> Result<s::session_owner::service::SessionServiceDependencyOutcome, s::session_owner::SessionOwnerError>;
        fn check_session_service_dependency_access(request: s::session_owner::service::SessionServiceDependencyAccess) -> Result<(), s::session_owner::SessionOwnerError>;
        fn complete_preparation(_joined: s::session_owner::preparation::JoinedPreparation, ) -> Result< s::session_owner::preparation::PreparationCompletion, s::session_owner::preparation::PreparationCommitError, >;
        fn request_session_owner_cancel(_request: s::session_owner::RequestSessionOwnerCancel, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn observe_session_owner_cancel(_request: s::session_owner::ObserveSessionOwnerCancel, _authority: s::session_owner::SessionOwnerAuthority, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn pending_session_owner_cancel(_authority: &s::session_owner::SessionOwnerAuthority, ) -> Result< Option<s::session_owner::SessionOwnerCancellation>, s::session_owner::SessionOwnerError, >;
        fn activate_owned_scope(request: s::session_owner::owned_scope::OwnedScopeActivation, authority: s::session_owner::SessionOwnerAuthority, parent: Option<s::session_owner::owned_scope::OwnedScopeParentAuthority>, ) -> Result< s::session_owner::owned_scope::OwnedScopeActivationOutcome, s::session_owner::SessionOwnerError, >;
        fn publish_owned_scope_activation(_request: s::session_owner::owned_scope::OwnedScopeActivation, _authority: s::session_owner::SessionOwnerAuthority, _parent: Option<s::session_owner::owned_scope::OwnedScopeParentAuthority>, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn join_owned_scope(_proof: s::session_owner::owned_scope::JoinedOwnedScope, _authority: s::session_owner::SessionOwnerAuthority, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn mutate_session_owner(_request: s::session_owner::MutateSessionOwner, _authority: Option<s::session_owner::SessionOwnerAuthority>, ) -> Result< s::session_owner::SessionOwnerOutcome, s::session_owner::SessionOwnerError, >;
        fn get_session_owner(_session: &SessionId, _id: &c::SessionOwnerId, ) -> Result< s::session_owner::SessionOwnerSnapshot, s::session_owner::SessionOwnerError, >;
        fn lookup_session_owner(_session: &SessionId, _id: &c::SessionOwnerOperationId, ) -> Result< Option<s::session_owner::SessionOwnerReceipt>, s::session_owner::SessionOwnerError, >;
        fn delete_session(_request: s::DeleteSession, ) -> Result<s::DeletionOutcome, s::DeletionError>;
        fn lookup_session_deletion(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::DeletionReceipt>, s::DeletionError>;
        fn session_disposition(_session: &SessionId, ) -> Result<s::SessionDisposition, s::DeletionError>;
        fn fork_session(_request: s::ForkSession, ) -> Result<s::ForkOutcome, s::ForkError>;
        fn lookup_session_fork(_target: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::ForkReceipt>, s::ForkError>;
        fn manage_session(_request: s::ManageSession, ) -> Result<s::ManagementReceipt, s::ManagementError>;
        fn lookup_session_management(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::ManagementReceipt>, s::ManagementError>;
        fn session_summary(_session: &SessionId, ) -> Result<s::SessionSummary, s::ManagementError>;
        fn public_run_state(_session: &SessionId, _max_bytes: usize, ) -> Result<s::PublicRunOutcome, StoreError>;
        fn list_sessions(_request: s::ListSessions, ) -> Result<s::SessionListPage, s::ManagementError>;
        fn approval_snapshot(_session: &SessionId, _run: &RunId, ) -> Result<Option<s::ApprovalSnapshot>, s::ApprovalError>;
        fn original_approval_plan(_session: &SessionId, _run: &RunId, _target: &s::ContinuationTarget, ) -> Result<c::InputFingerprint, s::ApprovalError>;
        fn lookup_approval(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::ApprovalReceipt>, s::ApprovalError>;
        fn answer_approval(_answer: s::ApprovalAnswer, ) -> Result<s::ApprovalReceipt, s::ApprovalError>;
        fn reclaim_approval_continuation(_request: s::ApprovalContinuationRecovery, ) -> Result<s::ApprovalClaimOutcome, s::ApprovalError>;
        fn terminate_approval(_termination: s::ApprovalTermination, ) -> Result<s::ApprovalReceipt, s::ApprovalError>;
        fn claim_approval_continuation(_claim: s::ApprovalContinuationClaim, ) -> Result<s::ApprovalClaimOutcome, s::ApprovalError>;
        fn frontend_call_origin(_session: &SessionId, _run: &RunId, _call: &ToolCallId, ) -> Result<Option<s::FrontendCallOrigin>, s::ToolContinuationError>;
        fn lookup_frontend_staging(_session: &SessionId, _id: &c::ControlId, ) -> Result< Option<s::FrontendStagingReceipt>, s::ToolContinuationError, >;
        fn claim_frontend_continuation(_claim: s::FrontendContinuationClaim, ) -> Result< s::ToolContinuationClaimOutcome, s::ToolContinuationError, >;
        fn recover_frontend_continuation(_request: s::FrontendContinuationRecovery, ) -> Result< s::ToolContinuationClaimOutcome, s::ToolContinuationError, >;
        fn tool_continuation_snapshot(_session: &SessionId, _run: &RunId, ) -> Result< Option<s::ToolContinuationSnapshot>, s::ToolContinuationError, >;
        fn original_tool_continuation_plan(_session: &SessionId, _run: &RunId, _target: &s::ContinuationTarget, ) -> Result<c::InputFingerprint, s::ToolContinuationError>;
        fn lookup_tool_continuation(_session: &SessionId, _id: &c::ControlId, ) -> Result< Option<s::ToolContinuationReceipt>, s::ToolContinuationError, >;
        fn answer_tool_interaction(_answer: s::ToolInteractionAnswer, ) -> Result<s::ToolContinuationReceipt, s::ToolContinuationError>;
        fn recover_tool_continuation(_request: s::ToolContinuationRecovery, ) -> Result< s::ToolContinuationClaimOutcome, s::ToolContinuationError, >;
        fn claim_tool_continuation(_claim: s::ToolContinuationClaim, ) -> Result< s::ToolContinuationClaimOutcome, s::ToolContinuationError, >;
        fn admit_bound_run(_request: s::BoundAdmitRequest, ) -> Result<AdmitOutcome, StoreError>;
        fn admit_bound_keyed_run(_request: s::BoundKeyedAdmitRequest, ) -> Result<KeyedAdmitOutcome, StoreError>;
        fn execution_binding(_session: &SessionId, _run: &RunId, ) -> Result<Option<s::RetainedExecutionBinding>, StoreError>;
        fn tool_phase(_run: &RunId, _id: &c::PhaseId, ) -> Result<s::VersionedPhase, s::PhaseError>;
        fn mark_phase_uncertain(_request: s::PhaseUncertainty, ) -> Result<s::PhaseControlOutcome, s::PhaseError>;
        fn record_phase_cleanup(_request: s::PhaseCleanup, ) -> Result<s::PhaseControlOutcome, s::PhaseError>;
        fn list_tool_phases(_request: s::PhasePageRequest, ) -> Result<s::PhasePage, s::PhaseError>;
        fn resolve_uncertain_phase(_resolution: s::PhaseResolution, ) -> Result<s::PhaseResolutionOutcome, s::PhaseError>;
        fn lookup_phase_mutation(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::PhaseReceipt>, s::PhaseError>;
        fn execution_attempt(_run: &RunId, _id: &c::AttemptId, ) -> Result<s::VersionedAttempt, s::AttemptError>;
        fn latest_execution_attempt(_run: &RunId, ) -> Result<Option<s::VersionedAttempt>, s::AttemptError>;
        fn lookup_attempt_mutation(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::AttemptReceipt>, s::AttemptError>;
        fn public_state(_session: &SessionId, ) -> Result<s::PublicStateSnapshot, s::PublicStateError>;
        fn lookup_public_state_mutation(_session: &SessionId, _id: &c::ControlId, ) -> Result<Option<s::PublicStateReceipt>, s::PublicStateError>;
        fn session_revision(_session: &SessionId, ) -> Result<c::SessionRevision, StoreError>;
        fn admit_run(request: AdmitRequest) -> Result<AdmitOutcome, StoreError>;
        fn admit_keyed_run(_request: KeyedAdmitRequest, ) -> Result<KeyedAdmitOutcome, StoreError>;
        fn lookup_admission(_session: &SessionId, _key: &AdmissionKey, ) -> Result<Option<AdmissionReceipt>, StoreError>;
        fn load_admission_execution(_session: &SessionId, _key: &AdmissionKey, ) -> Result< Option<s::AdmissionExecutionRecord>, s::AdmissionExecutionError, >;
        fn admission_execution_state(_run: &RunId, ) -> Result<Option<s::AdmissionExecutionState>, StoreError>;
        fn claim_unstarted_admission(_request: s::ClaimUnstartedAdmissionRequest, ) -> Result<s::ClaimedAdmission, s::AdmissionExecutionError>;
        fn abandon_run(_request: c::AbandonRequest, ) -> Result<c::AbandonOutcome, c::AbandonError>;
        fn execution(fence: RunFence) -> Result<Box<dyn ExecutionStore>, StoreError>;
        fn get_session(id: &SessionId) -> Result<Option<Session>, StoreError>;
        fn get_run(id: &RunId) -> Result<Option<Run>, StoreError>;
        fn snapshot(_request: s::SnapshotRequest, ) -> Result<s::SnapshotOutcome, StoreError>;
        fn public_history(request: s::SnapshotRequest, ) -> Result<s::PublicHistoryOutcome, StoreError>;
        fn list_messages(id: &SessionId, epoch: Option<EpochId>, ) -> Result<Vec<Message>, StoreError>;
        fn list_all_messages(id: &SessionId) -> Result<Vec<Message>, StoreError>;
        fn list_events(id: &SessionId, after: Option<EventCursor>, limit: usize, ) -> Result<Vec<EventRecord>, StoreError>;
        fn list_unfinished_runs() -> Result<Vec<Run>, StoreError>;
        fn list_unfinished_tool_calls(run: &RunId, ) -> Result<Vec<ToolCallRecord>, StoreError>;
        fn claim_expired_run(run: &RunId, owner: &str) -> Result<RunFence, StoreError>;
        fn get_extension_state(extension_id: &str, session: &SessionId, ) -> Result<BTreeMap<String, String>, StoreError>;
        fn enqueue_inbox(session: &SessionId, kind: InboxKind, message: Message, ) -> Result<(), StoreError>;
    }
    sync {
        fn supports_run_controls() -> bool;
        fn supports_future_queue() -> bool;
        fn supports_session_owners() -> bool;
        fn supports_preparation_protocol() -> bool;
        fn supports_preparation_resources() -> bool;
        fn supports_session_service_dependencies() -> bool;
        fn supports_session_owner_cancellation() -> bool;
        fn supports_owned_scopes() -> bool;
        fn supports_owned_attempts() -> bool;
        fn supports_owned_tool_phases() -> bool;
        fn supports_frontend_tools() -> bool;
        fn supports_tool_continuations() -> bool;
        fn supports_pre_effect_approvals() -> bool;
    }
}
