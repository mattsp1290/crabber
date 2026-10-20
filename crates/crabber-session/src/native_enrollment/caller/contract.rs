//! Real native mutation/rollback/ACK-loss tests, never producer evidence.
use super::Probe;
use crate::{AttemptError, AttemptMutationOutcome, Store};
use crabber_core::{InputFingerprint, ManualClock, RunId, SessionId};

pub(crate) trait Harness: Store {
    fn probe(&self) -> &Probe;
    fn fault(&self, stage: u8);
    fn phase_fault(&self, stage: u8);
    fn initial_fault(&self, stage: u8);
    async fn complete_with_proof(
        &self,
        joined: crate::session_owner::preparation::JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        crate::session_owner::preparation::PreparationCompletion,
        crate::session_owner::preparation::PreparationCommitError,
    >;
    fn approval_fault(&self, stage: u8);
    fn tool_fault(&self, stage: u8);
    async fn tool_with_proof(
        &self,
        input: super::tool::ToolInput<'_>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::ToolContinuationClaimOutcome, crate::ToolContinuationError>;
    async fn approval_with_proof(
        &self,
        request: super::ApprovalInput<'_>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::ApprovalClaimOutcome, crate::ApprovalError>;
    async fn initial_retained(&self, session: &SessionId) -> InputFingerprint;
    async fn phase_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::PhaseMutation,
        authority: Option<crate::PhaseWriteAuthority>,
        start: Option<crate::PhaseStart>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::PhaseMutationOutcome, crate::PhaseError>;

    async fn attempt_with_proof(
        &self,
        fence: crabber_core::RunFence,
        request: crate::AttemptMutation,
        authority: Option<crate::AttemptAuthority>,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<AttemptMutationOutcome, AttemptError>;
    async fn legacy_with_proof(
        &self,
        fence: crabber_core::RunFence,
        call: &crabber_core::ToolCallId,
        event: &crabber_core::EventRecord,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::StoreError>;
    async fn begin_with_proof(
        &self,
        fence: crabber_core::RunFence,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::AdmissionExecutionError>;
    async fn recover_admission_with_proof(
        &self,
        request: crate::ClaimUnstartedAdmissionRequest,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crate::ClaimedAdmission, crate::AdmissionExecutionError>;
    async fn legacy_expired_with_proof(
        &self,
        id: &RunId,
        owner: &str,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<crabber_core::RunFence, crate::StoreError>;
    async fn retained(&self, session: &SessionId, run: &RunId, birth: bool) -> InputFingerprint;
}

pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    use crate::attempt_contract::RawAttemptCase as Case;
    for case in [
        Case::Reserve,
        Case::ReserveBound,
        Case::Reclaim,
        Case::Begin,
    ] {
        for ack_loss in [false, true] {
            let (admitted, execution, request, authority) =
                crate::attempt_contract::raw_attempt_fixture(store, clock, case).await;
            let session = &admitted.session.id;
            let run = &admitted.run.id;
            let before = store.retained(session, run, false).await;
            let original = store.retained(session, run, true).await;
            store.probe().arm(session, &request.id, true);
            assert_eq!(
                execution
                    .mutate_attempt(request.clone(), authority.clone())
                    .await
                    .unwrap_err(),
                AttemptError::UnknownStoreOutcome
            );
            assert_eq!(store.probe().hits().len(), 1);
            assert_eq!(store.retained(session, run, false).await, before);
            assert_eq!(store.retained(session, run, true).await, original);
            store.probe().arm(session, &request.id, false);
            store.fault(if ack_loss { 2 } else { 0 });
            let result = execution
                .mutate_attempt(request.clone(), authority.clone())
                .await;
            store.fault(0);
            let saved = store
                .lookup_attempt_mutation(session, &request.id)
                .await
                .unwrap()
                .unwrap();
            if ack_loss {
                assert_eq!(result.unwrap_err(), AttemptError::UnknownStoreOutcome);
            } else {
                assert!(
                    matches!(result.unwrap(), AttemptMutationOutcome::Applied { receipt, .. } if receipt == saved)
                );
            }
            assert_eq!(store.probe().hits().len(), 1);
            assert_ne!(store.retained(session, run, false).await, before);
            assert_eq!(store.retained(session, run, true).await, original);
            let committed = store.retained(session, run, false).await;
            store.probe().arm(session, &request.id, true);
            assert!(
                matches!(execution.mutate_attempt(request.clone(), None).await.unwrap(), AttemptMutationOutcome::Replayed(receipt) if receipt == saved)
            );
            assert_eq!(
                store.probe().hits(),
                Vec::<crabber_core::InputFingerprint>::new()
            );
            assert_eq!(store.retained(session, run, false).await, committed);
            // A real failed revision check never reaches the fresh hook.
            let mut stale = request;
            stale.id = crabber_core::ControlId::new();
            store.probe().arm(session, &stale.id, true);
            assert_eq!(
                execution
                    .mutate_attempt(stale, authority)
                    .await
                    .unwrap_err(),
                AttemptError::RevisionConflict
            );
            assert_eq!(
                store.probe().hits(),
                Vec::<crabber_core::InputFingerprint>::new()
            );
            assert_eq!(store.retained(session, run, false).await, committed);
        }
    }
}

#[path = "budget_contract.rs"]
mod budget_contract;
pub(crate) use budget_contract::run as budgets;

pub(crate) fn snapshot_digest(value: &impl serde::Serialize) -> InputFingerprint {
    use sha2::{Digest, Sha256};
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(serde_json::to_vec(value).unwrap())
    ))
    .unwrap()
}

#[path = "phase_contract.rs"]
mod phase;
pub(crate) use phase::run as phases;

#[path = "compat_contract.rs"]
mod compat_contract;
pub(crate) use compat_contract::run as compatibility;

#[path = "recovery_contract.rs"]
mod recovery_contract;
pub(crate) use recovery_contract::run as recoveries;
#[path = "initial_contract.rs"]
mod initial;
pub(crate) use initial::run as initials;

#[path = "prepared_recovery_contract.rs"]
mod prepared_recovery_contract;
pub(crate) use prepared_recovery_contract::run as prepared_recoveries;
#[path = "recovery/legacy_contract.rs"]
mod legacy_expired;
pub(crate) use legacy_expired::run as legacy_expired_claims;
#[path = "approval_contract.rs"]
mod approval;
pub(crate) use approval::run as approvals;

#[path = "tool_contract.rs"]
mod tool;
pub(crate) use tool::run as tools;
