//! Apply the exact task-returned native intent under the preparation gate.
#[path = "native/control.rs"]
mod control;
#[path = "native/initial.rs"]
mod initial;
use crate::native_enrollment::caller::Caller;
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
impl super::super::MemoryStore {
    pub(super) fn apply_prepared_native(
        &self,
        state: &mut super::super::super::State,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
        queue_selection: Option<super::super::super::queue::selection::Selected>,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let completed = match operation {
            PreparedOperation::ExpiredRunResume(prepared) => PreparationCompletion::ExpiredRun(
                self.claim_prepared_expired(state, &prepared, proof, read_proof)?,
            ),
            PreparedOperation::AdmissionRecovery(recovery) => {
                PreparationCompletion::AdmissionRecovery(
                    crate::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(Box::new(
                        self.claim_prepared_admission(state, &recovery, proof, read_proof)?,
                    )),
                )
            }
            operation @ (PreparedOperation::FrontendClaim(_)
            | PreparedOperation::FrontendRecovery(_)
            | PreparedOperation::ToolClaim(_)
            | PreparedOperation::ToolRecovery(_)) => {
                match self.apply_prepared_tools(state, operation, proof, read_proof)? {
                    crate::memory::tool_continuation::native_caller::ToolMutation::Prepared(
                        verified,
                    ) => {
                        return Ok(PreparationCompletion::ToolContinuation(
                            verified.into_outcome(),
                        ));
                    }
                    crate::memory::tool_continuation::native_caller::ToolMutation::Replayed(
                        receipt,
                    ) => PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(receipt),
                    ),
                    crate::memory::tool_continuation::native_caller::ToolMutation::Applied(_) => {
                        return Err(
                            crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()
                        );
                    }
                }
            }

            PreparedOperation::ApprovalClaim(claim) => PreparationCompletion::Approval(
                self.claim_approval_guarded(state, &claim, Some(proof), Caller::Raw, read_proof)?
                    .0,
            ),

            PreparedOperation::ApprovalRecovery(recovery) => PreparationCompletion::Approval(
                self.recover_approval_guarded(
                    state,
                    &recovery,
                    Some(proof),
                    Caller::Raw,
                    read_proof,
                )?
                .0,
            ),

            PreparedOperation::Queue { request, .. } => {
                let digest = request.fingerprint()?;
                PreparationCompletion::Queue(self.queue_apply_guarded(
                    state,
                    request,
                    digest,
                    Some(proof),
                    read_proof,
                    queue_selection.ok_or(crate::StoreError::AdmissionConflict)?,
                )?)
            }
            PreparedOperation::Admit(bound) => {
                self.admit_prepared_bound(state, &bound, proof, read_proof)?
            }
            PreparedOperation::AdmitKeyed(bound) => {
                self.admit_prepared_keyed(state, &bound, proof, read_proof)?
            }
        };
        control::verify(state, proof.grant, &completed)?;
        Ok(completed)
    }
}

pub(in crate::memory) use control::{VerifiedTool, verify_tool};

impl crate::MemoryStore {
    fn apply_prepared_tools(
        &self,
        state: &mut crate::memory::State,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
        read_proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        crate::memory::tool_continuation::native_caller::ToolMutation,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        Ok(match operation {
            PreparedOperation::FrontendClaim(r) => {
                self.claim_frontend_guarded(state, &r, proof, Caller::Raw, read_proof)?
                    .0
            }
            PreparedOperation::FrontendRecovery(r) => {
                self.recover_frontend_guarded(state, &r, proof, Caller::Raw, read_proof)?
                    .0
            }
            PreparedOperation::ToolClaim(r) => {
                self.claim_tools_guarded(state, &r, Some(proof), Caller::Raw, read_proof)?
                    .0
            }
            PreparedOperation::ToolRecovery(r) => {
                self.recover_tools_guarded(state, &r, Some(proof), Caller::Raw, read_proof)?
                    .0
            }
            _ => return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
        })
    }
}
