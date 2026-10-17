//! Apply the exact task-returned native intent under the preparation gate.
#[path = "native/control.rs"]
mod control;
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
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
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        let completed = match operation {
            PreparedOperation::ExpiredRunResume(prepared) => PreparationCompletion::ExpiredRun(
                self.claim_prepared_expired(state, &prepared, proof)?,
            ),
            PreparedOperation::AdmissionRecovery(recovery) => {
                PreparationCompletion::AdmissionRecovery(
                    crate::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(Box::new(
                        self.claim_prepared_admission(state, &recovery, proof, read_proof)?,
                    )),
                )
            }
            PreparedOperation::FrontendClaim(claim) => PreparationCompletion::ToolContinuation(
                self.claim_frontend_guarded(state, &claim, proof)?.0,
            ),
            PreparedOperation::FrontendRecovery(recovery) => {
                PreparationCompletion::ToolContinuation(
                    self.recover_frontend_guarded(state, &recovery, proof)?.0,
                )
            }
            PreparedOperation::ApprovalClaim(claim) => PreparationCompletion::Approval(
                self.claim_approval_guarded(state, &claim, Some(proof))?.0,
            ),
            PreparedOperation::ToolClaim(claim) => PreparationCompletion::ToolContinuation(
                self.claim_tools_guarded(state, &claim, Some(proof))?.0,
            ),
            PreparedOperation::ApprovalRecovery(recovery) => PreparationCompletion::Approval(
                self.recover_approval_guarded(state, &recovery, Some(proof))?
                    .0,
            ),
            PreparedOperation::ToolRecovery(recovery) => PreparationCompletion::ToolContinuation(
                self.recover_tools_guarded(state, &recovery, Some(proof))?.0,
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
                bound.binding.validate(&bound.request)?;
                let admitted = super::super::super::admission::admit_transaction_with_proof(
                    state,
                    &bound.request,
                    self.clock.now(),
                    false,
                    guard,
                    read_proof,
                )?;
                let retained = RetainedExecutionBinding::new(
                    &admitted.run,
                    &bound.request.user_message,
                    bound.binding,
                );
                state
                    .execution_bindings
                    .insert(admitted.run.id.clone(), retained.encode()?);
                PreparationCompletion::Admitted(admitted)
            }
            PreparedOperation::AdmitKeyed(bound) => {
                let outcome = super::super::super::admission::admit_keyed_with_proof(
                    state,
                    &bound.keyed,
                    self.clock.now(),
                    guard,
                    read_proof,
                )?;
                if let KeyedAdmitOutcome::Started { admitted, .. } = &outcome {
                    let retained = RetainedExecutionBinding::new(
                        &admitted.run,
                        &bound.keyed.request.user_message,
                        bound.binding,
                    );
                    state
                        .execution_bindings
                        .insert(admitted.run.id.clone(), retained.encode()?);
                }
                PreparationCompletion::Keyed(outcome)
            }
        };
        control::verify(state, proof.grant, &completed)?;
        Ok(completed)
    }
}
