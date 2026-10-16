//! Apply the exact task-returned native intent under the preparation gate.
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
impl super::super::MemoryStore {
    pub(super) fn apply_prepared_native(
        &self,
        state: &mut super::super::super::State,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        Ok(match operation {
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
                )?)
            }
            PreparedOperation::Admit(bound) => {
                bound.binding.validate(&bound.request)?;
                let admitted = super::super::super::admission::admit_transaction_guarded(
                    state,
                    &bound.request,
                    self.clock.now(),
                    false,
                    guard,
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
                let outcome = super::super::super::admission::admit_keyed_guarded(
                    state,
                    &bound.keyed,
                    self.clock.now(),
                    guard,
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
        })
    }
}
