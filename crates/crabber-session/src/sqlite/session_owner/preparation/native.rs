//! Apply the exact task-returned native intent under the preparation gate.
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
impl super::super::SqliteStore {
    pub(super) async fn apply_prepared_native(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        Ok(match operation {
            PreparedOperation::AdmissionRecovery(recovery) => {
                PreparationCompletion::AdmissionRecovery(
                    crate::session_owner::preparation::AdmissionRecoveryOutcome::Claimed(Box::new(
                        super::super::super::admission_execution::claim_prepared(
                            self, tx, &recovery, proof,
                        )
                        .await?,
                    )),
                )
            }
            PreparedOperation::FrontendClaim(claim) => {
                let run = super::super::super::records::load_run(tx, &claim.claim.run_id).await?;
                PreparationCompletion::ToolContinuation(
                    self.claim_frontend_guarded(tx, &run, &claim, proof)
                        .await?
                        .0,
                )
            }
            PreparedOperation::FrontendRecovery(recovery) => {
                let run =
                    super::super::super::records::load_run(tx, &recovery.original.claim.run_id)
                        .await?;
                PreparationCompletion::ToolContinuation(
                    self.recover_frontend_guarded(tx, &run, &recovery, proof)
                        .await?
                        .0,
                )
            }
            PreparedOperation::ApprovalClaim(claim) => {
                let run = super::super::super::records::load_run(tx, &claim.run_id).await?;
                PreparationCompletion::Approval(
                    self.claim_approval_guarded(tx, &run, &claim, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ToolClaim(claim) => {
                let run = super::super::super::records::load_run(tx, &claim.run_id).await?;
                PreparationCompletion::ToolContinuation(
                    self.claim_tools_guarded(tx, &run, &claim, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ApprovalRecovery(recovery) => {
                let run =
                    super::super::super::records::load_run(tx, &recovery.original.run_id).await?;
                PreparationCompletion::Approval(
                    self.recover_approval_guarded(tx, &run, &recovery, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ToolRecovery(recovery) => {
                let run =
                    super::super::super::records::load_run(tx, &recovery.original.run_id).await?;
                PreparationCompletion::ToolContinuation(
                    self.recover_tools_guarded(tx, &run, &recovery, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::Queue { request, .. } => {
                let digest = request.fingerprint()?;
                PreparationCompletion::Queue(
                    Box::pin(self.queue_apply_guarded(tx, request, digest, Some(proof))).await?,
                )
            }
            PreparedOperation::Admit(bound) => {
                bound.binding.validate(&bound.request)?;
                let user = bound.request.user_message.clone();
                let admitted = self
                    .admit_transaction_guarded(tx, bound.request, false, guard)
                    .await?;
                super::super::super::execution_binding::save(
                    tx,
                    &RetainedExecutionBinding::new(&admitted.run, &user, bound.binding),
                )
                .await?;
                PreparationCompletion::Admitted(admitted)
            }
            PreparedOperation::AdmitKeyed(bound) => {
                let user = bound.keyed.request.user_message.clone();
                let outcome = self.admit_keyed_guarded(tx, bound.keyed, guard).await?;
                if let KeyedAdmitOutcome::Started { admitted, .. } = &outcome {
                    super::super::super::execution_binding::save(
                        tx,
                        &RetainedExecutionBinding::new(&admitted.run, &user, bound.binding),
                    )
                    .await?;
                }
                PreparationCompletion::Keyed(outcome)
            }
        })
    }
}
