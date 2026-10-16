//! Apply the exact task-returned native intent under the preparation gate.
#[path = "native/control.rs"]
mod control;
use crate::session_owner::preparation::{PreparationCompletion, PreparedOperation};
use crate::{KeyedAdmitOutcome, RetainedExecutionBinding};
impl super::super::PostgresStore {
    pub(super) async fn apply_prepared_native(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        operation: PreparedOperation,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
        let completed = match operation {
            PreparedOperation::ExpiredRunResume(prepared) => PreparationCompletion::ExpiredRun(
                self.claim_prepared_expired(tx, &prepared, proof).await?,
            ),
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
                let run = super::super::super::load_run(tx, &claim.claim.run_id, true).await?;
                PreparationCompletion::ToolContinuation(
                    self.claim_frontend_guarded(tx, &run, &claim, proof)
                        .await?
                        .0,
                )
            }
            PreparedOperation::FrontendRecovery(recovery) => {
                let run = super::super::super::load_run(tx, &recovery.original.claim.run_id, true)
                    .await?;
                PreparationCompletion::ToolContinuation(
                    self.recover_frontend_guarded(tx, &run, &recovery, proof)
                        .await?
                        .0,
                )
            }
            PreparedOperation::ApprovalClaim(claim) => {
                let run = super::super::super::load_run(tx, &claim.run_id, true).await?;
                PreparationCompletion::Approval(
                    self.claim_approval_guarded(tx, &run, &claim, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ToolClaim(claim) => {
                let run = super::super::super::load_run(tx, &claim.run_id, true).await?;
                PreparationCompletion::ToolContinuation(
                    self.claim_tools_guarded(tx, &run, &claim, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ApprovalRecovery(recovery) => {
                let run =
                    super::super::super::load_run(tx, &recovery.original.run_id, true).await?;
                PreparationCompletion::Approval(
                    self.recover_approval_guarded(tx, &run, &recovery, Some(proof))
                        .await?
                        .0,
                )
            }
            PreparedOperation::ToolRecovery(recovery) => {
                let run =
                    super::super::super::load_run(tx, &recovery.original.run_id, true).await?;
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
            PreparedOperation::Admit(bound) => self.admit_prepared_bound(tx, *bound, proof).await?,
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
        };
        control::verify(tx, proof.grant, &completed).await?;
        Ok(completed)
    }
}

impl super::super::PostgresStore {
    async fn admit_prepared_bound(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Postgres>,
        bound: crate::BoundAdmitRequest,
        proof: &crate::session_owner::preparation::proof::VerifiedPreparation<'_>,
    ) -> Result<PreparationCompletion, crate::session_owner::preparation::PreparationCommitError>
    {
        let guard = crate::queue::AdmissionGuard::Preparation(proof);
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
        Ok(PreparationCompletion::Admitted(admitted))
    }
}
