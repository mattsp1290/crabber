//! Prepared continuation writes share the enclosing source allowance.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{
    PreparationCommitError, PreparationCompletion, PreparedOperation, proof::VerifiedPreparation,
};
use sqlx::{Postgres, Transaction};
impl crate::PostgresStore {
    pub(super) async fn apply_prepared_continuation(
        &self,
        tx: &mut Transaction<'_, Postgres>,
        operation: PreparedOperation,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut Proof,
    ) -> Result<PreparationCompletion, PreparationCommitError> {
        let run_id = match &operation {
            PreparedOperation::FrontendClaim(claim) => &claim.claim.run_id,
            PreparedOperation::FrontendRecovery(recovery) => &recovery.original.claim.run_id,
            PreparedOperation::ApprovalClaim(claim) => &claim.run_id,
            PreparedOperation::ToolClaim(claim) => &claim.run_id,
            PreparedOperation::ApprovalRecovery(recovery) => &recovery.original.run_id,
            PreparedOperation::ToolRecovery(recovery) => &recovery.original.run_id,
            _ => return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
        };
        let run = crate::postgres::load_run_with_proof(tx, run_id, read_proof).await?;
        Ok(match operation {
            PreparedOperation::FrontendClaim(claim) => PreparationCompletion::ToolContinuation(
                self.claim_frontend_guarded(tx, &run, &claim, proof, read_proof)
                    .await?
                    .0,
            ),
            PreparedOperation::FrontendRecovery(recovery) => {
                PreparationCompletion::ToolContinuation(
                    self.recover_frontend_guarded(tx, &run, &recovery, proof, read_proof)
                        .await?
                        .0,
                )
            }
            PreparedOperation::ApprovalClaim(claim) => PreparationCompletion::Approval(
                self.claim_approval_guarded(tx, &run, &claim, Some(proof), read_proof)
                    .await?
                    .0,
            ),
            PreparedOperation::ToolClaim(claim) => PreparationCompletion::ToolContinuation(
                self.claim_tools_guarded(tx, &run, &claim, Some(proof), read_proof)
                    .await?
                    .0,
            ),
            PreparedOperation::ApprovalRecovery(recovery) => PreparationCompletion::Approval(
                self.recover_approval_guarded(tx, &run, &recovery, Some(proof), read_proof)
                    .await?
                    .0,
            ),
            PreparedOperation::ToolRecovery(recovery) => PreparationCompletion::ToolContinuation(
                self.recover_tools_guarded(tx, &run, &recovery, Some(proof), read_proof)
                    .await?
                    .0,
            ),
            _ => return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
        })
    }
}
