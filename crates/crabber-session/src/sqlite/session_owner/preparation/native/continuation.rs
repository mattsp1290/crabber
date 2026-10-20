//! Prepared continuation writes share the enclosing source allowance.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{
    PreparationCommitError, PreparedOperation, proof::VerifiedPreparation,
};
use crate::transaction::Transaction;
use sqlx::Sqlite;
impl crate::SqliteStore {
    pub(super) async fn apply_prepared_continuation(
        &self,
        tx: &mut Transaction<'_, Sqlite>,
        operation: PreparedOperation,
        proof: &VerifiedPreparation<'_>,
        read_proof: &mut Proof,
    ) -> Result<Continuation, PreparationCommitError> {
        let run_id = match &operation {
            PreparedOperation::FrontendClaim(claim) => &claim.claim.run_id,
            PreparedOperation::FrontendRecovery(recovery) => &recovery.original.claim.run_id,
            PreparedOperation::ApprovalClaim(claim) => &claim.run_id,
            PreparedOperation::ToolClaim(claim) => &claim.run_id,
            PreparedOperation::ApprovalRecovery(recovery) => &recovery.original.run_id,
            PreparedOperation::ToolRecovery(recovery) => &recovery.original.run_id,
            _ => return Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
        };
        let run = crate::sqlite::records::load_run_with_proof(tx, run_id, read_proof).await?;
        match operation {
            PreparedOperation::FrontendClaim(r) => tool_result(
                self.claim_frontend_guarded(
                    tx,
                    &run,
                    &r,
                    proof,
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            ),
            PreparedOperation::FrontendRecovery(r) => tool_result(
                self.recover_frontend_guarded(
                    tx,
                    &run,
                    &r,
                    proof,
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            ),
            PreparedOperation::ToolClaim(r) => tool_result(
                self.claim_tools_guarded(
                    tx,
                    &run,
                    &r,
                    Some(proof),
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            ),
            PreparedOperation::ToolRecovery(r) => tool_result(
                self.recover_tools_guarded(
                    tx,
                    &run,
                    &r,
                    Some(proof),
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            ),
            PreparedOperation::ApprovalClaim(r) => Ok(Continuation::Approval(
                self.claim_approval_guarded(
                    tx,
                    &run,
                    &r,
                    Some(proof),
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            )),
            PreparedOperation::ApprovalRecovery(r) => Ok(Continuation::Approval(
                self.recover_approval_guarded(
                    tx,
                    &run,
                    &r,
                    Some(proof),
                    crate::native_enrollment::caller::Caller::Raw,
                    read_proof,
                )
                .await?
                .0,
            )),
            _ => Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into()),
        }
    }
}

pub(super) enum Continuation {
    VerifiedTool(crate::sqlite::session_owner::preparation::VerifiedTool),
    Approval(crate::ApprovalClaimOutcome),
    ReplayedTool(crate::ToolContinuationReceipt),
}
fn tool_result(
    result: crate::sqlite::tool_continuation::native_caller::ToolMutation,
) -> Result<Continuation, PreparationCommitError> {
    use crate::sqlite::tool_continuation::native_caller::ToolMutation;
    match result {
        ToolMutation::Prepared(v) => Ok(Continuation::VerifiedTool(v)),
        ToolMutation::Replayed(r) => Ok(Continuation::ReplayedTool(r)),
        ToolMutation::Applied(_) => {
            Err(crate::session_owner::SessionOwnerError::UnknownStoreOutcome.into())
        }
    }
}
