//! Bounded original metadata reconciliation, never replacement dispatch authority.
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
};
use crate::{KeyedAdmitOutcome, StoreError};
impl super::super::SqliteStore {
    pub(super) async fn replay_prepared(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        joined: &JoinedPreparation,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        if let PreparedOperation::AdmitKeyed(bound) = joined.observed_operation()
            && let Some(outcome) = self.replay_keyed(tx, bound).await?
        {
            super::verify_replay(tx, joined.observed_grant()).await?;
            return Ok(Some(outcome));
        }
        if let PreparedOperation::Queue { request, .. } = joined.observed_operation()
            && let Some(saved) = self.queue_replay(tx, request).await?
        {
            super::verify_replay(tx, joined.observed_grant()).await?;
            return Ok(Some(PreparationCompletion::Queue(saved)));
        }
        if let Some(outcome) = Self::replay_frontend(tx, joined).await? {
            return Ok(Some(outcome));
        }
        match joined.observed_operation() {
            PreparedOperation::ApprovalClaim(claim) => {
                if let Some(saved) = super::super::super::approval::replay_prepared(
                    tx,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ApprovalRecovery(recovery) => {
                if let Some(saved) = super::super::super::approval::replay_prepared(
                    tx,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            _ => (),
        }

        Ok(None)
    }
}

impl super::super::SqliteStore {
    async fn replay_keyed(
        &self,
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        bound: &crate::BoundKeyedAdmitRequest,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let session = bound.binding.request.session_id.clone();

        bound.binding.validate_keyed(&bound.keyed)?;
        if super::super::super::records::lookup_receipt(tx, &session, &bound.keyed.options.key)
            .await?
            .is_some()
        {
            let outcome = self
                .admit_keyed_transaction(tx, bound.keyed.clone())
                .await?;
            if let KeyedAdmitOutcome::Replayed(receipt) = &outcome
                && super::super::super::execution_binding::read(tx, &session, &receipt.run_id)
                    .await?
                    .is_none_or(|saved| saved.capsule != bound.binding)
            {
                return Err(StoreError::AdmissionConflict.into());
            }

            return Ok(Some(PreparationCompletion::Keyed(outcome)));
        }

        Ok(None)
    }
}

impl super::super::SqliteStore {
    async fn replay_frontend(
        tx: &mut sqlx::Transaction<'_, sqlx::Sqlite>,
        joined: &JoinedPreparation,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        match joined.observed_operation() {
            PreparedOperation::FrontendClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &claim.claim.session_id,
                    &claim.claim.run_id,
                    &claim.claim.id,
                    &claim.fingerprint()?,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::FrontendRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay_prepared(
                    tx,
                    &recovery.original.claim.session_id,
                    &recovery.original.claim.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant()).await?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            _ => (),
        }
        Ok(None)
    }
}
