//! Bounded original metadata reconciliation, never replacement dispatch authority.
#[path = "replay/admission.rs"]
mod admission;
#[path = "replay/expired.rs"]
mod expired;
#[path = "replay/recovery.rs"]
mod recovery;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
};
use crate::{KeyedAdmitOutcome, StoreError};
impl super::super::SqliteStore {
    pub(super) async fn replay_prepared(
        &self,
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        if let Some(outcome) = Self::replay_expired(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_recovery(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_admission(tx, joined, proof).await? {
            return Ok(Some(outcome));
        }
        if let PreparedOperation::AdmitKeyed(bound) = joined.observed_operation()
            && let Some(outcome) = self.replay_keyed(tx, bound, proof).await?
        {
            super::verify_replay(tx, joined.observed_grant(), proof).await?;
            return Ok(Some(outcome));
        }
        if let PreparedOperation::Queue { request, .. } = joined.observed_operation()
            && let Some(saved) = self.queue_replay(tx, request, proof).await?
        {
            super::verify_replay(tx, joined.observed_grant(), proof).await?;
            return Ok(Some(PreparationCompletion::Queue(saved)));
        }
        if let Some(outcome) = Self::replay_frontend(tx, joined, proof).await? {
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
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        bound: &crate::BoundKeyedAdmitRequest,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let session = bound.binding.request.session_id.clone();

        bound.binding.validate_keyed(&bound.keyed)?;
        if super::super::super::records::lookup_receipt_with_proof(
            tx,
            &session,
            &bound.keyed.options.key,
            proof,
        )
        .await?
        .is_some()
        {
            proof
                .measure(&bound.keyed)
                .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
            let outcome = self
                .admit_keyed_guarded_with_proof(
                    tx,
                    bound.keyed.clone(),
                    crate::queue::AdmissionGuard::Ordinary,
                    proof,
                )
                .await?;
            if let KeyedAdmitOutcome::Replayed(receipt) = &outcome
                && super::super::super::execution_binding::read_with_proof(
                    tx,
                    &session,
                    &receipt.run_id,
                    proof,
                )
                .await
                .map_err(|error| error.into_domain_or(StoreError::Conflict))?
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
        tx: &mut crate::transaction::Transaction<'_, sqlx::Sqlite>,
        joined: &JoinedPreparation,
        proof: &mut crate::public_session::v2::proof::Proof,
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
                    proof,
                )
                .await?
                {
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
                    proof,
                )
                .await?
                {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(tx, joined.observed_grant(), proof).await?;
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
