//! Bounded original metadata reconciliation, never replacement dispatch authority.
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
};
use crate::{KeyedAdmitOutcome, StoreError};
impl super::super::MemoryStore {
    pub(super) fn replay_prepared(
        &self,
        state: &mut super::super::super::State,
        joined: &JoinedPreparation,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        if let PreparedOperation::AdmitKeyed(bound) = joined.observed_operation()
            && let Some(outcome) = self.replay_keyed(state, bound)?
        {
            super::verify_replay(state, joined.observed_grant())?;
            return Ok(Some(outcome));
        }
        if let PreparedOperation::Queue { request, .. } = joined.observed_operation()
            && let Some(saved) = super::super::super::queue::replay(state, request)?
        {
            super::verify_replay(state, joined.observed_grant())?;
            return Ok(Some(PreparationCompletion::Queue(saved)));
        }
        match joined.observed_operation() {
            PreparedOperation::ApprovalClaim(claim) => {
                if let Some(saved) = super::super::super::approval::replay(
                    state,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                )? {
                    super::verify_replay(state, joined.observed_grant())?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay(
                    state,
                    &claim.session_id,
                    &claim.run_id,
                    &claim.id,
                    &claim.fingerprint()?,
                )? {
                    super::verify_replay(state, joined.observed_grant())?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ApprovalRecovery(recovery) => {
                if let Some(saved) = super::super::super::approval::replay(
                    state,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                )? {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(state, joined.observed_grant())?;
                    return Ok(Some(PreparationCompletion::Approval(
                        crate::ApprovalClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::ToolRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay(
                    state,
                    &recovery.original.session_id,
                    &recovery.original.run_id,
                    &recovery.id,
                    &recovery.fingerprint()?,
                )? {
                    recovery.verify_receipt(&saved)?;
                    super::verify_replay(state, joined.observed_grant())?;
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

impl super::super::MemoryStore {
    fn replay_keyed(
        &self,
        state: &mut super::super::super::State,
        bound: &crate::BoundKeyedAdmitRequest,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let session = bound.binding.request.session_id.clone();
        let now = self.clock.now();

        bound.binding.validate_keyed(&bound.keyed)?;
        if state
            .receipts
            .contains_key(&(session.clone(), bound.keyed.options.key.clone()))
        {
            let outcome =
                super::super::super::admission::admit_keyed_transaction(state, &bound.keyed, now)?;
            if let KeyedAdmitOutcome::Replayed(receipt) = &outcome
                && super::super::super::execution_binding::read(state, &session, &receipt.run_id)?
                    .is_none_or(|saved| saved.capsule != bound.binding)
            {
                return Err(StoreError::AdmissionConflict.into());
            }

            return Ok(Some(PreparationCompletion::Keyed(outcome)));
        }

        Ok(None)
    }
}
