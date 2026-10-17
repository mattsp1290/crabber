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
impl super::super::MemoryStore {
    pub(super) fn replay_prepared(
        state: &super::super::super::State,
        joined: &JoinedPreparation,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        if let Some(outcome) = Self::replay_expired(state, joined)? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_recovery(state, joined)? {
            return Ok(Some(outcome));
        }
        if let Some(outcome) = Self::replay_admission(state, joined)? {
            return Ok(Some(outcome));
        }
        if let PreparedOperation::AdmitKeyed(bound) = joined.observed_operation()
            && let Some(outcome) = Self::replay_keyed(state, bound)?
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
        if let Some(outcome) = Self::replay_frontend(state, joined)? {
            return Ok(Some(outcome));
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
        state: &super::super::super::State,
        bound: &crate::BoundKeyedAdmitRequest,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        let session = bound.binding.request.session_id.clone();

        bound.binding.validate_keyed(&bound.keyed)?;
        if state
            .receipts
            .contains_key(&(session.clone(), bound.keyed.options.key.clone()))
        {
            super::super::super::deletion::ensure_live(state, &session)?;
            if let Some(existing) = state.sessions.get(&session) {
                existing.ensure_identity(
                    &bound.keyed.request.workspace_id,
                    &bound.keyed.request.directory,
                )?;
            }
            let receipt = state
                .receipts
                .get(&(session.clone(), bound.keyed.options.key.clone()))
                .ok_or(StoreError::AdmissionConflict)?;
            if receipt.fingerprint != bound.keyed.options.fingerprint
                || receipt.semantic_digest != bound.keyed.validated_digest()?
            {
                return Err(StoreError::AdmissionConflict.into());
            }
            if super::super::super::execution_binding::read(state, &session, &receipt.run_id)?
                .is_none_or(|saved| saved.capsule != bound.binding)
            {
                return Err(StoreError::AdmissionConflict.into());
            }
            let outcome = KeyedAdmitOutcome::Replayed(receipt.clone());
            return Ok(Some(PreparationCompletion::Keyed(outcome)));
        }

        Ok(None)
    }
}

impl super::super::MemoryStore {
    fn replay_frontend(
        state: &super::super::super::State,
        joined: &JoinedPreparation,
    ) -> Result<
        Option<PreparationCompletion>,
        crate::session_owner::preparation::PreparationCommitError,
    > {
        match joined.observed_operation() {
            PreparedOperation::FrontendClaim(claim) => {
                if let Some(saved) = super::super::super::tool_continuation::replay(
                    state,
                    &claim.claim.session_id,
                    &claim.claim.run_id,
                    &claim.claim.id,
                    &claim.fingerprint()?,
                )? {
                    super::verify_replay(state, joined.observed_grant())?;
                    return Ok(Some(PreparationCompletion::ToolContinuation(
                        crate::ToolContinuationClaimOutcome::Replayed(saved),
                    )));
                }
            }
            PreparedOperation::FrontendRecovery(recovery) => {
                if let Some(saved) = super::super::super::tool_continuation::replay(
                    state,
                    &recovery.original.claim.session_id,
                    &recovery.original.claim.run_id,
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
