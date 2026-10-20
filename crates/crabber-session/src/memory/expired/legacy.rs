//! Compatibility fence replacement under its original transaction and guards.
use super::super::{MemoryStore, State};
use crate::native_enrollment::caller::{Caller, Fresh, initial::Capture, recovery::LegacyExpired};
use crate::public_session::v2::proof::Proof;
use crate::{AdmissionExecutionState, StoreError};
use crabber_core::{RunFence, RunId};

fn unknown(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("native expired claim evidence unavailable".into())
}
impl MemoryStore {
    pub(in crate::memory) fn claim_legacy_expired_with_proof(
        &self,
        id: &RunId,
        owner: &str,
        proof: &mut Proof,
    ) -> Result<RunFence, StoreError> {
        proof.measure(&(id, owner)).map_err(unknown)?;
        let result = self.transact_selected(
            proof,
            |state, proof| {
                selected(state, id, proof)?;
                Ok(None)
            },
            |state, proof| {
                selected(state, id, proof)?;
                let previous = state.runs.get(id).ok_or(StoreError::NotFound)?;
                let now = self.clock.now();
                if previous.status.is_terminal() || previous.lease_until > now {
                    return Err(StoreError::Conflict);
                }
                proof.measure(previous).map_err(unknown)?;
                let previous = previous.clone();
                proof.measure(&previous).map_err(unknown)?;
                let mut run = previous.clone();
                owner.clone_into(&mut run.owner);
                run.claim_token = uuid::Uuid::new_v4().to_string();
                run.lease_until = now + time::Duration::seconds(30);
                run.updated_at = now;
                crate::ensure_storable(&run)?;
                let fence = RunFence {
                    run_id: id.clone(),
                    claim_token: run.claim_token.clone(),
                };
                let published =
                    super::super::native_parent::record_run_completed_publication_with_proof(
                        state,
                        &previous,
                        &run,
                        None,
                        proof,
                        Capture::Original,
                    )
                    .map_err(unknown)?;
                let publication = published
                    .updated
                    .as_ref()
                    .ok_or_else(|| unknown("missing actual issuer"))?;
                super::super::native_enrollment::record_raw_native(
                    state,
                    Caller::Raw,
                    &Fresh::LegacyExpired(LegacyExpired {
                        id,
                        owner,
                        previous: &previous,
                        updated: &run,
                        fence: &fence,
                        publication,
                    }),
                    proof,
                    #[cfg(test)]
                    &self.native_raw_probe,
                )
                .map_err(unknown)?;
                state.runs.insert(id.clone(), run);
                Ok(fence)
            },
        );
        #[cfg(test)]
        if result.is_ok() && self.native_raw_probe.after_legacy_expired(id) {
            return Err(unknown("lost original compatibility claim ACK"));
        }
        result
    }
}
fn selected(state: &State, id: &RunId, proof: &mut Proof) -> Result<(), StoreError> {
    let run = state.runs.get(id).ok_or(StoreError::NotFound)?;
    proof.measure(run).map_err(unknown)?;
    if state.session_owners.has_preparation(&run.session_id) {
        return Err(StoreError::AdmissionRecoveryRequired);
    }
    super::super::admission_journal::verify_with_proof(state, id, proof)?;
    if state
        .admission_executions
        .get(id)
        .is_some_and(|record| record.state == AdmissionExecutionState::Unstarted)
    {
        return Err(StoreError::AdmissionRecoveryRequired);
    }
    if super::super::tool_continuation::pending_in_session_with_proof(state, &run.session_id, proof)
        .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?
    {
        return Err(StoreError::Busy);
    }
    Ok(())
}
