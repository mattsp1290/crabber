//! The compatibility Pending-to-Running gate is always a Raw native caller.
use super::{MemoryExecution, StoreError};
use crate::native_enrollment::caller::{Caller, Fresh, LegacyTool};
use crate::public_session::v2::proof::Proof;
use crabber_core::{EventRecord, ToolCallId, ToolCallStatus};
fn invalid(_: impl std::fmt::Debug) -> StoreError {
    StoreError::Validation("legacy tool admission proof unavailable".into())
}
impl MemoryExecution {
    pub(super) fn claim_legacy_with_proof(
        &self,
        id: &ToolCallId,
        running: &EventRecord,
        proof: &mut Proof,
    ) -> Result<(), StoreError> {
        proof
            .measure(&(&self.fence, id, running))
            .map_err(invalid)?;
        let result = self.store.transact(|state| {
            let selected = state
                .runs
                .get(&self.fence.run_id)
                .ok_or(StoreError::NotFound)?;
            proof.measure(selected).map_err(invalid)?;
            let run = selected.clone();
            if run.claim_token != self.fence.claim_token
                || run.status.is_terminal()
                || run.lease_until <= self.store.clock.now()
            {
                return Err(StoreError::Conflict);
            }
            super::admission_journal::verify_with_proof(state, &run.id, proof)?;
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(StoreError::AdmissionRecoveryRequired);
            }
            if super::frontend::origin_with_proof(state, &run, id, proof)
                .map_err(invalid)?
                .is_some()
                || state
                    .phases
                    .values()
                    .any(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == *id)
            {
                return Err(StoreError::Conflict);
            }
            let call = state
                .calls
                .get(id)
                .filter(|call| call.run_id == run.id)
                .ok_or(StoreError::NotFound)?;
            proof.measure(call).map_err(invalid)?;
            if call.status != ToolCallStatus::Pending {
                return Err(StoreError::Conflict);
            }
            // The original accepted event is charged before the publisher owns its copy.
            let cursor = super::insert_event_captured(state, &run, running.clone())?;
            state.calls.get_mut(id).ok_or(StoreError::NotFound)?.status = ToolCallStatus::Running;
            *state
                .snapshot_revisions
                .entry(run.session_id.clone())
                .or_default() += 1;
            super::native_enrollment::record_raw_native(
                state,
                Caller::Raw,
                &Fresh::LegacyTool(LegacyTool {
                    fence: &self.fence,
                    run: &run,
                    id,
                    requested: running,
                    call: state.calls.get(id).ok_or(StoreError::NotFound)?,
                    cursor,
                }),
                proof,
                #[cfg(test)]
                &self.store.native_raw_probe,
            )
            .map_err(invalid)?;
            super::native_parent::record_run_completed_captured_with_proof(
                state, &run, &run, None, proof,
            )
            .map_err(invalid)?;
            Ok(())
        });
        #[cfg(test)]
        if result.is_ok()
            && self
                .store
                .native_raw_probe
                .after_legacy(&self.fence.run_id, id)
        {
            return Err(invalid("lost original legacy claim ACK"));
        }
        result
    }
}
