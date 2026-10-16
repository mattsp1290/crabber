use super::{Error, MemoryStore, State, cancel_waiting, receipt, records, revision, summaries};
use crate::{RunControlKind, RunControlStatus, RunControlSummary, WaitingCancellation};
fn current(state: &State, request: &WaitingCancellation) -> Result<RunControlSummary, Error> {
    let original = receipt(
        state,
        &request.target.session_id,
        &request.original_cancel_id,
    )?
    .ok_or(Error::NotFound)?;
    if original.target != request.target || original.kind != RunControlKind::Cancel {
        return Err(Error::SemanticConflict);
    }
    summaries(state, &request.target)?
        .into_iter()
        .find(|s| s.receipt.operation_id == request.original_cancel_id)
        .ok_or(Error::UnknownStoreOutcome)
}
impl MemoryStore {
    pub(in super::super) fn control_settle_waiting(
        &self,
        request: &WaitingCancellation,
    ) -> Result<RunControlSummary, Error> {
        crate::run_control::validate_target(&request.target)?;
        crate::run_control::valid_scope(&request.target.session_id, &request.original_cancel_id)?;
        let result = self.transact(|state| {
            let summary = current(state, request)?;
            let run = state
                .runs
                .get(&request.target.run_id)
                .filter(|run| run.session_id == request.target.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            crate::run_control::verify_target(&request.target, &run)?;
            crate::run_control::verify_terminal(&summary, run.status)?;
            if matches!(summary.status, RunControlStatus::TerminalCommitted { .. }) {
                return Ok((summary, false));
            }
            if revision(state, &run.session_id) != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            if run.status != crabber_core::RunStatus::Paused {
                return Err(Error::Busy);
            }
            let record = records(state, &request.target)?
                .into_iter()
                .find(|record| record.request.id == request.original_cancel_id)
                .ok_or(Error::UnknownStoreOutcome)?;
            cancel_waiting(state, &run, &record.request, self.clock.now())?;
            let summary = current(state, request)?;
            #[cfg(test)]
            crate::binding_fault::trip(&self.control_fault, 1)
                .map_err(crate::run_control::unknown)?;
            Ok((summary, true))
        });
        #[cfg(test)]
        if result.as_ref().is_ok_and(|(_, changed)| *changed) {
            crate::binding_fault::trip(&self.control_fault, 2)
                .map_err(crate::run_control::unknown)?;
        }
        result.map(|(summary, _)| summary)
    }
}
