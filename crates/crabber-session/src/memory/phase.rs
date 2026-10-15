use super::{MemoryExecution, MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{
    PhaseCommand, PhaseError as Error, PhaseMutation, PhaseMutationOutcome, PhaseReceipt,
    PhaseWriteAuthority, VersionedPhase, phase,
};
use crabber_core::{ControlId, EventCursor, PhaseId, Run, RunId, SessionId, SessionRevision};

fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
fn verify(state: &State, receipt: &PhaseReceipt) -> Result<(), Error> {
    let index = usize::try_from(receipt.event_cursor.0)
        .ok()
        .and_then(|n| n.checked_sub(1))
        .ok_or(Error::UnknownStoreOutcome)?;
    phase::verify_receipt(
        receipt,
        state.events.get(index).ok_or(Error::UnknownStoreOutcome)?,
    )
}
impl MemoryStore {
    pub(super) fn read_phase(&self, run_id: &RunId, id: &PhaseId) -> Result<VersionedPhase, Error> {
        let state = self.state.lock().unwrap();
        let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
        let record = state
            .phases
            .get(id)
            .filter(|r| r.snapshot.run_id == *run_id)
            .ok_or(Error::NotFound)?;
        if run.id != *run_id || record.snapshot.session_id != run.session_id {
            return Err(Error::UnknownStoreOutcome);
        }
        record.validate()?;
        Ok(VersionedPhase {
            revision: revision(&state, &run.session_id),
            phase: record.snapshot.clone(),
        })
    }
    pub(super) fn phase_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<PhaseReceipt>, Error> {
        crate::attempt::validate_identifier(&id.0).map_err(|_| Error::InvalidRequest)?;
        let state = self.state.lock().unwrap();
        match state.operation_receipts.get(&(session.clone(), id.clone())) {
            Some(OperationReceipt::Phase(receipt)) => {
                if receipt.phase.session_id != *session || receipt.operation_id != *id {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(&state, receipt)?;
                Ok(Some(receipt.clone()))
            }
            Some(_) => Err(Error::SemanticConflict),
            None => Ok(None),
        }
    }
}
impl MemoryExecution {
    pub(super) fn change_phase(
        &self,
        request: PhaseMutation,
        authority: Option<&PhaseWriteAuthority>,
    ) -> Result<PhaseMutationOutcome, Error> {
        phase::validate_request(&request)?;
        let result = self.store.transact(|state| {
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            let digest = request.fingerprint(&run.session_id, &run.id)?;
            let key = (run.session_id.clone(), request.id.clone());
            if let Some(saved) = state.operation_receipts.get(&key) {
                let OperationReceipt::Phase(receipt) = saved else {
                    return Err(Error::SemanticConflict);
                };
                if receipt.phase.session_id != run.session_id || receipt.operation_id != request.id
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(state, receipt)?;
                if receipt.phase.run_id != run.id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(PhaseMutationOutcome::Replayed(receipt.clone()));
            }
            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(|_| Error::StaleOwner)?;
            if state
                .admission_executions
                .get(&run.id)
                .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
            {
                return Err(Error::StaleOwner);
            }
            let current = revision(state, &run.session_id);
            if current != request.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let revision = current.next().map_err(|_| Error::RevisionExhausted)?;
            let (record, authority) = prepare_record(state, &run, &request, authority, now)?;
            let mut event = phase::event(&run, &request, &record.snapshot, now)?;
            let cursor = EventCursor(state.events.len() as u64 + 1);
            event.cursor = Some(cursor);
            let receipt = PhaseReceipt {
                operation: phase::operation(&request.command),
                resolver: phase::resolver(&request.command),
                operation_id: request.id,
                semantic_digest: digest,
                revision,
                event_cursor: cursor,
                phase: record.snapshot.clone(),
            };
            if matches!(request.command, PhaseCommand::Begin { .. }) {
                state
                    .calls
                    .get_mut(&record.snapshot.target.call_id)
                    .ok_or(Error::NotFound)?
                    .status = crabber_core::ToolCallStatus::Running;
                let snapshot_revision = state
                    .snapshot_revisions
                    .entry(run.session_id.clone())
                    .or_default();
                *snapshot_revision = snapshot_revision
                    .checked_add(1)
                    .ok_or(Error::RevisionExhausted)?;
            }
            state
                .phases
                .insert(record.snapshot.target.id.clone(), record);
            bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
            state.events.push(event);
            state
                .operation_receipts
                .insert(key, OperationReceipt::Phase(receipt.clone()));
            #[cfg(test)]
            if self
                .store
                .phase_fault
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(PhaseMutationOutcome::Applied { receipt, authority })
        });
        #[cfg(test)]
        if self
            .store
            .phase_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && matches!(&result, Ok(PhaseMutationOutcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}
fn prepare_record(
    state: &State,
    run: &Run,
    request: &PhaseMutation,
    authority: Option<&PhaseWriteAuthority>,
    now: time::OffsetDateTime,
) -> Result<(phase::PhaseRecord, Option<crate::PhaseAuthority>), Error> {
    let target = request.command.target();
    let attempt = state
        .attempts
        .get(&target.attempt.id)
        .filter(|a| a.snapshot.run_id == run.id)
        .ok_or(Error::NotFound)?;
    let call = state
        .calls
        .get(&target.call_id)
        .filter(|c| c.run_id == run.id)
        .ok_or(Error::NotFound)?;
    if matches!(request.command, PhaseCommand::Claim { .. }) {
        if state.phases.contains_key(&target.id) {
            return Err(Error::SemanticConflict);
        }
        let Some(PhaseWriteAuthority::Attempt(authority)) = authority else {
            return Err(Error::StaleOwner);
        };
        let previous = state
            .phases
            .values()
            .find(|r| r.snapshot.run_id == run.id && r.snapshot.target.call_id == call.id)
            .map(|r| &r.snapshot);
        let record =
            phase::PhaseRecord::claim(run, attempt, call, request, authority, now, previous)?;
        let grant = record.authority(authority.clone())?;
        Ok((record, Some(grant)))
    } else {
        let mut record = state
            .phases
            .get(&target.id)
            .filter(|r| r.snapshot.run_id == run.id)
            .cloned()
            .ok_or(Error::NotFound)?;
        let worker = match authority {
            Some(PhaseWriteAuthority::Phase(authority)) => Some(authority),
            _ => None,
        };
        record.change(run, attempt, call, request, worker, now)?;
        Ok((record, None))
    }
}

#[cfg(test)]
#[path = "phase_tests.rs"]
mod tests;
