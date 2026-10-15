use super::{MemoryExecution, MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome, AttemptReceipt, VersionedAttempt, attempt,
};
use crabber_core::{
    AttemptId, ControlId, EventCursor, RunId, SessionId, SessionRevision, ToolCallStatus,
};

fn verify(state: &State, receipt: &AttemptReceipt) -> Result<(), Error> {
    let index = usize::try_from(receipt.event_cursor.0)
        .ok()
        .and_then(|n| n.checked_sub(1))
        .ok_or(Error::UnknownStoreOutcome)?;
    attempt::verify_receipt(
        receipt,
        state.events.get(index).ok_or(Error::UnknownStoreOutcome)?,
    )
}
impl MemoryStore {
    pub(super) fn read_attempt(
        &self,
        run_id: &RunId,
        id: &AttemptId,
    ) -> Result<VersionedAttempt, Error> {
        let state = self.state.lock().unwrap();
        let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
        let record = state
            .attempts
            .get(id)
            .filter(|r| r.snapshot.run_id == *run_id)
            .ok_or(Error::NotFound)?;
        if run.id != *run_id || record.snapshot.session_id != run.session_id {
            return Err(Error::UnknownStoreOutcome);
        }
        record.validate()?;
        Ok(VersionedAttempt {
            revision: revision(&state, &run.session_id),
            attempt: record.snapshot.clone(),
        })
    }
    pub(super) fn attempt_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<AttemptReceipt>, Error> {
        crate::public_state::validate_id(id).map_err(|_| Error::InvalidRequest)?;
        let state = self.state.lock().unwrap();
        match state.operation_receipts.get(&(session.clone(), id.clone())) {
            Some(OperationReceipt::Attempt(receipt)) => {
                if receipt.attempt.session_id != *session || receipt.operation_id != *id {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(&state, receipt)?;
                Ok(Some(receipt.clone()))
            }
            Some(OperationReceipt::PublicState(_) | OperationReceipt::Phase(_)) => {
                Err(Error::SemanticConflict)
            }
            None => Ok(None),
        }
    }
}
fn revision(state: &State, session: &SessionId) -> SessionRevision {
    state
        .public_revisions
        .get(session)
        .copied()
        .unwrap_or(SessionRevision::INITIAL)
}
impl MemoryExecution {
    pub(super) fn change_attempt(
        &self,
        request: AttemptMutation,
        authority: Option<&AttemptAuthority>,
    ) -> Result<AttemptMutationOutcome, Error> {
        attempt::validate_request(&request)?;
        let result = self.store.transact(|state| {
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if run.id != self.fence.run_id {
                return Err(Error::UnknownStoreOutcome);
            }
            let digest = request.fingerprint(&run.session_id, &run.id)?;
            let key = (run.session_id.clone(), request.id.clone());
            if let Some(saved) = state.operation_receipts.get(&key) {
                let OperationReceipt::Attempt(receipt) = saved else {
                    return Err(Error::SemanticConflict);
                };
                if receipt.attempt.session_id != run.session_id
                    || receipt.operation_id != request.id
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                verify(state, receipt)?;
                if receipt.attempt.run_id != run.id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(AttemptMutationOutcome::Replayed(receipt.clone()));
            }
            let now = self.store.clock.now();
            attempt::verify_run(&run, &self.fence, now)?;
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
            let id = &request.command.target().id;
            let (record, grant) = prepare_record(state, &run, &request, authority, now)?;
            let authority = if grant {
                Some(record.authority()?)
            } else {
                None
            };
            let mut event = attempt::event(&run, &request, &record.snapshot, now)?;
            let cursor = EventCursor(state.events.len() as u64 + 1);
            event.cursor = Some(cursor);
            let receipt = AttemptReceipt {
                operation_id: request.id,
                semantic_digest: digest,
                revision,
                event_cursor: cursor,
                attempt: record.snapshot.clone(),
            };
            state.attempts.insert(id.clone(), record);
            bump_public_revision(state, &run.session_id).map_err(|_| Error::UnknownStoreOutcome)?;
            state.events.push(event);
            state
                .operation_receipts
                .insert(key, OperationReceipt::Attempt(receipt.clone()));
            #[cfg(test)]
            if self
                .store
                .attempt_fault
                .load(std::sync::atomic::Ordering::SeqCst)
                == 1
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(AttemptMutationOutcome::Applied { receipt, authority })
        });
        #[cfg(test)]
        if self
            .store
            .attempt_fault
            .load(std::sync::atomic::Ordering::SeqCst)
            == 2
            && matches!(&result, Ok(AttemptMutationOutcome::Applied { .. }))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}

fn prepare_record(
    state: &State,
    run: &crabber_core::Run,
    request: &AttemptMutation,
    authority: Option<&AttemptAuthority>,
    now: time::OffsetDateTime,
) -> Result<(attempt::AttemptRecord, bool), Error> {
    let id = &request.command.target().id;
    Ok(
        if matches!(request.command, AttemptCommand::Reserve { .. }) {
            if state.attempts.contains_key(id) {
                return Err(Error::SemanticConflict);
            }
            let latest = state
                .attempts
                .values()
                .filter(|r| r.snapshot.run_id == run.id)
                .max_by_key(|r| r.snapshot.target.generation)
                .map(|r| &r.snapshot);
            (attempt::AttemptRecord::reserve(run, request, latest)?, true)
        } else {
            let mut record = state
                .attempts
                .get(id)
                .filter(|r| r.snapshot.run_id == run.id)
                .cloned()
                .ok_or(Error::NotFound)?;
            // Legacy Running calls are owned work too. The phase store adds
            // its independently tracked quiescence predicate when integrated.
            let quiet = !state.phases.values().any(|p| {
                p.snapshot.run_id == run.id
                    && (p.snapshot.quiescence != crate::PhaseQuiescence::Complete
                        || matches!(
                            p.snapshot.state,
                            crate::PhaseState::Claimed
                                | crate::PhaseState::Started
                                | crate::PhaseState::Uncertain
                        ))
            }) && !state
                .calls
                .values()
                .any(|c| c.run_id == run.id && c.status == ToolCallStatus::Running);
            let grant = record.change(run, request, authority, now, quiet)?;
            (record, grant)
        },
    )
}

#[cfg(test)]
mod tests {

    #[tokio::test]
    async fn attempt_fault_contract() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::MemoryStore::with_clock(clock.clone());
        crate::attempt_contract::run_fault_contract(
            &store,
            &clock,
            |point: crate::attempt_contract::FaultPoint| {
                store
                    .attempt_fault
                    .store(point as u8, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await;
    }
}
