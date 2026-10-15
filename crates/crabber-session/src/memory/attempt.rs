#[path = "attempt/control.rs"]
mod control;
#[path = "attempt/pristine.rs"]
mod pristine;
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
        id: Option<&AttemptId>,
    ) -> Result<Option<VersionedAttempt>, Error> {
        if let Some(id) = id {
            attempt::validate_identifier(&id.0)?;
        }
        let state = self.state.lock().unwrap();
        let run = state.runs.get(run_id).ok_or(Error::NotFound)?;
        let record = if let Some(id) = id {
            state
                .attempts
                .get(id)
                .filter(|record| record.snapshot.run_id == *run_id)
        } else {
            state
                .attempts
                .values()
                .filter(|record| record.snapshot.run_id == *run_id)
                .max_by_key(|record| record.snapshot.target.generation)
        };
        let Some(record) = record else {
            return Ok(None);
        };
        if run.id != *run_id || record.snapshot.session_id != run.session_id {
            return Err(Error::UnknownStoreOutcome);
        }
        record.validate()?;
        Ok(Some(VersionedAttempt {
            revision: revision(&state, &run.session_id),
            attempt: record.snapshot.clone(),
        }))
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
            Some(
                OperationReceipt::Management(_)
                | OperationReceipt::PublicState(_)
                | OperationReceipt::Phase(_)
                | OperationReceipt::Approval(_),
            ) => Err(Error::SemanticConflict),
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
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
    ) -> Result<AttemptMutationOutcome, Error> {
        self.change_attempt_inner(request, authority, None)
    }
    pub(super) fn conclude_owned_attempt(
        &self,
        conclusion: &crate::AttemptConclusion,
        authority: Option<&AttemptAuthority>,
    ) -> Result<AttemptMutationOutcome, Error> {
        conclusion.validate()?;
        self.change_attempt_inner(&conclusion.finish, authority, Some(conclusion))
    }
    fn change_attempt_inner(
        &self,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
    ) -> Result<AttemptMutationOutcome, Error> {
        attempt::validate_request(request)?;
        let result = self
            .store
            .transact(|state| self.apply_attempt(state, request.clone(), authority, conclusion));
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
    pub(super) fn apply_attempt(
        &self,
        state: &mut State,
        request: AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
    ) -> Result<AttemptMutationOutcome, Error> {
        attempt::validate_request(&request)?;

        let run = state
            .runs
            .get(&self.fence.run_id)
            .cloned()
            .ok_or(Error::NotFound)?;
        if run.id != self.fence.run_id {
            return Err(Error::UnknownStoreOutcome);
        }
        let digest = if let Some(conclusion) = conclusion {
            conclusion.fingerprint(&run.session_id, &run.id)?
        } else {
            request.fingerprint(&run.session_id, &run.id)?
        };
        let key = (run.session_id.clone(), request.id.clone());
        if let Some(replayed) = replay(state, &run, &request, &digest, conclusion)? {
            return Ok(replayed);
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
        super::approval::guard_attempt(state, &run, &request)?;
        let current = revision(state, &run.session_id);
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        if let Some(conclusion) = conclusion {
            check_boundary(state, &run, conclusion)?;
        }
        if matches!(request.command, AttemptCommand::ReserveBound { .. }) {
            pristine::check(state, &run)?;
        }
        let id = &request.command.target().id;
        let (record, grant) =
            prepare_record(state, &run, &request, authority, now, conclusion.is_some())?;
        apply_run_transition(state, &run, &request, conclusion, now)?;
        if conclusion.is_some_and(|c| matches!(c.boundary, crate::RunBoundary::Terminal { .. })) {
            super::approval::release_terminal(state, &run, &request)?;
        }

        let revision = revision(state, &run.session_id)
            .next()
            .map_err(|_| Error::RevisionExhausted)?;
        let authority = if grant {
            Some(record.authority()?)
        } else {
            None
        };
        let mut event = if let Some(conclusion) = conclusion {
            crate::attempt_conclusion::event(
                &run,
                conclusion.validate()?,
                &record.snapshot,
                &request.id,
                digest.clone(),
                now,
            )?
        } else {
            attempt::event(&run, &request, &record.snapshot, now)?
        };
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
    }
}

fn replay(
    state: &State,
    run: &crabber_core::Run,
    request: &AttemptMutation,
    digest: &crabber_core::InputFingerprint,
    conclusion: Option<&crate::AttemptConclusion>,
) -> Result<Option<AttemptMutationOutcome>, Error> {
    let key = (run.session_id.clone(), request.id.clone());
    if let Some(saved) = state.operation_receipts.get(&key) {
        let OperationReceipt::Attempt(receipt) = saved else {
            return Err(Error::SemanticConflict);
        };
        if receipt.attempt.session_id != run.session_id || receipt.operation_id != request.id {
            return Err(Error::UnknownStoreOutcome);
        }
        verify(state, receipt)?;
        if receipt.attempt.run_id != run.id || &receipt.semantic_digest != digest {
            return Err(Error::SemanticConflict);
        }
        if let Some(conclusion) = conclusion {
            conclusion.verify_replay(receipt)?;
        }
        return Ok(Some(AttemptMutationOutcome::Replayed(receipt.clone())));
    }
    Ok(None)
}

fn apply_run_transition(
    state: &mut State,
    run: &crabber_core::Run,
    request: &AttemptMutation,
    conclusion: Option<&crate::AttemptConclusion>,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    if let Some(conclusion) = conclusion {
        apply_boundary(state, run, conclusion, now)?;
    } else if matches!(
        request.command,
        AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
    ) && run.status == crabber_core::RunStatus::Paused
    {
        let mut activated = run.clone();
        activated.status = crabber_core::RunStatus::Running;
        activated.updated_at = now;
        state.runs.insert(run.id.clone(), activated);
    }
    Ok(())
}

fn apply_boundary(
    state: &mut State,
    run: &crabber_core::Run,
    conclusion: &crate::AttemptConclusion,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    super::insert_event(state, run, conclusion.materialize(run)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut updated = run.clone();
    conclusion.apply_run(&mut updated, now)?;
    state.runs.insert(run.id.clone(), updated);
    Ok(())
}

fn check_boundary(
    state: &State,
    run: &crabber_core::Run,
    conclusion: &crate::AttemptConclusion,
) -> Result<(), Error> {
    if let crate::RunBoundary::Terminal { status, .. } = &conclusion.boundary {
        if *status == crabber_core::RunStatus::Completed
            && state
                .inbox
                .iter()
                .any(|row| row.session_id == run.session_id && row.consumed_by_run.is_none())
        {
            return Err(Error::PendingInput);
        }
        if state.calls.values().any(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running
                )
        }) {
            return Err(Error::CleanupBlocked);
        }
    }
    Ok(())
}

fn prepare_record(
    state: &State,
    run: &crabber_core::Run,
    request: &AttemptMutation,
    authority: Option<&AttemptAuthority>,
    now: time::OffsetDateTime,
    conclusion: bool,
) -> Result<(attempt::AttemptRecord, bool), Error> {
    let id = &request.command.target().id;
    Ok(
        if matches!(
            request.command,
            AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
        ) {
            if state.attempts.contains_key(id) {
                return Err(Error::SemanticConflict);
            }
            let latest = state
                .attempts
                .values()
                .filter(|r| r.snapshot.run_id == run.id)
                .max_by_key(|r| r.snapshot.target.generation)
                .map(|r| &r.snapshot);
            let record = attempt::AttemptRecord::reserve(run, request, latest)?;
            if latest.is_some() && !is_quiet(state, run) {
                return Err(Error::CleanupBlocked);
            }
            (record, true)
        } else {
            let mut record = state
                .attempts
                .get(id)
                .filter(|r| r.snapshot.run_id == run.id)
                .cloned()
                .ok_or(Error::NotFound)?;
            // Legacy Running calls are owned work too. The phase store adds
            // its independently tracked quiescence predicate when integrated.
            let quiet = is_quiet(state, run);
            let grant = record.change(run, request, authority, now, quiet)?;
            if conclusion {
                record.cleanup(crate::AttemptCleanupResult::Complete, quiet)?;
            }
            (record, grant)
        },
    )
}

fn is_quiet(state: &State, run: &crabber_core::Run) -> bool {
    !state.phases.values().any(|p| {
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
        .any(|c| c.run_id == run.id && c.status == ToolCallStatus::Running)
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
