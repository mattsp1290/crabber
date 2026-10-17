#[path = "attempt/accounting.rs"]
mod accounting;
#[path = "attempt/publication.rs"]
mod publication;
use crate::native_enrollment::caller::Caller;
use crate::public_session::v2::proof::Proof;
#[path = "attempt/control.rs"]
mod control;
pub(super) use control::read_turn as host_turn;
#[path = "attempt/pristine.rs"]
mod pristine;
use super::{MemoryExecution, MemoryStore, OperationReceipt, State};
use crate::{
    AttemptAuthority, AttemptCommand, AttemptError as Error, AttemptMutation,
    AttemptMutationOutcome, AttemptReceipt, VersionedAttempt, attempt,
};
use crabber_core::{AttemptId, ControlId, RunId, SessionId, SessionRevision, ToolCallStatus};

pub(super) fn verify(state: &State, receipt: &AttemptReceipt) -> Result<(), Error> {
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    attempt::verify_receipt(receipt, event)?;
    if crate::attempt::terminal::candidate(event) {
        let run = state
            .runs
            .get(&receipt.attempt.run_id)
            .ok_or(Error::UnknownStoreOutcome)?;
        let source = crate::attempt::terminal::settlement(receipt, event)?;
        let original = state
            .events
            .get(source.cursor.ok_or(Error::UnknownStoreOutcome)?)
            .ok_or(Error::UnknownStoreOutcome)?;
        let latest = state.events.iter().rev().find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == crabber_core::EventKind::RunSettled
                && event
                    .cursor
                    .is_some_and(|cursor| cursor < receipt.event_cursor)
        });
        if latest != Some(original) {
            return Err(Error::UnknownStoreOutcome);
        }
        crate::attempt::terminal::verify_scope(
            crate::attempt::terminal::TerminalFacts {
                session: &run.session_id,
                run: &run.id,
                status: run.status,
                error: run.error.as_deref(),
                usage: &run.usage,
            },
            receipt,
            event,
            original,
        )?;
    }
    Ok(())
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
                OperationReceipt::Fork(_)
                | OperationReceipt::RunControl(_)
                | OperationReceipt::FrontendStaging(_)
                | OperationReceipt::Queue(_)
                | OperationReceipt::Management(_)
                | OperationReceipt::PublicState(_)
                | OperationReceipt::Phase(_)
                | OperationReceipt::Approval(_)
                | OperationReceipt::ToolContinuation(_),
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
        let result = self.change_attempt_with_proof(
            request,
            authority,
            conclusion,
            &mut Proof::bounded(32 << 20),
        );
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
    pub(super) fn change_attempt_with_proof(
        &self,
        request: &AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
        proof: &mut Proof,
    ) -> Result<AttemptMutationOutcome, Error> {
        let mut guard = self.store.state.lock().unwrap();
        accounting::selected(
            &guard,
            &self.fence,
            request,
            authority,
            conclusion,
            false,
            proof,
        )?;
        let run = guard.runs.get(&self.fence.run_id).ok_or(Error::NotFound)?;
        let digest = if let Some(c) = conclusion {
            c.fingerprint(&run.session_id, &run.id)?
        } else {
            request.fingerprint(&run.session_id, &run.id)?
        };
        if let Some(replayed) = replay(&guard, run, request, &digest, conclusion)? {
            return Ok(replayed);
        }
        // Selected carriers were admitted against the original locked State.
        // This inherited whole-State transaction clone remains unbounded.
        let mut next = guard.clone();
        let result = self.apply_attempt_inner(
            &mut next,
            request.clone(),
            (authority, conclusion, false),
            Caller::Raw,
            proof,
        )?;
        *guard = next;
        Ok(result)
    }
    pub(super) fn apply_attempt(
        &self,
        state: &mut State,
        request: AttemptMutation,
        authority: Option<&AttemptAuthority>,
        conclusion: Option<&crate::AttemptConclusion>,
    ) -> Result<AttemptMutationOutcome, Error> {
        self.apply_attempt_inner(
            state,
            request,
            (authority, conclusion, false),
            Caller::Raw,
            &mut Proof::bounded(32 << 20),
        )
    }
    pub(super) fn apply_attempt_with_proof(
        &self,
        state: &mut State,
        request: AttemptMutation,
        authority: Option<&AttemptAuthority>,
        proof: &mut Proof,
    ) -> Result<AttemptMutationOutcome, Error> {
        self.apply_attempt_inner(state, request, (authority, None, false), Caller::Raw, proof)
    }
    pub(super) fn apply_claim_attempt_with_proof(
        &self,
        state: &mut State,
        request: AttemptMutation,
        proof: &mut Proof,
    ) -> Result<AttemptMutationOutcome, Error> {
        self.apply_attempt_inner(state, request, (None, None, true), Caller::Raw, proof)
    }
    fn apply_attempt_inner(
        &self,
        state: &mut State,
        request: AttemptMutation,
        context: (
            Option<&AttemptAuthority>,
            Option<&crate::AttemptConclusion>,
            bool,
        ),
        caller: Caller,
        read_proof: &mut Proof,
    ) -> Result<AttemptMutationOutcome, Error> {
        let (authority, conclusion, claiming) = context;
        accounting::selected(
            state,
            &self.fence,
            &request,
            authority,
            conclusion,
            claiming,
            read_proof,
        )?;
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
        if let Some(replayed) = replay(state, &run, &request, &digest, conclusion)? {
            return Ok(replayed);
        }
        let now = self.store.clock.now();
        attempt::verify_run(&run, &self.fence, now)?;
        super::admission_journal::verify(state, &run.id).map_err(|_| Error::UnknownStoreOutcome)?;
        if state
            .admission_executions
            .get(&run.id)
            .is_some_and(|r| r.state == crate::AdmissionExecutionState::Unstarted)
        {
            return Err(Error::StaleOwner);
        }
        super::approval::guard_attempt_with_proof(state, &run, &request, claiming, read_proof)?;
        super::tool_continuation::guard_attempt_with_proof(state, &run, &request, read_proof)?;
        let current = revision(state, &run.session_id);
        if current != request.expected_revision {
            return Err(Error::RevisionConflict);
        }
        if let Some(conclusion) = conclusion {
            check_boundary(state, &run, conclusion)?;
        }
        if matches!(request.command, AttemptCommand::ReserveBound { .. }) {
            pristine::check(state, &run, read_proof)?;
        }
        let (record, grant) =
            prepare_record(state, &run, &request, authority, now, conclusion.is_some())?;
        let settlement = apply_run_transition(state, &run, &request, conclusion, now, read_proof)?;
        if conclusion.is_some_and(|c| matches!(c.boundary, crate::RunBoundary::Terminal { .. })) {
            super::approval::release_terminal(state, &run, &request)?;
            super::tool_continuation::release_terminal(state, &run)?;
        }

        publication::publish(
            state,
            publication::Prepared {
                run: &run,
                request,
                record,
                grant,
                conclusion,
                settlement,
                digest,
                now,
            },
            caller,
            read_proof,
            #[cfg(test)]
            &self.store,
        )
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
    proof: &mut Proof,
) -> Result<Option<crabber_core::EventRecord>, Error> {
    if let Some(conclusion) = conclusion {
        return apply_boundary(state, run, conclusion, now);
    } else if matches!(
        request.command,
        AttemptCommand::Reserve { .. } | AttemptCommand::ReserveBound { .. }
    ) && run.status == crabber_core::RunStatus::Paused
    {
        let mut activated = run.clone();
        activated.status = crabber_core::RunStatus::Running;
        activated.updated_at = now;
        super::native_parent::record_run_with_proof(state, Some(run), &activated, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        state.runs.insert(run.id.clone(), activated);
    }
    Ok(None)
}

fn apply_boundary(
    state: &mut State,
    run: &crabber_core::Run,
    conclusion: &crate::AttemptConclusion,
    now: time::OffsetDateTime,
) -> Result<Option<crabber_core::EventRecord>, Error> {
    super::insert_event(state, run, conclusion.materialize(run)?)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let settlement = state
        .events
        .last()
        .cloned()
        .ok_or(Error::UnknownStoreOutcome)?;
    let mut updated = run.clone();
    conclusion.apply_run(&mut updated, now)?;
    super::native_parent::record_run(state, Some(run), &updated)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    super::run_control::finalize(state, &updated, now).map_err(|_| Error::UnknownStoreOutcome)?;
    state.runs.insert(run.id.clone(), updated);
    Ok(Some(settlement))
}

fn check_boundary(
    state: &State,
    run: &crabber_core::Run,
    conclusion: &crate::AttemptConclusion,
) -> Result<(), Error> {
    if let crate::RunBoundary::Terminal { status, .. } = &conclusion.boundary {
        if *status == crabber_core::RunStatus::Completed
            && (super::run_control::pending(state, run).map_err(|_| Error::UnknownStoreOutcome)?
                || state
                    .inbox
                    .iter()
                    .any(|row| row.session_id == run.session_id && row.consumed_by_run.is_none()))
        {
            return Err(Error::PendingInput);
        }
        if state.calls.values().any(|call| {
            call.run_id == run.id
                && matches!(
                    call.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
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

#[cfg(test)]
#[path = "attempt/terminal_tests.rs"]
mod terminal_tests;

pub(super) fn pristine_for_driver(
    state: &super::State,
    run: &crabber_core::Run,
    binding: &crate::RetainedExecutionBinding,
) -> Result<(), crate::AttemptError> {
    pristine::check_with_binding(state, run, binding)
}
