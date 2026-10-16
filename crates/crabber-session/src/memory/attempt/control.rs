use super::super::{MemoryStore, OperationReceipt, State, bump_public_revision};
use crate::{
    AttemptControlOutcome as Outcome, AttemptError as Error, AttemptReceipt,
    attempt_control::{self, Control},
};
use crabber_core::Run;
impl MemoryStore {
    pub(in super::super) fn host_attempt(&self, control: &Control) -> Result<Outcome, Error> {
        let digest = control.fingerprint()?;
        let result = self.transact(|state| {
            let (session, id, key, expected, target) = control.scope();
            let mut run = state
                .runs
                .get(id)
                .filter(|run| run.session_id == *session)
                .cloned()
                .ok_or(Error::NotFound)?;
            let key = (session.clone(), key.clone());
            if let Some(saved) = state.operation_receipts.get(&key) {
                let OperationReceipt::Attempt(receipt) = saved else {
                    return Err(Error::SemanticConflict);
                };
                super::verify(state, receipt)?;
                if receipt.attempt.run_id != *id || receipt.semantic_digest != digest {
                    return Err(Error::SemanticConflict);
                }
                return Ok(Outcome::Replayed(receipt.clone()));
            }
            let current = super::revision(state, session);
            if current != expected {
                return Err(Error::RevisionConflict);
            }
            let mut record = state
                .attempts
                .values()
                .filter(|r| r.snapshot.run_id == *id)
                .max_by_key(|r| r.snapshot.target.generation)
                .cloned()
                .ok_or(Error::NotFound)?;
            if record.snapshot.target != *target {
                return Err(Error::GenerationConflict);
            }
            super::super::approval::guard_attempt(state, &run, &control.mutation())?;
            super::super::tool_continuation::guard_attempt(state, &run, &control.mutation())?;
            let now = self.clock.now();
            let turn = reconcile_children(state, &run, control, now)?;
            apply_control(state, &mut run, &mut record, control, current, now)?;
            if matches!(control, Control::Retire(_)) {
                if let Some(turn) = &turn.close {
                    super::super::insert_event(
                        state,
                        &run,
                        attempt_control::close_turn(&run, turn.clone(), now),
                    )
                    .map_err(|_| Error::UnknownStoreOutcome)?;
                }
                super::super::insert_event(
                    state,
                    &run,
                    attempt_control::terminal_event(&run, now, turn.source),
                )
                .map_err(|_| Error::UnknownStoreOutcome)?;
                super::super::approval::release_terminal(state, &run, &control.mutation())?;
                attempt_control::finish_record(&record)?;
                super::super::run_control::finalize(state, &run, now)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            state.runs.insert(run.id.clone(), run.clone());
            state.attempts.insert(target.id.clone(), record.clone());
            let mut event = control.event(&run, &record.snapshot, now)?;
            let cursor = state
                .events
                .next_cursor()
                .map_err(|_| Error::UnknownStoreOutcome)?;
            event.cursor = Some(cursor);
            bump_public_revision(state, session).map_err(|_| Error::UnknownStoreOutcome)?;
            let receipt = AttemptReceipt {
                operation_id: control.scope().2.clone(),
                semantic_digest: digest,
                revision: super::revision(state, session),
                event_cursor: cursor,
                attempt: record.snapshot,
            };
            state
                .events
                .append(event)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            state
                .operation_receipts
                .insert(key, OperationReceipt::Attempt(receipt.clone()));
            #[cfg(test)]
            if self.attempt_fault.load(std::sync::atomic::Ordering::SeqCst) == 1 {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(Outcome::Applied(receipt))
        });
        #[cfg(test)]
        if self.attempt_fault.load(std::sync::atomic::Ordering::SeqCst) == 2
            && matches!(&result, Ok(Outcome::Applied(_)))
        {
            return Err(Error::UnknownStoreOutcome);
        }
        result
    }
}
fn apply_control(
    state: &mut State,
    run: &mut Run,
    record: &mut crate::attempt::AttemptRecord,
    control: &Control,
    current: crabber_core::SessionRevision,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    let previous =
        crate::native_parent::RunState::from_run(run).map_err(|_| Error::UnknownStoreOutcome)?;
    control.apply(run, record, current, now)?;
    super::super::native_parent::record_run_states(
        state,
        Some(&previous),
        crate::native_parent::RunState::from_run(run).map_err(|_| Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)
}

fn reconcile_children(
    state: &mut State,
    run: &Run,
    control: &Control,
    now: time::OffsetDateTime,
) -> Result<attempt_control::TurnContext, Error> {
    let Control::Retire(request) = control else {
        return Ok(attempt_control::TurnContext::default());
    };
    let context = super::super::approval::retirement_context(state, run)?;
    let turn = read_turn(state, run, context.source.clone())?;
    retire_calls(
        state,
        run,
        control,
        request,
        now,
        context.source.as_ref().or(turn.source.as_ref()),
        context.order,
    )?;
    Ok(turn)
}
fn retire_calls(
    state: &mut State,
    run: &Run,
    control: &Control,
    request: &crate::AttemptRetirement,
    now: time::OffsetDateTime,
    turn: Option<&crabber_core::TurnId>,
    order: Option<Vec<crabber_core::ToolCallId>>,
) -> Result<(), Error> {
    let mut pending = Vec::new();
    for call in state.calls.values().filter(|c| c.run_id == run.id) {
        let is_pending = attempt_control::validate_logical(call)?;
        let mut has_phase = false;
        for phase in state
            .phases
            .values()
            .filter(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == call.id)
        {
            let parent = state
                .attempts
                .get(&phase.snapshot.target.attempt.id)
                .ok_or(Error::UnknownStoreOutcome)?;
            parent.validate()?;
            if parent.snapshot.target != phase.snapshot.target.attempt
                || parent.snapshot.run_id != run.id
                || parent.snapshot.session_id != run.session_id
            {
                return Err(Error::UnknownStoreOutcome);
            }
            has_phase = true;
            attempt_control::validate_phase(run, call, phase)?;
        }
        if is_pending {
            if has_phase || pending.len() == 64 {
                return Err(Error::CleanupBlocked);
            }
            pending.push(call.clone());
        }
    }
    if state.phases.values().any(|p| {
        p.snapshot.run_id == run.id
            && state
                .calls
                .get(&p.snapshot.target.call_id)
                .is_none_or(|call| call.run_id != run.id)
    }) {
        return Err(Error::UnknownStoreOutcome);
    }
    attempt_control::check_pending(request, &pending)?;
    for call in attempt_control::order_pending(run, pending, order)? {
        let records = attempt_control::cancel_records(control, run, &call, now, turn.cloned());
        super::super::insert_message(state, run, records.message)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        super::super::insert_event(state, run, records.event)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let saved = state
            .calls
            .get_mut(&call.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        saved.status = records.call_status;
        saved.result = Some(records.result);
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
    }
    if !super::is_quiet(state, run) {
        return Err(Error::CleanupBlocked);
    }
    Ok(())
}

pub(in super::super) fn read_turn(
    state: &State,
    run: &Run,
    source: Option<crabber_core::TurnId>,
) -> Result<attempt_control::TurnContext, Error> {
    let mut turn = attempt_control::TurnState::new(run, source)?;
    for event in state.events.iter().filter(|event| {
        event.run_id == run.id
            && matches!(
                event.kind,
                crabber_core::EventKind::TurnStarted | crabber_core::EventKind::TurnCompleted
            )
    }) {
        turn.observe(run, event)?;
    }
    Ok(turn.finish())
}

#[cfg(test)]
mod tests {
    #[cfg(any(feature = "postgres", feature = "sqlite"))]
    #[tokio::test]
    async fn parent_retirement_requires_original_claimed_approval_provenance() {
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::MemoryStore::with_clock(clock.clone());
        for case in 0..6 {
            let (request, id) =
                crate::attempt_control_contract::integrity::claimed(&store, &clock).await;
            {
                let mut state = store.state.lock().unwrap();
                crate::attempt_control_contract::integrity::forge_approval(
                    state.approvals.get_mut(&id).unwrap(),
                    case,
                );
            }
            crate::attempt_control_contract::integrity::refused(&store, request).await;
        }
    }

    #[tokio::test]
    async fn attempt_control_contract() {
        crate::attempt_control_contract::run_contract(|clock| {
            super::super::super::MemoryStore::with_clock(clock)
        })
        .await;
    }
    #[tokio::test]
    async fn attempt_control_faults() {
        let store = super::super::super::MemoryStore::new();
        crate::attempt_control_contract::faults::run(&store, |point| {
            store
                .attempt_fault
                .store(point, std::sync::atomic::Ordering::SeqCst);
        })
        .await;
    }
}
