use super::{
    AttemptMutationOutcome, AttemptState, Error, MemoryExecution, Run, State,
    ToolContinuationOutcome, ToolContinuationReceipt, ToolContinuationRecord,
    ToolContinuationState, ToolContinuationSuspension, ToolSuspensionAuthority, after, before,
    fault, replay, revision,
};
use crabber_core::{ContinuationGeneration, RunStatus, ToolCallStatus};
impl MemoryExecution {
    pub(in super::super) fn suspend_tools(
        &self,
        batch: &ToolContinuationSuspension,
        authority: &ToolSuspensionAuthority,
    ) -> Result<ToolContinuationReceipt, Error> {
        let fault = fault(&self.store);
        let result = self.store.transact(|state| {
            let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
            let run = state
                .runs
                .get(&self.fence.run_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            super::super::credential_access::check(state, &run, &mut proof)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let digest = batch.fingerprint(&run.session_id, &run.id)?;
            if let Some(saved) = replay(state, &run.session_id, &run.id, &batch.id, &digest)? {
                return Ok((saved, false));
            }
            if revision(state, &run.session_id) != batch.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let now = self.store.clock.now();
            crate::attempt::verify_run(&run, &self.fence, now).map_err(|_| Error::StaleOwner)?;
            validate(state, &run, batch)?;
            super::super::approval::retirement_context(state, &run)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            let parent = state
                .attempts
                .get(&batch.attempt.id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if authority.phases.len() != batch.phases.len() {
                return Err(Error::InvalidRequest);
            }
            let mut record = new_record(state, &run, batch, now)?;
            record.handoff = super::super::continuation_handoff::prepare(
                state,
                &run,
                crate::continuation_handoff::Domain::Tool,
                &batch.continuation,
                &batch.id,
                &digest,
                &batch.attempt,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?
            .map(Box::new);
            suspend_phases(state, &run, &parent, batch, authority, now)?;
            super::phases::frontend_waiting(state, &run, batch)?;
            let conclusion = batch.conclusion(&run)?;
            if !matches!(
                self.apply_attempt(
                    state,
                    conclusion.finish.clone(),
                    Some(&authority.attempt),
                    Some(&conclusion)
                )
                .map_err(|_| Error::CleanupBlocked)?,
                AttemptMutationOutcome::Applied { .. }
            ) {
                return Err(Error::SemanticConflict);
            }
            if let Some(old) = state.tool_barriers.get(&run.session_id).cloned() {
                state
                    .tool_continuations
                    .get_mut(&old)
                    .ok_or(Error::UnknownStoreOutcome)?
                    .state = ToolContinuationState::Released;
            }
            let handoff = record.handoff.clone();
            if let Some(proof) = &handoff {
                super::super::continuation_handoff::release(state, &run, proof)
                    .map_err(|_| Error::UnknownStoreOutcome)?;
            }
            state
                .tool_barriers
                .insert(run.session_id.clone(), batch.continuation.id.clone());
            state
                .tool_continuations
                .insert(batch.continuation.id.clone(), record);
            let receipt = super::publish_handoff(
                state,
                &run,
                batch.id.clone(),
                digest,
                ToolContinuationOutcome::Suspended {
                    continuation: batch.continuation.clone(),
                },
                now,
                handoff.as_deref(),
            )?;
            publish_waiting(state, batch, &receipt, &mut proof)?;
            before(fault)?;
            Ok((receipt, true))
        });
        after(result, fault)
    }
}
fn validate(state: &State, run: &Run, batch: &ToolContinuationSuspension) -> Result<(), Error> {
    if run.status != RunStatus::Running
        || batch.expected_plan.as_str() != run.plan_fingerprint
        || state
            .tool_continuations
            .contains_key(&batch.continuation.id)
    {
        return Err(Error::InvalidTransition);
    }
    if let Some(old) = super::active(state, run)? {
        if old.run_id != run.id
            || old.state != ToolContinuationState::Claimed
            || old.batch.next_attempt != batch.attempt
            || old.batch.continuation.generation.next().ok() != Some(batch.continuation.generation)
        {
            return Err(Error::GenerationConflict);
        }
    } else {
        let previous = super::scan::run(state, run)?.0;
        let generation = previous
            .map(|r| {
                r.batch
                    .continuation
                    .generation
                    .next()
                    .map_err(|_| Error::GenerationConflict)
            })
            .transpose()?
            .unwrap_or(ContinuationGeneration::INITIAL);
        if batch.continuation.generation != generation {
            return Err(Error::GenerationConflict);
        }
    }
    let latest = state
        .attempts
        .values()
        .filter(|a| a.snapshot.run_id == run.id)
        .max_by_key(|a| a.snapshot.target.generation)
        .ok_or(Error::NotFound)?;
    if latest.snapshot.target != batch.attempt || latest.snapshot.state != AttemptState::Started {
        return Err(Error::GenerationConflict);
    }
    let unfinished = state
        .calls
        .values()
        .filter(|c| {
            c.run_id == run.id
                && matches!(
                    c.status,
                    ToolCallStatus::Pending | ToolCallStatus::Running | ToolCallStatus::Waiting
                )
        })
        .collect::<Vec<_>>();
    if unfinished.len() != batch.calls.len() {
        return Err(Error::InvalidTransition);
    }
    for frozen in &batch.calls {
        let call = unfinished
            .iter()
            .find(|c| c.id == frozen.target.call_id)
            .ok_or(Error::InvalidTransition)?;
        frozen
            .verify_call(call)
            .map_err(|_| Error::SemanticConflict)?;
        if call.result.is_some() {
            return Err(Error::InvalidTransition);
        }
        let phase = batch.phases.iter().find(|p| p.previous.call_id == call.id);
        let latest = state
            .phases
            .values()
            .filter(|p| p.snapshot.run_id == run.id && p.snapshot.target.call_id == call.id)
            .max_by_key(|p| p.snapshot.target.generation);
        let frontend = batch.frontend.iter().find(|f| f.target.call_id == call.id);
        let origin = super::super::frontend::origin(state, run, &call.id)?;
        if let Some(frontend) = frontend {
            super::super::frontend::verify_frozen(state, run, frontend)?;
            if latest.is_some()
                || phase.is_some()
                || state.phases.contains_key(&frontend.target.id)
                || call.status != ToolCallStatus::Pending
            {
                return Err(Error::CleanupBlocked);
            }
            continue;
        }
        if origin.is_some() {
            return Err(Error::SemanticConflict);
        }
        match (phase, latest) {
            (Some(frozen), Some(native))
                if native.snapshot.target == frozen.previous
                    && call.status == ToolCallStatus::Running => {}
            (None, None)
                if call.status == ToolCallStatus::Pending
                    && frozen.target.generation == crabber_core::PhaseGeneration::INITIAL => {}
            _ => return Err(Error::CleanupBlocked),
        }
    }
    super::evidence::validate(state, run, Some(batch))?;
    unique_questions(state, batch)
}
fn unique_questions(state: &State, batch: &ToolContinuationSuspension) -> Result<(), Error> {
    for frozen in &batch.phases {
        for question in &frozen.interactions {
            if state
                .tool_continuations
                .values()
                .any(|record| record.questions.iter().any(|q| q.view.id == question.id))
                || state
                    .approvals
                    .values()
                    .any(|record| record.questions.iter().any(|q| q.view.id == question.id))
            {
                return Err(Error::SemanticConflict);
            }
        }
    }
    Ok(())
}

fn new_record(
    state: &State,
    run: &Run,
    batch: &ToolContinuationSuspension,
    now: time::OffsetDateTime,
) -> Result<ToolContinuationRecord, Error> {
    let binding = super::super::execution_binding::read(state, &run.session_id, &run.id)
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    let workspace = binding.capsule.request.workspace_id;
    ToolContinuationRecord::new(
        run.session_id.clone(),
        run.id.clone(),
        workspace,
        batch.clone(),
        now,
    )
}

fn suspend_phases(
    state: &mut State,
    run: &Run,
    parent: &crate::attempt::AttemptRecord,
    batch: &ToolContinuationSuspension,
    authority: &ToolSuspensionAuthority,
    now: time::OffsetDateTime,
) -> Result<(), Error> {
    for (index, phase) in batch.phases.iter().enumerate() {
        let owner = &authority.phases[index];
        let call = state
            .calls
            .get(&phase.previous.call_id)
            .cloned()
            .ok_or(Error::NotFound)?;
        let native = state
            .phases
            .get_mut(&phase.previous.id)
            .ok_or(Error::NotFound)?;
        native
            .suspend_checkpoint(run, parent, &call, phase, owner, now)
            .map_err(|_| Error::StaleOwner)?;
        state.calls.get_mut(&call.id).ok_or(Error::NotFound)?.status = ToolCallStatus::Waiting;
        *state
            .snapshot_revisions
            .entry(run.session_id.clone())
            .or_default() += 1;
    }

    Ok(())
}

// Publish acceptance and its safe presentation under the same original State
// lock and Proof, before the existing fault/commit boundary.
fn publish_waiting(
    state: &mut super::super::State,
    batch: &ToolContinuationSuspension,
    receipt: &ToolContinuationReceipt,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    super::super::continuation_input::accept(
        state,
        crate::continuation_input::Input::ToolSuspension(batch),
        crate::continuation_input::Receipt::Tool(receipt),
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut waiting = batch.waiting_event(receipt)?;
    waiting.cursor = Some(
        state
            .events
            .next_cursor()
            .map_err(|_| Error::UnknownStoreOutcome)?,
    );
    proof
        .measure(&waiting)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    state
        .events
        .append(waiting)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(())
}
