use super::{MemoryStore, OperationReceipt, State};
use crate::public_session::budget::{self, Budget, Failure, unknown};
use crate::{
    PublicApprovalBarrier, PublicControlHeader, PublicHeaderOutcome, PublicHeaderRequest,
    PublicHeaderUsage, PublicSessionError, PublicSessionLimit,
};
use crabber_core::{EventCursor, EventRecord, Run, SessionRevision};
impl MemoryStore {
    pub(super) fn read_public_header(
        &self,
        request: &PublicHeaderRequest,
    ) -> Result<PublicHeaderOutcome, PublicSessionError> {
        budget::validate_request(request)?;
        let state = self.state.lock().unwrap();
        let revision = state
            .public_revisions
            .get(&request.session_id)
            .copied()
            .unwrap_or(SessionRevision::INITIAL);
        if !state.sessions.contains_key(&request.session_id) {
            return Err(PublicSessionError::NotFound);
        }
        if revision != request.expected_revision {
            return Ok(PublicHeaderOutcome::Invalidated);
        }
        budget::outcome(read(&state, request, revision))
    }
}
fn read(
    state: &State,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
) -> Result<PublicControlHeader, Failure> {
    let mut safe = Budget::new(request.max_bytes, PublicSessionLimit::HeaderBytes);
    let mut proof = Budget::new(request.max_proof_bytes, PublicSessionLimit::ProofBytes);
    let header = base(state, request, revision, &mut safe, &mut proof)?;
    budget::finish_header(header, safe, &proof)
}
pub(super) fn base(
    state: &State,
    request: &PublicHeaderRequest,
    revision: SessionRevision,
    safe: &mut Budget,
    proof: &mut Budget,
) -> Result<PublicControlHeader, Failure> {
    let session = state
        .sessions
        .get(&request.session_id)
        .ok_or_else(|| unknown("session"))?;
    let baseline = crate::management::Metadata::default();
    let metadata = state
        .management
        .get(&request.session_id)
        .unwrap_or(&baseline);
    safe.measure(&(session, metadata))?;
    crate::management::validate_stored(session, metadata).map_err(unknown)?;
    let summary = crate::management::summary(session, metadata.clone(), revision);
    let mut run = None;
    for id in state.run_order.iter().rev() {
        let candidate = state.runs.get(id).ok_or_else(|| unknown("run order"))?;
        if candidate.id != *id {
            return Err(unknown("run index"));
        }
        if candidate.session_id == request.session_id {
            run = Some(candidate);
            break;
        }
    }
    if run.map(|run| &run.id) != request.run_id.as_ref() {
        return Err(unknown("latest run"));
    }
    let attempt = run
        .and_then(|run| {
            state
                .attempts
                .values()
                .filter(|record| record.snapshot.run_id == run.id)
                .max_by_key(|record| record.snapshot.target.generation)
        })
        .map(|record| &record.snapshot);
    if let Some(attempt) = attempt
        && (attempt.session_id != request.session_id
            || attempt.target.id.0.is_empty()
            || attempt.target.id.0.len() > 128
            || attempt.target.id.0.chars().any(char::is_control))
    {
        return Err(unknown("attempt"));
    }
    safe.measure(&attempt)?;
    let baseline = crabber_core::PublicStateContents::default();
    let contents = state
        .public_contents
        .get(&request.session_id)
        .unwrap_or(&baseline);
    safe.measure(contents)?;
    let (approval, barrier) = approval(state, run, attempt, request, safe, proof)?;
    Ok(PublicControlHeader {
        version: 1,
        revision,
        session: summary,
        attempt: attempt.cloned(),
        approval,
        barrier,
        contents: contents.clone(),
        read_usage: PublicHeaderUsage::default(),
    })
}
fn approval(
    state: &State,
    run: Option<&Run>,
    attempt: Option<&crate::AttemptSnapshot>,
    request: &PublicHeaderRequest,
    safe: &mut Budget,
    proof: &mut Budget,
) -> Result<
    (
        Option<crate::PublicApprovalSummary>,
        Option<PublicApprovalBarrier>,
    ),
    Failure,
> {
    let barrier = state.approval_barriers.get(&request.session_id);
    let record = run.and_then(|run| {
        state
            .approvals
            .values()
            .filter(|record| record.session_id == request.session_id && record.run_id == run.id)
            .max_by_key(|record| record.batch.continuation.generation)
    });
    let Some(record) = record else {
        if barrier.is_some() || run.is_some_and(|run| super::approval::has_source(state, run)) {
            return Err(unknown("barrier"));
        }
        return Ok((None, None));
    };
    let run = run.ok_or_else(|| unknown("run"))?;
    if barrier.is_some_and(|id| *id != record.batch.continuation.id) {
        return Err(unknown("barrier scope"));
    }
    proof.measure(record)?;
    preflight(state, record, proof)?;
    super::approval::preflight_current(state, run, record, proof)?;
    super::approval::verify_record(state, run, record).map_err(unknown)?;
    budget::validate_approval_state(record, barrier.is_some(), run.status, attempt)?;
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let saved = approval_receipt(state, &record.session_id, &answer.id)?;
            crate::approval::verify_receipt(saved, event(state, saved.event_cursor)?)
                .map_err(unknown)?;
            crate::approval::verify_recovery_answer(answer, saved).map_err(unknown)?;
        }
    }
    Ok((
        Some(budget::approval(record, request.max_interactions, safe)?),
        barrier.map(|_| PublicApprovalBarrier {
            run_id: run.id.clone(),
            continuation: record.batch.continuation.clone(),
        }),
    ))
}
fn approval_receipt<'a>(
    state: &'a State,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
) -> Result<&'a crate::ApprovalReceipt, Failure> {
    let Some(OperationReceipt::Approval(receipt)) =
        state.operation_receipts.get(&(session.clone(), id.clone()))
    else {
        return Err(unknown("approval receipt"));
    };
    Ok(receipt)
}
fn event(state: &State, cursor: EventCursor) -> Result<&EventRecord, Failure> {
    state.events.get(cursor).ok_or_else(|| unknown("event"))
}
pub(super) fn preflight(
    state: &State,
    record: &crate::approval::ApprovalRecord,
    proof: &mut Budget,
) -> Result<(), Failure> {
    let suspension = approval_receipt(state, &record.session_id, &record.batch.id)?;
    proof.measure(suspension)?;
    proof.measure(event(state, suspension.event_cursor)?)?;
    let key = crate::approval::derived_id("suspend-attempt", &record.batch.id);
    let Some(OperationReceipt::Attempt(native)) = state
        .operation_receipts
        .get(&(record.session_id.clone(), key))
    else {
        return Err(unknown("native receipt"));
    };
    proof.measure(native)?;
    proof.measure(event(state, native.event_cursor)?)?;
    let source = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == record.session_id
                && event
                    .cursor
                    .is_some_and(|cursor| cursor < native.event_cursor)
        })
        .ok_or_else(|| unknown("source"))?;
    proof.measure(source)?;
    for question in &record.questions {
        if let Some(answer) = &question.answer {
            let saved = approval_receipt(state, &record.session_id, &answer.id)?;
            proof.measure(saved)?;
            proof.measure(event(state, saved.event_cursor)?)?;
        }
    }
    Ok(())
}

#[test]
fn oversized_approval_proof_is_not_cloned() {
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    let store = MemoryStore::with_clock(clock.clone());
    let f = runtime.block_on(crate::approval_contract::fixture::staged(&store, &clock));
    runtime
        .block_on(f.execution.suspend_for_approval(f.batch.clone(), f.owner))
        .unwrap();
    store
        .state
        .lock()
        .unwrap()
        .approvals
        .get_mut(&f.batch.continuation.id)
        .unwrap()
        .workspace_id = "private-carrier".repeat(100_000);
    crate::public_session::allocation::proof_allocation(
        &runtime,
        &store,
        f.run.session_id,
        f.run.id,
    );
}
