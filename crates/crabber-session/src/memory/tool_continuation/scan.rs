//! Admission and native controls inspect records without trusting a barrier/state filter.
use super::{Error, State, ToolContinuationRecord, ToolContinuationState};
use crabber_core::{EventKind, EventRecord, Run, SessionId};

pub(in super::super) fn origin(event: &EventRecord) -> bool {
    event.kind == EventKind::ContinuationChanged
        && event
            .payload
            .get("kind")
            .and_then(serde_json::Value::as_str)
            == Some("tool_continuation")
        && event
            .payload
            .pointer("/outcome/operation")
            .and_then(serde_json::Value::as_str)
            == Some("suspended")
}
pub(super) fn run(
    state: &State,
    run: &Run,
) -> Result<
    (
        Option<ToolContinuationRecord>,
        Option<ToolContinuationRecord>,
    ),
    Error,
> {
    run_with_proof(
        state,
        run,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
}
pub(in crate::memory) fn run_with_proof(
    state: &State,
    run: &Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<
    (
        Option<ToolContinuationRecord>,
        Option<ToolContinuationRecord>,
    ),
    Error,
> {
    super::super::continuation_input::consistent(
        state,
        crate::continuation_input::Domain::Tool,
        &run.session_id,
        Some(&run.id),
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut latest: Option<ToolContinuationRecord> = None;
    let mut active = None;
    let mut count = 0;
    for record in state
        .tool_continuations
        .values()
        .filter(|record| record.session_id == run.session_id && record.run_id == run.id)
    {
        let record = super::checked_record_with_proof(state, &record.batch.continuation.id, proof)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        count += 1;
        if record.state != ToolContinuationState::Released {
            if active.is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
            proof
                .measure(&record)
                .map_err(|_| Error::UnknownStoreOutcome)?;
            active = Some(record.clone());
        }
        if latest.as_ref().is_none_or(|old| {
            old.batch.continuation.generation < record.batch.continuation.generation
        }) {
            latest = Some(record);
        }
    }
    if count
        != state
            .events
            .iter()
            .filter(|event| {
                event.session_id == run.session_id && event.run_id == run.id && origin(event)
            })
            .count()
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((latest, active))
}
pub(in super::super) fn pending(state: &State, session: &SessionId) -> Result<bool, Error> {
    pending_with_proof(
        state,
        session,
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
    .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in crate::memory) fn pending_with_proof(
    state: &State,
    session: &SessionId,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<bool, crate::public_session::v2::proof::ToolReadError> {
    super::super::continuation_input::consistent(
        state,
        crate::continuation_input::Domain::Tool,
        session,
        None,
        proof,
    )
    .map_err(|error| error.map_domain(|_| Error::UnknownStoreOutcome))?;
    let mut count = 0;
    let mut pending = false;
    for record in state
        .tool_continuations
        .values()
        .filter(|record| record.session_id == *session)
    {
        let record = super::checked_record_with_proof(state, &record.batch.continuation.id, proof)
            .map_err(|error| error.map_domain(|_| Error::UnknownStoreOutcome))?;
        count += 1;
        pending |= record.state != ToolContinuationState::Released;
    }
    proof
        .measure(&state.tool_barriers.get(session))
        .map_err(crate::public_session::v2::proof::ToolReadError::Budget)?;
    if count
        != state
            .events
            .iter()
            .filter(|event| event.session_id == *session && origin(event))
            .count()
        || state.tool_barriers.get(session).is_some_and(|id| {
            state.tool_continuations.get(id).is_none_or(|record| {
                record.session_id != *session || record.state == ToolContinuationState::Released
            })
        })
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(pending)
}
