//! Admission and native controls inspect records without trusting a barrier/state filter.
use super::{Error, State, ToolContinuationRecord, ToolContinuationState, checked_record};
use crabber_core::{EventKind, EventRecord, Run, SessionId};

fn origin(event: &EventRecord) -> bool {
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
    let mut latest: Option<ToolContinuationRecord> = None;
    let mut active = None;
    let mut count = 0;
    for record in state
        .tool_continuations
        .values()
        .filter(|record| record.session_id == run.session_id && record.run_id == run.id)
    {
        let record = checked_record(state, &record.batch.continuation.id)?;
        count += 1;
        if record.state != ToolContinuationState::Released {
            if active.is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
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
    let mut count = 0;
    let mut pending = false;
    for record in state
        .tool_continuations
        .values()
        .filter(|record| record.session_id == *session)
    {
        let record = checked_record(state, &record.batch.continuation.id)?;
        count += 1;
        pending |= record.state != ToolContinuationState::Released;
    }
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
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(pending)
}
