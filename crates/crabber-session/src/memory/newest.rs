//! Borrow and measure before cloning any history payload.
use super::{State, StoreError};
use crate::public_history::newest::Window;
use crate::{PublicNewestHistoryOutcome, SnapshotLimit, SnapshotRequest};
use crabber_core::{EventCursor, Message, SessionId, SessionRevision};

pub(super) fn read(
    state: &State,
    request: &SnapshotRequest,
) -> Result<PublicNewestHistoryOutcome, StoreError> {
    crate::public_history::newest::validate(request)?;
    if !state.sessions.contains_key(&request.session_id) {
        return Err(StoreError::NotFound);
    }
    let revision = state
        .public_revisions
        .get(&request.session_id)
        .copied()
        .unwrap_or(SessionRevision::INITIAL);
    let high_water = state
        .events
        .iter()
        .rev()
        .find(|e| e.session_id == request.session_id)
        .and_then(|e| e.cursor)
        .unwrap_or(EventCursor(0));
    let mut window = Window::new(revision, high_water, request.limits);
    for message in state.messages.for_session(&request.session_id).iter().rev() {
        match append(state, &request.session_id, &mut window, message)? {
            Ok(()) => window.commit(),
            Err(limit) => return Ok(window.finish(Some(limit))),
        }
    }
    Ok(window.finish(None))
}
fn append(
    state: &State,
    session: &SessionId,
    window: &mut Window,
    message: &Message,
) -> Result<Result<(), SnapshotLimit>, StoreError> {
    let ids = match window.message(message, None) {
        Ok(ids) => ids,
        Err(limit) => return Ok(Err(limit)),
    };
    for id in ids {
        let outcome = if let Some(call) = state.calls.get(&id) {
            if state
                .runs
                .get(&call.run_id)
                .is_none_or(|run| run.session_id != *session)
            {
                return Err(StoreError::Validation(
                    "invalid newest history call scope".into(),
                ));
            }
            window.native(call, None)
        } else if let Some(call) = state
            .imported_calls
            .for_session(session)
            .iter()
            .find(|call| call.id == id)
        {
            window.imported(call, None)
        } else {
            Ok(())
        };
        if let Err(limit) = outcome {
            return Ok(Err(limit));
        }
    }
    if let Some(provenance) = state
        .imported_provenance
        .for_session(session)
        .iter()
        .find(|p| p.message_id == message.id)
        && let Err(limit) = window.provenance(provenance, None)
    {
        return Ok(Err(limit));
    }
    Ok(Ok(()))
}
