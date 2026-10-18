//! Exact replay or a precharged original body before the inherited State clone.
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCommitError, PreparationCompletion, PreparationPurpose,
    PreparedOperation,
};
use crate::{
    StoreError,
    memory::{State, queue},
};
pub(super) enum Before<'a> {
    Other,
    Replayed(Box<PreparationCompletion>),
    Selected(Box<queue::selection::Selected<'a>>),
}
pub(super) fn before<'a>(
    state: &'a State,
    joined: &JoinedPreparation,
    proof: &mut Proof,
    now: time::OffsetDateTime,
) -> Result<Before<'a>, PreparationCommitError> {
    let PreparedOperation::Queue { request, .. } = joined.observed_operation() else {
        return Ok(Before::Other);
    };
    if let Some(outcome) = queue::replay_with_proof(state, request, proof)? {
        super::verify_replay(state, joined.observed_grant())?;
        return Ok(Before::Replayed(Box::new(PreparationCompletion::Queue(
            outcome,
        ))));
    }
    let selected = queue::selection::select(state, request, proof)?;
    let original = joined.observed_grant().original_reservation();
    let PreparationPurpose::Queue { authority } = &original.purpose else {
        return Err(crate::session_owner::SessionOwnerError::SemanticConflict.into());
    };
    let scope = crate::queue::source::Scope {
        session: authority.session_id.clone(),
        queue: authority.queue_id.clone(),
    };
    if !state
        .queue_publications
        .protocol(&scope, proof)
        .map_err(queue::protocol::domain)?
    {
        return Err(StoreError::HostUnsupported.into());
    }
    queue::preparation_context_selected(
        state,
        selected.record().ok_or(StoreError::NotFound)?,
        selected.prepared(),
        authority,
        &original.intent,
        now,
    )?;
    Ok(Before::Selected(Box::new(selected)))
}
