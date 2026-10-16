//! Verify the actual newly published native source before the transaction commits.
use crate::native_parent::completion_history as history;
use crate::session_owner::{
    SessionOwnerError as Error,
    preparation::{PreparationCompletion, PreparationGrant},
};
pub(super) fn verify(
    state: &crate::memory::State,
    grant: &PreparationGrant,
    completed: &PreparationCompletion,
) -> Result<(), Error> {
    let Some((session, run, cursor)) = history::metadata(completed) else {
        return Ok(());
    };
    let ids = crate::session_owner::preparation::identity::ids(
        &grant.original_reservation().key,
        session,
    )?;
    // Approval claim also publishes Paused→Running after the actual fence claim.
    // Only these two fresh native issuances are candidates, not old history.
    let current = state
        .native_run_issuances
        .range((session.clone(), run.clone(), 0)..=(session.clone(), run.clone(), u64::MAX))
        .rev()
        .take(2)
        .map(|(_, value)| value)
        .find(|issuer| {
            issuer
                .completion_source
                .as_ref()
                .is_some_and(|source| source.root_join_id == ids.join)
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    let previous = state
        .native_run_issuances
        .get(&(
            session.clone(),
            run.clone(),
            current
                .sequence
                .checked_sub(1)
                .ok_or(Error::UnknownStoreOutcome)?,
        ))
        .ok_or(Error::UnknownStoreOutcome)?;
    let audit = |id| {
        state
            .session_owners
            .source_audit(session, id)
            .ok_or(Error::UnknownStoreOutcome)
    };
    let event = state
        .events
        .iter()
        .find(|event| event.cursor == Some(cursor))
        .ok_or(Error::UnknownStoreOutcome)?;
    if crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    let control = history::control(completed, event).map_err(|_| Error::UnknownStoreOutcome)?;
    if !history::verify_control(
        previous,
        current,
        audit(&ids.reserve)?,
        audit(&ids.begin)?,
        audit(&ids.join)?,
        &control,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
