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
    verify_selected(
        state,
        grant,
        history::Selection::Completion(completed),
        &mut crate::public_session::v2::proof::Proof::legacy(),
    )
}
pub(super) fn verify_selected(
    state: &crate::memory::State,
    grant: &PreparationGrant,
    input: history::Selection<'_>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let Some((session, run, cursor)) = input.metadata() else {
        return Ok(());
    };
    proof
        .measure(&grant.original_reservation().key)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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
    proof
        .measure(&(previous, current, event))
        .map_err(|_| Error::UnknownStoreOutcome)?;
    for id in [&ids.reserve, &ids.begin, &ids.join] {
        proof
            .measure(audit(id)?)
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    let control = input
        .control(event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
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

/// Non-Clone result of the real prepared control check in this native mutation.
pub(in crate::memory) struct VerifiedTool(crate::ToolContinuationClaimOutcome);
impl VerifiedTool {
    pub(in crate::memory) fn outcome(&self) -> &crate::ToolContinuationClaimOutcome {
        &self.0
    }
    pub(in crate::memory) fn into_outcome(self) -> crate::ToolContinuationClaimOutcome {
        self.0
    }
}
pub(in crate::memory) fn verify_tool(
    state: &crate::memory::State,
    grant: &PreparationGrant,
    outcome: crate::ToolContinuationClaimOutcome,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<VerifiedTool, Error> {
    let crate::ToolContinuationClaimOutcome::Applied { receipt, .. } = &outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    verify_selected(state, grant, history::Selection::Tool(receipt), proof)?;
    Ok(VerifiedTool(outcome))
}
