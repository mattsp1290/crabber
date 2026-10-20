use super::{Error, Proof, State, audit};
use crate::native_parent::RunIssuance;
pub(super) use crate::session_owner::driver::successor::source::scope::Scope;
use crate::session_owner::{audit::Audit, preparation::PreparationKey};
pub(super) struct Pair<'a> {
    pub previous: &'a RunIssuance,
    pub next: &'a RunIssuance,
    pub audits: Vec<Audit>,
}
pub(super) fn issuer<'a>(
    state: &'a State,
    scope: Scope<'_>,
    sequence: u64,
    proof: &mut Proof,
) -> Result<&'a RunIssuance, Error> {
    crate::memory::native_parent::source_protocol::original(
        state,
        scope.session,
        scope.run,
        sequence,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::UnknownStoreOutcome)
}
pub(super) fn pair<'a>(
    state: &'a State,
    scope: Scope<'_>,
    key: &PreparationKey,
    proof: &mut Proof,
) -> Result<Pair<'a>, Error> {
    let join = crate::session_owner::preparation::identity::ids(key, scope.session)?.join;
    let mut sequence = None;
    for ((session, run, slot), value) in &state.native_run_issuances {
        if session == scope.session
            && run == scope.run
            && value
                .completion_source
                .as_ref()
                .is_some_and(|v| v.root_join_id == join)
        {
            super::bounded(&(session, run, slot, value), 2048, proof)?;
            if sequence.replace(*slot).is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
        }
    }
    let sequence = sequence.ok_or(Error::Unsupported)?;
    let next = issuer(state, scope, sequence, proof)?;
    let previous = issuer(
        state,
        scope,
        sequence
            .checked_sub(1)
            .filter(|v| *v > 0)
            .ok_or(Error::UnknownStoreOutcome)?,
        proof,
    )?;
    let joined = audit(state, scope.session, &join, proof)?;
    let mut audits = Vec::with_capacity(5);
    for id in crate::session_owner::driver::successor::source::claim::audit_ids(joined)? {
        audits.push(audit(state, scope.session, &id, proof)?.clone());
    }
    Ok(Pair {
        previous,
        next,
        audits,
    })
}
pub(in crate::memory::session_owner::drivers::successor) fn ancestry(
    state: &State,
    scope: Scope<'_>,
    facts: &crate::session_owner::driver::successor::source::ContinuationFacts,
    current: &RunIssuance,
    proof: &mut Proof,
) -> Result<(), Error> {
    let origin = issuer(state, scope, facts.head, proof)?;
    if crate::native_parent::issuance_digest(origin).map_err(|_| Error::UnknownStoreOutcome)?
        != facts.head_digest
        || origin.state.status != crabber_core::RunStatus::Running
        || current.sequence < origin.sequence
    {
        return Err(Error::StaleOwner);
    }
    for sequence in origin.sequence + 1..=current.sequence {
        crate::session_owner::driver::successor::source::scope::status_only(
            origin,
            issuer(state, scope, sequence, proof)?,
        )?;
    }
    if current.state.token != origin.state.token {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
