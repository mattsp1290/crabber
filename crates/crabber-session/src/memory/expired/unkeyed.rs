//! Original Witness2 and exact immutable source carriers; no missing-journal inference.
use super::{Error, State};
use crate::session_owner::preparation::expired::unkeyed::Original;
use crabber_core::{RunId, SessionId};
pub(in crate::memory) fn read(
    state: &State,
    session: &SessionId,
    run: &RunId,
) -> Result<Option<Box<Original>>, Error> {
    let Some(witness) = state.preparation_admissions.get(run) else {
        return Ok(None);
    };
    bounded(witness, 8192)?;
    witness.validate()?;
    if witness.run_id() != run || witness.session_id() != session {
        return Err(Error::UnknownStoreOutcome);
    }
    if !witness.is_unkeyed() {
        return Ok(None);
    }
    if state.admission_executions.contains_key(run)
        || state.admission_execution_protocols.contains(run)
        || state
            .admission_execution_sources
            .keys()
            .any(|(id, _)| id == run)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let current = state
        .sessions
        .get(session)
        .ok_or(Error::UnknownStoreOutcome)?;
    let initial = state
        .native_run_issuances
        .get(&(session.clone(), run.clone(), 1))
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(current, 16384)?;
    bounded(initial, 2048)?;
    if state
        .preparation_admission_joins
        .get(&(session.clone(), witness.join_id()?))
        != Some(run)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let mut audits = Vec::with_capacity(5);
    for id in witness.audit_ids()? {
        let audit = state
            .session_owners
            .source_audit(session, &id)
            .ok_or(Error::UnknownStoreOutcome)?;
        bounded(audit, 32768)?;
        audits.push(audit.clone());
    }
    let original = Box::new(Original {
        session: current.clone(),
        witness: witness.clone(),
        initial: initial.clone(),
        audits: audits.try_into().map_err(|_| Error::UnknownStoreOutcome)?,
    });
    original.digest()?;
    Ok(Some(original))
}
fn bounded(value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}

#[cfg(test)]
#[path = "unkeyed/tests.rs"]
mod tests;
