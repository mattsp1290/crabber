//! Run-local negative enrollment facts under the existing Memory session gate.
//! The inherited whole-State transaction clone is not bounded by this module.
use super::State;
use crate::native_enrollment::{Origin, Status, Unclassified};
use crate::native_parent::{RunIssuance, source_protocol::JournalSource};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{RunId, SessionId};

pub(super) struct Birth {
    origin: Origin,
    unclassified: Unclassified,
}

/// Prepare all fallible source work before the original publisher mutates its
/// maps. This is consumed within that same guarded transaction, never retained
/// as a verified bool across another lock or state clone.
pub(super) fn prepare_birth(
    state: &State,
    issuer: &RunIssuance,
    source: &JournalSource,
    proof: &mut Proof,
) -> Result<Birth, Failure> {
    vacant(state, &issuer.state.run_id)?;
    let (origin, unclassified) = Origin::raw_birth(issuer, source, proof)
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    proof.measure(&origin)?;
    proof.measure(&unclassified)?;
    Ok(Birth {
        origin,
        unclassified,
    })
}

fn vacant(state: &State, run: &RunId) -> Result<(), Failure> {
    if state
        .execution_enrollment_protocols
        .iter()
        .any(|(_, id)| id == run)
        || state
            .execution_enrollment_origins
            .iter()
            .any(|((_, id), value)| id == run || &value.run_id == run)
        || state
            .execution_enrollment_unclassified
            .iter()
            .any(|((_, id), value)| id == run || &value.run_id == run)
    {
        return Err(unknown("existing execution enrollment birth"));
    }
    Ok(())
}

pub(super) fn publish_birth(state: &mut State, birth: Birth) -> Result<(), Failure> {
    vacant(state, &birth.origin.run_id)?;
    let scope = (birth.origin.session_id.clone(), birth.origin.run_id.clone());
    state.execution_enrollment_protocols.insert(scope.clone());
    state
        .execution_enrollment_origins
        .insert(scope.clone(), birth.origin);
    state
        .execution_enrollment_unclassified
        .insert(scope, birth.unclassified);
    Ok(())
}

/// Authenticate only this original birth/negative fact. Later current state or
/// native issuer heads cannot replace it. There is deliberately no Complete.
pub(super) fn original(
    state: &State,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Status, Failure> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(unknown)?;
    proof.measure(&(session, run))?;
    let actual = state
        .runs
        .get(run)
        .ok_or_else(|| unknown("execution enrollment Run"))?;
    proof.measure(&(&actual.id, &actual.session_id))?;
    if actual.id != *run || actual.session_id != *session {
        return Err(unknown("execution enrollment Run routing"));
    }
    let scope = (session.clone(), run.clone());
    for (owner, id) in &state.execution_enrollment_protocols {
        if id == run && owner != session {
            return Err(unknown("execution enrollment cohort routing"));
        }
    }
    let origin = state.execution_enrollment_origins.get(&scope);
    let unclassified = state.execution_enrollment_unclassified.get(&scope);
    for ((owner, id), value) in &state.execution_enrollment_origins {
        if id != run && &value.run_id != run {
            continue;
        }
        proof.measure(&(owner, id, value))?;
        if owner != session || id != run || value.session_id != *owner || value.run_id != *id {
            return Err(unknown("execution enrollment origin routing"));
        }
    }
    for ((owner, id), value) in &state.execution_enrollment_unclassified {
        if id != run && &value.run_id != run {
            continue;
        }
        proof.measure(&(owner, id, value))?;
        if owner != session || id != run || value.session_id != *owner || value.run_id != *id {
            return Err(unknown("execution enrollment poison routing"));
        }
    }
    if !state.execution_enrollment_protocols.contains(&scope) {
        return if origin.is_none() && unclassified.is_none() {
            Ok(Status::Legacy)
        } else {
            Err(unknown("legacy execution enrollment source"))
        };
    }
    let origin = origin.ok_or_else(|| unknown("missing execution enrollment origin"))?;
    let unclassified =
        unclassified.ok_or_else(|| unknown("missing execution enrollment poison"))?;
    let first_key = (session.clone(), run.clone(), 1);
    let first = state
        .native_run_sources
        .get(&first_key)
        .ok_or_else(|| unknown("execution enrollment native source"))?;
    let issuer = state
        .native_run_issuances
        .get(&first_key)
        .ok_or_else(|| unknown("execution enrollment first grant"))?;
    origin
        .verify_original(unclassified, first, issuer, proof)
        .map_err(crate::public_session::v2::proof::ReadError::public)?;
    Ok(Status::Unclassified)
}

/// Whole-scope callers must retain this reverse membership check even when the
/// current Run set is empty. A selected per-present-Run loop misses erasure.
pub(super) fn routes(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    for (owner, run) in &state.execution_enrollment_protocols {
        if related(state, session, owner, run) {
            proof.measure(&(owner, run))?;
            if owner != session || original(state, owner, run, proof)? != Status::Unclassified {
                return Err(unknown("orphaned execution enrollment cohort"));
            }
        }
    }
    for ((owner, run), value) in &state.execution_enrollment_origins {
        if related(state, session, owner, run) || &value.session_id == session {
            proof.measure(&(owner, run, value))?;
            if owner != session || original(state, owner, run, proof)? != Status::Unclassified {
                return Err(unknown("orphaned execution enrollment origin"));
            }
        }
    }
    for ((owner, run), value) in &state.execution_enrollment_unclassified {
        if related(state, session, owner, run) || &value.session_id == session {
            proof.measure(&(owner, run, value))?;
            if owner != session || original(state, owner, run, proof)? != Status::Unclassified {
                return Err(unknown("orphaned execution enrollment poison"));
            }
        }
    }
    Ok(())
}

fn related(state: &State, session: &SessionId, owner: &SessionId, run: &RunId) -> bool {
    owner == session
        || state
            .runs
            .get(run)
            .is_some_and(|actual| &actual.session_id == session)
}

#[cfg(test)]
#[path = "native_enrollment/tests.rs"]
mod tests;
