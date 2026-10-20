//! Exact retained-set coverage, on the original readiness State and Proof.
use super::{Failure, Proof, SessionId, State, unknown};
use crate::memory::deletion::run_history::CheckedRuns;
use crate::native_enrollment::{initial_consumer::Origin, pending_initial::Correlation};
use crate::session_owner::coverage::FixedInitialOwnerRole;

pub(super) fn uncovered(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let result = inspect(state, session, checked, proof);
    // Defining Ledger methods may preserve domain errors; never lose the
    // incoming allowance's sticky exhaustion while composing their result.
    proof.charge(0)?;
    result
}
fn inspect(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let mut uncovered = checked.uncovered_runs();
    uncovered |= pending_uncovered(state, session, checked, proof)?;
    for source in checked.sources() {
        let expected = source.correlation();
        proof.measure(expected)?;
        let slot = state
            .pending_initial_protocols
            .get(&expected.key)
            .ok_or_else(|| unknown("covered source missing original slot"))?;
        if slot.epoch != expected.identity.epoch {
            return Err(unknown("covered source changed winning epoch"));
        }
        let actual = state
            .pending_initial
            .epoch_for_slot(slot)
            .ok_or_else(|| unknown("covered source missing epoch"))?;
        if actual.correlation != *expected {
            return Err(unknown("covered source changed original epoch"));
        }
        let origin = state
            .initial_consumer
            .origin(&expected.key)
            .ok_or_else(|| unknown("covered source missing consumer"))?;
        let cohort = state
            .initial_consumer_cohorts
            .get(&expected.key)
            .ok_or_else(|| unknown("covered source missing consumer cohort"))?;
        if !match_origin(origin, checked, proof)? || !match_origin(cohort, checked, proof)? {
            return Err(unknown("covered source lost its consumer original"));
        }
    }
    let count = checked
        .sources()
        .len()
        .checked_mul(3)
        .ok_or(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))?;
    let bytes = count
        .checked_mul(std::mem::size_of::<FixedInitialOwnerRole<'_>>())
        .ok_or(Failure::Limited(crate::PublicSessionLimitV2::ProofBytes))?;
    proof.charge(bytes)?;
    let mut roles = Vec::with_capacity(count);
    for source in checked.sources() {
        for role in source.owner_roles() {
            roles.push(FixedInitialOwnerRole {
                reserve: role.reserve,
                begin: role.begin,
                activation: role.activation,
                joined: role.joined,
            });
        }
    }
    uncovered |= state
        .session_owners
        .uncovered_fixed_initial_history(session, &roles, proof)?;
    Ok(uncovered)
}
pub(super) fn pending_uncovered(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    Ok(slots(state, session, checked, proof)?
        | epochs(state, session, checked, proof)?
        | consumers(state, session, checked, proof)?)
}

fn slots(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let mut uncovered = false;
    for (key, original) in &state.pending_initial_protocols {
        if &key.session_id != session && &original.key.session_id != session {
            continue;
        }
        proof.measure(&(key, original))?;
        if key != &original.key || state.pending_initial.slot(key) != Some(original) {
            return Err(unknown("fixed coverage original slot routing"));
        }
        let epoch = state
            .pending_initial
            .epoch_for_slot(original)
            .ok_or_else(|| unknown("fixed coverage missing slot epoch"))?;
        let view = state
            .pending_initial
            .selected(key, &epoch.correlation.identity, proof)?;
        view.verify(key, proof)?;
        if !matched(&epoch.correlation, checked, proof)? {
            uncovered = true;
        }
    }
    Ok(uncovered)
}
fn epochs(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let mut uncovered = false;
    for (id, epoch) in state.pending_initial.epoch_entries() {
        if &epoch.correlation.key.session_id != session
            && !checked.sources().iter().any(|s| {
                &s.correlation().identity.epoch == id
                    || s.correlation().identity.epoch == epoch.correlation.identity.epoch
            })
            && !state.pending_initial_protocols.iter().any(|(key, slot)| {
                (&key.session_id == session || &slot.key.session_id == session)
                    && (&slot.epoch == id || slot.epoch == epoch.correlation.identity.epoch)
            })
        {
            continue;
        }
        proof.measure(&(id, epoch))?;
        let expected = &epoch.correlation;
        if id != &expected.identity.epoch {
            return Err(unknown("fixed coverage epoch index"));
        }
        let slot = state
            .pending_initial
            .slot(&expected.key)
            .ok_or_else(|| unknown("fixed coverage epoch missing slot"))?;
        if &slot.epoch != id {
            return Err(unknown("fixed coverage nonwinning epoch"));
        }
        let birth = state
            .pending_initial
            .selected(&expected.key, &expected.identity, proof)?;
        birth.verify(&expected.key, proof)?;
        if !matched(expected, checked, proof)? {
            uncovered = true;
        }
    }
    Ok(uncovered)
}
fn matched(
    expected: &Correlation,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let mut found = false;
    for source in checked.sources() {
        if source.correlation() != expected {
            continue;
        }
        if found {
            return Err(unknown("fixed coverage duplicate source"));
        }
        proof.measure(source.witness())?;
        if source.witness().run_id() != &source.run().id
            || source.witness().session_id() != &expected.key.session_id
        {
            return Err(unknown("fixed coverage original witness routing"));
        }
        let birth = source.birth();
        let assignment = birth
            .assignment
            .ok_or_else(|| unknown("fixed coverage missing assignment"))?;
        if assignment.run_id != source.run().id || assignment.correlation != *expected {
            return Err(unknown("fixed coverage Run assignment"));
        }
        found = true;
    }
    Ok(found)
}
fn consumers(
    state: &State,
    session: &SessionId,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let mut uncovered = false;
    for (key, origin) in state.initial_consumer.origin_entries() {
        if &key.session_id != session
            && &origin.correlation().key.session_id != session
            && !checked
                .sources()
                .iter()
                .any(|s| s.correlation().identity.epoch == origin.correlation().identity.epoch)
        {
            continue;
        }
        proof.measure(&(key, origin))?;
        if key != &origin.correlation().key {
            return Err(unknown("fixed coverage consumer index"));
        }
        uncovered |= !match_origin(origin, checked, proof)?;
    }
    for (key, origin) in &state.initial_consumer_cohorts {
        if &key.session_id != session
            && &origin.correlation().key.session_id != session
            && !checked
                .sources()
                .iter()
                .any(|s| s.correlation().identity.epoch == origin.correlation().identity.epoch)
        {
            continue;
        }
        proof.measure(&(key, origin.as_ref()))?;
        if key != &origin.correlation().key {
            return Err(unknown("fixed coverage consumer cohort index"));
        }
        let actual = state
            .initial_consumer
            .origin(key)
            .ok_or_else(|| unknown("fixed coverage orphan consumer cohort"))?;
        proof.measure(actual)?;
        if actual.digest()? != origin.digest()? {
            return Err(unknown("fixed coverage changed consumer cohort"));
        }
        uncovered |= !match_origin(origin, checked, proof)?;
    }
    Ok(uncovered)
}
fn match_origin(
    origin: &Origin,
    checked: &CheckedRuns<'_>,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    origin.validate(proof)?;
    if !matched(origin.correlation(), checked, proof)? {
        return Ok(false);
    }
    let source = checked
        .sources()
        .iter()
        .find(|s| s.correlation() == origin.correlation())
        .ok_or_else(|| unknown("fixed coverage source disappeared"))?;
    let saved = source
        .consumer()
        .origin
        .ok_or_else(|| unknown("fixed coverage missing original consumer"))?;
    proof.measure(saved)?;
    if saved.digest()? != origin.digest()? {
        return Err(unknown("fixed coverage different consumer original"));
    }
    Ok(true)
}
