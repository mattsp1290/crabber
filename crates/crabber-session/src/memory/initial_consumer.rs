//! Selected protected consumer originals under the actual Memory lock.
use super::State;
#[path = "initial_consumer/classification.rs"]
mod classification;
use crate::native_enrollment::initial_consumer::{Correlation, RecordDelta, Selection};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
pub(in crate::memory) use classification::classify_run;
use crabber_core::SessionId;

pub(super) fn routes(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    let present = state
        .initial_consumer
        .routes(session, &state.initial_consumer_cohorts, proof)?;
    for (key, origin) in state
        .initial_consumer_cohorts
        .iter()
        .filter(|(key, origin)| {
            &key.session_id == session || &origin.correlation().key.session_id == session
        })
    {
        proof.measure(&(key, origin.as_ref()))?;
        if key != &origin.correlation().key {
            return Err(unknown("consumer cohort route"));
        }
    }
    let mut keys = std::collections::BTreeSet::new();
    for key in state
        .initial_consumer
        .keys()
        .filter(|key| &key.session_id == session)
    {
        if !keys.contains(key) {
            proof.measure(key)?;
            keys.insert(key);
        }
    }
    for key in keys {
        let origin = state
            .initial_consumer
            .origin(key)
            .ok_or_else(|| unknown("consumer missing anchor"))?;
        let correlation = origin.correlation();
        let view = selected(state, correlation, proof)?;
        let birth =
            state
                .pending_initial
                .selected(&correlation.key, &correlation.identity, proof)?;
        view.verify_birth(birth, proof)?;
    }
    Ok(present)
}
pub(super) fn selected<'a>(
    state: &'a State,
    expected: &Correlation,
    proof: &mut Proof,
) -> Result<Selection<'a>, Failure> {
    state.initial_consumer.selected(
        expected,
        state
            .initial_consumer_cohorts
            .get(&expected.key)
            .map(AsRef::as_ref),
        proof,
    )
}
pub(super) fn contains_scope(state: &State, session: &SessionId) -> bool {
    state.initial_consumer.contains_scope(session)
        || state.initial_consumer_cohorts.iter().any(|(key, origin)| {
            &key.session_id == session || &origin.correlation().key.session_id == session
        })
}
pub(super) fn refuse_raw_driver(
    state: &State,
    session: &SessionId,
    run: &crabber_core::RunId,
    proof: &mut Proof,
) -> Result<(), crate::session_owner::SessionOwnerError> {
    routes(state, session, proof)
        .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
    if state
        .pending_initial
        .assignment_for_run(session, run, proof)
        .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?
        .is_some()
    {
        return Err(crate::session_owner::SessionOwnerError::Unsupported);
    }
    Ok(())
}
pub(super) fn publish(
    state: &mut State,
    delta: RecordDelta,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let original = delta
        .origin
        .as_ref()
        .map(|origin| {
            proof.measure(&(&origin.correlation().key, origin))?;
            Ok::<_, Failure>((origin.correlation().key.clone(), origin.clone()))
        })
        .transpose()?;
    if original
        .as_ref()
        .is_some_and(|(key, _)| state.initial_consumer_cohorts.contains_key(key))
    {
        return Err(unknown("immutable consumer cohort replacement"));
    }
    let cohort = state
        .initial_consumer_cohorts
        .get(&delta.publication.correlation.key)
        .map(AsRef::as_ref);
    state.initial_consumer.publish(delta, cohort, proof)?;
    if let Some((key, origin)) = original {
        state
            .initial_consumer_cohorts
            .insert(key, std::sync::Arc::new(origin));
    }
    Ok(())
}
pub(super) fn purge(
    state: &mut State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if !contains_scope(state, session) {
        routes(state, session, proof)?;
        return Ok(());
    }
    let tombstone = state
        .tombstones
        .get(session)
        .ok_or_else(|| unknown("consumer purge requires tombstone"))?;
    proof.measure(tombstone)?;
    tombstone.verify(session).map_err(unknown)?;
    routes(state, session, proof)?;
    state.initial_consumer.purge(session);
    state
        .initial_consumer_cohorts
        .retain(|key, _| &key.session_id != session);
    Ok(())
}
pub(super) fn refuse_raw_activation(
    state: &State,
    request: &crate::session_owner::owned_scope::OwnedScopeActivation,
    proof: &mut Proof,
) -> Result<(), crate::session_owner::SessionOwnerError> {
    let session = request.session_id();
    crate::memory::pending_initial::routes(state, session, proof)
        .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
    for slot in state
        .pending_initial_protocols
        .values()
        .filter(|slot| &slot.key.session_id == session)
    {
        proof
            .measure(slot)
            .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
        let epoch = state
            .pending_initial
            .epoch_for_slot(slot)
            .ok_or(crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
        let birth = state
            .pending_initial
            .selected(&slot.key, &epoch.correlation.identity, proof)
            .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
        if let Some(assignment) = birth.assignment {
            proof
                .measure(&(&assignment.correlation.key.session_id, &assignment.run_id))
                .map_err(|_| crate::session_owner::SessionOwnerError::UnknownStoreOutcome)?;
            let identity = crate::session_owner::validation::digest(&(
                "crabber.execution-driver.initial.identity.v1",
                session,
                &assignment.run_id,
            ))?;
            if request.target().id.0 == format!("driver-owner-{}", identity.as_str()) {
                return Err(crate::session_owner::SessionOwnerError::Unsupported);
            }
        }
    }
    Ok(())
}
native_pending_initial_items! {
    #[cfg(test)]
    #[path="initial_consumer/classification_tests.rs"]
    mod classification_tests;
    #[path="initial_consumer/classification_native.rs"]
    mod classification_native;
    #[cfg(test)]
    #[path="initial_consumer/retirement_budget_tests.rs"]
    mod retirement_budget_tests;
    #[cfg(test)]
    #[path="initial_consumer/retirement_tests.rs"]
    mod retirement_tests;
    #[path="initial_consumer/retirement.rs"]
    pub(in crate::memory) mod retirement;
    #[path="initial_consumer/native.rs"]
    pub(in crate::memory) mod native;
    #[cfg(test)]
    #[path="initial_consumer/tests.rs"]
    mod tests;
    #[cfg(test)]
    #[path="initial_consumer/fault_tests.rs"]
    mod fault_tests;
    #[cfg(test)]
    #[path="initial_consumer/budget_tests.rs"]
    mod budget_tests;
}
