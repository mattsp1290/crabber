//! Exact native ownership/journal membership; structural proof never substitutes
//! for the separate positive resource coverage required for public deletion.
use super::super::State;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{RunId, SessionId};

pub(super) fn verify(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    preparations(state, session, proof)?;
    journal(state, session, proof)
}
fn in_scope(state: &State, session: &SessionId, id: &RunId) -> bool {
    state
        .runs
        .get(id)
        .is_some_and(|run| &run.session_id == session)
}
fn preparations(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    for (id, witness) in &state.preparation_admissions {
        if witness.session_id() != session && !in_scope(state, session, id) {
            continue;
        }
        proof.measure(&(id, witness))?;
        witness.validate().map_err(unknown)?;
        let run = state
            .runs
            .get(id)
            .ok_or_else(|| unknown("orphan preparation witness"))?;
        proof.measure(&(&run.id, &run.session_id))?;
        if id != witness.run_id()
            || run.id != *id
            || run.session_id != *session
            || witness.session_id() != session
        {
            return Err(unknown("preparation witness routing"));
        }
        let join = witness.join_id().map_err(unknown)?;
        let key = (session.clone(), join);
        let indexed = state
            .preparation_admission_joins
            .get(&key)
            .ok_or_else(|| unknown("preparation join index"))?;
        proof.measure(&(&key, indexed))?;
        if indexed != id {
            return Err(unknown("preparation join target"));
        }
        original_admission(state, witness, proof)?;
    }
    preparation_indexes(state, session, proof)
}
pub(super) fn original_admission(
    state: &State,
    witness: &crate::session_owner::preparation::admission::Witness,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let session = witness.session_id();
    let initial = state
        .native_run_issuances
        .get(&(session.clone(), witness.run_id().clone(), 1))
        .ok_or_else(|| unknown("preparation initial issuer"))?;
    proof.measure(initial)?;
    let ids = witness.audit_ids().map_err(unknown)?;
    let mut audits = Vec::with_capacity(5);
    for operation in &ids {
        let audit = state
            .session_owners
            .source_audit(session, operation)
            .ok_or_else(|| unknown("preparation original source"))?;
        proof.measure(audit)?;
        audits.push(audit);
    }
    let current = state
        .sessions
        .get(session)
        .ok_or_else(|| unknown("preparation session"))?;
    proof.measure(current)?;
    witness
        .verify_sources(
            current,
            initial,
            &crate::session_owner::preparation::admission::SourceAudits {
                reserve: audits[0],
                begin: audits[1],
                join: audits[2],
                child_reserve: audits[3],
                child_begin: audits[4],
            },
        )
        .map_err(unknown)?;
    Ok(())
}
fn preparation_indexes(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for ((scope, join), id) in &state.preparation_admission_joins {
        if scope != session
            && !in_scope(state, session, id)
            && state
                .preparation_admissions
                .get(id)
                .is_none_or(|w| w.session_id() != session)
        {
            continue;
        }
        proof.measure(&(scope, join, id))?;
        let witness = state
            .preparation_admissions
            .get(id)
            .ok_or_else(|| unknown("orphan preparation join"))?;
        if scope != session
            || witness.session_id() != session
            || witness.run_id() != id
            || witness.join_id().map_err(unknown)? != *join
            || !in_scope(state, session, id)
        {
            return Err(unknown("preparation join routing"));
        }
    }
    // Immutable initial issuance independently dictates witness membership.
    for ((scope, id, sequence), initial) in &state.native_run_issuances {
        if scope != session && initial.state.session_id != *session {
            continue;
        }
        proof.measure(&(scope, id, sequence, initial))?;
        if scope != session
            || initial.state.session_id != *session
            || initial.state.run_id != *id
            || initial.sequence != *sequence
            || !in_scope(state, session, id)
        {
            return Err(unknown("owned issuer routing"));
        }
        if *sequence == 1 {
            initial.validate().map_err(unknown)?;
            let witness = state.preparation_admissions.get(id);
            match (&initial.ownership_handoff, witness) {
                (None, None) => {}
                (Some(digest), Some(witness))
                    if witness.fingerprint().map_err(unknown)? == *digest => {}
                _ => return Err(unknown("owned issuer witness membership")),
            }
        }
    }
    Ok(())
}
fn journal(state: &State, session: &SessionId, proof: &mut Proof) -> Result<(), Failure> {
    for ((id, kind), source) in &state.admission_execution_sources {
        let issuance = source.issuance();
        if issuance.state.session_id != *session && !in_scope(state, session, id) {
            continue;
        }
        proof.measure(&(id, kind, source))?;
        if source.kind() != *kind
            || issuance.state.session_id != *session
            || issuance.state.run_id != *id
            || !in_scope(state, session, id)
        {
            return Err(unknown("execution journal routing"));
        }
        let original = state
            .native_run_issuances
            .get(&(session.clone(), id.clone(), issuance.sequence))
            .ok_or_else(|| unknown("execution journal issuer"))?;
        proof.measure(original)?;
        if original != issuance {
            return Err(unknown("execution journal issuer mismatch"));
        }
        let record = state
            .admission_executions
            .get(id)
            .ok_or_else(|| unknown("orphan execution source"))?;
        proof.measure(record)?;
        if record.receipt.run_id != *id || record.receipt.session_id != *session {
            return Err(unknown("execution source record routing"));
        }
    }
    for id in &state.admission_execution_protocols {
        let record = state.admission_executions.get(id);
        let run = state.runs.get(id);
        // An unroutable cohort can never confer coverage or be erased by a guessed scope.
        let (Some(run), Some(record)) = (run, record) else {
            proof.measure(id)?;
            return Err(unknown("orphan execution cohort"));
        };
        if !in_scope(state, session, id) && record.receipt.session_id != *session {
            continue;
        }
        proof.measure(id)?;
        proof.measure(&(&run.id, &run.session_id))?;
        if !in_scope(state, session, id) {
            return Err(unknown("execution cohort routing"));
        }
    }
    for (id, record) in &state.admission_executions {
        if record.receipt.session_id != *session && !in_scope(state, session, id) {
            continue;
        }
        proof.measure(&(id, record))?;
        if record.receipt.session_id != *session
            || record.receipt.run_id != *id
            || !in_scope(state, session, id)
        {
            return Err(unknown("execution record routing"));
        }
        // Both source carriers were charged above before this validator encodes
        // or canonicalizes bounded source comparisons.
        super::super::admission_journal::verify(state, id).map_err(unknown)?;
    }
    Ok(())
}
