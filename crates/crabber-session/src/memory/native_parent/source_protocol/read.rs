//! Borrowed complete membership and current-state reads on one allowance.
use super::original;
use crate::memory::State;
use crate::native_parent::RunIssuance;
use crate::native_parent::source_protocol::Stream;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{RunId, SessionId};

/// A full reader authenticates every actual projection against protected slots
/// and compares the streamed chain with the independent monotonic head.
pub(in crate::memory) fn history(
    state: &State,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(unknown)?;
    let actual = state
        .runs
        .get(run)
        .ok_or_else(|| unknown("native source Run"))?;
    proof.measure(&(&actual.id, &actual.session_id))?;
    if actual.id != *run || actual.session_id != *session {
        return Err(unknown("native source Run routing"));
    }
    let scope = (session.clone(), run.clone());
    let protocol = state.native_run_protocols.contains(&scope);
    proof.measure(&(&scope, protocol))?;
    if state
        .native_run_protocols
        .iter()
        .any(|(scope, id)| id == run && scope != session)
    {
        return Err(unknown("native source cohort routing"));
    }
    let head = state.native_run_source_heads.get(&scope);
    if !protocol {
        if state.native_session_protocols.contains(session) {
            return Err(unknown("missing fresh Run protocol"));
        }

        if state
            .native_run_source_heads
            .keys()
            .any(|(_, id)| id == run)
            || state.native_run_sources.keys().any(|(_, id, _)| id == run)
        {
            return Err(unknown("legacy native source membership"));
        }
        return Ok(false);
    }
    let head = head.ok_or_else(|| unknown("native source head"))?;
    proof.measure(head)?;
    head.validate().map_err(unknown)?;
    if head.session_id != *session || head.run_id != *run {
        return Err(unknown("native source head routing"));
    }
    let mut stream = Stream::default();
    for ((scope, id, sequence), source) in &state.native_run_sources {
        if id != run {
            continue;
        }
        proof.measure(&(scope, id, sequence, source))?;
        if scope != session
            || source.session_id != *scope
            || source.run_id != *id
            || source.sequence != *sequence
        {
            return Err(unknown("native source routing"));
        }
        let issuer = state
            .native_run_issuances
            .get(&(scope.clone(), id.clone(), *sequence))
            .ok_or_else(|| unknown("missing native issuer projection"))?;
        proof.measure(issuer)?;
        stream.observe(source, issuer).map_err(unknown)?;
    }
    for key in state.native_run_issuances.keys() {
        if &key.1 == run {
            proof.measure(key)?;
            if &key.0 != session || !state.native_run_sources.contains_key(key) {
                return Err(unknown("unregistered native issuer projection"));
            }
        }
    }
    stream.finish(head).map_err(unknown)?;
    Ok(true)
}

/// Fresh current-state checks consume full protected membership. The exact
/// original metadata reader intentionally has the weaker historical contract.
pub(in crate::memory) fn retained_current<'a>(
    state: &'a State,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<&'a RunIssuance>, Failure> {
    let protected = history(state, session, run, proof)?;
    let actual = state
        .runs
        .get(run)
        .ok_or_else(|| unknown("current native Run"))?;
    proof.measure(&(
        &actual.id,
        &actual.session_id,
        actual.status,
        &actual.claim_token,
    ))?;
    let sequence = if protected {
        Some(
            state
                .native_run_source_heads
                .get(&(session.clone(), run.clone()))
                .ok_or_else(|| unknown("current native source head"))?
                .sequence,
        )
    } else {
        state
            .native_run_issuances
            .range(
                (session.clone(), run.clone(), 0)..=(session.clone(), run.clone(), i64::MAX as u64),
            )
            .next_back()
            .map(|((_, _, sequence), _)| *sequence)
    };
    let Some(sequence) = sequence else {
        return Ok(None);
    };
    let issuer = original(state, session, run, sequence, proof)?
        .ok_or_else(|| unknown("current native issuer"))?;
    issuer.verify(actual).map_err(unknown)?;
    Ok(Some(issuer))
}

/// Fresh effects require independently protected membership, including on legacy stores.
pub(in crate::memory) fn current<'a>(
    state: &'a State,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<&'a RunIssuance>, Failure> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(unknown)?;
    proof.measure(&(session, run))?;
    if !state
        .native_run_protocols
        .contains(&(session.clone(), run.clone()))
    {
        return Err(unknown("unprotected fresh Run"));
    }
    retained_current(state, session, run, proof)
}

/// Session inventory must check orphaned protected rows before considering an
/// empty current Run set. A per-present-Run loop alone misses whole-Run erasure.
pub(in crate::memory) fn routes(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(), Failure> {
    for (scope, id) in &state.native_run_protocols {
        let actual = state.runs.get(id);
        if scope != session && actual.is_none_or(|run| &run.session_id != session) {
            continue;
        }
        proof.measure(&(scope, id))?;
        crate::attempt::validate_identifier(&id.0).map_err(unknown)?;
        if scope != session || !actual.is_some_and(|run| run.id == *id && run.session_id == *scope)
        {
            return Err(unknown("orphaned native source cohort"));
        }
    }
    for ((scope, id), head) in &state.native_run_source_heads {
        if scope != session
            && &head.session_id != session
            && state
                .runs
                .get(id)
                .is_none_or(|run| &run.session_id != session)
        {
            continue;
        }
        proof.measure(&(scope, id, head))?;
        head.validate().map_err(unknown)?;
        if head.session_id != *scope
            || head.run_id != *id
            || !state
                .native_run_protocols
                .contains(&(scope.clone(), id.clone()))
        {
            return Err(unknown("orphaned native source head"));
        }
    }
    for ((scope, id, sequence), source) in &state.native_run_sources {
        if scope != session
            && &source.session_id != session
            && state
                .runs
                .get(id)
                .is_none_or(|run| &run.session_id != session)
        {
            continue;
        }
        proof.measure(&(scope, id, sequence, source))?;
        source.validate().map_err(unknown)?;
        if source.session_id != *scope
            || source.run_id != *id
            || source.sequence != *sequence
            || !state
                .native_run_protocols
                .contains(&(scope.clone(), id.clone()))
        {
            return Err(unknown("orphaned native source slot"));
        }
    }
    Ok(())
}
