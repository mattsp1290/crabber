//! Exact original metadata does not depend on today's mutable Run fence or on
//! the presence of every later projection. It never produces a grant.
use crate::memory::State;
use crate::native_parent::RunIssuance;
use crate::native_parent::source_protocol::JournalSource;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{RunId, SessionId};

pub(in crate::memory) fn read<'a>(
    state: &'a State,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut Proof,
) -> Result<Option<&'a RunIssuance>, Failure> {
    crate::native_parent::source_protocol::validate_scope(session, run).map_err(unknown)?;
    if sequence == 0 || sequence > i64::MAX as u64 {
        return Err(unknown("original native issuer sequence"));
    }
    let scope = (session.clone(), run.clone());
    let key = (session.clone(), run.clone(), sequence);
    let protocol = state.native_run_protocols.contains(&scope);
    proof.measure(&(&scope, sequence, protocol))?;
    let issuer = state.native_run_issuances.get(&key);
    if let Some(issuer) = issuer {
        let actual = state
            .runs
            .get(run)
            .ok_or_else(|| unknown("original native Run"))?;
        proof.measure(&(&actual.id, &actual.session_id))?;
        if actual.id != *run || actual.session_id != *session {
            return Err(unknown("original native Run routing"));
        }
        proof.measure(issuer)?;
        if crate::snapshot::encoded_bytes(issuer, 2048).is_none()
            || issuer.state.session_id != *session
            || issuer.state.run_id != *run
            || issuer.sequence != sequence
        {
            return Err(unknown("original native issuer routing/bound"));
        }
        issuer.validate().map_err(unknown)?;
    }
    if !protocol {
        if state.native_session_protocols.contains(session) {
            return Err(unknown("missing fresh Run protocol"));
        }

        if state.native_run_source_heads.contains_key(&scope)
            || state.native_run_sources.keys().any(|(_, id, _)| id == run)
        {
            return Err(unknown("original legacy native source membership"));
        }
        return Ok(issuer);
    }
    // The immutable first slot is the original cohort anchor. Current heads
    // and later projections are deliberately outside this metadata proof.
    let first = source(state, session, run, 1, proof)?;
    let Some(issuer) = issuer else {
        return if sequence > 1 && !state.native_run_sources.contains_key(&key) {
            Ok(None)
        } else {
            Err(unknown("missing original native issuer"))
        };
    };
    let selected = if sequence == 1 {
        first
    } else {
        source(state, session, run, sequence, proof)?
    };
    selected.verify(issuer).map_err(unknown)?;
    if sequence > 1 {
        let previous = source(state, session, run, sequence - 1, proof)?;
        if selected.previous_digest.as_ref() != Some(&previous.chain_digest) {
            return Err(unknown("original native source predecessor"));
        }
    }
    Ok(Some(issuer))
}

fn source<'a>(
    state: &'a State,
    session: &SessionId,
    run: &RunId,
    sequence: u64,
    proof: &mut Proof,
) -> Result<&'a JournalSource, Failure> {
    let source = state
        .native_run_sources
        .get(&(session.clone(), run.clone(), sequence))
        .ok_or_else(|| unknown("original native source slot"))?;
    proof.measure(source)?;
    source.validate().map_err(unknown)?;
    if source.session_id != *session || source.run_id != *run || source.sequence != sequence {
        return Err(unknown("original native source slot routing"));
    }
    Ok(source)
}
