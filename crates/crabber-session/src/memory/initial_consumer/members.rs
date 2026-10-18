//! Native selected member originals are checked independently of the consumer
//! projection. Historical receipts are looked up by their actual captured keys.
use crate::memory::{OperationReceipt, State};
use crate::native_enrollment::initial_consumer::{self as protocol, Original, Selection};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
fn same<T: serde::Serialize>(actual: &T, original: &T, proof: &mut Proof) -> Result<(), Failure> {
    proof.measure(actual)?;
    if protocol::digest(actual)? != protocol::digest(original)? {
        return Err(unknown("changed consumer native original"));
    }
    Ok(())
}
pub(super) fn verify(
    state: &State,
    view: &Selection<'_>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    crate::memory::initial_consumer::policy::verify_retained(state, view, proof)?;
    let mut latest_attempt = None;
    for result in &view.results {
        member(state, &result.original, proof)?;
    }
    for publication in view.publications {
        if let Some(result) = view
            .results
            .iter()
            .find(|r| r.digest().ok().as_ref() == Some(&publication.original_digest))
        {
            match &result.original {
                Original::ReserveInitial(v) | Original::BeginInitial(v) => {
                    latest_attempt = Some(&v.after);
                }
                Original::Conclude(v) => latest_attempt = Some(&v.attempt.after),
                _ => (),
            }
        }
    }
    if let Some(expected) = latest_attempt {
        let actual = state
            .attempts
            .get(&expected.snapshot.target.id)
            .ok_or_else(|| unknown("missing current consumer Attempt"))?;
        crate::native_enrollment::caller::measure_record(actual, proof)?;
        if actual.snapshot != expected.snapshot
            || actual.token != expected.token
            || actual.run_token != expected.run_token
            || actual.owner != expected.owner
            || actual.lease_until != expected.lease_until
        {
            return Err(unknown("consumer current Attempt does not match original"));
        }
        if state
            .attempts
            .values()
            .filter(|r| r.snapshot.run_id == expected.snapshot.run_id)
            .count()
            != 1
        {
            return Err(unknown("consumer additional Attempt target"));
        }
    }
    if let Some(origin) = view.origin {
        crate::memory::initial_consumer::retirement::verify_retained(
            state,
            origin.correlation(),
            view,
            proof,
        )?;
    }
    Ok(())
}
fn attempt(state: &State, v: &protocol::AttemptOriginal, proof: &mut Proof) -> Result<(), Failure> {
    let retained = state
        .operation_receipts
        .get(&(
            v.receipt.attempt.session_id.clone(),
            v.receipt.operation_id.clone(),
        ))
        .ok_or_else(|| unknown("missing native Attempt receipt"))?;
    let OperationReceipt::Attempt(retained) = retained else {
        return Err(unknown("native Attempt receipt domain"));
    };
    same(retained, &v.receipt, proof)?;
    same(
        state
            .events
            .get(v.receipt.event_cursor)
            .ok_or_else(|| unknown("missing native Attempt event"))?,
        &v.event,
        proof,
    )?;
    if let Some(publication) = &v.publication {
        let id = (
            &publication.issuer.state.session_id,
            &publication.issuer.state.run_id,
            publication.issuer.sequence,
        );
        same(
            state
                .native_run_issuances
                .get(&(id.0.clone(), id.1.clone(), id.2))
                .ok_or_else(|| unknown("missing native consumer issuer"))?,
            &publication.issuer,
            proof,
        )?;
        same(
            state
                .native_run_sources
                .get(&(id.0.clone(), id.1.clone(), id.2))
                .ok_or_else(|| unknown("missing native consumer source"))?,
            &publication.source,
            proof,
        )?;
    }
    Ok(())
}

fn member(state: &State, original: &Original, proof: &mut Proof) -> Result<(), Failure> {
    match original {
        Original::Register(v) => register(state, v, proof)?,
        Original::Activate(v) => same(
            state
                .session_owners
                .source_audit(&v.receipt.session_id, &v.receipt.operation_id)
                .ok_or_else(|| unknown("missing native activation"))?,
            &v.audit,
            proof,
        )?,
        Original::KeyedBegin(v) => {
            use crate::admission_journal::{Kind, Source};
            let init = state
                .admission_execution_sources
                .get(&(v.record.receipt.run_id.clone(), Kind::Init))
                .ok_or_else(|| unknown("missing native Init"))?;
            let begin = state
                .admission_execution_sources
                .get(&(v.record.receipt.run_id.clone(), Kind::Begin))
                .ok_or_else(|| unknown("missing native keyed Begin"))?;
            proof.measure(&(init, begin))?;
            let (Source::Init(init), Source::Begin(begin)) = (init, begin) else {
                return Err(unknown("native keyed source kinds"));
            };
            same(init, &v.init, proof)?;
            same(begin, &v.begin, proof)?;
            same(
                state
                    .admission_executions
                    .get(&v.record.receipt.run_id)
                    .ok_or_else(|| unknown("missing native keyed record"))?,
                &v.record,
                proof,
            )?;
        }
        Original::ReserveInitial(v) | Original::BeginInitial(v) => {
            attempt(state, v, proof)?;
        }
        Original::AssistantMessage(v) => same(
            state
                .messages
                .iter()
                .find(|m| m.id == v.message.id)
                .ok_or_else(|| unknown("missing native assistant message"))?,
            &v.message,
            proof,
        )?,
        Original::Lifecycle(v) => same(
            state
                .events
                .get(v.cursor)
                .ok_or_else(|| unknown("missing native lifecycle event"))?,
            &v.event,
            proof,
        )?,
        Original::Conclude(v) => {
            attempt(state, &v.attempt, proof)?;
            same(
                state
                    .events
                    .get(
                        v.settlement
                            .cursor
                            .ok_or_else(|| unknown("missing settlement cursor"))?,
                    )
                    .ok_or_else(|| unknown("missing native settlement"))?,
                &v.settlement,
                proof,
            )?;
            same(
                state
                    .runs
                    .get(&v.after_run.id)
                    .ok_or_else(|| unknown("missing consumer Run"))?,
                &v.after_run,
                proof,
            )?;
        }
    }
    Ok(())
}
fn register(
    state: &State,
    v: &protocol::RegisterOriginal,
    proof: &mut Proof,
) -> Result<(), Failure> {
    same(
        state
            .session_owners
            .source_audit(&v.receipt.session_id, &v.receipt.reserve.operation_id)
            .ok_or_else(|| unknown("missing native driver Reserve"))?,
        &v.reserve,
        proof,
    )?;
    same(
        state
            .session_owners
            .source_audit(&v.receipt.session_id, &v.receipt.begin.operation_id)
            .ok_or_else(|| unknown("missing native driver Begin"))?,
        &v.begin,
        proof,
    )?;
    Ok(())
}
