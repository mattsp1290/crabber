//! No-effect abandonment uses the same narrow original-source validator as SQL.
use super::super::super::State;
use crate::abandonment::history::{self, Commit, Event, Evidence, RunView};
use crate::admission_journal::{Kind, Source};
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{EventKind, EventRecord, Run};
use serde::de::DeserializeOwned;

pub(super) fn verify(state: &State, run: &Run, proof: &mut Proof) -> Result<bool, Failure> {
    let Some(saved) = state.abandonments.get(&run.id) else {
        return Ok(false);
    };
    proof.measure(saved)?;
    let cursor = saved
        .outcome
        .terminal_event
        .cursor
        .ok_or_else(|| unknown("abandonment cursor"))?;
    let source = state
        .events
        .get(cursor)
        .ok_or_else(|| unknown("original abandonment event"))?;
    proof.measure(source)?;
    if source.cursor != Some(cursor) {
        return Err(unknown("original abandonment cursor"));
    }
    let current = view(run, proof)?;
    let source_event = event(source)?;
    let commit = Commit {
        request: saved.request.clone(),
        no_effect_v1: saved.no_effect_v1.clone(),
        run: view(&saved.outcome.run, proof)?,
        event: event(&saved.outcome.terminal_event)?,
        interrupted: i64::try_from(saved.outcome.interrupted_tools.len()).map_err(unknown)?,
    };
    if commit.no_effect_v1.is_none() {
        if source_event.evidence.no_effect_v1.is_some()
            || commit.event.evidence.no_effect_v1.is_some()
        {
            return Err(unknown("removed no-effect marker"));
        }
        return Ok(false);
    }
    let marker = commit
        .no_effect_v1
        .as_ref()
        .ok_or_else(|| unknown("no-effect marker"))?;
    let init = pristine(state, run, source, proof)?;
    let original = state
        .native_run_issuances
        .get(&(
            run.session_id.clone(),
            run.id.clone(),
            marker.issuance.sequence,
        ))
        .ok_or_else(|| unknown("pre-abandon issuer"))?;
    let revoked = state
        .native_run_issuances
        .get(&(
            run.session_id.clone(),
            run.id.clone(),
            marker
                .issuance
                .sequence
                .checked_add(1)
                .ok_or_else(|| unknown("issuer overflow"))?,
        ))
        .ok_or_else(|| unknown("revoked issuer"))?;
    let terminal = state
        .native_run_issuances
        .range(
            (run.session_id.clone(), run.id.clone(), 0)
                ..=(run.session_id.clone(), run.id.clone(), u64::MAX),
        )
        .next_back()
        .map(|(_, source)| source)
        .ok_or_else(|| unknown("terminal issuer"))?;
    for issuer in [original, revoked, terminal] {
        proof.measure(issuer)?;
    }
    // Memory retains the assigned publication cursor. The protected SQL source
    // stores the pre-publication frame; normalize only that generated field.
    let mut original_event = source_event;
    original_event.cursor = None;
    history::verify(
        &current,
        &commit,
        &original_event,
        cursor,
        init,
        [original, revoked, terminal],
    )
    .map_err(unknown)?;
    Ok(true)
}
fn pristine<'a>(
    state: &'a State,
    run: &Run,
    source: &EventRecord,
    proof: &mut Proof,
) -> Result<&'a crate::admission_journal::Init, Failure> {
    let record = state
        .admission_executions
        .get(&run.id)
        .ok_or_else(|| unknown("no-effect execution"))?;
    proof.measure(record)?;
    if record.state != crate::AdmissionExecutionState::Unstarted {
        return Err(unknown("no-effect execution started"));
    }
    let Some(Source::Init(init)) = state
        .admission_execution_sources
        .get(&(run.id.clone(), Kind::Init))
    else {
        return Err(unknown("original execution Init"));
    };
    proof.measure(init)?;
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    if state
        .messages
        .iter()
        .filter(|message| {
            message.run_id.as_ref() == Some(&run.id) && message.session_id == run.session_id
        })
        .take(count + 1)
        .count()
        != count
        || state.calls.values().any(|call| call.run_id == run.id)
        || state
            .attempts
            .values()
            .any(|record| record.snapshot.run_id == run.id)
        || state
            .phases
            .values()
            .any(|record| record.snapshot.run_id == run.id)
        || state
            .epochs
            .values()
            .any(|epoch| epoch.run_id == run.id && epoch.id != run.epoch_id)
        || state.events.iter().rev().find(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == EventKind::RunSettled
        }) != Some(source)
    {
        return Err(unknown("contradictory no-effect history"));
    }
    Ok(init)
}
fn view(run: &Run, proof: &mut Proof) -> Result<RunView, Failure> {
    proof.measure(&(
        &run.id,
        &run.session_id,
        &run.owner,
        &run.claim_token,
        &run.config_hash,
        &run.plan_fingerprint,
        &run.usage,
        &run.epoch_id,
        run.created_at,
        run.updated_at,
        run.lease_until,
        run.status,
    ))?;
    Ok(RunView {
        id: run.id.clone(),
        session_id: run.session_id.clone(),
        status: run.status,
        owner: run.owner.clone(),
        claim_token: run.claim_token.clone(),
        lease_until: run.lease_until,
        epoch_id: run.epoch_id.clone(),
        config_hash: run.config_hash.clone(),
        plan_fingerprint: run.plan_fingerprint.clone(),
        usage: run.usage.clone(),
        created_at: run.created_at,
        updated_at: run.updated_at,
        checkpoint_absent: run.checkpoint.is_none(),
        error_absent: run.error.is_none(),
    })
}
fn field<T: DeserializeOwned>(value: &serde_json::Value, key: &str) -> Result<T, Failure> {
    serde_json::from_value(
        value
            .get(key)
            .ok_or_else(|| unknown("original abandonment field"))?
            .clone(),
    )
    .map_err(unknown)
}
fn event(source: &EventRecord) -> Result<Event, Failure> {
    let evidence = source
        .payload
        .get("abandonment_v1")
        .ok_or_else(|| unknown("abandonment source domain"))?;
    let run = evidence
        .get("run")
        .ok_or_else(|| unknown("original abandoned Run fields"))?;
    let mut narrow = serde_json::Map::new();
    for key in [
        "id",
        "session_id",
        "status",
        "owner",
        "claim_token",
        "lease_until",
        "epoch_id",
        "config_hash",
        "plan_fingerprint",
        "usage",
        "created_at",
        "updated_at",
    ] {
        narrow.insert(
            key.into(),
            run.get(key)
                .ok_or_else(|| unknown("original Run field"))?
                .clone(),
        );
    }
    narrow.insert(
        "checkpoint_absent".into(),
        serde_json::json!(run.get("checkpoint").is_none_or(serde_json::Value::is_null)),
    );
    narrow.insert(
        "error_absent".into(),
        serde_json::json!(run.get("error").is_none_or(serde_json::Value::is_null)),
    );
    Ok(Event {
        cursor: source.cursor,
        session_id: source.session_id.clone(),
        run_id: source.run_id.clone(),
        turn_id: source.turn_id.clone(),
        kind: source.kind.clone(),
        correlation: source.correlation.clone(),
        live_only: source.live_only,
        created_at: source.created_at,
        status: field(&source.payload, "status")?,
        usage: field(&source.payload, "usage")?,
        evidence: Evidence {
            request: field(evidence, "request")?,
            run: serde_json::from_value(narrow.into()).map_err(unknown)?,
            no_effect_v1: evidence
                .get("no_effect_v1")
                .map(|value| serde_json::from_value(value.clone()))
                .transpose()
                .map_err(unknown)?,
            interrupted: i64::try_from(
                evidence
                    .get("interrupted_tools")
                    .and_then(serde_json::Value::as_array)
                    .ok_or_else(|| unknown("interrupted tools"))?
                    .len(),
            )
            .map_err(unknown)?,
        },
    })
}
