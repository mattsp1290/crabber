//! Memory counterpart of the immutable execution source relation.
use super::State;
use crate::admission_journal::{self, Begin, Init, Kind, Source};
use crate::native_parent::RunIssuance;
use crate::{AdmissionExecutionRecord, StoreError};
use crabber_core::{Run, RunId};
fn invalid() -> StoreError {
    StoreError::AdmissionConflict
}
fn issuer<'a>(
    state: &'a State,
    run: &Run,
    sequence: Option<u64>,
) -> Result<&'a RunIssuance, StoreError> {
    let range = (run.session_id.clone(), run.id.clone(), 0)
        ..=(run.session_id.clone(), run.id.clone(), i64::MAX as u64);
    let value = if let Some(sequence) = sequence {
        state
            .native_run_issuances
            .get(&(run.session_id.clone(), run.id.clone(), sequence))
    } else {
        state
            .native_run_issuances
            .range(range)
            .next_back()
            .map(|(_, v)| v)
    }
    .ok_or_else(invalid)?;
    value.validate().map_err(|_| invalid())?;
    if value.state.session_id != run.session_id
        || value.state.run_id != run.id
        || sequence.is_some_and(|s| s != value.sequence)
    {
        return Err(invalid());
    }
    Ok(value)
}
pub(super) fn init(
    state: &mut State,
    record: &AdmissionExecutionRecord,
    run: &Run,
) -> Result<(), StoreError> {
    let issuance = issuer(state, run, Some(1))?.clone();
    issuance.verify(run).map_err(|_| invalid())?;
    let source = Source::Init(Init::new(record, issuance)?);
    source.encode()?;
    if !state.admission_execution_protocols.insert(run.id.clone())
        || state
            .admission_execution_sources
            .insert((run.id.clone(), Kind::Init), source)
            .is_some()
    {
        return Err(invalid());
    }
    Ok(())
}
pub(super) fn verify(state: &State, run: &RunId) -> Result<(), StoreError> {
    let init = state
        .admission_execution_sources
        .get(&(run.clone(), Kind::Init));
    let begin = state
        .admission_execution_sources
        .get(&(run.clone(), Kind::Begin));
    let protocol = state.admission_execution_protocols.contains(run);
    let Some(record) = state.admission_executions.get(run) else {
        return if !protocol && init.is_none() && begin.is_none() {
            Ok(())
        } else {
            Err(invalid())
        };
    };
    let actual = state.runs.get(run).ok_or_else(invalid)?;
    if record.receipt.session_id != actual.session_id || record.receipt.run_id != *run {
        return Err(invalid());
    }
    admission_journal::verify(record, protocol, init, begin)?;
    for source in [init, begin].into_iter().flatten() {
        source.encode()?;
        let issuance = source.issuance();
        if issuance.state.session_id != record.receipt.session_id
            || issuance.state.run_id != *run
            || state.native_run_issuances.get(&(
                record.receipt.session_id.clone(),
                run.clone(),
                issuance.sequence,
            )) != Some(issuance)
        {
            return Err(invalid());
        }
    }
    Ok(())
}
pub(super) fn begin(state: &mut State, id: &RunId) -> Result<(), StoreError> {
    verify(state, id)?;
    let run = state.runs.get(id).ok_or_else(invalid)?;
    if state.admission_execution_protocols.contains(&run.id) {
        let Some(Source::Init(init)) = state
            .admission_execution_sources
            .get(&(run.id.clone(), Kind::Init))
        else {
            return Err(invalid());
        };
        let issuance = issuer(state, run, None)?;
        issuance.verify(run).map_err(|_| invalid())?;
        let source = Source::Begin(Begin::new(init, issuance.clone())?);
        source.encode()?;
        if state
            .admission_execution_sources
            .insert((run.id.clone(), Kind::Begin), source)
            .is_some()
        {
            return Err(invalid());
        }
    }
    Ok(())
}

#[cfg(test)]
#[path = "admission_journal/tests.rs"]
mod tests;

pub(super) fn no_effect(
    state: &State,
    run: &Run,
    request: &crabber_core::AbandonRequest,
) -> Result<Option<admission_journal::NoEffect>, StoreError> {
    verify(state, &run.id)?;
    let Some(record) = state.admission_executions.get(&run.id) else {
        return Ok(None);
    };
    if !state.admission_execution_protocols.contains(&run.id)
        || record.state != crate::AdmissionExecutionState::Unstarted
    {
        return Ok(None);
    }
    let Some(Source::Init(init)) = state
        .admission_execution_sources
        .get(&(run.id.clone(), Kind::Init))
    else {
        return Err(invalid());
    };
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let mut messages = Vec::with_capacity(count);
    let mut bytes = 0usize;
    for message in state
        .messages
        .iter()
        .filter(|m| m.run_id.as_ref() == Some(&run.id) && m.session_id == run.session_id)
        .take(count + 1)
    {
        if messages.len() == count {
            return Err(invalid());
        }
        bytes = bytes
            .checked_add(
                crate::snapshot::encoded_bytes(message, 16 * 1024 * 1024 - bytes)
                    .ok_or_else(invalid)?,
            )
            .ok_or_else(invalid)?;
        messages.push(message.clone());
    }
    record
        .validate_messages(run, &messages)
        .map_err(|_| invalid())?;
    if state
        .receipts
        .get(&(run.session_id.clone(), record.key.clone()))
        != Some(&record.receipt)
        || run.checkpoint.is_some()
        || run.error.is_some()
        || run.usage != crabber_core::Usage::default()
        || state.events.iter().any(|e| e.run_id == run.id)
        || state.calls.values().any(|c| c.run_id == run.id)
        || state
            .epochs
            .values()
            .any(|e| e.run_id == run.id && e.id != run.epoch_id)
    {
        return Err(invalid());
    }
    let issuance = issuer(state, run, None)?;
    issuance.verify(run).map_err(|_| invalid())?;
    let value = admission_journal::NoEffect::new(init, issuance.clone())?;
    value.verify(init, request)?;
    Ok(Some(value))
}
