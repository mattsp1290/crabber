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
    if record.receipt.run_id != *run {
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
mod tests {
    use super::*;
    use crate::{MemoryStore, Store};
    use crabber_core::SessionId;
    #[tokio::test]
    async fn execution_sources_reject_rollback_and_missing_new_init() {
        let store = MemoryStore::new();
        let now = time::OffsetDateTime::now_utc();
        let keyed = crate::admission_execution_contract::keyed(&SessionId::new(), now);
        let crate::KeyedAdmitOutcome::Started { receipt, admitted } =
            store.admit_keyed_run(keyed.clone()).await.unwrap()
        else {
            panic!()
        };
        assert_eq!(
            store
                .state
                .lock()
                .unwrap()
                .admission_execution_sources
                .len(),
            1
        );
        let execution = store.execution(admitted.fence).await.unwrap();
        execution.begin_admission_execution().await.unwrap();
        assert_eq!(
            store
                .state
                .lock()
                .unwrap()
                .admission_execution_sources
                .len(),
            2
        );
        assert_eq!(
            execution.begin_admission_execution().await.unwrap_err(),
            crate::AdmissionExecutionError::AlreadyStarted
        );
        store
            .state
            .lock()
            .unwrap()
            .admission_executions
            .get_mut(&receipt.run_id)
            .unwrap()
            .state = crate::AdmissionExecutionState::Unstarted;
        assert!(
            store
                .load_admission_execution(&receipt.session_id, &keyed.options.key)
                .await
                .is_err()
        );
        assert!(execution.begin_admission_execution().await.is_err());
        let mut state = store.state.lock().unwrap();
        state
            .admission_execution_sources
            .remove(&(receipt.run_id.clone(), Kind::Begin));
        state
            .admission_execution_sources
            .remove(&(receipt.run_id.clone(), Kind::Init));
        assert!(verify(&state, &receipt.run_id).is_err());
    }
}
