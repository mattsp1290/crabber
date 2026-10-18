#[path = "admission_execution/inspection.rs"]
mod inspection;
#[path = "admission_execution/prepared.rs"]
mod prepared;
#[path = "admission_execution/recovery.rs"]
mod recovery;
use super::{MemoryExecution, MemoryStore};
use crate::{AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState};
use crabber_core::{AdmissionKey, SessionId};
pub(super) use inspection::{validate_source, validate_source_with_proof};

impl MemoryStore {
    pub(super) fn load_execution_record(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Result<Option<AdmissionExecutionRecord>, crate::StoreError> {
        let state = self.state.lock().expect("memory store poisoned");
        let Some(receipt) = state.receipts.get(&(session.clone(), key.clone())) else {
            return Ok(None);
        };
        super::admission_journal::verify(&state, &receipt.run_id)?;
        Ok(state.admission_executions.get(&receipt.run_id).cloned())
    }
}

fn require_raw_origin(
    state: &super::State,
    run: &crabber_core::Run,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), Error> {
    let enrollment = super::native_enrollment::original(state, &run.session_id, &run.id, proof)
        .map_err(|_| Error::UnknownStoreFailure)?;
    crate::native_enrollment::access::allow(enrollment).map_err(|_| Error::Unsupported)
}

impl MemoryExecution {
    pub(super) fn begin_admission(&self) -> Result<(), Error> {
        self.begin_admission_with_proof(&mut crate::public_session::v2::proof::Proof::bounded(
            32 << 20,
        ))
    }
    pub(super) fn begin_admission_with_proof(
        &self,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), Error> {
        proof
            .measure(&self.fence)
            .map_err(|_| Error::UnknownStoreFailure)?;
        let result = self.store.transact(|state| {
            super::admission_journal::verify_with_proof(state, &self.fence.run_id, proof)
                .map_err(|_| Error::UnknownStoreFailure)?;
            let run = state
                .runs
                .get(&self.fence.run_id)
                .ok_or(Error::MissingEvidence)?;
            proof.measure(run).map_err(|_| Error::UnknownStoreFailure)?;
            require_raw_origin(state, run, proof)?;
            let run = run.clone();
            if run.claim_token != self.fence.claim_token {
                return Err(Error::StaleOwner);
            }
            if run.status.is_terminal() {
                return Err(Error::AlreadyTerminal);
            }
            if run.lease_until <= self.store.clock.now() {
                return Err(Error::StaleOwner);
            }
            let record = state
                .admission_executions
                .get(&run.id)
                .ok_or(Error::MissingEvidence)?;
            let count = record
                .capsule
                .input_envelope
                .as_ref()
                .map_or(1, |input| input.messages().len());
            let messages: Vec<_> = state
                .messages
                .iter()
                .filter(|message| {
                    message.run_id.as_ref() == Some(&run.id) && message.session_id == run.session_id
                })
                .take(count)
                .map(|message| {
                    proof
                        .measure(message)
                        .map_err(|_| Error::UnknownStoreFailure)?;
                    Ok(message.clone())
                })
                .collect::<Result<Vec<_>, Error>>()?;
            record.validate_messages(&run, &messages)?;
            if state
                .receipts
                .get(&(run.session_id.clone(), record.key.clone()))
                != Some(&record.receipt)
            {
                return Err(Error::SemanticConflict);
            }
            if record.state != AdmissionExecutionState::Unstarted {
                return Err(Error::AlreadyStarted);
            }
            let sources = super::admission_journal::begin_captured_with_proof(
                state,
                &self.fence.run_id,
                proof,
            )
            .map_err(|_| Error::UnknownStoreFailure)?;
            let record = state
                .admission_executions
                .get_mut(&self.fence.run_id)
                .ok_or(Error::MissingEvidence)?;
            if record.receipt.run_id != self.fence.run_id {
                return Err(Error::SemanticConflict);
            }
            if record.state != AdmissionExecutionState::Unstarted {
                return Err(Error::AlreadyStarted);
            }
            record.state = AdmissionExecutionState::Started;
            super::native_enrollment::record_raw_native(
                state,
                crate::native_enrollment::caller::Caller::Raw,
                &crate::native_enrollment::caller::Fresh::AdmissionBegin(
                    crate::native_enrollment::caller::AdmissionBegin {
                        fence: &self.fence,
                        run: &run,
                        record: state
                            .admission_executions
                            .get(&run.id)
                            .ok_or(Error::MissingEvidence)?,
                        sources: sources.as_ref().map(|(init, begin)| (init, begin)),
                    },
                ),
                proof,
                #[cfg(test)]
                &self.store.native_raw_probe,
            )
            .map_err(|_| Error::UnknownStoreFailure)?;
            Ok(())
        });
        #[cfg(test)]
        if result.is_ok() && self.store.native_raw_probe.after_begin(&self.fence.run_id) {
            return Err(Error::UnknownStoreFailure);
        }
        result
    }
}

#[cfg(test)]
#[path = "admission_execution/source_tests.rs"]
mod source_tests;
