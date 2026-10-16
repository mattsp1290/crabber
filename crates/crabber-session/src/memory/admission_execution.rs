#[path = "admission_execution/inspection.rs"]
mod inspection;
#[path = "admission_execution/prepared.rs"]
mod prepared;
use super::{MemoryExecution, MemoryStore};
use crate::{
    AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState,
    ClaimUnstartedAdmissionRequest, ClaimedAdmission,
};
use crabber_core::{AdmissionKey, RunFence, SessionId};
pub(super) use inspection::validate_source;

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
    pub(super) fn claim_admission(
        &self,
        claim: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, Error> {
        self.transact(|state| {
            if state.session_owners.has_preparation(&claim.session_id) {
                return Err(Error::PreparationBusy);
            }
            self.claim_admission_transaction(state, claim, None)
        })
    }
    pub(in crate::memory) fn claim_admission_transaction(
        &self,
        state: &mut super::State,
        claim: ClaimUnstartedAdmissionRequest,
        completion: Option<crate::native_parent::CompletionSource>,
    ) -> Result<ClaimedAdmission, Error> {
        super::admission_journal::verify(state, &claim.expected_fence.run_id)
            .map_err(|_| Error::UnknownStoreFailure)?;
        let run = state
            .runs
            .get_mut(&claim.expected_fence.run_id)
            .ok_or(Error::MissingEvidence)?;
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
            .cloned()
            .collect();
        record.validate_messages(run, &messages)?;
        if state
            .receipts
            .get(&(claim.session_id.clone(), claim.key.clone()))
            != Some(&record.receipt)
        {
            return Err(Error::SemanticConflict);
        }
        record.verify_claim(&claim, run)?;
        let now = self.clock.now();
        if run.lease_until > now {
            return Err(Error::LiveLease);
        }
        let lease = time::Duration::try_from(claim.lease).map_err(|_| Error::SemanticConflict)?;
        if lease <= time::Duration::ZERO {
            return Err(Error::SemanticConflict);
        }
        let previous = crate::native_parent::RunState::from_run(run)
            .map_err(|_| Error::UnknownStoreFailure)?;
        run.owner = claim.owner;
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
        run.updated_at = now;
        crate::ensure_storable(run).map_err(|_| Error::UnknownStoreFailure)?;
        let outcome = ClaimedAdmission {
            record: record.clone(),
            run: run.clone(),
            fence: RunFence {
                run_id: run.id.clone(),
                claim_token: run.claim_token.clone(),
            },
        };
        super::native_parent::record_run_states_with_completion(
            state,
            Some(&previous),
            crate::native_parent::RunState::from_run(&outcome.run)
                .map_err(|_| Error::UnknownStoreFailure)?,
            completion,
        )
        .map_err(|_| Error::UnknownStoreFailure)?;
        Ok(outcome)
    }
}

impl MemoryExecution {
    pub(super) fn begin_admission(&self) -> Result<(), Error> {
        self.store.transact(|state| {
            super::admission_journal::verify(state, &self.fence.run_id)
                .map_err(|_| Error::UnknownStoreFailure)?;
            let run = state
                .runs
                .get(&self.fence.run_id)
                .ok_or(Error::MissingEvidence)?;
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
                .cloned()
                .collect();
            record.validate_messages(run, &messages)?;
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
            super::admission_journal::begin(state, &self.fence.run_id)
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
            Ok(())
        })
    }
}

#[cfg(test)]
#[path = "admission_execution/source_tests.rs"]
mod source_tests;
