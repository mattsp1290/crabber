use super::{MemoryExecution, MemoryStore};
use crate::{
    AdmissionExecutionError as Error, AdmissionExecutionRecord, AdmissionExecutionState,
    ClaimUnstartedAdmissionRequest, ClaimedAdmission,
};
use crabber_core::{AdmissionKey, RunFence, SessionId};

impl MemoryStore {
    pub(super) fn load_execution_record(
        &self,
        session: &SessionId,
        key: &AdmissionKey,
    ) -> Option<AdmissionExecutionRecord> {
        let state = self.state.lock().expect("memory store poisoned");
        let receipt = state.receipts.get(&(session.clone(), key.clone()))?;
        state.admission_executions.get(&receipt.run_id).cloned()
    }
    pub(super) fn claim_admission(
        &self,
        claim: ClaimUnstartedAdmissionRequest,
    ) -> Result<ClaimedAdmission, Error> {
        self.transact(|state| {
            let run = state
                .runs
                .get_mut(&claim.expected_fence.run_id)
                .ok_or(Error::MissingEvidence)?;
            let record = state
                .admission_executions
                .get(&run.id)
                .ok_or(Error::MissingEvidence)?;
            let user = state
                .messages
                .iter()
                .find(|m| m.id == record.receipt.user_message_id)
                .ok_or(Error::SemanticConflict)?;
            record.validate_binding(run, user)?;
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
            let lease =
                time::Duration::try_from(claim.lease).map_err(|_| Error::SemanticConflict)?;
            if lease <= time::Duration::ZERO {
                return Err(Error::SemanticConflict);
            }
            run.owner = claim.owner;
            run.claim_token = uuid::Uuid::new_v4().to_string();
            run.lease_until = now.checked_add(lease).ok_or(Error::SemanticConflict)?;
            run.updated_at = now;
            crate::ensure_storable(run).map_err(|_| Error::UnknownStoreFailure)?;
            Ok(ClaimedAdmission {
                record: record.clone(),
                run: run.clone(),
                fence: RunFence {
                    run_id: run.id.clone(),
                    claim_token: run.claim_token.clone(),
                },
            })
        })
    }
}

impl MemoryExecution {
    pub(super) fn begin_admission(&self) -> Result<(), Error> {
        self.store.transact(|state| {
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
            let user = state
                .messages
                .iter()
                .find(|m| m.id == record.receipt.user_message_id)
                .ok_or(Error::SemanticConflict)?;
            record.validate_binding(run, user)?;
            if state
                .receipts
                .get(&(run.session_id.clone(), record.key.clone()))
                != Some(&record.receipt)
            {
                return Err(Error::SemanticConflict);
            }
            let record = state
                .admission_executions
                .get_mut(&run.id)
                .ok_or(Error::MissingEvidence)?;
            if record.receipt.run_id != run.id || record.receipt.session_id != run.session_id {
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
