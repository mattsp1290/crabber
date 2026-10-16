//! Host assertions and Boolean choices are persisted independently of dispatch.
use super::{Error, publish, replay, revision};
use crate::{ApprovalAnswer, ApprovalContinuationState};

impl super::super::MemoryStore {
    pub(in super::super) fn answer_approval_inner(
        &self,
        answer: &ApprovalAnswer,
    ) -> Result<crate::ApprovalReceipt, Error> {
        let digest = answer.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let result = self.transact(|state| {
            if let Some(saved) = replay(
                state,
                &answer.session_id,
                &answer.run_id,
                &answer.id,
                &digest,
            )? {
                return Ok((saved, false));
            }
            let run = state
                .runs
                .get(&answer.run_id)
                .filter(|run| run.session_id == answer.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if revision(state, &run.session_id) != answer.expected_revision {
                return Err(Error::RevisionConflict);
            }
            super::verify_session(state, &run.session_id)?;
            let current = state
                .approval_barriers
                .get(&run.session_id)
                .ok_or(Error::InvalidTransition)?;
            if *current != answer.continuation.id {
                return Err(Error::GenerationConflict);
            }
            let mut record = state
                .approvals
                .get(current)
                .cloned()
                .ok_or(Error::UnknownStoreOutcome)?;
            super::verify_record(state, &run, &record)?;
            if record.batch.continuation != answer.continuation || record.run_id != run.id {
                return Err(Error::GenerationConflict);
            }
            if record.state != ApprovalContinuationState::Waiting
                || run.status != crabber_core::RunStatus::Paused
            {
                return Err(Error::InvalidTransition);
            }
            let outcome = record.accept_answer(answer, self.clock.now())?;
            state.approvals.insert(current.clone(), record);
            let receipt = publish(
                state,
                &run,
                answer.id.clone(),
                digest.clone(),
                outcome,
                self.clock.now(),
            )?;
            super::before_commit(fault)?;
            Ok((receipt, true))
        });
        super::after_commit(result, fault)
    }
}
