//! Host responses commit with the private record, revision, audit and receipt.
use super::super::SqliteStore;
use super::write::{active, publish, replay, save};
use super::{Error, finish, load, sql};
use crate::{ApprovalAnswer, ApprovalReceipt};

impl SqliteStore {
    pub(in super::super) async fn answer_approval_inner(
        &self,
        answer: &ApprovalAnswer,
    ) -> Result<ApprovalReceipt, Error> {
        let digest = answer.fingerprint()?;
        #[cfg(test)]
        let fault = self
            .approval_fault
            .load(std::sync::atomic::Ordering::SeqCst);
        #[cfg(not(test))]
        let fault = 0;
        let (mut tx, run) = self
            .approval_transaction(&answer.session_id, &answer.run_id)
            .await?;
        let result = async {
            if let Some(saved) = replay(&mut tx, &run, &answer.id, &digest).await? {
                return Ok((saved, false));
            }
            let (mut record, revision) = load(&mut tx, &answer.session_id, &answer.run_id)
                .await?
                .ok_or(Error::InvalidTransition)?;
            if revision != answer.expected_revision {
                return Err(Error::RevisionConflict);
            }
            if run.status != crabber_core::RunStatus::Paused {
                return Err(Error::InvalidTransition);
            }
            active(&mut tx, &record).await?;
            let now = self.clock.now();
            let outcome = record.accept_answer(answer, now)?;
            save(&mut tx, &record).await?;
            let receipt = publish(&mut tx, &run, answer.id.clone(), digest, outcome, now).await?;
            sql::before_commit(fault)?;
            Ok((receipt, true))
        }
        .await;
        sql::after_commit(finish(tx, result).await, fault)
    }
}
