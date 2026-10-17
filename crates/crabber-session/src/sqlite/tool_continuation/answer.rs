use super::{
    Error, SqliteStore, ToolContinuationOutcome, ToolContinuationReceipt, ToolInteractionAnswer,
    after, before, domain, fault, finish, revision, write,
};
impl SqliteStore {
    pub(in super::super) async fn answer_tools(
        &self,
        answer: ToolInteractionAnswer,
    ) -> Result<ToolContinuationReceipt, Error> {
        let digest = answer.fingerprint()?;
        let fault = fault(self);
        let (mut tx, run) = self
            .tool_transaction(&answer.session_id, &answer.run_id)
            .await?;
        let result = async {
            if let Some(saved) = write::replay(&mut tx, &run, &answer.id, &digest).await? {
                return Ok((saved, false));
            }
            if revision(&mut tx, &run.session_id).await? != answer.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = write::active(&mut tx, &run)
                .await?
                .ok_or(Error::InvalidTransition)?;
            let now = self.clock.now();
            record.answer(&answer, now)?;
            write::save(&mut tx, &record, false).await?;
            let receipt = write::publish(
                &mut tx,
                &run,
                answer.id.clone(),
                digest,
                ToolContinuationOutcome::Answered {
                    continuation: answer.continuation.clone(),
                    interaction_id: answer.interaction_id.clone(),
                    state: domain::reply_state(&answer.reply),
                },
                now,
            )
            .await?;
            super::super::continuation_input::accept(
                &mut tx,
                crate::continuation_input::Input::ToolAnswer(&answer),
                crate::continuation_input::Receipt::Tool(&receipt),
            )
            .await
            .map_err(|_| Error::UnknownStoreOutcome)?;
            before(fault)?;
            Ok((receipt, true))
        }
        .await;
        after(finish(tx, result).await, fault)
    }
}
