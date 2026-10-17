use super::{
    ControlId, Error, MemoryStore, RunId, SessionId, ToolContinuationOutcome,
    ToolContinuationReceipt, ToolContinuationSnapshot, ToolInteractionAnswer, after, before,
    domain, fault, publish, receipt, replay, revision,
};
impl MemoryStore {
    pub(in super::super) fn tool_receipt(
        &self,
        session: &SessionId,
        id: &ControlId,
    ) -> Result<Option<ToolContinuationReceipt>, Error> {
        receipt(
            &self.state.lock().expect("memory store poisoned"),
            session,
            id,
        )
    }
    pub(in super::super) fn read_tools(
        &self,
        session: &SessionId,
        run: &RunId,
    ) -> Result<Option<ToolContinuationSnapshot>, Error> {
        let state = self.state.lock().expect("memory store poisoned");
        let stored = state
            .runs
            .get(run)
            .filter(|r| r.session_id == *session)
            .ok_or(Error::NotFound)?;
        let (latest, _) = super::scan::run(&state, stored)?;
        Ok(latest.map(|record| record.snapshot(revision(&state, session))))
    }
    pub(in super::super) fn answer_tools(
        &self,
        answer: &ToolInteractionAnswer,
    ) -> Result<ToolContinuationReceipt, Error> {
        let digest = answer.fingerprint()?;
        let fault = fault(self);
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
                .filter(|r| r.session_id == answer.session_id)
                .cloned()
                .ok_or(Error::NotFound)?;
            if revision(state, &run.session_id) != answer.expected_revision {
                return Err(Error::RevisionConflict);
            }
            let mut record = super::active(state, &run)?.ok_or(Error::InvalidTransition)?;
            if record.run_id != run.id {
                return Err(Error::NotFound);
            }
            let now = self.clock.now();
            record.answer(answer, now)?;
            let outcome = ToolContinuationOutcome::Answered {
                continuation: answer.continuation.clone(),
                interaction_id: answer.interaction_id.clone(),
                state: domain::reply_state(&answer.reply),
            };
            state
                .tool_continuations
                .insert(record.batch.continuation.id.clone(), record);
            let receipt = publish(state, &run, answer.id.clone(), digest, outcome, now)?;
            super::super::continuation_input::accept(
                state,
                crate::continuation_input::Input::ToolAnswer(answer),
                crate::continuation_input::Receipt::Tool(&receipt),
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
            before(fault)?;
            Ok((receipt, true))
        });
        after(result, fault)
    }
}
