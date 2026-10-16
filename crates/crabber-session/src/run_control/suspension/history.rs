//! Authenticated historical suspension facts without current Run reconstruction.
use super::{
    AttemptCleanup, AttemptReceipt, AttemptSnapshot, AttemptState, Error,
    MAX_SUSPENSION_PROOF_BYTES, RunBoundary, SuspensionProof, unknown,
};
use crabber_core::{EventRecord, RunId, SessionId};
impl SuspensionProof {
    pub(crate) fn history_scope<'a>(
        &'a self,
        session: &SessionId,
        run: &RunId,
        latest: &AttemptSnapshot,
        original: &AttemptReceipt,
        audit: &EventRecord,
        paused: &EventRecord,
    ) -> Result<crate::run_control::terminal::FrozenFacts<'a>, Error> {
        if crate::snapshot::encoded_bytes(self, MAX_SUSPENSION_PROOF_BYTES).is_none()
            || original != &self.receipt
            || original.attempt.session_id != *session
            || original.attempt.run_id != *run
            || latest != &original.attempt
            || latest.state != AttemptState::Suspended
            || latest.cleanup != AttemptCleanup::Complete
        {
            return Err(Error::UnknownStoreOutcome);
        }
        crate::attempt::verify_receipt(original, audit).map_err(unknown)?;
        self.conclusion.verify_replay(original).map_err(unknown)?;
        let RunBoundary::Suspended { checkpoint, usage } = &self.conclusion.boundary else {
            return Err(Error::UnknownStoreOutcome);
        };
        let mut expected =
            crate::attempt_conclusion::materialize_scope(&self.conclusion, session, run)
                .map_err(unknown)?;
        expected.cursor = paused.cursor;
        if expected != *paused
            || paused
                .cursor
                .is_none_or(|cursor| cursor >= original.event_cursor)
            || checkpoint
                .get("version")
                .and_then(serde_json::Value::as_u64)
                != Some(4)
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let value = checkpoint
            .get("source_turn_id")
            .filter(|value| !value.is_null());
        let turn = self
            .conclusion
            .turn_id
            .as_ref()
            .map(serde_json::to_value)
            .transpose()
            .map_err(unknown)?;
        if value != turn.as_ref() {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(crate::run_control::terminal::FrozenFacts {
            usage,
            turn: self.conclusion.turn_id.as_ref(),
        })
    }
}
