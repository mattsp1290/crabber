//! Only the original bounded batch and protected suspension receipt participate.
use super::{Error, MemoryStore};
use crabber_core::{InputFingerprint, RunId, SessionId};
impl MemoryStore {
    pub(in super::super) fn read_original_tool_plan(
        &self,
        session: &SessionId,
        run: &RunId,
        target: &crate::ContinuationTarget,
    ) -> Result<InputFingerprint, Error> {
        crate::continuation_origin::validate_scope(session, run, target)
            .map_err(|()| Error::InvalidRequest)?;
        let state = self.state.lock().unwrap();
        let record = state
            .tool_continuations
            .get(&target.id)
            .ok_or(Error::UnknownStoreOutcome)?;
        if record.version != 1 || record.session_id != *session || record.run_id != *run {
            return Err(Error::UnknownStoreOutcome);
        }
        // Preflight before fingerprint canonicalization or any private-body clone.
        if crate::snapshot::encoded_bytes(&record.batch, 4 * 1024 * 1024).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        let mut proof = crate::public_session::v2::proof::Proof::bounded(32 << 20);
        proof
            .measure(&record.batch)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        let receipt = super::receipt_with_proof(&state, session, &record.batch.id, &mut proof)
            .map_err(|e| e.into_domain_or(Error::UnknownStoreOutcome))?
            .ok_or(Error::UnknownStoreOutcome)?;
        let event = state
            .events
            .get(receipt.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?;
        state
            .continuation_inputs
            .verify(
                crate::continuation_input::Input::ToolSuspension(&record.batch),
                crate::continuation_input::Receipt::Tool(&receipt),
                event,
                &mut proof,
            )
            .map_err(|_| Error::UnknownStoreOutcome)?;
        crate::continuation_origin::tool(&record.batch, session, run, target, &receipt)
    }
}
