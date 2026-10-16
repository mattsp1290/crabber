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
        let receipt =
            super::receipt(&state, session, &record.batch.id)?.ok_or(Error::UnknownStoreOutcome)?;
        crate::continuation_origin::tool(&record.batch, session, run, target, &receipt)
    }
}
