//! Narrow persisted identity and materialization facts, never an execution grant.
use crabber_core::{RunId, RunStatus, SessionId};
use serde::{Deserialize, Serialize};
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunFacts {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub created_at: time::OffsetDateTime,
}
impl RunFacts {
    pub fn validate(&self, session: &SessionId, run: &RunId) -> Result<(), crate::StoreError> {
        if self.session_id != *session || self.run_id != *run {
            return Err(crate::StoreError::AdmissionConflict);
        }
        crate::public_run::validate_session(session)?;
        crate::attempt::validate_identifier(&run.0)
            .map_err(|_| crate::StoreError::AdmissionConflict)
    }
}
