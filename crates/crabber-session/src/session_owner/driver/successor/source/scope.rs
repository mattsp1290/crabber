//! Borrowed selection scope and private status ancestry, never a claim locator.
use super::super::Error;
use crate::native_parent::RunIssuance;
use crabber_core::{RunId, RunStatus, SessionId};
#[derive(Clone, Copy)]
pub(crate) struct Scope<'a> {
    pub session: &'a SessionId,
    pub run: &'a RunId,
}
pub(crate) fn status_only(origin: &RunIssuance, later: &RunIssuance) -> Result<(), Error> {
    later.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    if later.version != 1
        || later.completion_source.is_some()
        || later.ownership_handoff.is_some()
        || later.state.session_id != origin.state.session_id
        || later.state.run_id != origin.state.run_id
        || later.state.token != origin.state.token
        || later.state.status != RunStatus::Running
    {
        return Err(Error::StaleOwner);
    }
    Ok(())
}
