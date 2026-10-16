//! Private append-only Run fencing state. Heartbeats are deliberately absent.
use super::Error;
use crabber_core::{InputFingerprint, Run, RunId, RunStatus, SessionId};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunState {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub token: InputFingerprint,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct RunIssuance {
    pub version: u32,
    pub sequence: u64,
    pub state: RunState,
}
impl RunState {
    pub fn from_run(run: &Run) -> Result<Self, Error> {
        Self::new(&run.session_id, &run.id, run.status, &run.claim_token)
    }
    pub fn new(
        session: &SessionId,
        run: &RunId,
        status: RunStatus,
        token: &str,
    ) -> Result<Self, Error> {
        scope(session, run)?;
        if token.is_empty() || token.len() > 512 || !token.bytes().all(|b| b.is_ascii_graphic()) {
            return Err(Error);
        }
        let source = ("crabber.native-run-fence.v1", session, run, token);
        let token = InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(serde_json::to_vec(&source).map_err(|_| Error)?)
        ))
        .map_err(|_| Error)?;
        Ok(Self {
            session_id: session.clone(),
            run_id: run.clone(),
            status,
            token,
        })
    }
    fn validate(&self) -> Result<(), Error> {
        scope(&self.session_id, &self.run_id)
    }
}
fn scope(session: &SessionId, run: &RunId) -> Result<(), Error> {
    if session.0.is_empty() || session.0.len() > 512 || session.0.chars().any(char::is_control) {
        return Err(Error);
    }
    crate::attempt::validate_identifier(&run.0).map_err(|_| Error)
}
impl RunIssuance {
    pub fn validate(&self) -> Result<(), Error> {
        self.state.validate()?;
        if self.version != 1
            || self.sequence == 0
            || self.sequence > i64::MAX as u64
            || crate::snapshot::encoded_bytes(self, 2048).is_none()
        {
            return Err(Error);
        }
        Ok(())
    }
    pub fn verify(&self, run: &Run) -> Result<(), Error> {
        self.validate()?;
        if self.state != RunState::from_run(run)? {
            return Err(Error);
        }
        Ok(())
    }
}
/// Called only at an actual Run write, before publication. An existing anchor
/// authenticates the previous state first; a stale restored token cannot renew
/// or save itself into a fresh source. Legacy metadata writes remain unanchored.
pub(crate) fn advance(
    previous: Option<&RunState>,
    next: RunState,
    latest: Option<&RunIssuance>,
) -> Result<Option<RunIssuance>, Error> {
    next.validate()?;
    if let Some(previous) = previous {
        previous.validate()?;
        if previous.session_id != next.session_id || previous.run_id != next.run_id {
            return Err(Error);
        }
    }
    if let Some(latest) = latest {
        latest.validate()?;
        if Some(&latest.state) != previous {
            return Err(Error);
        }
        if latest.state == next {
            return Ok(None);
        }
    } else if let Some(previous) = previous
        && previous.token == next.token
    {
        return Ok(None);
    }
    let sequence = latest.map_or(Ok(1), |old| {
        old.sequence
            .checked_add(1)
            .filter(|n| i64::try_from(*n).is_ok())
            .ok_or(Error)
    })?;
    Ok(Some(RunIssuance {
        version: 1,
        sequence,
        state: next,
    }))
}
