//! Private append-only Run fencing state. Heartbeats are deliberately absent.
use super::{CompletionSource, Error};
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
    /// Present only on the actual first issuance of an owned admission. It
    /// anchors a separate bounded preparation-to-Run witness, never authority.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub ownership_handoff: Option<InputFingerprint>,
    /// Present only on the actual noninitial claim made under a verified
    /// preparation. Omitted old formats keep their original canonical shape.
    #[serde(default, skip_serializing_if = "Option::is_none")]
    pub completion_source: Option<CompletionSource>,
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
pub(super) fn scope(session: &SessionId, run: &RunId) -> Result<(), Error> {
    if session.0.is_empty() || session.0.len() > 512 || session.0.chars().any(char::is_control) {
        return Err(Error);
    }
    crate::attempt::validate_identifier(&run.0).map_err(|_| Error)
}
impl RunIssuance {
    pub fn validate(&self) -> Result<(), Error> {
        self.state.validate()?;
        if let Some(source) = &self.completion_source {
            source.validate()?;
            if !source.accepts_status(self.state.status) {
                return Err(Error);
            }
        }
        let format = match (
            self.version,
            &self.ownership_handoff,
            &self.completion_source,
        ) {
            (1, None, None) => true,
            (2, Some(_), None) => self.sequence == 1,
            (3, None, Some(_)) => self.sequence > 1,
            _ => false,
        };
        if !format
            || self.sequence == 0
            || self.sequence > i64::MAX as u64
            || crate::snapshot::encoded_bytes(self, 2048).is_none()
        {
            return Err(Error);
        }
        Ok(())
    }
    /// Attach only to an actual newly rotated noninitial claim issuance.
    pub(crate) fn completed(
        mut self,
        previous: &RunState,
        source: CompletionSource,
    ) -> Result<Self, Error> {
        self.validate()?;
        source.validate()?;
        if self.version != 1
            || self.sequence <= 1
            || self.state.token == previous.token
            || self.state.session_id != previous.session_id
            || self.state.run_id != previous.run_id
            || !source.accepts_status(self.state.status)
        {
            return Err(Error);
        }
        self.version = 3;
        self.completion_source = Some(source);
        self.validate()?;
        Ok(self)
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
        ownership_handoff: None,
        completion_source: None,
    }))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn owned_admission_anchor_is_explicit_and_only_valid_on_first_issuance() {
        let state = RunState::new(
            &SessionId::new(),
            &RunId::new(),
            RunStatus::Running,
            "original-admission-token",
        )
        .unwrap();
        let mut issuance = advance(None, state, None).unwrap().unwrap();
        issuance.validate().unwrap();
        let legacy = serde_json::to_value(&issuance).unwrap();
        assert!(legacy.get("ownership_handoff").is_none());
        assert!(legacy.get("completion_source").is_none());
        let decoded: RunIssuance = serde_json::from_value(legacy).unwrap();
        assert!(decoded.ownership_handoff.is_none());

        issuance.ownership_handoff = Some(InputFingerprint::new("a".repeat(64)).unwrap());
        assert!(issuance.validate().is_err());
        issuance.version = 2;
        issuance.validate().unwrap();
        issuance.sequence = 2;
        assert!(issuance.validate().is_err());
        issuance.sequence = 1;
        issuance.ownership_handoff = None;
        assert!(issuance.validate().is_err());
    }
    #[test]
    fn noninitial_completion_is_a_distinct_strict_format() {
        let state = RunState::new(
            &SessionId("\"".repeat(512)),
            &RunId("r".repeat(128)),
            RunStatus::Running,
            "recovered-native-token",
        )
        .unwrap();
        let source = CompletionSource {
            root_join_id: crabber_core::SessionOwnerOperationId("j".repeat(128)),
            root_join_digest: InputFingerprint::new("a".repeat(64)).unwrap(),
            native_operation: super::super::completion::CompletionOperation::AdmissionRecovery,
            native_semantic_digest: InputFingerprint::new("b".repeat(64)).unwrap(),
        };
        let mut issuance = RunIssuance {
            version: 3,
            sequence: 2,
            state,
            ownership_handoff: None,
            completion_source: Some(source),
        };
        issuance.validate().unwrap();
        let original = super::super::issuance_digest(&issuance).unwrap();
        issuance
            .completion_source
            .as_mut()
            .unwrap()
            .native_operation = super::super::completion::CompletionOperation::ExpiredRunResume;
        assert_ne!(super::super::issuance_digest(&issuance).unwrap(), original);
        issuance.sequence = 1;
        assert!(issuance.validate().is_err());
        issuance.sequence = 2;
        issuance.version = 1;
        assert!(issuance.validate().is_err());
        issuance.version = 3;
        issuance.ownership_handoff = Some(InputFingerprint::new("c".repeat(64)).unwrap());
        assert!(issuance.validate().is_err());
        issuance.ownership_handoff = None;
        issuance.completion_source = None;
        assert!(issuance.validate().is_err());
    }
}
