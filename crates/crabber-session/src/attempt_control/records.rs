use crate::{
    AttemptCleanup, AttemptCommand, AttemptFinish, AttemptMutation, AttemptSnapshot, AttemptState,
    AttemptTarget, attempt::AttemptRecord,
};
use crate::{
    AttemptError, AttemptReceipt, AttemptRetirement, AttemptRetirementOutcome, AttemptUncertainty,
    AttemptUncertaintyCause,
};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionId, SessionRevision};
use crabber_core::{EventKind, EventRecord, Run, RunStatus};
use serde::{Deserialize, Serialize};
use sha2::{Digest, Sha256};
use time::OffsetDateTime;
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) enum Control {
    Uncertain(AttemptUncertainty),
    Retire(AttemptRetirement),
}
impl Control {
    pub(crate) fn scope(
        &self,
    ) -> (
        &SessionId,
        &RunId,
        &ControlId,
        SessionRevision,
        &AttemptTarget,
    ) {
        match self {
            Self::Uncertain(r) => (
                &r.session_id,
                &r.run_id,
                &r.id,
                r.expected_revision,
                &r.target,
            ),
            Self::Retire(r) => (
                &r.session_id,
                &r.run_id,
                &r.id,
                r.expected_revision,
                &r.target,
            ),
        }
    }
    pub(crate) fn mutation(&self) -> AttemptMutation {
        let (_, _, id, expected_revision, target) = self.scope();
        AttemptMutation {
            id: id.clone(),
            expected_revision,
            command: AttemptCommand::Finish {
                target: target.clone(),
                outcome: match self {
                    Self::Uncertain(_) => AttemptFinish::OutcomeUnknown,
                    Self::Retire(r) => match r.outcome {
                        AttemptRetirementOutcome::Failed => AttemptFinish::Failed,
                        AttemptRetirementOutcome::Interrupted => AttemptFinish::Interrupted,
                    },
                },
            },
        }
    }
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, AttemptError> {
        let (session, run, _, _, _) = self.scope();
        self.mutation().fingerprint(session, run)?;
        let claim = match self {
            Self::Uncertain(AttemptUncertainty {
                cause: AttemptUncertaintyCause::HostStopped { claim },
                ..
            }) => Some(claim),
            Self::Retire(r) => Some(&r.joined),
            Self::Uncertain(_) => None,
        };
        if let Some(claim) = claim {
            for (text, max) in [(&claim.resolver_id, 512), (&claim.authority_claim, 4096)] {
                if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
                    return Err(AttemptError::InvalidRequest);
                }
            }
        }
        let domain = match self {
            Self::Uncertain(_) => "crabber.attempt-uncertainty.v1",
            Self::Retire(_) => "crabber.attempt-retirement.v1",
        };
        let mut value = serde_json::json!([domain, self]);
        value.sort_all_objects();
        InputFingerprint::new(format!(
            "{:x}",
            Sha256::digest(value.to_string().as_bytes())
        ))
        .map_err(|_| AttemptError::InvalidRequest)
    }
    pub(crate) fn apply(
        &self,
        run: &mut Run,
        record: &mut AttemptRecord,
        revision: SessionRevision,
        now: OffsetDateTime,
    ) -> Result<(), AttemptError> {
        let (session, id, _, expected, target) = self.scope();
        record.validate()?;
        if run.session_id != *session
            || run.id != *id
            || record.snapshot.session_id != *session
            || record.snapshot.run_id != *id
            || record.snapshot.target != *target
        {
            return Err(AttemptError::NotFound);
        }
        if revision != expected {
            return Err(AttemptError::RevisionConflict);
        }
        if run.status.is_terminal() {
            return Err(AttemptError::InvalidTransition);
        }
        match self {
            Self::Uncertain(r) => {
                if record.snapshot.state != AttemptState::Started {
                    return Err(AttemptError::InvalidTransition);
                }
                if matches!(r.cause, AttemptUncertaintyCause::ExpiredOrRevokedOwner)
                    && record.lease_until > now
                    && record.run_token == run.claim_token
                {
                    return Err(AttemptError::LiveOwner);
                }
                record.snapshot.state = AttemptState::OutcomeUnknown;
            }
            Self::Retire(r) => {
                if record.snapshot.state != AttemptState::OutcomeUnknown {
                    return Err(AttemptError::InvalidTransition);
                }
                record.snapshot.state = match r.outcome {
                    AttemptRetirementOutcome::Failed => AttemptState::Failed,
                    AttemptRetirementOutcome::Interrupted => AttemptState::Interrupted,
                };
                record.snapshot.cleanup = AttemptCleanup::Complete;
                run.status = match r.outcome {
                    AttemptRetirementOutcome::Failed => RunStatus::Failed,
                    AttemptRetirementOutcome::Interrupted => RunStatus::Interrupted,
                };
                run.error = Some(crate::attempt_control::terminal::ERROR.into());
            }
        }
        run.claim_token = uuid::Uuid::new_v4().to_string();
        run.lease_until = now;
        run.updated_at = now;
        record.token = uuid::Uuid::new_v4().to_string();
        // Preserve the original parent token so child reconciliation observes
        // revocation; never fabricate fresh worker ownership.
        record.validate()?;
        crate::ensure_storable(run).map_err(|_| AttemptError::UnknownStoreOutcome)
    }
    pub(crate) fn event(
        &self,
        run: &Run,
        snapshot: &AttemptSnapshot,
        now: OffsetDateTime,
    ) -> Result<EventRecord, AttemptError> {
        Ok(EventRecord {
            cursor: None,
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            turn_id: None,
            kind: EventKind::ExecutionAttemptChanged,
            payload: serde_json::to_value(Audit {
                version: 3,
                request: self.clone(),
                attempt: snapshot.clone(),
            })
            .map_err(|_| AttemptError::InvalidRequest)?,
            correlation: Some(self.scope().2.0.clone()),
            live_only: false,
            created_at: now,
        })
    }
}
#[derive(Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
struct Audit {
    version: u32,
    request: Control,
    attempt: AttemptSnapshot,
}
pub(crate) fn verify_control(
    receipt: &AttemptReceipt,
    event: &EventRecord,
) -> Result<(), AttemptError> {
    let audit: Audit = serde_json::from_value(event.payload.clone())
        .map_err(|_| AttemptError::UnknownStoreOutcome)?;
    let (session, run, id, expected, target) = audit.request.scope();
    if audit.version != 3
        || audit.attempt != receipt.attempt
        || receipt.operation_id != *id
        || receipt.attempt.session_id != *session
        || receipt.attempt.run_id != *run
        || receipt.attempt.target != *target
        || receipt.revision <= expected
        || receipt.semantic_digest
            != audit
                .request
                .fingerprint()
                .map_err(|_| AttemptError::UnknownStoreOutcome)?
    {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    let valid = match audit.request {
        Control::Uncertain(_) => {
            receipt.attempt.state == AttemptState::OutcomeUnknown
                && receipt.attempt.cleanup == AttemptCleanup::Pending
                && audit.request.scope().3.next().ok() == Some(receipt.revision)
        }
        Control::Retire(r) => {
            receipt.attempt.cleanup == AttemptCleanup::Complete
                && receipt.attempt.state
                    == match r.outcome {
                        AttemptRetirementOutcome::Failed => AttemptState::Failed,
                        AttemptRetirementOutcome::Interrupted => AttemptState::Interrupted,
                    }
        }
    };
    if !valid {
        return Err(AttemptError::UnknownStoreOutcome);
    }
    Ok(())
}
