//! The compound record retains original external results, not fabricated answers.
use super::{Error, InputFingerprint};
use crate::{
    ToolContinuationOutcome as Outcome, ToolContinuationReceipt,
    tool_continuation::ToolContinuationRecord,
};
impl ToolContinuationRecord {
    pub(crate) fn capture_recovery(
        &mut self,
        request: &crate::ToolContinuationRecovery,
        frontend: Option<&crate::FrontendContinuationRecovery>,
    ) -> Result<(), Error> {
        match frontend {
            Some(request) => {
                self.frontend_recovery = Some(Box::new(FrontendRecoveryRecord::capture(request)?));
            }
            None => self.recovery = Some(request.clone()),
        }
        Ok(())
    }

    /// Descriptor identity only; callers still authenticate its receipt/event.
    pub(crate) fn latest_claim_source(
        &self,
    ) -> Result<(crabber_core::ControlId, InputFingerprint), Error> {
        self.validate_frontend()?;
        if let Some(recovery) = &self.frontend_recovery {
            Ok((recovery.id.clone(), recovery.fingerprint()?))
        } else if let Some(recovery) = &self.recovery {
            Ok((recovery.id.clone(), recovery.fingerprint()?))
        } else {
            Ok((
                self.claim
                    .as_ref()
                    .ok_or(Error::UnknownStoreOutcome)?
                    .id
                    .clone(),
                self.claim_digest()?,
            ))
        }
    }
    pub(crate) fn validate_frontend(&self) -> Result<(), Error> {
        if self.batch.frontend.is_empty() {
            return if self.frontend_claim.is_none() && self.frontend_recovery.is_none() {
                Ok(())
            } else {
                Err(Error::UnknownStoreOutcome)
            };
        }
        let Some(frontend) = &self.frontend_claim else {
            return if self.claim.is_none()
                && self.recovery.is_none()
                && self.frontend_recovery.is_none()
            {
                Ok(())
            } else {
                Err(Error::UnknownStoreOutcome)
            };
        };
        let claim = self.claim.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        if self.recovery.is_some()
            || claim.id != frontend.claim.id
            || claim.fingerprint()? != frontend.claim.fingerprint()?
        {
            return Err(Error::UnknownStoreOutcome);
        }
        frontend
            .validate_for_batch(&self.batch)
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if let Some(recovery) = &self.frontend_recovery {
            if recovery.original_digest != frontend.fingerprint()?
                || recovery.original.id != frontend.claim.id
                || recovery.original.fingerprint()? != frontend.claim.fingerprint()?
                || recovery.attempt != self.batch.next_attempt
            {
                return Err(Error::UnknownStoreOutcome);
            }
            recovery.fingerprint()?;
        }
        Ok(())
    }
    pub(crate) fn claim_digest(&self) -> Result<InputFingerprint, Error> {
        self.validate_frontend()?;
        if let Some(frontend) = &self.frontend_claim {
            frontend.fingerprint()
        } else {
            self.claim
                .as_ref()
                .ok_or(Error::UnknownStoreOutcome)?
                .fingerprint()
        }
    }
    pub(crate) fn verify_claim_receipt(
        &self,
        receipt: &ToolContinuationReceipt,
    ) -> Result<(), Error> {
        let claim = self.claim.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        let attempt = match &receipt.outcome {
            Outcome::Claimed {
                continuation,
                attempt,
            } if self.frontend_claim.is_none() && continuation == &self.batch.continuation => {
                attempt
            }
            Outcome::FrontendClaimed {
                continuation,
                attempt,
            } if self.frontend_claim.is_some() && continuation == &self.batch.continuation => {
                attempt
            }
            _ => return Err(Error::UnknownStoreOutcome),
        };
        if receipt.operation_id != claim.id
            || receipt.semantic_digest != self.claim_digest()?
            || receipt.session_id != self.session_id
            || receipt.run_id != self.run_id
            || attempt.target != self.batch.next_attempt
            || attempt.session_id != self.session_id
            || attempt.run_id != self.run_id
            || attempt.state != crate::AttemptState::Unstarted
            || attempt.cleanup != crate::AttemptCleanup::Pending
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}

/// The authenticated original claim keeps the full output once. A recovery
/// retains only its exact source descriptor and digest, never a second body.
#[derive(Debug, Clone, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct FrontendRecoveryRecord {
    pub id: crabber_core::ControlId,
    pub expected_revision: crabber_core::SessionRevision,
    pub original: crate::ToolContinuationClaim,
    pub original_digest: InputFingerprint,
    pub attempt: crate::AttemptTarget,
    pub owner: String,
    pub lease_ms: u64,
}
impl FrontendRecoveryRecord {
    pub(crate) fn capture(request: &crate::FrontendContinuationRecovery) -> Result<Self, Error> {
        Ok(Self {
            id: request.id.clone(),
            expected_revision: request.expected_revision,
            original: request.original.claim.clone(),
            original_digest: request.original.fingerprint()?,
            attempt: request.attempt.clone(),
            owner: request.owner.clone(),
            lease_ms: request.lease_ms,
        })
    }
    pub(crate) fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        recovery_fingerprint(
            &self.id,
            self.expected_revision,
            &self.original_digest,
            &self.attempt,
            &self.owner,
            self.lease_ms,
        )
    }
    pub(crate) fn verify_receipt(&self, receipt: &ToolContinuationReceipt) -> Result<(), Error> {
        verify_recovery_receipt(
            &self.id,
            &self.fingerprint()?,
            &self.original,
            &self.attempt,
            receipt,
        )
    }
}
pub(crate) fn recovery_fingerprint(
    id: &crabber_core::ControlId,
    revision: crabber_core::SessionRevision,
    original: &InputFingerprint,
    attempt: &crate::AttemptTarget,
    owner: &str,
    lease_ms: u64,
) -> Result<InputFingerprint, Error> {
    use crate::tool_continuation::canonical::identity;
    identity(&id.0, 128)?;
    identity(&attempt.id.0, 128)?;
    identity(owner, 512)?;
    if lease_ms == 0 || lease_ms > 900_000 {
        return Err(Error::InvalidRequest);
    }
    recovery_semantics(id, revision, original, attempt)
}

pub(crate) fn recovery_semantics(
    id: &crabber_core::ControlId,
    revision: crabber_core::SessionRevision,
    original: &InputFingerprint,
    attempt: &crate::AttemptTarget,
) -> Result<InputFingerprint, Error> {
    use crate::tool_continuation::canonical::{digest, identity};
    identity(&id.0, 128)?;
    identity(&attempt.id.0, 128)?;
    digest(&(
        "crabber.frontend-continuation.recover-unstarted.v1",
        id,
        revision,
        original,
        attempt,
    ))
}
pub(crate) fn verify_recovery_receipt(
    id: &crabber_core::ControlId,
    digest: &InputFingerprint,
    original: &crate::ToolContinuationClaim,
    attempt: &crate::AttemptTarget,
    receipt: &ToolContinuationReceipt,
) -> Result<(), Error> {
    if receipt.operation_id != *id
        || receipt.semantic_digest != *digest
        || receipt.session_id != original.session_id
        || receipt.run_id != original.run_id
        || !matches!(&receipt.outcome,Outcome::FrontendReclaimedUnstarted{continuation,original_claim_id,attempt:actual} if continuation==&original.continuation && original_claim_id==&original.id && actual.target==*attempt && actual.session_id==original.session_id && actual.run_id==original.run_id && actual.state==crate::AttemptState::Unstarted && actual.cleanup==crate::AttemptCleanup::Pending)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
