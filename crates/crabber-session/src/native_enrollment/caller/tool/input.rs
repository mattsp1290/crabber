//! Full actual compound controls, including frontend resolver/results.
use crate::native_parent::CompletionOperation;
use crate::public_session::v2::proof::{Failure, Proof};
use crate::{
    FrontendContinuationClaim, FrontendContinuationRecovery, ToolContinuationClaim,
    ToolContinuationRecovery,
};
use crabber_core::{ControlId, InputFingerprint};
#[derive(Clone, Copy)]
pub(crate) enum ToolInput<'a> {
    Claim(&'a ToolContinuationClaim),
    FrontendClaim(&'a FrontendContinuationClaim),
    Recovery(&'a ToolContinuationRecovery),
    FrontendRecovery(&'a FrontendContinuationRecovery),
}
impl<'a> ToolInput<'a> {
    pub(crate) fn prepared(
        operation: &'a crate::session_owner::preparation::PreparedOperation,
    ) -> Option<Self> {
        use crate::session_owner::preparation::PreparedOperation as Op;
        match operation {
            Op::ToolClaim(r) => Some(Self::Claim(r)),
            Op::ToolRecovery(r) => Some(Self::Recovery(r)),
            Op::FrontendClaim(r) => Some(Self::FrontendClaim(r)),
            Op::FrontendRecovery(r) => Some(Self::FrontendRecovery(r)),
            _ => None,
        }
    }
    pub(crate) fn original(self) -> &'a ToolContinuationClaim {
        match self {
            Self::Claim(r) => r,
            Self::FrontendClaim(r) => &r.claim,
            Self::Recovery(r) => &r.original,
            Self::FrontendRecovery(r) => &r.original.claim,
        }
    }
    pub(crate) fn claiming(
        self,
    ) -> Result<
        (
            &'a ToolContinuationClaim,
            Option<&'a FrontendContinuationClaim>,
        ),
        crate::ToolContinuationError,
    > {
        match self {
            Self::Claim(r) => Ok((r, None)),
            Self::FrontendClaim(r) => Ok((&r.claim, Some(r))),
            _ => Err(crate::ToolContinuationError::InvalidRequest),
        }
    }
    pub(crate) fn recovering(
        self,
        proof: &mut Proof,
    ) -> Result<
        (
            std::borrow::Cow<'a, ToolContinuationRecovery>,
            Option<&'a FrontendContinuationRecovery>,
        ),
        crate::ToolContinuationError,
    > {
        match self {
            Self::Recovery(r) => Ok((std::borrow::Cow::Borrowed(r), None)),
            Self::FrontendRecovery(r) => {
                proof
                    .measure(&(&r.original.claim, &r.attempt, &r.owner))
                    .map_err(|_| crate::ToolContinuationError::UnknownStoreOutcome)?;
                Ok((
                    std::borrow::Cow::Owned(
                        crate::tool_continuation::canonical::frontend_recovery(r),
                    ),
                    Some(r),
                ))
            }
            _ => Err(crate::ToolContinuationError::InvalidRequest),
        }
    }
    pub(crate) fn id(self) -> &'a ControlId {
        match self {
            Self::Claim(r) => &r.id,
            Self::FrontendClaim(r) => &r.claim.id,
            Self::Recovery(r) => &r.id,
            Self::FrontendRecovery(r) => &r.id,
        }
    }
    pub(crate) fn frontend(self) -> Option<&'a FrontendContinuationClaim> {
        match self {
            Self::FrontendClaim(r) => Some(r),
            Self::FrontendRecovery(r) => Some(&r.original),
            _ => None,
        }
    }
    pub(crate) fn target(self) -> Option<&'a crate::AttemptTarget> {
        match self {
            Self::Recovery(r) => Some(&r.attempt),
            Self::FrontendRecovery(r) => Some(&r.attempt),
            _ => None,
        }
    }
    pub(crate) fn bookkeeping(self) -> (&'a str, u64) {
        match self {
            Self::Claim(r) => (&r.owner, r.lease_ms),
            Self::FrontendClaim(r) => (&r.claim.owner, r.claim.lease_ms),
            Self::Recovery(r) => (&r.owner, r.lease_ms),
            Self::FrontendRecovery(r) => (&r.owner, r.lease_ms),
        }
    }
    fn revision(self) -> crabber_core::SessionRevision {
        match self {
            Self::Claim(r) => r.expected_revision,
            Self::FrontendClaim(r) => r.claim.expected_revision,
            Self::Recovery(r) => r.expected_revision,
            Self::FrontendRecovery(r) => r.expected_revision,
        }
    }
    pub(crate) fn completion(
        self,
        preparation: Option<&crate::session_owner::preparation::proof::VerifiedPreparation<'_>>,
        digest: &InputFingerprint,
        proof: &mut Proof,
    ) -> Result<
        (
            crabber_core::SessionRevision,
            Option<crate::native_parent::CompletionSource>,
        ),
        crate::ToolContinuationError,
    > {
        proof
            .measure(self.id())
            .map_err(|_| crate::ToolContinuationError::UnknownStoreOutcome)?;
        crate::native_parent::CompletionSource::control(
            preparation,
            self.operation(),
            &crate::session_owner::preparation::PreparationKey::ToolContinuation(self.id().clone()),
            (&self.original().session_id, &self.original().run_id),
            self.revision(),
            digest,
        )
        .map_err(|_| crate::ToolContinuationError::SemanticConflict)
    }
    pub(crate) fn operation(self) -> CompletionOperation {
        match self {
            Self::Claim(_) => CompletionOperation::ToolClaim,
            Self::FrontendClaim(_) => CompletionOperation::FrontendClaim,
            Self::Recovery(_) => CompletionOperation::ToolRecovery,
            Self::FrontendRecovery(_) => CompletionOperation::FrontendRecovery,
        }
    }
    pub(crate) fn measure(self, proof: &mut Proof) -> Result<(), Failure> {
        match self {
            Self::Claim(r) => proof.measure(r),
            Self::FrontendClaim(r) => proof.measure(r),
            Self::Recovery(r) => proof.measure(r),
            Self::FrontendRecovery(r) => proof.measure(r),
        }
    }
    pub(crate) fn fingerprint(self) -> Result<InputFingerprint, crate::ToolContinuationError> {
        match self {
            Self::Claim(r) => r.fingerprint(),
            Self::FrontendClaim(r) => r.fingerprint(),
            Self::Recovery(r) => r.fingerprint(),
            Self::FrontendRecovery(r) => r.fingerprint(),
        }
    }
    pub(crate) fn verify_receipt(
        self,
        receipt: &crate::ToolContinuationReceipt,
    ) -> Result<(), crate::ToolContinuationError> {
        if receipt.operation_id != *self.id()
            || receipt.semantic_digest != self.fingerprint()?
            || receipt.session_id != self.original().session_id
            || receipt.run_id != self.original().run_id
        {
            return Err(crate::ToolContinuationError::SemanticConflict);
        }
        match self {
            Self::Recovery(r) => r.verify_receipt(receipt),
            Self::FrontendRecovery(r) => r.verify_receipt(receipt),
            Self::Claim(r) if matches!(&receipt.outcome,crate::ToolContinuationOutcome::Claimed{continuation,..} if continuation==&r.continuation) => {
                Ok(())
            }
            Self::FrontendClaim(r) if matches!(&receipt.outcome,crate::ToolContinuationOutcome::FrontendClaimed{continuation,..} if continuation==&r.claim.continuation) => {
                Ok(())
            }
            _ => Err(crate::ToolContinuationError::SemanticConflict),
        }
    }
    #[cfg(test)]
    pub(crate) fn test_digest(self) -> InputFingerprint {
        match self {
            Self::Claim(r) => super::super::contract::snapshot_digest(&("tool_claim", r)),
            Self::FrontendClaim(r) => {
                super::super::contract::snapshot_digest(&("frontend_claim", r))
            }
            Self::Recovery(r) => super::super::contract::snapshot_digest(&("tool_recovery", r)),
            Self::FrontendRecovery(r) => {
                super::super::contract::snapshot_digest(&("frontend_recovery", r))
            }
        }
    }
}
