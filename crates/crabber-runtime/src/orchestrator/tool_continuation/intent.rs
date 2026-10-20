//! Correlate native ACKs to the actual original typed invocation, before I/O.
use super::{
    ContinuationTarget, ControlId, Error, InputFingerprint, RunId, RuntimeError, SessionId,
    ToolContinuationOutcome, ToolContinuationReceipt, ToolResumeControl,
};
#[derive(Debug)]
enum Kind {
    ToolClaim,
    FrontendClaim,
    ToolRecovery {
        original: ControlId,
        target: crabber_session::AttemptTarget,
    },
    FrontendRecovery {
        original: ControlId,
        target: crabber_session::AttemptTarget,
    },
}
pub(in crate::orchestrator) struct Expected {
    session: SessionId,
    run: RunId,
    operation: ControlId,
    digest: InputFingerprint,
    continuation: ContinuationTarget,
    kind: Kind,
}
impl Expected {
    pub(in crate::orchestrator) fn new(
        control: &ToolResumeControl,
    ) -> Result<Box<Self>, RuntimeError> {
        // Existing canonical fingerprint methods count/cap the complete borrowed
        // typed intent before JSON materialization/hash. Only then copy the small
        // validated identifiers; no frontend output/checkpoint is cloned here.
        let (digest, claim, operation, kind) = match control {
            ToolResumeControl::Claim(claim) => {
                (claim.fingerprint()?, claim, &claim.id, Kind::ToolClaim)
            }
            ToolResumeControl::FrontendClaim(request) => (
                request.fingerprint()?,
                &request.claim,
                &request.claim.id,
                Kind::FrontendClaim,
            ),
            ToolResumeControl::Recover(request) => (
                request.fingerprint()?,
                &request.original,
                &request.id,
                Kind::ToolRecovery {
                    original: request.original.id.clone(),
                    target: request.attempt.clone(),
                },
            ),
            ToolResumeControl::FrontendRecover(request) => (
                request.fingerprint()?,
                &request.original.claim,
                &request.id,
                Kind::FrontendRecovery {
                    original: request.original.claim.id.clone(),
                    target: request.attempt.clone(),
                },
            ),
        };
        Ok(Box::new(Self {
            session: claim.session_id.clone(),
            run: claim.run_id.clone(),
            operation: operation.clone(),
            digest,
            continuation: claim.continuation.clone(),
            kind,
        }))
    }
    pub(in crate::orchestrator) fn verify(
        &self,
        receipt: &ToolContinuationReceipt,
    ) -> Result<(), RuntimeError> {
        if receipt.session_id != self.session
            || receipt.run_id != self.run
            || receipt.operation_id != self.operation
            || receipt.semantic_digest != self.digest
        {
            return Err(Error::SemanticConflict.into());
        }
        let valid = match (&self.kind, &receipt.outcome) {
            (Kind::ToolClaim, ToolContinuationOutcome::Claimed { continuation, .. })
            | (
                Kind::FrontendClaim,
                ToolContinuationOutcome::FrontendClaimed { continuation, .. },
            ) => continuation == &self.continuation,
            (
                Kind::ToolRecovery { original, target },
                ToolContinuationOutcome::ReclaimedUnstarted {
                    continuation,
                    original_claim_id,
                    attempt,
                },
            )
            | (
                Kind::FrontendRecovery { original, target },
                ToolContinuationOutcome::FrontendReclaimedUnstarted {
                    continuation,
                    original_claim_id,
                    attempt,
                },
            ) => {
                continuation == &self.continuation
                    && original_claim_id == original
                    && attempt.target == *target
            }
            _ => false,
        };
        if !valid {
            return Err(Error::SemanticConflict.into());
        }
        Ok(())
    }
}
