//! Borrow the genuine native ACK; retain full typed metadata and actual fence.
use super::{Error, Origin, PendingSuccessorReplay, Source};
use crate::session_owner::preparation::PreparationCompletion;
use crabber_core::{RunFence, SessionContext};
#[derive(Clone, Copy)]
pub(super) enum Receipt<'a> {
    Approval(&'a crate::ApprovalReceipt),
    Tool(&'a crate::ToolContinuationReceipt),
}
pub(super) struct Completed<'a> {
    pub(super) receipt: Receipt<'a>,
    pub(super) fence: Option<&'a RunFence>,
}
impl<'a> Completed<'a> {
    pub(super) fn new(completion: &'a PreparationCompletion) -> Result<Self, Error> {
        match completion {
            PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Applied {
                receipt,
                grant,
            }) => Ok(Self {
                receipt: Receipt::Approval(receipt),
                fence: Some(&grant.fence),
            }),
            PreparationCompletion::Approval(crate::ApprovalClaimOutcome::Replayed(receipt)) => {
                Ok(Self {
                    receipt: Receipt::Approval(receipt),
                    fence: None,
                })
            }
            PreparationCompletion::ToolContinuation(
                crate::ToolContinuationClaimOutcome::Applied { receipt, grant },
            ) => Ok(Self {
                receipt: Receipt::Tool(receipt),
                fence: Some(&grant.fence),
            }),
            PreparationCompletion::ToolContinuation(
                crate::ToolContinuationClaimOutcome::Replayed(receipt),
            ) => Ok(Self {
                receipt: Receipt::Tool(receipt),
                fence: None,
            }),
            _ => Err(Error::SemanticConflict),
        }
    }
}
impl Source {
    pub(super) fn metadata(
        &self,
        receipt: Receipt<'_>,
        context: &SessionContext,
    ) -> Result<PendingSuccessorReplay, Error> {
        let (session, run, id, digest) = match receipt {
            Receipt::Approval(r) => (
                &r.session_id,
                &r.run_id,
                &r.operation_id,
                &r.semantic_digest,
            ),
            Receipt::Tool(r) => (
                &r.session_id,
                &r.run_id,
                &r.operation_id,
                &r.semantic_digest,
            ),
        };
        if session != context.session_id()
            || *run != self.run
            || *id != self.id
            || *digest != self.digest
        {
            return Err(Error::SemanticConflict);
        }
        match receipt {
            Receipt::Approval(r) => match (&r.outcome, self.origin) {
                (crate::ApprovalOutcome::Claimed { .. }, Origin::ApprovalClaim) => {
                    Ok(PendingSuccessorReplay::ApprovalClaim(Box::new(r.clone())))
                }
                (crate::ApprovalOutcome::ReclaimedUnstarted { .. }, Origin::ApprovalRecovery) => {
                    Ok(PendingSuccessorReplay::ApprovalRecovery(Box::new(
                        r.clone(),
                    )))
                }
                _ => Err(Error::SemanticConflict),
            },
            Receipt::Tool(r) => match (&r.outcome, self.origin) {
                (crate::ToolContinuationOutcome::Claimed { .. }, Origin::ToolClaim) => {
                    Ok(PendingSuccessorReplay::ToolClaim(Box::new(r.clone())))
                }
                (
                    crate::ToolContinuationOutcome::ReclaimedUnstarted { .. },
                    Origin::ToolRecovery,
                ) => Ok(PendingSuccessorReplay::ToolRecovery(Box::new(r.clone()))),
                (crate::ToolContinuationOutcome::FrontendClaimed { .. }, Origin::FrontendClaim) => {
                    Ok(PendingSuccessorReplay::FrontendClaim(Box::new(r.clone())))
                }
                (
                    crate::ToolContinuationOutcome::FrontendReclaimedUnstarted { .. },
                    Origin::FrontendRecovery,
                ) => Ok(PendingSuccessorReplay::FrontendRecovery(Box::new(
                    r.clone(),
                ))),
                _ => Err(Error::SemanticConflict),
            },
        }
    }
}
