//! Approval completion and inspection remain in the original parked task.
use super::{
    Control, Error, Fresh, Notice, PendingSuccessorCompletion, PendingSuccessorError,
    PendingSuccessorReplay, Prepared, Registration, Replay, administrative,
};
use crate::{
    Store,
    session_owner::{
        driver::{RegisterSuccessorExecutionDriver, SuccessorExecutionDriverOrigin as Origin},
        preparation::{
            InspectPreparedApprovalDriver, JoinedPreparation, PreparationCompletion,
            PreparedOperation,
        },
    },
};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionContext};

pub(super) struct Source {
    pub(super) run: RunId,
    pub(super) origin: Origin,
    id: ControlId,
    digest: InputFingerprint,
    join: crabber_core::SessionOwnerOperationId,
}
impl Source {
    pub(super) fn new(original: &JoinedPreparation) -> Result<Option<Self>, Error> {
        let (run, id, digest, origin) = match original.observed_operation() {
            PreparedOperation::ApprovalClaim(claim) => (
                &claim.run_id,
                &claim.id,
                claim.fingerprint(),
                Origin::ApprovalClaim,
            ),
            PreparedOperation::ApprovalRecovery(recovery) => (
                &recovery.original.run_id,
                &recovery.id,
                recovery.fingerprint(),
                Origin::ApprovalRecovery,
            ),
            _ => return Ok(None),
        };
        Ok(Some(Self {
            run: run.clone(),
            id: id.clone(),
            digest: digest.map_err(|_| Error::InvalidRequest)?,
            origin,
            join: original
                .observed_grant()
                .original_reservation()
                .reconciliation()?
                .join_id,
        }))
    }
    fn metadata(
        &self,
        receipt: &crate::ApprovalReceipt,
        context: &SessionContext,
    ) -> Result<PendingSuccessorReplay, Error> {
        if &receipt.session_id != context.session_id()
            || receipt.run_id != self.run
            || receipt.operation_id != self.id
            || receipt.semantic_digest != self.digest
        {
            return Err(Error::SemanticConflict);
        }
        match (&receipt.outcome, self.origin) {
            (crate::ApprovalOutcome::Claimed { .. }, Origin::ApprovalClaim) => Ok(
                PendingSuccessorReplay::ApprovalClaim(Box::new(receipt.clone())),
            ),
            (crate::ApprovalOutcome::ReclaimedUnstarted { .. }, Origin::ApprovalRecovery) => Ok(
                PendingSuccessorReplay::ApprovalRecovery(Box::new(receipt.clone())),
            ),
            _ => Err(Error::SemanticConflict),
        }
    }
}
pub(super) async fn prepare(
    store: &dyn Store,
    context: SessionContext,
    original: JoinedPreparation,
    source: Source,
    control: &Control,
) -> Result<Prepared, PendingSuccessorError> {
    let captured = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(async {
            store
                .complete_preparation_with_driver_source(original)
                .await
        })
        .await?
    };
    let PreparationCompletion::Approval(outcome) = &captured.completion else {
        return Err(Error::SemanticConflict.into());
    };
    if let crate::ApprovalClaimOutcome::Replayed(receipt) = outcome {
        if captured.claim.is_some() {
            return Err(Error::SemanticConflict.into());
        }
        let expected = {
            let Some(_call) = control.enter_call()? else {
                return Ok(Prepared::Stopped);
            };
            administrative(async {
                store
                    .lookup_approval(context.session_id(), &source.id)
                    .await
                    .map_err(crate::session_owner::preparation::PreparationCommitError::from)
            })
            .await?
        }
        .ok_or(Error::UnknownStoreOutcome)?;
        if expected != *receipt {
            return Err(Error::SemanticConflict.into());
        }
        return Ok(Prepared::Replayed(Replay::Successor(
            source.metadata(receipt, &context)?,
        )));
    }
    let crate::ApprovalClaimOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::SemanticConflict.into());
    };
    let claim = captured.claim.ok_or(Error::UnknownStoreOutcome)?;
    if claim.root_join_id != source.join {
        return Err(Error::SemanticConflict.into());
    }
    let request = RegisterSuccessorExecutionDriver {
        context: context.clone(),
        fence: grant.fence.clone(),
        claim,
    };
    let inspected = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(async {
            store
                .inspect_prepared_approval_driver(InspectPreparedApprovalDriver {
                    request: request.clone(),
                    origin: source.origin,
                    operation_id: source.id.clone(),
                    semantic_digest: source.digest.clone(),
                })
                .await
                .map_err(crate::session_owner::preparation::PreparationCommitError::from)
        })
        .await?
    };
    if inspected.receipt != *receipt || inspected.claim != request.claim {
        return Err(Error::SemanticConflict.into());
    }
    let metadata = source.metadata(receipt, &context)?;
    Ok(Prepared::Fresh(Box::new(Fresh {
        registration: Registration::Successor(request),
        notice: Notice::Successor(PendingSuccessorCompletion::Fresh(metadata)),
        completion: captured.completion,
    })))
}
