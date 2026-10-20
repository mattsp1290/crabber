//! Six typed continuations complete and reconcile in their original parked task.
#[path = "continuation/inspection.rs"]
mod inspection;
#[path = "continuation/receipt.rs"]
mod receipt;
use super::{
    Control, Error, Fresh, Notice, PendingSuccessorCompletion, PendingSuccessorError,
    PendingSuccessorReplay, Prepared, Registration, Replay, administrative,
};
use crate::{
    Store,
    session_owner::{
        driver::{RegisterSuccessorExecutionDriver, SuccessorExecutionDriverOrigin as Origin},
        preparation::{JoinedPreparation, PreparedOperation},
    },
};
use crabber_core::{ControlId, InputFingerprint, RunId, SessionContext};
use receipt::Completed;

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
            PreparedOperation::ApprovalClaim(v) => (
                &v.run_id,
                &v.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::ApprovalClaim,
            ),
            PreparedOperation::ApprovalRecovery(v) => (
                &v.original.run_id,
                &v.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::ApprovalRecovery,
            ),
            PreparedOperation::ToolClaim(v) => (
                &v.run_id,
                &v.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::ToolClaim,
            ),
            PreparedOperation::ToolRecovery(v) => (
                &v.original.run_id,
                &v.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::ToolRecovery,
            ),
            PreparedOperation::FrontendClaim(v) => (
                &v.claim.run_id,
                &v.claim.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::FrontendClaim,
            ),
            PreparedOperation::FrontendRecovery(v) => (
                &v.original.claim.run_id,
                &v.id,
                v.fingerprint().map_err(|_| Error::InvalidRequest)?,
                Origin::FrontendRecovery,
            ),
            _ => return Ok(None),
        };
        Ok(Some(Self {
            run: run.clone(),
            id: id.clone(),
            digest,
            origin,
            join: original
                .observed_grant()
                .original_reservation()
                .reconciliation()?
                .join_id,
        }))
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
    let completed = Completed::new(&captured.completion)?;
    let metadata = source.metadata(completed.receipt, &context)?;
    let Some(fence) = completed.fence else {
        if captured.claim.is_some() {
            return Err(Error::SemanticConflict.into());
        }
        let expected = {
            let Some(_call) = control.enter_call()? else {
                return Ok(Prepared::Stopped);
            };
            administrative(source.lookup(store, &context)).await?
        }
        .ok_or(Error::UnknownStoreOutcome)?;
        if expected != metadata {
            return Err(Error::SemanticConflict.into());
        }
        return Ok(Prepared::Replayed(Replay::Successor(metadata)));
    };
    crate::session_owner::identifier(&fence.claim_token, 512, false)?;
    if fence.run_id != source.run {
        return Err(Error::SemanticConflict.into());
    }
    let claim = captured.claim.ok_or(Error::UnknownStoreOutcome)?;
    if claim.root_join_id != source.join {
        return Err(Error::SemanticConflict.into());
    }
    let request = RegisterSuccessorExecutionDriver {
        context: context.clone(),
        fence: fence.clone(),
        claim,
    };
    let inspected = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(source.inspect(store, &context, &request, &captured.completion)).await?
    };
    if inspected != metadata {
        return Err(Error::SemanticConflict.into());
    }
    Ok(Prepared::Fresh(Box::new(Fresh {
        registration: Registration::Successor(request),
        notice: Notice::Successor(PendingSuccessorCompletion::Fresh(metadata)),
        completion: captured.completion,
    })))
}
