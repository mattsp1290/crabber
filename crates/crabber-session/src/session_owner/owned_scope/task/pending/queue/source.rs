//! Queue completion stays inside the original parked task, including its fence.
use super::super::{
    Error, PendingSuccessorError,
    core::{
        Control, Notice, Replay,
        execute::{Fresh, Prepared, Registration, administrative},
    },
};
use super::{PendingQueueCompletion, QueueAdmissionMetadata};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    driver::RegisterExecutionDriver,
    preparation::{
        JoinedPreparation, PreparationCompletion, PreparedOperation, inspection, queue_admission,
    },
};
use crate::{QueueAction, QueueOutcome, QueueReceipt, QueueState, Store};
use crabber_core::SessionContext;

pub(in crate::session_owner::owned_scope::task::pending) struct Source;
impl Source {
    pub(in crate::session_owner::owned_scope::task::pending) fn new(
        store: &dyn Store,
        context: &SessionContext,
        original: &JoinedPreparation,
    ) -> Result<Self, Error> {
        if !store.supports_queue_preparation_admission_inspection() {
            return Err(Error::Unsupported);
        }
        queue_admission::original(original, &mut Proof::bounded(inspection::MAX_PROOF)).map_err(
            |error| match error {
                crate::session_owner::preparation::PreparationCommitError::Owner(error) => error,
                _ => Error::InvalidRequest,
            },
        )?;
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        if context.session_id() != &binding.session_id
            || context.workspace_id() != binding.workspace_id
            || context.directory() != binding.directory
        {
            return Err(Error::IdentityMismatch);
        }
        Ok(Self)
    }
}
pub(in crate::session_owner::owned_scope::task::pending) async fn prepare(
    store: &dyn Store,
    context: SessionContext,
    original: JoinedPreparation,
    _source: Source,
    control: &Control,
) -> Result<Prepared, PendingSuccessorError> {
    let completion = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(async { store.complete_preparation(original.clone()).await }).await?
    };
    let PreparationCompletion::Queue(outcome) = &completion else {
        return Err(Error::SemanticConflict.into());
    };
    let receipt = {
        let Some(_call) = control.enter_call()? else {
            return Ok(Prepared::Stopped);
        };
        administrative(async { store.inspect_queue_preparation_admission(&original).await })
            .await?
            .ok_or(Error::UnknownStoreOutcome)?
    };
    validate(&context, &original, &receipt)?;
    // The complete ACK must agree in full with the protected original metadata.
    if outcome.receipt() != &receipt {
        return Err(Error::SemanticConflict.into());
    }
    let admission = receipt
        .summary
        .admission
        .as_ref()
        .ok_or(Error::SemanticConflict)?;
    let admitted = match outcome {
        QueueOutcome::Replayed(_) => {
            return Ok(Prepared::Replayed(Replay::Queue(Box::new(receipt))));
        }
        QueueOutcome::Applied {
            authority: None,
            admitted: Some(admitted),
            ..
        } => admitted,
        QueueOutcome::Applied { .. } => return Err(Error::SemanticConflict.into()),
    };
    let PreparedOperation::Queue {
        request,
        binding: Some(restored),
    } = original.observed_operation()
    else {
        return Err(Error::SemanticConflict.into());
    };
    let QueueAction::Admit { owner, lease, .. } = &request.action else {
        return Err(Error::SemanticConflict.into());
    };
    let run = &admitted.run;
    crate::session_owner::identifier(&run.id.0, 128, false)?;
    crate::session_owner::identifier(&admitted.fence.claim_token, 512, false)?;
    if &admitted.session.id != context.session_id()
        || admitted.session.workspace_id != context.workspace_id()
        || admitted.session.directory != context.directory()
        || run.session_id != admitted.session.id
        || run.id != admitted.fence.run_id
        || run.id != admission.run_id
        || run.claim_token != admitted.fence.claim_token
        || run.owner != *owner
        || run.config_hash != restored.keyed.request.config_hash
        || run.plan_fingerprint != restored.keyed.request.plan_fingerprint
        || run.status != crabber_core::RunStatus::Running
        || Some(run.lease_until)
            != run
                .created_at
                .checked_add(time::Duration::try_from(*lease).map_err(|_| Error::SemanticConflict)?)
    {
        return Err(Error::SemanticConflict.into());
    }
    let registration = Registration::Queue(RegisterExecutionDriver {
        context,
        fence: admitted.fence.clone(),
    });
    Ok(Prepared::Fresh(Box::new(Fresh {
        registration,
        notice: Notice::Queue(PendingQueueCompletion::Fresh(QueueAdmissionMetadata {
            receipt,
        })),
        completion,
    })))
}
fn validate(
    context: &SessionContext,
    original: &JoinedPreparation,
    receipt: &QueueReceipt,
) -> Result<(), Error> {
    let PreparedOperation::Queue { request, .. } = original.observed_operation() else {
        return Err(Error::SemanticConflict);
    };
    let QueueAction::Admit { authority, .. } = &request.action else {
        return Err(Error::SemanticConflict);
    };
    if crate::snapshot::encoded_bytes(receipt, 16384).is_none()
        || receipt.session_id != *context.session_id()
        || receipt.operation_id != request.id
        || receipt.semantic_digest != request.fingerprint().map_err(|_| Error::SemanticConflict)?
        || receipt.summary.session_id != *context.session_id()
        || receipt.summary.queue_id != authority.queue_id
        || receipt.summary.claim_generation != authority.generation
        || receipt.summary.state != QueueState::Admitted
        || !receipt.summary.prepared
        || receipt.summary.admission.is_none()
        || receipt.revision <= request.expected_revision
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
