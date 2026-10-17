use super::super::{OwnedScopeActivation, driver::MainIngress};
use super::{
    BoundSuccessorControl, DriverMainContext, Error, PendingSuccessorCompletion,
    PendingSuccessorControl, PendingSuccessorError, PendingSuccessorReplay, TaskOutput,
};
use crate::{
    Store,
    session_owner::{
        driver::{
            NativeClaimLocator, RegisterSuccessorExecutionDriver, SuccessorExecutionDriverOutcome,
        },
        owned_scope::{
            OwnedDispatchPermit, OwnedScopeActivationOutcome, OwnedScopeParentAuthority,
        },
        preparation::{
            AdmissionRecoveryOutcome, ExpiredRunOutcome, JoinedPreparation, PreparationCompletion,
            PreparationPurpose, PreparedOperation,
        },
    },
};
use crabber_core::{RunId, SessionContext};
use futures::FutureExt;
use std::{future::Future, sync::Arc};

pub(super) struct Source {
    run: RunId,
    claim: NativeClaimLocator,
    admission: bool,
    key: Option<crabber_core::AdmissionKey>,
}
impl Source {
    pub(super) fn new(
        context: &SessionContext,
        original: &JoinedPreparation,
    ) -> Result<Self, Error> {
        original
            .verify_original_intent()
            .map_err(|_| Error::InvalidRequest)?;
        let reservation = original.observed_grant().original_reservation();
        let binding = &reservation.owner.binding;
        if original.observed_registration().is_none() || reservation.resource_contract.is_none() {
            return Err(Error::Unsupported);
        }
        if context.session_id() != &binding.session_id
            || context.workspace_id() != binding.workspace_id
            || context.directory() != binding.directory
        {
            return Err(Error::IdentityMismatch);
        }
        let (run, issuance, admission) = match original.observed_operation() {
            PreparedOperation::AdmissionRecovery(value) => (
                &value.source.request().expected_fence.run_id,
                value.source.expected_issuance(),
                true,
            ),
            PreparedOperation::ExpiredRunResume(value) => (
                &value.source.request().expected_fence.run_id,
                value.source.expected_issuance(),
                false,
            ),
            _ => return Err(Error::Unsupported),
        };
        if !matches!(&reservation.purpose, PreparationPurpose::ExistingRun { run_id, .. } if run_id == run)
        {
            return Err(Error::SemanticConflict);
        }
        let claim = NativeClaimLocator {
            issuance: issuance.checked_add(1).ok_or(Error::InvalidRequest)?,
            root_join_id: reservation.reconciliation()?.join_id,
        };
        claim.validate()?;
        Ok(Self {
            run: run.clone(),
            claim,
            admission,
            key: match original.observed_operation() {
                PreparedOperation::AdmissionRecovery(value) => {
                    Some(value.source.request().key.clone())
                }
                _ => None,
            },
        })
    }
    fn request(
        &self,
        context: SessionContext,
        completion: &PreparationCompletion,
    ) -> Result<RegisterSuccessorExecutionDriver, Error> {
        let fence = match completion {
            PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Claimed(value))
                if self.admission =>
            {
                &value.fence
            }
            PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, fence })
                if !self.admission
                    && receipt.issuance == self.claim.issuance
                    && receipt.operation_id == self.claim.root_join_id
                    && receipt.run_id == self.run
                    && &receipt.session_id == context.session_id() =>
            {
                fence
            }
            _ => return Err(Error::UnknownStoreOutcome),
        };
        crate::session_owner::identifier(&fence.claim_token, 512, false)?;
        if fence.run_id != self.run {
            return Err(Error::SemanticConflict);
        }
        Ok(RegisterSuccessorExecutionDriver {
            context,
            fence: fence.clone(),
            claim: self.claim.clone(),
        })
    }
    async fn expected_receipt(
        &self,
        store: &dyn Store,
        context: &SessionContext,
        control: &PendingSuccessorControl,
    ) -> Result<Option<crabber_core::AdmissionReceipt>, PendingSuccessorError> {
        if let Some(key) = &self.key {
            let Some(_call) = control.enter_call()? else {
                return Ok(None);
            };
            let receipt = administrative(async {
                store
                    .lookup_admission(context.session_id(), key)
                    .await
                    .map_err(crate::session_owner::preparation::PreparationCommitError::from)
            })
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
            if receipt.run_id != self.run
                || &receipt.session_id != context.session_id()
                || crate::snapshot::encoded_bytes(&receipt, 8192).is_none()
            {
                return Err(Error::UnknownStoreOutcome.into());
            }
            Ok(Some(receipt))
        } else {
            Ok(None)
        }
    }
    fn replay(
        &self,
        context: &SessionContext,
        completion: &PreparationCompletion,
        expected_receipt: Option<&crabber_core::AdmissionReceipt>,
    ) -> Result<PendingSuccessorReplay, Error> {
        Ok(match completion {
            PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Replayed(
                receipt,
            )) if self.admission
                && Some(receipt) == expected_receipt
                && receipt.run_id == self.run
                && &receipt.session_id == context.session_id() =>
            {
                PendingSuccessorReplay::Admission(receipt.clone())
            }
            PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Replayed(receipt))
                if !self.admission
                    && receipt.run_id == self.run
                    && &receipt.session_id == context.session_id()
                    && receipt.issuance == self.claim.issuance
                    && receipt.operation_id == self.claim.root_join_id =>
            {
                PendingSuccessorReplay::Expired(receipt.clone())
            }
            _ => return Err(Error::SemanticConflict),
        })
    }
}

pub(super) async fn run<T, F, W>(
    store: Arc<dyn Store>,
    context: SessionContext,
    original: JoinedPreparation,
    source: Source,
    control: &PendingSuccessorControl,
    ingress: Arc<MainIngress>,
    business: F,
) -> Result<TaskOutput<T>, PendingSuccessorError>
where
    F: FnOnce(DriverMainContext, PreparationCompletion) -> W + Send + 'static,
    W: Future<Output = T> + Send + 'static,
{
    if control.stopped() {
        return Ok(TaskOutput::Stopped);
    }
    let expected_receipt = source
        .expected_receipt(store.as_ref(), &context, control)
        .await?;
    // These futures stay inside the same actual main; ordinary stop never selects
    // them away, aborts them or reconstructs their private result from replay.
    let completion = {
        let Some(_call) = control.enter_call()? else {
            return Ok(TaskOutput::Stopped);
        };
        administrative(async { store.complete_preparation(original).await }).await?
    };
    if completion.is_replayed() {
        let replay = source.replay(&context, &completion, expected_receipt.as_ref())?;
        control.completed(PendingSuccessorCompletion::Replayed(replay.clone()))?;
        return Ok(TaskOutput::Replayed(replay));
    }
    let request = source.request(context, &completion)?;
    let metadata = match &completion {
        PreparationCompletion::AdmissionRecovery(AdmissionRecoveryOutcome::Claimed(value))
            if Some(&value.record.receipt) == expected_receipt.as_ref() =>
        {
            PendingSuccessorReplay::Admission(value.record.receipt.clone())
        }
        PreparationCompletion::ExpiredRun(ExpiredRunOutcome::Claimed { receipt, .. }) => {
            PendingSuccessorReplay::Expired(receipt.clone())
        }
        _ => return Err(Error::SemanticConflict.into()),
    };
    control.completed(PendingSuccessorCompletion::Fresh(metadata))?;
    if control.stopped() {
        return Ok(TaskOutput::Stopped);
    }
    let outcome = {
        let Some(_call) = control.enter_call()? else {
            return Ok(TaskOutput::Stopped);
        };
        administrative(async {
            store
                .register_successor_execution_driver(request.clone())
                .await
        })
        .await?
    };
    let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = outcome else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    if grant.receipt() != &receipt
        || grant.original_request().fingerprint()? != request.fingerprint()?
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let activation = OwnedScopeActivation::new(grant.scope_source()?)?;
    control.bind(BoundSuccessorControl {
        grant: (*grant).clone(),
        activation: activation.clone(),
    })?;
    if control.stopped() {
        return Ok(TaskOutput::Stopped);
    }
    let outcome = {
        let Some(_call) = control.enter_call()? else {
            return Ok(TaskOutput::Stopped);
        };
        administrative(async {
            store
                .activate_owned_scope(
                    activation.clone(),
                    grant.authority().clone(),
                    Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant)),
                )
                .await
        })
        .await?
    };
    let OwnedScopeActivationOutcome::Applied { permit, .. } = outcome else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let OwnedDispatchPermit {
        activation: permitted,
    } = *permit;
    if permitted != activation {
        return Err(Error::SemanticConflict.into());
    }
    if !control.dispatch()? {
        return Ok(TaskOutput::Stopped);
    }
    let context = DriverMainContext::dispatched(permitted, ingress);
    Ok(TaskOutput::Business(business(context, completion).await))
}

// Catch only the actual administrative future, retaining the fixed executor and
// binding outside the unwind boundary. Business construction/polling still has
// its genuine physical panic disposition. A Store panic grants no dispatch.
async fn administrative<T, E>(
    call: impl Future<Output = Result<T, E>>,
) -> Result<T, PendingSuccessorError>
where
    PendingSuccessorError: From<E>,
{
    match std::panic::AssertUnwindSafe(call).catch_unwind().await {
        Ok(result) => result.map_err(PendingSuccessorError::from),
        Err(payload) => {
            super::retain::discard(payload);
            Err(Error::UnknownStoreOutcome.into())
        }
    }
}
