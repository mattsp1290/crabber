use super::super::{OwnedScopeActivation, driver::MainIngress};
use super::{
    BoundSuccessorControl, DriverMainContext, Error, PendingSuccessorControl, PendingSuccessorError,
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
use std::{future::Future, sync::Arc};

pub(super) struct Source {
    run: RunId,
    claim: NativeClaimLocator,
    admission: bool,
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
}

pub(super) async fn run<T, F, W>(
    store: Arc<dyn Store>,
    context: SessionContext,
    original: JoinedPreparation,
    source: Source,
    control: &PendingSuccessorControl,
    ingress: Arc<MainIngress>,
    business: F,
) -> Result<Option<T>, PendingSuccessorError>
where
    F: FnOnce(DriverMainContext, PreparationCompletion) -> W + Send + 'static,
    W: Future<Output = T> + Send + 'static,
{
    if control.stopped() {
        return Ok(None);
    }
    // These futures stay inside the same actual main; ordinary stop never selects
    // them away, aborts them or reconstructs their private result from replay.
    let completion = {
        let Some(_call) = control.enter_call()? else {
            return Ok(None);
        };
        store.complete_preparation(original).await?
    };
    if completion.is_replayed() {
        return Err(Error::UnknownStoreOutcome.into());
    }
    let request = source.request(context, &completion)?;
    if control.stopped() {
        return Ok(None);
    }
    let outcome = {
        let Some(_call) = control.enter_call()? else {
            return Ok(None);
        };
        store
            .register_successor_execution_driver(request.clone())
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
        return Ok(None);
    }
    let outcome = {
        let Some(_call) = control.enter_call()? else {
            return Ok(None);
        };
        store
            .activate_owned_scope(
                activation.clone(),
                grant.authority().clone(),
                Some(OwnedScopeParentAuthority::SuccessorExecutionDriver(grant)),
            )
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
        return Ok(None);
    }
    let context = DriverMainContext::dispatched(permitted, ingress);
    Ok(Some(business(context, completion).await))
}
