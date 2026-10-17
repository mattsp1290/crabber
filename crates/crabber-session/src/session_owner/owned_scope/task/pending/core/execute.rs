use super::super::super::driver::MainIngress;
use super::super::{
    BoundSuccessorControl, DriverMainContext, Error, PendingSuccessorError,
    initial::BoundInitialControl,
};
use super::{Binding, Control, Notice, Replay, Source, TaskOutput};
use crate::{
    Store,
    session_owner::{
        driver::{
            ExecutionDriverOutcome, RegisterExecutionDriver, RegisterSuccessorExecutionDriver,
            SuccessorExecutionDriverOutcome,
        },
        owned_scope::{OwnedDispatchPermit, OwnedScopeActivation, OwnedScopeActivationOutcome},
        preparation::{JoinedPreparation, PreparationCompletion},
    },
};
use crabber_core::SessionContext;
use futures::FutureExt;
use std::{future::Future, sync::Arc};

pub(in crate::session_owner::owned_scope::task::pending) enum Registration {
    Initial(RegisterExecutionDriver),
    Successor(RegisterSuccessorExecutionDriver),
}
impl Registration {
    async fn register(&self, store: &dyn Store) -> Result<Binding, PendingSuccessorError> {
        match self {
            Self::Initial(request) => {
                let outcome = administrative(async {
                    store.register_execution_driver(request.clone()).await
                })
                .await?;
                let ExecutionDriverOutcome::Applied { receipt, grant } = outcome else {
                    return Err(Error::UnknownStoreOutcome.into());
                };
                if grant.receipt() != &receipt
                    || grant.original_request().fingerprint()? != request.fingerprint()?
                {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                let activation = OwnedScopeActivation::new(grant.scope_source()?)?;
                Ok(Binding::Initial(Box::new(BoundInitialControl {
                    grant: *grant,
                    activation,
                })))
            }
            Self::Successor(request) => {
                let outcome = administrative(async {
                    store
                        .register_successor_execution_driver(request.clone())
                        .await
                })
                .await?;
                let SuccessorExecutionDriverOutcome::Applied { receipt, grant } = outcome else {
                    return Err(Error::UnknownStoreOutcome.into());
                };
                if grant.receipt() != &receipt
                    || grant.original_request().fingerprint()? != request.fingerprint()?
                {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                let activation = OwnedScopeActivation::new(grant.scope_source()?)?;
                Ok(Binding::Successor(Box::new(BoundSuccessorControl {
                    grant: *grant,
                    activation,
                })))
            }
        }
    }
}
pub(in crate::session_owner::owned_scope::task::pending) struct Fresh {
    pub(in crate::session_owner::owned_scope::task::pending) registration: Registration,
    pub(in crate::session_owner::owned_scope::task::pending) notice: Notice,
    pub(in crate::session_owner::owned_scope::task::pending) completion: PreparationCompletion,
}
pub(in crate::session_owner::owned_scope::task::pending) enum Prepared {
    Fresh(Box<Fresh>),
    Replayed(Replay),
    Stopped,
}

pub(super) async fn run<T, F, W>(
    store: Arc<dyn Store>,
    context: SessionContext,
    original: JoinedPreparation,
    source: Source,
    control: &Control,
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
    let prepared = match source {
        Source::Initial(source) => {
            super::super::initial::source::prepare(
                store.as_ref(),
                context,
                original,
                source,
                control,
            )
            .await?
        }
        Source::Successor(source) => {
            super::super::execute::prepare(store.as_ref(), context, original, source, control)
                .await?
        }
    };
    let fresh = match prepared {
        Prepared::Stopped => return Ok(TaskOutput::Stopped),
        Prepared::Replayed(replay) => {
            control.completed(replay.notice())?;
            return Ok(TaskOutput::Replayed(replay));
        }
        Prepared::Fresh(fresh) => fresh,
    };
    control.completed(fresh.notice)?;
    let binding = {
        let Some(_call) = control.enter_call()? else {
            return Ok(TaskOutput::Stopped);
        };
        fresh.registration.register(store.as_ref()).await?
    };
    control.bind(binding.clone())?;
    let outcome = {
        let Some(_call) = control.enter_call()? else {
            return Ok(TaskOutput::Stopped);
        };
        administrative(async {
            store
                .activate_owned_scope(
                    binding.activation().clone(),
                    binding.authority().clone(),
                    Some(binding.parent()),
                )
                .await
        })
        .await?
    };
    let OwnedScopeActivationOutcome::Applied { permit, .. } = outcome else {
        return Err(Error::UnknownStoreOutcome.into());
    };
    let OwnedDispatchPermit { activation } = *permit;
    if &activation != binding.activation() {
        return Err(Error::SemanticConflict.into());
    }
    if !control.dispatch()? {
        return Ok(TaskOutput::Stopped);
    }
    let context = DriverMainContext::dispatched(activation, ingress);
    Ok(TaskOutput::Business(
        business(context, fresh.completion).await,
    ))
}

// Both future construction and polling are inside each caller's async block.
pub(in crate::session_owner::owned_scope::task::pending) async fn administrative<T, E>(
    call: impl Future<Output = Result<T, E>>,
) -> Result<T, PendingSuccessorError>
where
    PendingSuccessorError: From<E>,
{
    match std::panic::AssertUnwindSafe(call).catch_unwind().await {
        Ok(result) => result.map_err(PendingSuccessorError::from),
        Err(payload) => {
            super::super::retain::discard(payload);
            Err(Error::UnknownStoreOutcome.into())
        }
    }
}
