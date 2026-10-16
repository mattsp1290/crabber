//! Distinct native sources share physical supervision, never execution grants.
use super::RuntimeError;
use crabber_session::{
    Store,
    session_owner::{
        SessionOwnerAuthority, SessionOwnerReceipt,
        driver::{
            ExecutionDriverGrant, ExecutionDriverOutcome, RegisterExecutionDriver,
            RegisterSuccessorExecutionDriver, SuccessorExecutionDriverGrant,
            SuccessorExecutionDriverOutcome,
        },
        owned_scope::{OwnedScopeParentAuthority, OwnedScopeSource},
    },
};

pub(in crate::orchestrator) enum Request {
    Initial(RegisterExecutionDriver),
    Successor(RegisterSuccessorExecutionDriver),
}
pub(super) enum Grant {
    Initial(Box<ExecutionDriverGrant>),
    Successor(Box<SuccessorExecutionDriverGrant>),
}
impl Request {
    pub(super) fn run_id(&self) -> &crabber_core::RunId {
        match self {
            Self::Initial(request) => &request.fence.run_id,
            Self::Successor(request) => &request.fence.run_id,
        }
    }
    pub(super) async fn register(self, store: &dyn Store) -> Result<Grant, RuntimeError> {
        match self {
            Self::Initial(request) => {
                let ExecutionDriverOutcome::Applied { grant, receipt } = super::store_call(async {
                    store.register_execution_driver(request.clone()).await
                })
                .await?
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                if grant.original_request().fingerprint()? != request.fingerprint()?
                    || grant.receipt() != &receipt
                {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                }
                Ok(Grant::Initial(grant))
            }
            Self::Successor(request) => {
                let SuccessorExecutionDriverOutcome::Applied { grant, receipt } =
                    super::store_call(async {
                        store
                            .register_successor_execution_driver(request.clone())
                            .await
                    })
                    .await?
                else {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                };
                if grant.original_request().fingerprint()? != request.fingerprint()?
                    || grant.receipt() != &receipt
                {
                    return Err(RuntimeError::PreparationRecoveryRequired);
                }
                Ok(Grant::Successor(grant))
            }
        }
    }
}
impl Grant {
    pub(super) fn authority(&self) -> &SessionOwnerAuthority {
        match self {
            Self::Initial(grant) => grant.authority(),
            Self::Successor(grant) => grant.authority(),
        }
    }
    pub(super) fn begin(&self) -> &SessionOwnerReceipt {
        match self {
            Self::Initial(grant) => &grant.receipt().begin,
            Self::Successor(grant) => &grant.receipt().worker.begin,
        }
    }
    pub(super) fn scope_source(&self) -> Result<OwnedScopeSource, RuntimeError> {
        Ok(match self {
            Self::Initial(grant) => grant.scope_source()?,
            Self::Successor(grant) => grant.scope_source()?,
        })
    }
    pub(super) fn parent(&self) -> OwnedScopeParentAuthority {
        match self {
            Self::Initial(grant) => OwnedScopeParentAuthority::ExecutionDriver(grant.clone()),
            Self::Successor(grant) => {
                OwnedScopeParentAuthority::SuccessorExecutionDriver(grant.clone())
            }
        }
    }
}

pub(in crate::orchestrator) fn for_entry(
    entry: &super::super::InitialEntry,
    retained: bool,
    context: crabber_core::SessionContext,
    fence: crabber_core::RunFence,
) -> Option<Request> {
    if !retained {
        return None;
    }
    match entry {
        super::super::InitialEntry::Unkeyed | super::super::InitialEntry::Keyed => {
            Some(Request::Initial(RegisterExecutionDriver { context, fence }))
        }
        super::super::InitialEntry::RecoveredKeyed(Some(claim)) => {
            Some(Request::Successor(RegisterSuccessorExecutionDriver {
                context,
                fence,
                claim: claim.clone(),
            }))
        }
        super::super::InitialEntry::RecoveredKeyed(None) => None,
    }
}
