//! Closed initial/successor adapters preserve their distinct native grants.
use crate::orchestrator::{RuntimeError, driver};
use crabber_session::session_owner::owned_scope::{
    BoundInitialControl, BoundSuccessorControl, InitialAdmissionReplay, JoinedOwnedTask,
    OwnedMainDisposition, OwnedScopeActivation, PendingInitialCompletion, PendingInitialControl,
    PendingInitialTask, PendingSuccessorCompletion, PendingSuccessorControl, PendingSuccessorError,
    PendingSuccessorReplay, PendingSuccessorTask,
};

pub(super) enum Main {
    Initial(PendingInitialTask<()>),
    Successor(PendingSuccessorTask<()>),
}
#[derive(Clone)]
pub(super) enum Control {
    Initial(PendingInitialControl),
    Successor(PendingSuccessorControl),
}
pub(super) enum Bound {
    Initial(BoundInitialControl),
    Successor(BoundSuccessorControl),
}
#[derive(Clone)]
pub(in crate::orchestrator) enum Metadata {
    Initial(InitialAdmissionReplay),
    Successor(PendingSuccessorReplay),
}
#[derive(Clone)]
pub(super) struct Completion {
    pub metadata: Metadata,
    pub replayed: bool,
}
pub(super) struct Observation<'a> {
    pub main: Option<&'a JoinedOwnedTask<()>>,
    pub disposition: OwnedMainDisposition,
    pub replayed: Option<Metadata>,
}
impl Main {
    pub(super) fn control(&self) -> Control {
        match self {
            Self::Initial(main) => Control::Initial(main.control()),
            Self::Successor(main) => Control::Successor(main.control()),
        }
    }
    pub(super) fn start(&mut self) -> Result<(), RuntimeError> {
        match self {
            Self::Initial(main) => main.start()?,
            Self::Successor(main) => main.start()?,
        }
        Ok(())
    }
    pub(super) async fn join(&mut self) -> Result<Observation<'_>, RuntimeError> {
        Ok(match self {
            Self::Initial(main) => {
                let observed = main.join().await?;
                Observation {
                    main: observed.bound_main(),
                    disposition: observed.disposition(),
                    replayed: observed.replayed().cloned().map(Metadata::Initial),
                }
            }
            Self::Successor(main) => {
                let observed = main.join().await?;
                Observation {
                    main: observed.bound_main(),
                    disposition: observed.disposition(),
                    replayed: observed.replayed().cloned().map(Metadata::Successor),
                }
            }
        })
    }
    pub(super) async fn take_error(
        &mut self,
    ) -> Result<Option<PendingSuccessorError>, RuntimeError> {
        Ok(match self {
            Self::Initial(main) => main.take_error().await?,
            Self::Successor(main) => main.take_error().await?,
        })
    }
}
impl Control {
    pub(super) fn request_stop(&self) {
        match self {
            Self::Initial(control) => control.request_stop(),
            Self::Successor(control) => control.request_stop(),
        }
    }
    pub(super) async fn completion(&self) -> Result<Completion, RuntimeError> {
        Ok(match self {
            Self::Initial(control) => match control.completion().await? {
                PendingInitialCompletion::Fresh(metadata) => Completion {
                    metadata: Metadata::Initial(metadata.receipt().clone()),
                    replayed: false,
                },
                PendingInitialCompletion::Replayed(metadata) => Completion {
                    metadata: Metadata::Initial(metadata),
                    replayed: true,
                },
            },
            Self::Successor(control) => match control.completion().await? {
                PendingSuccessorCompletion::Fresh(metadata) => Completion {
                    metadata: Metadata::Successor(metadata),
                    replayed: false,
                },
                PendingSuccessorCompletion::Replayed(metadata) => Completion {
                    metadata: Metadata::Successor(metadata),
                    replayed: true,
                },
            },
        })
    }
    pub(super) async fn bound(&self) -> Result<Bound, RuntimeError> {
        Ok(match self {
            Self::Initial(control) => Bound::Initial(control.bound().await?),
            Self::Successor(control) => Bound::Successor(control.bound().await?),
        })
    }
}
impl Bound {
    pub(super) fn authority(&self) -> &crabber_session::session_owner::SessionOwnerAuthority {
        match self {
            Self::Initial(bound) => bound.grant().authority(),
            Self::Successor(bound) => bound.grant().authority(),
        }
    }

    pub(super) fn activation(&self) -> &OwnedScopeActivation {
        match self {
            Self::Initial(bound) => bound.activation(),
            Self::Successor(bound) => bound.activation(),
        }
    }
    pub(super) fn grant(&self) -> driver::registration::Grant {
        match self {
            Self::Initial(bound) => {
                driver::registration::Grant::Initial(Box::new(bound.grant().clone()))
            }
            Self::Successor(bound) => {
                driver::registration::Grant::Successor(Box::new(bound.grant().clone()))
            }
        }
    }
}
impl Metadata {
    pub(super) fn run_id(&self) -> &crabber_core::RunId {
        match self {
            Self::Initial(receipt) => receipt.run_id(),
            Self::Successor(PendingSuccessorReplay::Admission(receipt)) => &receipt.run_id,
            Self::Successor(PendingSuccessorReplay::Expired(receipt)) => &receipt.run_id,
            Self::Successor(
                PendingSuccessorReplay::ApprovalClaim(receipt)
                | PendingSuccessorReplay::ApprovalRecovery(receipt),
            ) => &receipt.run_id,
        }
    }
    pub(super) fn admission(&self) -> Option<&crabber_core::AdmissionReceipt> {
        match self {
            Self::Initial(InitialAdmissionReplay::Keyed(receipt))
            | Self::Successor(PendingSuccessorReplay::Admission(receipt)) => Some(receipt),
            _ => None,
        }
    }
}
