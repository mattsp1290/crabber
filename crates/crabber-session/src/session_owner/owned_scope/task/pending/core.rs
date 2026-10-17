//! One physical task, short control state and original result observer.
pub(super) mod execute;
mod state;
use super::{
    BoundSuccessorControl, DriverMainContext, Error, JoinedOwnedTask, OwnedMainDisposition,
    PendingSuccessorCompletion, PendingSuccessorError, PendingSuccessorReplay,
    initial::{BoundInitialControl, InitialAdmissionReplay, PendingInitialCompletion},
};
use crate::{
    Store,
    session_owner::{
        SessionOwnerAuthority,
        owned_scope::{OwnedScopeActivation, OwnedScopeParentAuthority},
        preparation::{JoinedPreparation, PreparationCompletion},
    },
};
use crabber_core::SessionContext;
pub(super) use state::Control;
use std::{future::Future, sync::Arc};
use tokio::sync::{OwnedSemaphorePermit, oneshot};

#[derive(Clone, Copy, PartialEq, Eq)]
pub(super) enum Family {
    Initial,
    Successor,
}
pub(super) enum Source {
    Initial(super::initial::source::Source),
    Successor(super::execute::Source),
}
impl Source {
    fn family(&self) -> Family {
        match self {
            Self::Initial(_) => Family::Initial,
            Self::Successor(_) => Family::Successor,
        }
    }
}
#[derive(Clone)]
pub(super) enum Binding {
    Initial(Box<BoundInitialControl>),
    Successor(Box<BoundSuccessorControl>),
}
impl Binding {
    pub(super) fn family(&self) -> Family {
        match self {
            Self::Initial(_) => Family::Initial,
            Self::Successor(_) => Family::Successor,
        }
    }
    pub(super) fn activation(&self) -> &OwnedScopeActivation {
        match self {
            Self::Initial(value) => value.activation(),
            Self::Successor(value) => value.activation(),
        }
    }
    fn authority(&self) -> &SessionOwnerAuthority {
        match self {
            Self::Initial(value) => value.grant().authority(),
            Self::Successor(value) => value.grant().authority(),
        }
    }
    fn parent(&self) -> OwnedScopeParentAuthority {
        match self {
            Self::Initial(value) => {
                OwnedScopeParentAuthority::ExecutionDriver(Box::new(value.grant().clone()))
            }
            Self::Successor(value) => {
                OwnedScopeParentAuthority::SuccessorExecutionDriver(Box::new(value.grant().clone()))
            }
        }
    }
}
#[derive(Clone)]
pub(super) enum Notice {
    Initial(PendingInitialCompletion),
    Successor(PendingSuccessorCompletion),
}
impl Notice {
    pub(super) fn family(&self) -> Family {
        match self {
            Self::Initial(_) => Family::Initial,
            Self::Successor(_) => Family::Successor,
        }
    }
}
pub(super) enum Replay {
    Initial(InitialAdmissionReplay),
    Successor(PendingSuccessorReplay),
}
impl Replay {
    fn notice(&self) -> Notice {
        match self {
            Self::Initial(value) => {
                Notice::Initial(PendingInitialCompletion::Replayed(value.clone()))
            }
            Self::Successor(value) => {
                Notice::Successor(PendingSuccessorCompletion::Replayed(value.clone()))
            }
        }
    }
    fn family(&self) -> Family {
        match self {
            Self::Initial(_) => Family::Initial,
            Self::Successor(_) => Family::Successor,
        }
    }
}
pub(super) enum TaskOutput<T> {
    Business(T),
    Replayed(Replay),
    Stopped,
}
pub(super) struct Observation<T> {
    pub(super) family: Family,
    pub(super) main: Option<JoinedOwnedTask<T>>,
    pub(super) disposition: OwnedMainDisposition,
    pub(super) error: Option<PendingSuccessorError>,
    pub(super) replay: Option<Replay>,
    pub(super) _capacity: OwnedSemaphorePermit,
}
impl<T> Drop for Observation<T> {
    fn drop(&mut self) {
        if let Some(main) = &mut self.main {
            super::retain::discard(main.value.take());
        }
    }
}

pub(super) struct Task<T> {
    start: Option<oneshot::Sender<()>>,
    result: oneshot::Receiver<Observation<T>>,
    observed: Option<Observation<T>>,
    pub(super) control: Control,
}
impl<T: Send + 'static> Task<T> {
    pub(super) fn register<F, W>(
        owner: &super::PendingDriverTasks,
        store: Arc<dyn Store>,
        context: SessionContext,
        original: JoinedPreparation,
        source: Source,
        business: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(DriverMainContext, PreparationCompletion) -> W + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        let capacity = owner.capacity()?;
        let control = Control::new(source.family());
        let (start, ready) = oneshot::channel();
        let (send, result) = oneshot::channel();
        let current = control.clone();
        let ingress = super::super::driver::MainIngress::new();
        let seal = ingress.guard();
        let task = tokio::spawn(async move {
            let _seal = seal;
            if ready.await.is_err() || current.stopped() {
                return Ok(TaskOutput::Stopped);
            }
            execute::run(
                store, context, original, source, &current, ingress, business,
            )
            .await
        });
        owner.retain(task, control.clone(), send, capacity);
        Ok(Self {
            start: Some(start),
            result,
            observed: None,
            control,
        })
    }
    pub(super) fn start(&mut self) -> Result<(), Error> {
        self.start
            .take()
            .ok_or(Error::InvalidTransition)?
            .send(())
            .map_err(|()| Error::UnknownStoreOutcome)
    }
    pub(super) async fn observation(&mut self, family: Family) -> Result<Observation<T>, Error> {
        drop(self.start.take());
        if self.observed.is_none() {
            self.observed = Some(
                (&mut self.result)
                    .await
                    .map_err(|_| Error::UnknownStoreOutcome)?,
            );
        }
        let original = self.observed.as_ref().ok_or(Error::UnknownStoreOutcome)?;
        if original.family != family
            || original
                .replay
                .as_ref()
                .is_some_and(|replay| replay.family() != family)
        {
            // Do not release the original observation/capacity on a bad tag.
            return Err(Error::SemanticConflict);
        }
        self.observed.take().ok_or(Error::UnknownStoreOutcome)
    }
}
impl<T> Drop for Task<T> {
    fn drop(&mut self) {
        self.control.request_stop();
    }
}
