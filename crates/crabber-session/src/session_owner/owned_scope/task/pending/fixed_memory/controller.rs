//! Same original controller state survives unbound task output and caller Drop.
use super::super::fixed_preparation::FixedPreparationOwner;
use super::super::{
    PendingInitialProducer, PendingInitialProducerControl, PendingInitialProducerObservation,
    PendingInitialProducerStatus, PendingSuccessorError,
    core::{self, Control},
};
use super::{FixedMemoryInitialControl, FixedMemoryInitialInput, builder};
use crate::public_session::v2::proof::Proof;
use crate::session_owner::{
    SessionOwnerError as Error, owned_scope::OwnedMainDisposition,
    preparation::PreparationCommitError,
};
use crabber_extension::LibraryScriptedSource;
use std::sync::{Arc, Mutex};

struct State {
    attached: bool,
    stopped: bool,
    preparation: Option<Arc<FixedPreparationOwner>>,
    inner: Option<PendingInitialProducerControl>,
    output: Option<PendingInitialProducerObservation>,
    cleanup_error: Option<PendingSuccessorError>,
    cleanup_done: bool,
}
pub(in crate::session_owner::owned_scope::task::pending) struct RetainedFixedController {
    core: Arc<FixedMemoryInitialControl>,
    state: Mutex<State>,
    #[cfg(test)]
    pub(super) hooks: Mutex<Option<Arc<super::super::fixed_preparation::FixedInitialTestHooks>>>,
}
impl RetainedFixedController {
    pub(super) fn new(core: Arc<FixedMemoryInitialControl>) -> Self {
        Self {
            core,
            state: Mutex::new(State {
                attached: false,
                stopped: false,
                preparation: None,
                inner: None,
                output: None,
                cleanup_error: None,
                cleanup_done: false,
            }),
            #[cfg(test)]
            hooks: Mutex::new(None),
        }
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn attach(
        self: &Arc<Self>,
        _task: tokio::task::Id,
        _ingress: Arc<crate::session_owner::owned_scope::task::driver::MainIngress>,
        control: &Control,
    ) -> Result<(), Error> {
        let mut state = self.lock();
        if state.attached {
            return Err(Error::InvalidTransition);
        }
        state.attached = true;
        drop(state);
        control.install_fixed_controller(self)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn request_stop(&self) {
        let (preparation, inner) = {
            let mut state = self.lock();
            state.stopped = true;
            (state.preparation.clone(), state.inner.clone())
        };
        if let Some(preparation) = preparation {
            preparation.request_stop();
        }
        if let Some(inner) = inner {
            inner.request_stop();
        }
    }
    fn preparation(&self, preparation: &Arc<FixedPreparationOwner>) -> Result<(), Error> {
        let stopped = {
            let mut state = self.lock();
            if state.preparation.is_some() {
                return Err(Error::InvalidTransition);
            }
            state.preparation = Some(preparation.clone());
            state.stopped
        };
        if stopped {
            preparation.request_stop();
        }
        Ok(())
    }
    fn inner(&self, inner: &PendingInitialProducerControl) -> Result<(), Error> {
        let stopped = {
            let mut state = self.lock();
            if state.inner.is_some() {
                return Err(Error::InvalidTransition);
            }
            state.inner = Some(inner.clone());
            state.stopped
        };
        if stopped {
            inner.request_stop();
        }
        Ok(())
    }
    pub(super) fn status(&self) -> Option<PendingInitialProducerStatus> {
        let inner = self.lock().inner.clone();
        inner.map(|inner| inner.status())
    }
    pub(super) fn take_output(
        &self,
    ) -> (
        Option<PendingInitialProducerObservation>,
        Option<PendingSuccessorError>,
    ) {
        let mut state = self.lock();
        (state.output.take(), state.cleanup_error.take())
    }
    pub(in crate::session_owner::owned_scope::task::pending) async fn after_join(
        &self,
        _disposition: OwnedMainDisposition,
    ) {
        self.request_stop();
        let preparation = self.lock().preparation.clone();
        if let Some(preparation) = preparation
            && let Err(error) = preparation.cleanup_partial().await
        {
            self.lock().cleanup_error = Some(error.into());
        }
        self.lock().cleanup_done = true;
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn resources_pending(&self) -> bool {
        let (done, preparation) = {
            let state = self.lock();
            (state.cleanup_done, state.preparation.clone())
        };
        !done || preparation.is_some_and(|preparation| preparation.resources_pending())
    }
    pub(in crate::session_owner::owned_scope::task::pending) async fn join_original_resources(
        &self,
    ) {
        let preparation = self.lock().preparation.clone();
        if let Some(preparation) = preparation {
            preparation.join_original_resources().await;
        }
    }
}

pub(super) async fn run(
    retained: Arc<RetainedFixedController>,
    control: Control,
    input: FixedMemoryInitialInput,
    source: LibraryScriptedSource,
) -> Result<core::TaskOutput<()>, PendingSuccessorError> {
    if control.stopped() {
        return Ok(core::TaskOutput::Stopped);
    }
    let mut proof = Proof::bounded(32 << 20);
    measure_context(&input.context, &mut proof)?;
    let context = input.context.clone();
    let expected_revision = input.input.expected_revision();
    let values = builder::build(input, &source, FixedMemoryInitialControl::now(), &mut proof)?;
    let reservation = builder::reservation(&values, &context, &mut proof)?;
    measure_context(&context, &mut proof)?;
    let preparation = FixedPreparationOwner::new(
        retained.core.clone(),
        context.clone(),
        values.operation,
        reservation,
    );
    #[cfg(test)]
    let test = retained.hooks.lock().unwrap().clone();
    #[cfg(test)]
    if let Some(test) = &test {
        preparation.install_test_hooks(test.clone())?;
    }
    retained.preparation(&preparation)?;
    retained
        .core
        .store()
        .fixed_initial_context(&context, expected_revision, &mut proof)?;
    if control.stopped() {
        return Ok(core::TaskOutput::Stopped);
    }
    let (context, original, loan) = preparation.run(&mut proof).await?;
    #[cfg(test)]
    if let Some(test) = &test {
        test.after_loan().await;
    }
    if control.stopped() {
        return Ok(core::TaskOutput::Stopped);
    }
    #[cfg(test)]
    let original = match &test {
        Some(test) => match test.original(original) {
            Some(original) => original,
            None => return Ok(core::TaskOutput::Stopped),
        },
        None => original,
    };
    #[cfg(test)]
    let source = match &test {
        Some(test) => test.source(source),
        None => source,
    };
    #[cfg(test)]
    let substituted_core = test.as_ref().and_then(|test| test.core());
    #[cfg(test)]
    let actual_core = substituted_core.as_ref().unwrap_or(&retained.core);
    #[cfg(not(test))]
    let actual_core = &retained.core;
    let mut inner = PendingInitialProducer::register_fixed_scripted(
        retained.core.tasks(),
        actual_core,
        context,
        original,
        source,
        loan,
    )?;
    retained.inner(&inner.control())?;
    #[cfg(test)]
    if let Some(test) = &test {
        test.after_inner().await;
    }
    inner.start()?;
    inner.join().await?;
    let observed = inner
        .take_fixed_observation()
        .ok_or(Error::UnknownStoreOutcome)?;
    retained.lock().output = Some(observed);
    Ok(core::TaskOutput::Business(()))
}

fn measure_context(
    context: &crabber_core::SessionContext,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    proof
        .measure(&(
            context.session_id(),
            context.workspace_id(),
            context.directory(),
        ))
        .map_err(|_| Error::Capacity.into())
}
