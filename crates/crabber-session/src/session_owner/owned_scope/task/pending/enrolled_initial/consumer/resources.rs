//! The same concrete source and actual caller-resource owner through joined retry.
mod model;
use super::super::{Error, NATIVE_BOUND, PreparationCommitError, Proof, RetainedInitialSource};
use crate::native_enrollment::pending_initial::Correlation;
use crate::session_owner::preparation::JoinedPreparation;
use crabber_core::SessionContext;
use crabber_extension::{
    CallbackInvocationCapability, CallbackInvocationOwner, LibraryScriptedSource,
    LibraryScriptedSourceObservation, ToolResourceOwner,
};
pub(super) use model::ModelFailure;
use std::sync::{Arc, Mutex, OnceLock};

#[derive(serde::Serialize)]
struct Recipe {
    provider_id: String,
    model_id: String,
    responses: Vec<String>,
}
#[derive(Clone, Copy, PartialEq, Eq)]
enum Attachment {
    NotEntered,
    Attaching,
    Attached,
    Failed,
    Drained,
}
struct State {
    task: Option<tokio::task::Id>,
    attachment: Attachment,
    pristine: Option<LibraryScriptedSource>,
    callbacks: Option<CallbackInvocationOwner>,
    capability: Option<CallbackInvocationCapability>,
    observation: Option<LibraryScriptedSourceObservation>,
    failed: bool,
    calls: Vec<crabber_core::ControlId>,
}
pub(in crate::session_owner::owned_scope::task::pending) struct InitialResources {
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) owner:
        ToolResourceOwner,
    source: Arc<RetainedInitialSource>,
    recipe: Recipe,
    activation: Mutex<
        Option<Result<crabber_extension::LibraryScriptedActive, crabber_extension::ExtensionError>>,
    >,
    stream: Mutex<
        Option<
            Result<
                crabber_extension::LibraryScriptedStream,
                crabber_extension::LibraryScriptedError,
            >,
        >,
    >,
    state: Mutex<State>,
    joined_calls: OnceLock<Vec<crabber_core::ControlId>>,
}
impl InitialResources {
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn new(
        pristine: LibraryScriptedSource,
        source: Arc<RetainedInitialSource>,
        proof: &mut Proof,
    ) -> Result<Arc<Self>, PreparationCommitError> {
        #[derive(serde::Serialize)]
        struct View<'a> {
            provider_id: &'a str,
            model_id: &'a str,
            responses: &'a [String],
        }
        let (selection, responses) = pristine.recipe();
        proof
            .measure(&View {
                provider_id: &selection.provider_id,
                model_id: &selection.model_id,
                responses,
            })
            .map_err(|_| Error::Capacity)?;
        let recipe = Recipe {
            provider_id: selection.provider_id.clone(),
            model_id: selection.model_id.clone(),
            responses: responses.to_vec(),
        };
        Ok(Arc::new(Self {
            owner: ToolResourceOwner::new(),
            source,
            recipe,
            state: Mutex::new(State {
                task: None,
                attachment: Attachment::NotEntered,
                pristine: Some(pristine),
                callbacks: None,
                capability: None,
                observation: None,
                failed: false,
                calls: Vec::new(),
            }),
            activation: Mutex::new(None),
            stream: Mutex::new(None),
            joined_calls: OnceLock::new(),
        }))
    }
    fn lock(&self) -> std::sync::MutexGuard<'_, State> {
        self.state
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn registered(
        &self,
        task: tokio::task::Id,
    ) -> Result<(), Error> {
        let mut state = self.lock();
        if state.task.is_some() || state.attachment != Attachment::NotEntered {
            return Err(Error::InvalidTransition);
        }
        state.task = Some(task);
        Ok(())
    }
    // Caller holds owner.guard() before this function. The actual observer is
    // installed only inside the already-supervised original task, never prestart.
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn attach(
        &self,
    ) -> Result<(), Error> {
        {
            let mut state = self.lock();
            if state.task != tokio::task::try_id() || state.attachment != Attachment::NotEntered {
                return Err(Error::InvalidTransition);
            }
            state.attachment = Attachment::Attaching;
        }
        let Ok(callbacks) = CallbackInvocationOwner::attach(self.owner.resources()) else {
            self.lock().attachment = Attachment::Failed;
            return Err(Error::UnknownStoreOutcome);
        };
        // No await or fallible step separates actual attach from retention.
        let capability = callbacks.capability();
        let mut state = self.lock();
        state.callbacks = Some(callbacks);
        state.capability = Some(capability);
        state.attachment = Attachment::Attached;
        Ok(())
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn association(
        &self,
    ) -> Result<InitialResourceAssociation<'_>, Error> {
        let state = self.lock();
        if (self.joined_calls.get().is_none() && state.task != tokio::task::try_id())
            || !matches!(state.attachment, Attachment::Attached | Attachment::Drained)
        {
            return Err(Error::InvalidTransition);
        }
        Ok(InitialResourceAssociation { resources: self })
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn register_call(
        &self,
        call: &crabber_core::ControlId,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        proof.measure(call).map_err(|_| Error::Capacity)?;
        let mut state = self.lock();
        if state.task != tokio::task::try_id()
            || self.joined_calls.get().is_some()
            || state.calls.contains(call)
        {
            return Err(Error::InvalidTransition.into());
        }
        state.calls.push(call.clone());
        Ok(())
    }
    // Called only by the existing retainer after its original JoinHandle await.
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn original_joined(
        &self,
    ) -> Result<(), Error> {
        if self.joined_calls.get().is_some() {
            return Err(Error::InvalidTransition);
        }
        let calls = std::mem::take(&mut self.lock().calls);
        self.joined_calls
            .set(calls)
            .map_err(|_| Error::InvalidTransition)
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn capability(
        &self,
    ) -> Result<CallbackInvocationCapability, Error> {
        self.lock()
            .capability
            .clone()
            .ok_or(Error::InvalidTransition)
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn take_pristine(
        &self,
    ) -> Result<LibraryScriptedSource, Error> {
        self.lock().pristine.take().ok_or(Error::InvalidTransition)
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn observe_source(
        &self,
        observation: LibraryScriptedSourceObservation,
    ) {
        self.lock().observation = Some(observation);
    }
    fn dispose<T>(&self, value: T) {
        if let Err(payload) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(value)))
        {
            self.fail();
            crate::session_owner::owned_scope::task::pending::retain::discard(payload);
        }
    }
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) fn fail(&self) {
        self.lock().failed = true;
        self.owner.close();
    }
    /// A signal is never a successful drain. The same owner/observations remain
    /// retained after timeouts, including on abort before source activation.
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) async fn drain(
        &self,
    ) -> Result<DrainedInitialResources<'_>, Error> {
        self.owner.close();
        let (callbacks, observation, pristine) = {
            let mut state = self.lock();
            (
                state.callbacks.take(),
                state.observation.clone(),
                state.pristine.take(),
            )
        };
        let activation = self
            .activation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take();
        if let Some(Ok(active)) = &activation {
            self.observe_source(active.observation());
        }
        let dispose = || {
            self.dispose(stream);
            self.dispose(activation);
            self.dispose(pristine);
            self.dispose(callbacks);
        };
        // Stage values are moved out before disposal and remain scoped to their
        // actual creator even when the original task was aborted after Ready.
        if let Ok(capability) = self.capability() {
            crabber_extension::with_callback_invocations(capability, async {
                dispose();
            })
            .await;
        } else {
            dispose();
        }
        let observation = self.lock().observation.clone().or(observation);
        if let Some(observation) = observation {
            observation
                .close(NATIVE_BOUND)
                .await
                .map_err(|_| Error::UnknownStoreOutcome)?;
        }
        let report = self.owner.drain(NATIVE_BOUND).await;
        if !report.sealed
            || report.pending != 0
            || !report.failures.is_empty()
            || self.lock().failed
        {
            return Err(Error::UnknownStoreOutcome);
        }
        self.lock().attachment = Attachment::Drained;
        Ok(DrainedInitialResources { resources: self })
    }
}

/// Borrowed actual holder identity. It is not a serializable resource declaration.
pub(crate) struct InitialResourceAssociation<'a> {
    resources: &'a InitialResources,
}
impl InitialResourceAssociation<'_> {
    pub(in crate::session_owner::owned_scope::task::pending) fn same_holder(
        &self,
        other: &Self,
    ) -> bool {
        std::ptr::eq(self.resources, other.resources)
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn belongs_to(
        &self,
        holder: Option<&InitialResources>,
    ) -> bool {
        holder.is_some_and(|holder| std::ptr::eq(holder, self.resources))
    }
    pub(in crate::session_owner::owned_scope::task::pending) fn verify_policy_close(
        &self,
        original: &JoinedPreparation,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        self.verify_close(original, &self.resources.source.context, expected, proof)
            .map(|_| ())
    }

    pub(crate) fn verify(
        &self,
        original: &JoinedPreparation,
        context: &SessionContext,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        proof
            .measure(&self.resources.recipe)
            .map_err(|_| Error::Capacity)?;
        proof
            .measure(&(
                context.session_id(),
                context.workspace_id(),
                context.directory(),
            ))
            .map_err(|_| Error::Capacity)?;
        if !std::ptr::eq(original, &raw const self.resources.source.original)
            || context.session_id() != self.resources.source.context.session_id()
            || context.workspace_id() != self.resources.source.context.workspace_id()
            || context.directory() != self.resources.source.context.directory()
            || &expected.key.session_id != context.session_id()
        {
            return Err(Error::IdentityMismatch.into());
        }
        Ok(())
    }
    pub(crate) fn verify_close(
        &self,
        original: &JoinedPreparation,
        context: &SessionContext,
        expected: &Correlation,
        proof: &mut Proof,
    ) -> Result<&[crabber_core::ControlId], PreparationCommitError> {
        self.verify(original, context, expected, proof)?;
        let calls = self
            .resources
            .joined_calls
            .get()
            .ok_or(Error::InvalidTransition)?;
        proof.measure(calls).map_err(|_| Error::Capacity)?;
        let (attachment, failed) = {
            let state = self.resources.lock();
            (state.attachment, state.failed)
        };
        let report = self.resources.owner.snapshot();
        if attachment != Attachment::Drained
            || failed
            || !report.sealed
            || report.pending != 0
            || !report.failures.is_empty()
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
        Ok(calls)
    }
}
/// Constructor-private result of actual sealed resource joins, tied to one holder.
pub(crate) struct DrainedInitialResources<'a> {
    resources: &'a InitialResources,
}
impl DrainedInitialResources<'_> {
    pub(crate) fn verify(
        &self,
        association: &InitialResourceAssociation<'_>,
        proof: &mut Proof,
    ) -> Result<(), PreparationCommitError> {
        proof
            .measure(&self.resources.recipe)
            .map_err(|_| Error::Capacity)?;
        let (attachment, failed) = {
            let state = self.resources.lock();
            (state.attachment, state.failed)
        };
        if !std::ptr::eq(self.resources, association.resources)
            || attachment != Attachment::Drained
            || failed
        {
            return Err(Error::InvalidTransition.into());
        }
        let report = self.resources.owner.snapshot();
        if !report.sealed || report.pending != 0 || !report.failures.is_empty() {
            return Err(Error::UnknownStoreOutcome.into());
        }
        Ok(())
    }
}
