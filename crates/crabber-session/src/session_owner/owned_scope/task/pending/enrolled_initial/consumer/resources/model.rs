//! Literal source construction, polling and disposal stay under its actual caller.
use super::{Arc, InitialResources, Proof};
use crate::session_owner::owned_scope::task::pending::enrolled_initial::{NATIVE_BOUND, observe};
use crabber_core::SessionContext;
use crabber_extension::{LibraryScriptedStream, with_callback_invocations};
use futures::{Stream, StreamExt};
use std::ops::{Deref, DerefMut};

type Delta = <LibraryScriptedStream as Stream>::Item;
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) enum ModelFailure {
    Semantic,
    Observation,
}
/// Actual returned values also get independent Drop containment, including on
/// cancellation between their named original future and subsequent use.
struct LiteralValue<T> {
    value: Option<T>,
    resources: Arc<InitialResources>,
}
impl<T> LiteralValue<T> {
    fn new(value: T, resources: &Arc<InitialResources>) -> Self {
        Self {
            value: Some(value),
            resources: resources.clone(),
        }
    }
}
impl<T> Deref for LiteralValue<T> {
    type Target = T;
    fn deref(&self) -> &T {
        self.value.as_ref().expect("retained literal value")
    }
}
impl<T> DerefMut for LiteralValue<T> {
    fn deref_mut(&mut self) -> &mut T {
        self.value.as_mut().expect("retained literal value")
    }
}
impl<T> Drop for LiteralValue<T> {
    fn drop(&mut self) {
        self.resources.dispose(self.value.take());
    }
}
impl InitialResources {
    pub(in crate::session_owner::owned_scope::task::pending::enrolled_initial) async fn model(
        self: &Arc<Self>,
        context: SessionContext,
        proof: &mut Proof,
    ) -> Result<String, ModelFailure> {
        let capability = self.capability().map_err(|_| ModelFailure::Observation)?;
        with_callback_invocations(capability, self.model_scoped(context, proof)).await
    }
    async fn model_scoped(
        self: &Arc<Self>,
        context: SessionContext,
        proof: &mut Proof,
    ) -> Result<String, ModelFailure> {
        let source = self
            .take_pristine()
            .map_err(|_| ModelFailure::Observation)?;
        let (selection, responses) = source.recipe();
        // The concrete leaf clones the selected response during stream creation.
        // Charge that real new text and the route before either clone occurs.
        proof
            .measure(&(
                &selection.provider_id,
                &selection.model_id,
                responses.first(),
            ))
            .map_err(|_| ModelFailure::Observation)?;
        let selection = selection.clone();
        let observed = observe::original(
            || Box::pin(source.activate(context)),
            |result| {
                *self
                    .activation
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
            },
            Some(tokio::time::Instant::now() + NATIVE_BOUND),
            || {},
            || self.fail(),
        )
        .await;
        if observed.is_err() {
            self.fail();
            return Err(ModelFailure::Observation);
        }
        let active = self
            .activation
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(ModelFailure::Observation)?
            .map_err(|_| ModelFailure::Semantic)?;
        self.observe_source(active.observation());
        let active = LiteralValue::new(active, self);
        let observed = observe::original(
            || Box::pin(active.stream(selection)),
            |result| {
                *self
                    .stream
                    .lock()
                    .unwrap_or_else(std::sync::PoisonError::into_inner) = Some(result);
            },
            Some(tokio::time::Instant::now() + NATIVE_BOUND),
            || {},
            || self.fail(),
        )
        .await;
        if observed.is_err() {
            self.fail();
            return Err(ModelFailure::Observation);
        }
        let stream = self
            .stream
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner)
            .take()
            .ok_or(ModelFailure::Observation)?
            .map_err(|_| ModelFailure::Semantic)?;
        let mut stream = LiteralValue::new(stream, self);
        let result = self.text(&mut stream).await;
        drop(stream);
        drop(active);
        if self.lock().failed {
            return Err(ModelFailure::Observation);
        }
        result
    }
    async fn text(
        self: &Arc<Self>,
        stream: &mut LibraryScriptedStream,
    ) -> Result<String, ModelFailure> {
        let mut text = None;
        loop {
            let mut staged = None;
            let observed = observe::original(
                || Box::pin(stream.next()),
                |value| staged = Some(value),
                Some(tokio::time::Instant::now() + NATIVE_BOUND),
                || {},
                || self.fail(),
            )
            .await;
            if observed.is_err() {
                self.fail();
                return Err(ModelFailure::Observation);
            }
            match staged.ok_or(ModelFailure::Observation)? {
                Some(Delta::TextDelta(value)) if text.is_none() => text = Some(value),
                Some(Delta::Completed) => return text.ok_or(ModelFailure::Semantic),
                None
                | Some(
                    Delta::Error(_)
                    | Delta::TextDelta(_)
                    | Delta::ReasoningDelta(_)
                    | Delta::Reasoning { .. }
                    | Delta::ReasoningSignature { .. }
                    | Delta::ToolCallStart { .. }
                    | Delta::ToolCallArgsDelta { .. }
                    | Delta::ToolCallDone { .. }
                    | Delta::ProviderState { .. }
                    | Delta::FinishReason(_)
                    | Delta::Usage(_),
                ) => return Err(ModelFailure::Semantic),
            }
        }
    }
}
