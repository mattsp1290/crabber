//! The host registers `FnOnce` before requesting activation. Capturing a future
//! whose constructor already performed effects would violate this boundary.
use super::{Error, OwnedDispatchPermit, OwnedMainDisposition, OwnedScopeActivation};
mod driver;
pub use driver::DriverMainContext;
use driver::MainIngress;
use std::{future::Future, sync::Arc};
use tokio::{sync::oneshot, task::JoinHandle};

/// A retained task waiting behind a closed local gate. Register this with the
/// host supervisor before asking storage to activate the exact registration.
/// Dropping this handle closes the gate but supplies no cleanup/join proof.
pub struct OwnedTaskRegistration<T> {
    activation: OwnedScopeActivation,
    gate: Option<oneshot::Sender<Option<DriverMainContext>>>,
    kind: RegistrationKind,
    task: Option<JoinHandle<Option<T>>>,
    joined: Option<JoinedOwnedTask<T>>,
}
enum RegistrationKind {
    Generic,
    Driver(Arc<MainIngress>),
}
impl<T: Send + 'static> OwnedTaskRegistration<T> {
    /// The factory is invoked only after a fresh matching dispatch permit.
    /// Resources registered by the factory need independent actual cleanup
    /// joins; observing this main task alone never proves resource quiescence.
    /// # Errors
    /// Refuses malformed scope identity before registering or invoking work.
    pub fn register<F, W>(source: super::OwnedScopeSource, factory: F) -> Result<Self, Error>
    where
        F: FnOnce() -> W + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        Self::register_inner(source, RegistrationKind::Generic, move |_| Some(factory()))
    }
    /// Register an initial Driver factory behind its original task gate.
    /// Only a matching fresh activation permit delivers the opaque context.
    /// Generic registration and metadata replay cannot create this context.
    /// Resources still need independently observed cleanup; this proves no
    /// complete Run membership or whole-resource policy.
    /// # Errors
    /// Refuses malformed Driver metadata before registering or invoking work.
    pub fn register_driver<F, W>(
        grant: &crate::session_owner::driver::ExecutionDriverGrant,
        factory: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(DriverMainContext) -> W + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        grant.original_request().fingerprint()?;
        let source = grant.scope_source()?;
        if &source.session != grant.original_request().context.session_id() {
            return Err(Error::SemanticConflict);
        }
        Self::register_inner(
            source,
            RegistrationKind::Driver(MainIngress::new()),
            move |context| context.map(factory),
        )
    }
    fn register_inner<F, W>(
        source: super::OwnedScopeSource,
        kind: RegistrationKind,
        factory: F,
    ) -> Result<Self, Error>
    where
        F: FnOnce(Option<DriverMainContext>) -> Option<W> + Send + 'static,
        W: Future<Output = T> + Send + 'static,
    {
        let activation = OwnedScopeActivation::new(source)?;
        let (gate, ready) = oneshot::channel();
        let seal = match &kind {
            RegistrationKind::Generic => None,
            RegistrationKind::Driver(ingress) => Some(ingress.guard()),
        };
        let task = tokio::spawn(async move {
            // The captured guard also closes if this future is never polled.
            let _seal = seal;
            let context = ready.await.ok()?;
            Some(factory(context)?.await)
        });
        Ok(Self {
            activation,
            gate: Some(gate),
            kind,
            task: Some(task),
            joined: None,
        })
    }
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
    /// Open this registered gate using fresh matching activation authority.
    /// Refusal retains the actual task and closed gate for explicit cleanup.
    /// # Errors
    /// Refuses another scope, nonce, or a gate already consumed/closed.
    pub fn dispatch(&mut self, permit: OwnedDispatchPermit) -> Result<(), Error> {
        let OwnedDispatchPermit { activation } = permit;
        if self.activation != activation {
            return Err(Error::SemanticConflict);
        }
        let gate = self.gate.take().ok_or(Error::InvalidTransition)?;
        let context = match &self.kind {
            RegistrationKind::Generic => None,
            RegistrationKind::Driver(ingress) => {
                Some(DriverMainContext::dispatched(activation, ingress.clone()))
            }
        };
        gate.send(context).map_err(|_| Error::UnknownStoreOutcome)
    }
    /// Close an unacknowledged gate and observe the actual waiting task join.
    /// Interrupted observation retains the same actual task and returned value.
    /// An already dispatched task is observed through its actual join too.
    /// # Errors
    /// Refuses an internally missing task; never substitutes a synthetic join.
    pub async fn join(&mut self) -> Result<&JoinedOwnedTask<T>, Error> {
        drop(self.gate.take());
        if self.joined.is_none() {
            let (disposition, value) =
                match self.task.as_mut().ok_or(Error::UnknownStoreOutcome)?.await {
                    Ok(Some(value)) => (OwnedMainDisposition::Returned, Some(value)),
                    Ok(None) => (OwnedMainDisposition::NoDispatch, None),
                    Err(error) if error.is_panic() => {
                        discard_panic(error);
                        (OwnedMainDisposition::Panicked, None)
                    }
                    Err(_) => (OwnedMainDisposition::Aborted, None),
                };
            self.task = None;
            self.joined = Some(JoinedOwnedTask {
                activation: self.activation.clone(),
                disposition,
                value,
            });
        }
        self.joined.as_ref().ok_or(Error::UnknownStoreOutcome)
    }

    /// Move the actual returned value into its next retained owner exactly
    /// once. The original main-task evidence stays in this registration for
    /// separately observed resource cleanup. Taking the value never proves
    /// that its resources have joined or that a durable handoff committed.
    /// # Errors
    /// Returns the same observation errors as `join`; interrupted observation
    /// retains both the actual task and its value.
    pub async fn take_value(&mut self) -> Result<Option<T>, Error> {
        self.join().await?;
        Ok(self
            .joined
            .as_mut()
            .ok_or(Error::UnknownStoreOutcome)?
            .value
            .take())
    }
}

/// Physical main-task evidence, inseparable from the original registration.
/// There is no constructor from a returned value, receipt, timeout or boolean.
/// A later Store join also requires separately observed resource cleanup.
pub struct JoinedOwnedTask<T> {
    activation: OwnedScopeActivation,
    disposition: OwnedMainDisposition,
    value: Option<T>,
}
impl<T> JoinedOwnedTask<T> {
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.activation
    }
    #[must_use]
    pub fn disposition(&self) -> OwnedMainDisposition {
        self.disposition
    }
    #[must_use]
    pub fn value(&self) -> Option<&T> {
        self.value.as_ref()
    }
}

// A user panic payload may itself panic when dropped. Dispose it without
// unwinding through the owner before its actual join observation is retained.
pub(super) fn discard_panic(error: tokio::task::JoinError) {
    let payload = error.into_panic();
    if let Err(nested) = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload))) {
        std::mem::forget(nested);
    }
}

#[cfg(test)]
#[path = "task/driver_tests.rs"]
mod driver_tests;

#[cfg(test)]
#[path = "task/ingress_tests.rs"]
mod ingress_tests;
