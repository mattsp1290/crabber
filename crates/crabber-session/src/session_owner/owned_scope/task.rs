//! The host registers `FnOnce` before requesting activation. Capturing a future
//! whose constructor already performed effects would violate this boundary.
use super::{Error, OwnedDispatchPermit, OwnedMainDisposition, OwnedScopeActivation};
use std::future::Future;
use tokio::{sync::oneshot, task::JoinHandle};

/// A retained task waiting behind a closed local gate. Register this with the
/// host supervisor before asking storage to activate the exact registration.
/// Dropping this handle closes the gate but supplies no cleanup/join proof.
pub struct OwnedTaskRegistration<T> {
    activation: OwnedScopeActivation,
    gate: Option<oneshot::Sender<()>>,
    task: Option<JoinHandle<Option<T>>>,
    joined: Option<JoinedOwnedTask<T>>,
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
        let activation = OwnedScopeActivation::new(source)?;
        let (gate, ready) = oneshot::channel();
        let task = tokio::spawn(async move {
            ready.await.ok()?;
            Some(factory().await)
        });
        Ok(Self {
            activation,
            gate: Some(gate),
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
        self.gate
            .take()
            .ok_or(Error::InvalidTransition)?
            .send(())
            .map_err(|()| Error::UnknownStoreOutcome)
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
                    Err(error) if error.is_panic() => (OwnedMainDisposition::Panicked, None),
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
