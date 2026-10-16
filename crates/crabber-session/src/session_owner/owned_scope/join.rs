//! An exact main join plus an actual retained cleanup-task join. Host bridges
//! must put sealed scope joins and all independent dependency releases in that
//! cleanup task; returning a result or dropping a plan supplies no evidence.
use super::{Error, JoinedOwnedTask, OwnedMainDisposition, OwnedScopeActivation};
use crate::session_owner::{MutateSessionOwner, SessionOwnerCommand, SessionOwnerTermination};
use crabber_core::{InputFingerprint, SessionOwnerOperationId};
use serde::{Deserialize, Serialize};

/// Finite cleanup failure. A failed/panicked cleanup yields no Join proof.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum OwnedCleanupFailure {
    Failed,
    Panicked,
    Aborted,
}

/// Opaque immutable observation, cloneable only to retry the same authenticated
/// completion after ACK loss. It never contains a dispatch permit or resource.
#[derive(Clone)]
pub struct JoinedOwnedScope {
    pub(crate) source: JoinSource,
}
#[derive(Clone, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct JoinSource {
    pub activation: OwnedScopeActivation,
    pub disposition: OwnedMainDisposition,
}
/// Retains the actual cleanup join across interrupted observation. The trusted
/// host must supply the task that owns the sealed resource joins and every
/// independent dependency-release acknowledgement for this exact registration.
/// An unrelated task violates the host bridge contract; there is no constructor
/// from a cleanup result, receipt, timeout or Drop notification.
pub struct OwnedCleanupTask {
    activation: OwnedScopeActivation,
    task: Option<tokio::task::JoinHandle<Result<(), OwnedCleanupFailure>>>,
    result: Option<Result<(), OwnedCleanupFailure>>,
}
impl OwnedCleanupTask {
    #[must_use]
    pub fn from_host_task(
        activation: OwnedScopeActivation,
        task: tokio::task::JoinHandle<Result<(), OwnedCleanupFailure>>,
    ) -> Self {
        Self {
            activation,
            task: Some(task),
            result: None,
        }
    }
    async fn observe(&mut self) -> Result<(), OwnedCleanupFailure> {
        if let Some(result) = self.result {
            return result;
        }
        let result = match self.task.as_mut().ok_or(OwnedCleanupFailure::Failed)?.await {
            Ok(result) => result,
            Err(error) if error.is_panic() => Err(OwnedCleanupFailure::Panicked),
            Err(_) => Err(OwnedCleanupFailure::Aborted),
        };
        self.task = None;
        self.result = Some(result);
        result
    }
}
impl<T> JoinedOwnedTask<T> {
    /// Observe the actual separately retained resource cleanup task. Interrupted
    /// observation keeps its join handle in `cleanup`. Keep the main value and
    /// physical cleanup owner on failures; no task is redispatched here.
    /// # Errors
    /// Refuses another registration and failed/panicked/aborted cleanup.
    pub async fn observe_resources(
        &self,
        cleanup: &mut OwnedCleanupTask,
    ) -> Result<JoinedOwnedScope, OwnedCleanupFailure> {
        if cleanup.activation != *self.activation() {
            return Err(OwnedCleanupFailure::Failed);
        }
        cleanup.observe().await?;
        Ok(JoinedOwnedScope {
            source: JoinSource {
                activation: self.activation().clone(),
                disposition: self.disposition(),
            },
        })
    }
}
impl JoinedOwnedScope {
    #[must_use]
    pub fn activation(&self) -> &OwnedScopeActivation {
        &self.source.activation
    }
    /// Original metadata only.
    /// # Errors
    /// Refuses malformed scope identities.
    pub fn request(&self) -> Result<MutateSessionOwner, Error> {
        self.source.request()
    }
    /// Source-bound metadata digest; never a join or dispatch grant.
    /// # Errors
    /// Refuses malformed or oversized source metadata.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.source.fingerprint()
    }
    /// Immutable proof inspection for trusted adapter verification. The returned
    /// serializable source is metadata and cannot reconstruct this observation.
    #[must_use]
    pub fn inspection(&self) -> &JoinSource {
        &self.source
    }
}
impl JoinSource {
    /// Original metadata only.
    /// # Errors
    /// Refuses malformed scope identities.
    pub fn request(&self) -> Result<MutateSessionOwner, Error> {
        let activation = &self.activation;
        let key = super::super::validation::digest(&(
            "crabber.owned-scope.join-key.v1",
            activation.operation_id(),
        ))?;
        Ok(MutateSessionOwner {
            id: SessionOwnerOperationId(format!("owned-join-{}", key.as_str())),
            session_id: activation.session_id().clone(),
            target: activation.target().clone(),
            command: SessionOwnerCommand::Join {
                termination: match self.disposition {
                    OwnedMainDisposition::Returned => SessionOwnerTermination::Returned,
                    OwnedMainDisposition::Panicked => SessionOwnerTermination::Panicked,
                    OwnedMainDisposition::NoDispatch | OwnedMainDisposition::Aborted => {
                        SessionOwnerTermination::Cancelled
                    }
                },
            },
        })
    }
    /// Source-bound metadata digest; never a join or dispatch grant.
    /// # Errors
    /// Refuses malformed or oversized source metadata.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        super::super::validation::digest(&(
            "crabber.owned-scope.join.v1",
            self.request()?.fingerprint()?,
            self.activation.fingerprint()?,
            self.disposition,
        ))
    }
}
