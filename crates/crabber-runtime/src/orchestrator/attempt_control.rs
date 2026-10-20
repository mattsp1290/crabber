//! Parent reconciliation stays independent of plan/factory restoration.
use super::Orchestrator;
use crabber_session::{AttemptControlOutcome, AttemptError, AttemptRetirement, AttemptUncertainty};
impl Orchestrator {
    /// Revoke an exact lost/stopped parent without changing its child outcomes.
    /// # Errors
    /// Returns unsupported, scope, owner, revision, transition or storage failures.
    pub async fn mark_attempt_uncertain(
        &self,
        request: AttemptUncertainty,
    ) -> Result<AttemptControlOutcome, AttemptError> {
        self.store.mark_attempt_uncertain(request).await
    }
    /// Retire a reconciled parent after the host verifies whole-attempt joins.
    /// # Errors
    /// Unresolved effects/results/resources refuse retirement without mutation.
    pub async fn retire_attempt(
        &self,
        request: AttemptRetirement,
    ) -> Result<AttemptControlOutcome, AttemptError> {
        self.store.retire_attempt(request).await
    }
}
