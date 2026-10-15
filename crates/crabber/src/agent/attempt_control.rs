//! Parent controls do not initialize extensions or acquire an execution plan.
use super::Agent;
use crabber_session::{AttemptControlOutcome, AttemptError, AttemptRetirement, AttemptUncertainty};
impl Agent {
    /// Revoke an exact lost/stopped parent without changing its child outcomes.
    /// # Errors
    /// Returns unsupported, scope, owner, revision, transition or storage failures.
    pub async fn mark_attempt_uncertain(
        &self,
        request: AttemptUncertainty,
    ) -> Result<AttemptControlOutcome, AttemptError> {
        self.runtime.mark_attempt_uncertain(request).await
    }
    /// Retire a reconciled parent after the host verifies whole-attempt joins.
    /// # Errors
    /// Unresolved effects/results/resources refuse retirement without mutation.
    pub async fn retire_attempt(
        &self,
        request: AttemptRetirement,
    ) -> Result<AttemptControlOutcome, AttemptError> {
        self.runtime.retire_attempt(request).await
    }
}
