//! One source event definition shared by runtime and transactional adapters.
use super::{ToolContinuationError as Error, ToolContinuationSuspension};
use crate::{AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, RunBoundary};
impl ToolContinuationSuspension {
    /// Materialize the exact source boundary after actual owned resources join.
    /// # Errors
    /// Rejects malformed checkpoint descriptors or inconsistent source bounds.
    pub fn conclusion(&self, run: &crabber_core::Run) -> Result<AttemptConclusion, Error> {
        self.validate()?;
        let conclusion = AttemptConclusion {
            finish: AttemptMutation {
                id: super::derived_id("tool-continuation-suspend-attempt", &self.id),
                expected_revision: self.expected_revision,
                command: AttemptCommand::Finish {
                    target: self.attempt.clone(),
                    outcome: AttemptFinish::Suspended,
                },
            },
            boundary: RunBoundary::Suspended {
                checkpoint: super::checkpoint(self),
                usage: self.usage.clone(),
            },
            created_at: self.source_created_at,
            turn_id: self.source_turn_id.clone(),
            elapsed: (self.source_created_at - run.created_at)
                .try_into()
                .unwrap_or_default(),
        };
        conclusion.validate().map_err(|_| Error::InvalidRequest)?;
        Ok(conclusion)
    }
}
