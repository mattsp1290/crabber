//! The runtime and Store materialize the same committed pause source event.
use super::{ApprovalError as Error, ApprovalSuspension};
use crate::{AttemptCommand, AttemptConclusion, AttemptFinish, AttemptMutation, RunBoundary};
use crabber_core::Run;
use time::OffsetDateTime;

impl ApprovalSuspension {
    /// Canonical native conclusion for a trusted custom Store transaction.
    /// This descriptor grants no authority and does not prove resource cleanup.
    /// # Errors
    /// Rejects invalid batch/presentation data before any native boundary write.
    pub fn conclusion(&self, run: &Run, now: OffsetDateTime) -> Result<AttemptConclusion, Error> {
        self.validate()?;
        let created_at = self.source_created_at.unwrap_or(now);
        let conclusion = AttemptConclusion {
            finish: AttemptMutation {
                id: super::derived_id("suspend-attempt", &self.id),
                expected_revision: self.expected_revision,
                command: AttemptCommand::Finish {
                    target: self.attempt.clone(),
                    outcome: AttemptFinish::Suspended,
                },
            },
            boundary: RunBoundary::Suspended {
                checkpoint: serde_json::json!({"version":4,"approval":self.continuation,"turns_used":self.turns_used}),
                usage: self.usage.clone(),
            },
            created_at,
            turn_id: self.source_turn_id.clone(),
            elapsed: (created_at - run.created_at).try_into().unwrap_or_default(),
        };
        conclusion.validate().map_err(super::attempt_error)?;
        Ok(conclusion)
    }
}
