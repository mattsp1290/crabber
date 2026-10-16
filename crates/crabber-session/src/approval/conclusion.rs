//! Exact native pause descriptor from immutable scope facts; no authority.
use crate::{
    ApprovalError as Error, ApprovalSuspension, AttemptCommand, AttemptConclusion, AttemptFinish,
    AttemptMutation, RunBoundary,
};
use time::OffsetDateTime;
/// Caller must validate the batch; both full-Run and immutable-origin entrypoints do so.
pub(crate) fn at(
    batch: &ApprovalSuspension,
    run_created_at: OffsetDateTime,
    now: OffsetDateTime,
) -> Result<AttemptConclusion, Error> {
    let created_at = batch.source_created_at.unwrap_or(now);
    let conclusion = AttemptConclusion {
        finish: AttemptMutation {
            id: crate::approval::derived_id("suspend-attempt", &batch.id),
            expected_revision: batch.expected_revision,
            command: AttemptCommand::Finish {
                target: batch.attempt.clone(),
                outcome: AttemptFinish::Suspended,
            },
        },
        boundary: RunBoundary::Suspended {
            checkpoint: serde_json::json!({"version":4,"approval":batch.continuation,"turns_used":batch.turns_used}),
            usage: batch.usage.clone(),
        },
        created_at,
        turn_id: batch.source_turn_id.clone(),
        elapsed: (created_at - run_created_at).try_into().unwrap_or_default(),
    };
    conclusion
        .validate()
        .map_err(crate::approval::attempt_error)?;
    Ok(conclusion)
}
