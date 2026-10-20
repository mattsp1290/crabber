//! Exact static suspension provenance, independent of current continuation state.
use crate::{ApprovalError as Error, ApprovalReceipt, ApprovalSuspension, AttemptReceipt};
use crabber_core::EventRecord;
pub(crate) fn verify_native_origin(
    batch: &ApprovalSuspension,
    suspension: &ApprovalReceipt,
    audit: &EventRecord,
    native: &AttemptReceipt,
    native_audit: &EventRecord,
    source: &EventRecord,
) -> Result<(), Error> {
    crate::continuation_origin::approval(
        batch,
        &suspension.session_id,
        &suspension.run_id,
        &batch.continuation,
        suspension,
    )?;
    crate::approval::verify_receipt(suspension, audit)?;
    crate::attempt::verify_receipt(native, native_audit).map_err(|_| Error::UnknownStoreOutcome)?;
    // Elapsed is excluded from suspension semantics and its RunPaused frame.
    // No current Run or reconstructed creation-time/authority is required.
    let conclusion = crate::AttemptConclusion {
        finish: crate::AttemptMutation {
            id: crate::approval::derived_id("suspend-attempt", &batch.id),
            expected_revision: batch.expected_revision,
            command: crate::AttemptCommand::Finish {
                target: batch.attempt.clone(),
                outcome: crate::AttemptFinish::Suspended,
            },
        },
        boundary: crate::RunBoundary::Suspended {
            checkpoint: serde_json::json!({"version":4,"approval":batch.continuation,"turns_used":batch.turns_used}),
            usage: batch.usage.clone(),
        },
        created_at: batch.source_created_at.unwrap_or(audit.created_at),
        turn_id: batch.source_turn_id.clone(),
        elapsed: std::time::Duration::ZERO,
    };
    conclusion
        .verify_replay(native)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let mut expected = crate::attempt_conclusion::materialize_scope(
        &conclusion,
        &suspension.session_id,
        &suspension.run_id,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    expected.cursor = source.cursor;
    if native.attempt.session_id != suspension.session_id
        || native.attempt.run_id != suspension.run_id
        || native.revision <= batch.expected_revision
        || suspension.revision
            != native
                .revision
                .next()
                .map_err(|_| Error::UnknownStoreOutcome)?
        || native.event_cursor >= suspension.event_cursor
        || source.cursor.is_none_or(|c| c >= native.event_cursor)
        || serde_json::to_value(source).map_err(|_| Error::UnknownStoreOutcome)?
            != serde_json::to_value(expected).map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
