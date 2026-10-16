//! Canonical public boundary materialization from verified immutable routing.
use crate::{AttemptConclusion, AttemptError, RunBoundary};
use crabber_core::{EventKind, EventRecord, RunId, SessionId};
use serde_json::json;
pub(crate) fn scoped(
    conclusion: &AttemptConclusion,
    session_id: &SessionId,
    run_id: &RunId,
) -> Result<EventRecord, AttemptError> {
    conclusion.validate()?;
    let (kind, payload, correlation) = match &conclusion.boundary {
        RunBoundary::Terminal { status, usage, .. } => (
            EventKind::RunSettled,
            json!({"status":status,"usage":usage,
                "input_tokens":usage.input_tokens,"output_tokens":usage.output_tokens,"duration_ms":conclusion.elapsed.as_millis(),"duration_ns":conclusion.elapsed.as_nanos()}),
            Some(run_id.to_string()),
        ),
        RunBoundary::Suspended { usage, .. } => {
            (EventKind::RunPaused, json!({"usage":usage}), None)
        }
    };
    Ok(EventRecord {
        cursor: None,
        session_id: session_id.clone(),
        run_id: run_id.clone(),
        turn_id: conclusion.turn_id.clone(),
        kind,
        payload,
        correlation,
        live_only: false,
        created_at: conclusion.created_at,
    })
}
