//! Shared generic event-ingress checks; atomic publishers use their own validators.
use crate::StoreError;
use crabber_core::{EventRecord, Run};

pub(crate) fn validate(run: &Run, event: &EventRecord) -> Result<(), StoreError> {
    if event.kind == crabber_core::EventKind::ToolCallPending
        && event.payload.get("frontend_origin").is_some()
    {
        return Err(StoreError::Validation(
            "frontend origin requires atomic staging".into(),
        ));
    }
    if matches!(
        event.kind,
        crabber_core::EventKind::RunControlChanged
            | crabber_core::EventKind::PublicStateSnapshot
            | crabber_core::EventKind::PublicStateDelta
            | crabber_core::EventKind::ActivitySnapshot
            | crabber_core::EventKind::ActivityDelta
            | crabber_core::EventKind::PublicPresentation
            | crabber_core::EventKind::InvocationStarted
            | crabber_core::EventKind::InvocationFinished
            | crabber_core::EventKind::InvocationError
            | crabber_core::EventKind::ToolProgress
            | crabber_core::EventKind::ExecutionAttemptChanged
            | crabber_core::EventKind::InteractionChanged
            | crabber_core::EventKind::ContinuationChanged
            | crabber_core::EventKind::ContinuationWaiting
            | crabber_core::EventKind::ToolPhaseChanged
    ) {
        return Err(StoreError::Validation(
            "public state events require atomic state mutation".into(),
        ));
    }
    if event.live_only || event.kind.is_live_only() {
        return Err(StoreError::Validation(
            "live-only events cannot be persisted".into(),
        ));
    }
    if event.session_id != run.session_id || event.run_id != run.id {
        return Err(StoreError::Validation(
            "event belongs to another run".into(),
        ));
    }
    Ok(())
}
