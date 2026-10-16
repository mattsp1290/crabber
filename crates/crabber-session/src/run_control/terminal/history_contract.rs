//! Actual independently persisted waiting-cancellation source fixtures.
use super::Source;
use crate::Store;
use crabber_core::{EventRecord, ManualClock, Run};
pub(crate) struct Fixture {
    pub current: Run,
    pub audit: EventRecord,
    pub original: EventRecord,
    pub origin_id: crabber_core::ControlId,
    pub summary: crate::RunControlSummary,
}
pub(crate) async fn fixture(
    store: &impl Store,
    clock: &ManualClock,
    tool: bool,
    previous: bool,
) -> Fixture {
    let actual =
        crate::run_control_contract::waiting_terminal_fixture(store, clock, tool, previous).await;
    let summary = store
        .run_controls(&actual.request.target)
        .await
        .unwrap()
        .controls
        .into_iter()
        .find(|summary| summary.receipt.operation_id == actual.request.id)
        .unwrap();
    let current = store
        .get_run(&actual.request.target.run_id)
        .await
        .unwrap()
        .unwrap();
    let events = store
        .list_events(&current.session_id, None, 1000)
        .await
        .unwrap();
    let audit = events
        .iter()
        .find(|event| event.cursor == Some(summary.event_cursor))
        .unwrap()
        .clone();
    let saved: Source = serde_json::from_value(audit.payload["terminal"].clone()).unwrap();
    let origin_id = events
        .iter()
        .find(|event| {
            event.kind == crabber_core::EventKind::ExecutionAttemptChanged
                && event.payload.get("attempt").is_some_and(|value| {
                    serde_json::from_value::<crate::AttemptSnapshot>(value.clone())
                        .is_ok_and(|attempt| attempt.state == crate::AttemptState::Suspended)
                })
        })
        .unwrap()
        .correlation
        .clone()
        .map(crabber_core::ControlId)
        .unwrap();
    let original = events
        .into_iter()
        .find(|event| event.cursor == saved.settlement.cursor)
        .unwrap();
    assert_eq!(saved.settlement, original);
    Fixture {
        current,
        audit,
        original,
        origin_id,
        summary,
    }
}
