use super::{evidence, fresh, mixed_output, prepare, resolver, result_revision};
use crate::{
    PhaseCommand, PhaseQuiescence, PhaseReceipt, PhaseResolution, PhaseResolutionOutcome,
    PhaseSettlementRecords, PhaseState, Store, UncertainPhaseChoice,
};
use crabber_core::{ContentBlock, EventKind, EventRecord, ManualClock, Message, MessageId, PartId};
use serde_json::Value;

struct Before {
    state: Value,
    expected: Option<PhaseSettlementRecords>,
    messages: Vec<Message>,
    events: Vec<EventRecord>,
}
async fn capture(store: &dyn Store, request: &PhaseResolution) -> Before {
    let run = store.get_run(&request.run_id).await.unwrap().unwrap();
    let calls = crate::phase_contract::settlement::read_calls(store, &request.session_id)
        .await
        .unwrap();
    let call = calls
        .iter()
        .find(|call| call.id == request.resolution.command.target().call_id)
        .unwrap();
    Before {
        state: evidence(store, request).await,
        expected: request.materialize(&run, call).unwrap(),
        messages: store
            .list_messages(&request.session_id, None)
            .await
            .unwrap(),
        events: store
            .list_events(&request.session_id, None, 1000)
            .await
            .unwrap(),
    }
}
pub(super) async fn run(
    store: &dyn Store,
    clock: &ManualClock,
    choice: UncertainPhaseChoice,
    quiet: PhaseQuiescence,
) {
    let f = prepare(store, choice.clone(), quiet, true).await;
    let before = capture(store, &f.request).await;
    clock.set(f.admitted.run.lease_until);
    let receipt = fresh(
        store
            .resolve_uncertain_phase(f.request.clone())
            .await
            .unwrap(),
    );
    assert_eq!(receipt.phase.quiescence, quiet);
    assert_eq!(receipt.resolver, Some(resolver()));
    assert_eq!(receipt.revision, result_revision(&f.request));
    let after = evidence(store, &f.request).await;
    // Session revision changes, but neither the parent attempt's state nor
    // ownership/lease and approval state may be rewritten by reconciliation.
    assert_eq!(after["run"], before.state["run"]);
    assert_eq!(after["attempt"], before.state["attempt"]);
    assert_eq!(after["approval"], before.state["approval"]);
    records(store, &f.request, &receipt, before, &choice).await;
    replay(store, &f.request, &receipt).await;
    if matches!(choice, UncertainPhaseChoice::Refuse) {
        later_success(store, &f.request, &receipt).await;
    }
}
async fn records(
    store: &dyn Store,
    request: &PhaseResolution,
    receipt: &PhaseReceipt,
    before: Before,
    choice: &UncertainPhaseChoice,
) {
    let after = store
        .list_messages(&request.session_id, None)
        .await
        .unwrap();
    let events = store
        .list_events(&request.session_id, None, 1000)
        .await
        .unwrap();
    if let Some(expected) = before.expected {
        assert_eq!(after.len(), before.messages.len() + 1);
        assert_eq!(after.last().unwrap(), &expected.message);
        let calls = crate::phase_contract::settlement::read_calls(store, &request.session_id)
            .await
            .unwrap();
        let call = calls
            .iter()
            .find(|call| call.id == receipt.phase.target.call_id)
            .unwrap();
        assert_eq!(call.status, expected.call_status);
        assert_eq!(call.result, Some(expected.result));
        let source = events
            .iter()
            .find(|event| event.kind == EventKind::ToolCallSettled)
            .unwrap();
        assert_eq!(source.payload, expected.event.payload);
        assert_eq!(events.len(), before.events.len() + 2);
        if !matches!(choice, UncertainPhaseChoice::Failure { output: None }) {
            assert!(matches!(&after.last().unwrap().parts[0].content,
                ContentBlock::ToolResult {content,..} if content.len()==3));
        }
    } else {
        assert_eq!(after, before.messages);
        assert_eq!(receipt.phase.state, PhaseState::Uncertain);
        assert_eq!(events.len(), before.events.len() + 1);
    }
}
async fn replay(store: &dyn Store, request: &PhaseResolution, receipt: &PhaseReceipt) {
    let before = evidence(store, request).await;
    let mut retry = request.clone();
    retry.message_id = MessageId::new();
    retry.part_id = PartId::new();
    retry.duration_ms += 1;
    assert_eq!(
        store.resolve_uncertain_phase(retry).await.unwrap(),
        PhaseResolutionOutcome::Replayed(receipt.clone())
    );
    assert_eq!(evidence(store, request).await, before);
}
async fn later_success(store: &dyn Store, request: &PhaseResolution, receipt: &PhaseReceipt) {
    let mut later = request.clone();
    later.resolution.id = crabber_core::ControlId::new();
    later.resolution.expected_revision = receipt.revision;
    if let PhaseCommand::ResolveUncertain { choice, .. } = &mut later.resolution.command {
        *choice = UncertainPhaseChoice::Success {
            output: mixed_output(),
        };
    }
    fresh(store.resolve_uncertain_phase(later).await.unwrap());
    assert_eq!(
        store
            .resolve_uncertain_phase(request.clone())
            .await
            .unwrap(),
        PhaseResolutionOutcome::Replayed(receipt.clone())
    );
}
