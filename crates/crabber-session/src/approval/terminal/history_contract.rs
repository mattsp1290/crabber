//! Actual native transactions with a preceding identical public settlement.
use crate::{ApprovalSuspension, Store};
use crabber_core::{Clock, EventKind, EventRecord, ManualClock, Run};
pub(crate) struct Fixture {
    pub current: Run,
    pub audit: EventRecord,
    pub original: EventRecord,
    pub older: EventRecord,
    pub batch: ApprovalSuspension,
}
pub(crate) async fn fixture(store: &impl Store, clock: &ManualClock, expire: bool) -> Fixture {
    clock.set(time::OffsetDateTime::UNIX_EPOCH);
    let mut f = crate::approval_contract::fixture::staged(store, clock).await;
    f.batch.usage.input_tokens = 3;
    f.batch.usage.output_tokens = 5;
    let end = clock.now() + time::Duration::seconds(if expire { 2 } else { 0 });
    let status = if expire {
        crabber_core::RunStatus::Failed
    } else {
        crabber_core::RunStatus::Interrupted
    };
    f.execution
        .append_event(crate::attempt_control::terminal_event_scope(
            &f.run.session_id,
            &f.run.id,
            status,
            &f.batch.usage,
            end,
            f.batch.source_turn_id.clone(),
        ))
        .await
        .unwrap();
    f.batch.expected_revision = store.session_revision(&f.run.session_id).await.unwrap();
    f.execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let request = crate::approval_contract::termination::request(store, &f, expire).await;
    clock.set(end);
    let receipt = store.terminate_approval(request.clone()).await.unwrap();
    assert_eq!(store.terminate_approval(request).await.unwrap(), receipt);
    let current = store.get_run(&f.run.id).await.unwrap().unwrap();
    let events = store
        .list_events(&current.session_id, None, 1000)
        .await
        .unwrap();
    let audit = events
        .iter()
        .find(|event| event.cursor == Some(receipt.event_cursor))
        .unwrap()
        .clone();
    let original = super::settlement(&receipt, &audit).unwrap();
    let older = events
        .iter()
        .find(|event| event.kind == EventKind::RunSettled && event.cursor < original.cursor)
        .unwrap()
        .clone();
    let mut compare = older.clone();
    compare.cursor = original.cursor;
    assert_eq!(compare, original);
    Fixture {
        current,
        audit,
        original,
        older,
        batch: f.batch,
    }
}
