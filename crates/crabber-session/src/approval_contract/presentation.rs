//! Source presentation is stable on replay and never sets the decision clock.
use super::fixture::Fixture;
use crate::{ApprovalReceipt, Store};
use crabber_core::{Clock, EventKind, ManualClock, TurnId};

pub(super) async fn suspend(
    store: &dyn Store,
    clock: &ManualClock,
    f: &mut Fixture,
) -> ApprovalReceipt {
    f.batch.source_turn_id = Some(TurnId::new());
    f.batch.source_created_at = Some(clock.now() + time::Duration::nanoseconds(7));
    let mut expected = f
        .batch
        .conclusion(&f.run, clock.now())
        .unwrap()
        .materialize(&f.run)
        .unwrap();
    let receipt = f
        .execution
        .suspend_for_approval(f.batch.clone(), f.owner.clone())
        .await
        .unwrap();
    let events = store
        .list_events(&f.run.session_id, None, 1000)
        .await
        .unwrap();
    let source: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::RunPaused)
        .collect();
    assert_eq!(source.len(), 1);
    expected.cursor = source[0].cursor;
    assert_eq!(*source[0], expected);
    let snapshot = store
        .approval_snapshot(&f.run.session_id, &f.run.id)
        .await
        .unwrap()
        .unwrap();
    assert!(
        snapshot
            .interactions
            .iter()
            .all(|question| question.expires_at
                == Some(clock.now() + time::Duration::milliseconds(1000)))
    );
    let mut retry = f.batch.clone();
    retry.source_turn_id = Some(TurnId::new());
    retry.source_created_at = Some(clock.now() + time::Duration::seconds(1));
    assert_eq!(
        retry.fingerprint(&f.run.session_id, &f.run.id).unwrap(),
        receipt.semantic_digest
    );
    assert_eq!(
        f.execution
            .suspend_for_approval(retry, f.owner.clone())
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(
        store
            .list_events(&f.run.session_id, None, 1000)
            .await
            .unwrap(),
        events
    );
    receipt
}
