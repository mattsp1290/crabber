use super::*;
use crabber_core::ReasoningEffort;

fn snapshot(retained: &RetainedQueue) -> serde_json::Value {
    serde_json::to_value((&retained.summary, &retained.input, &retained.prepared)).unwrap()
}

pub(super) async fn reject_drift(store: &dyn Store) {
    let mut f = fixture(store).await;
    let QueueAction::Enqueue { input, .. } = &mut f.enqueue.action else {
        panic!("enqueue fixture")
    };
    input.request.model_controls.reasoning_effort = Some(ReasoningEffort::High);
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    let authority = claim(store, &f).await;
    let before_revision = store.session_revision(&f.session).await.unwrap();
    let before = store.retained_queue(&f.session, &f.queue).await.unwrap();
    // The prepared capsule is independently valid, but retains default controls
    // instead of the High intent. Reject at the original/prepared boundary.
    let prepare = mutation(
        store,
        &f,
        QueueAction::Prepare {
            authority,
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    assert_eq!(
        store.mutate_queue(prepare.clone()).await.err(),
        Some(StoreError::AdmissionConflict)
    );
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        before_revision
    );
    assert_eq!(
        snapshot(&store.retained_queue(&f.session, &f.queue).await.unwrap()),
        snapshot(&before)
    );
    assert!(
        store
            .lookup_queue(&f.session, &prepare.id)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        store
            .lookup_admission(&f.session, &before.input.admission.key)
            .await
            .unwrap()
            .is_none()
    );

    let mut changed = f.enqueue;
    let QueueAction::Enqueue { input, .. } = &mut changed.action else {
        panic!("enqueue fixture")
    };
    input.request.model_controls.reasoning_effort = Some(ReasoningEffort::Low);
    assert_eq!(
        store.mutate_queue(changed).await.err(),
        Some(StoreError::AdmissionConflict)
    );
    assert_eq!(
        store.session_revision(&f.session).await.unwrap(),
        before_revision
    );
    assert_eq!(
        snapshot(&store.retained_queue(&f.session, &f.queue).await.unwrap()),
        snapshot(&before)
    );
}
