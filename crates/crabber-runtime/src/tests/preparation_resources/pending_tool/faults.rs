use super::*;
pub(super) async fn held_ack(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    unkeyed: bool,
) {
    let (h, choice, effects) = fixture::paused(store, clock, frontend, unkeyed).await;
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let original = choice.clone();
    let caller = tokio::spawn(async move { original.invoke(&runtime).await.map(|_| ()) });
    tokio::time::timeout(
        Duration::from_secs(5),
        h.gate.preparation_entered.notified(),
    )
    .await
    .unwrap();
    let desc = choice.descriptor(&h).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
    assert_eq!(h.child_state(&desc).await, SessionOwnerState::Active);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    assert_eq!(effects.resumes.load(Ordering::SeqCst), 0);
    caller.abort();
    assert!(caller.await.unwrap_err().is_cancelled());
    h.runtime.cleanup_retained_preparation(&desc).await.unwrap();
    assert_eq!(h.child_state(&desc).await, SessionOwnerState::Active);
    h.gate.preparation_release.notify_one();
    h.finish_cleanup_state(&desc, true).await;
    assert_eq!(effects.resumes.load(Ordering::SeqCst), 0);
    assert_eq!(h.fake.requests().len(), 1);
    assert!(h.gate.successor_request.lock().unwrap().is_none());
    replay_without_factory(&h, &choice).await;
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), 2);
}
pub(super) async fn preflight(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    unkeyed: bool,
) {
    for fault in [5, 6, 9] {
        let (h, choice, effects) =
            fixture::paused(store.clone(), clock.clone(), frontend, unkeyed).await;
        if fault == 9 {
            h.gate.successor_disabled.store(true, Ordering::SeqCst);
        } else {
            h.gate.preparation_fault.store(fault, Ordering::SeqCst);
        }
        let before = h
            .memory
            .session_revision(&choice.original().session_id)
            .await
            .unwrap();
        assert!(choice.invoke(&h.runtime).await.is_err());
        assert_eq!(h.provider.calls.load(Ordering::SeqCst), 1);
        assert_eq!(effects.resumes.load(Ordering::SeqCst), 0);
        assert_eq!(h.fake.requests().len(), 1);
        assert!(
            h.runtime
                .lookup_preparation_operation(
                    &choice.original().session_id,
                    &PreparationKey::ToolContinuation(choice.id().clone())
                )
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            h.memory
                .session_revision(&choice.original().session_id)
                .await
                .unwrap(),
            before
        );
        assert!(h.gate.successor_request.lock().unwrap().is_none());
    }
}
