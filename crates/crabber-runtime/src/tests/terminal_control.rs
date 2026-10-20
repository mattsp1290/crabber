//! Native control polling must not supersede our own retained terminal ACK.
use super::*;
use std::time::Duration;

async fn held_ack(lose_ack: bool) {
    let inner = Arc::new(MemoryStore::new());
    let (entered_tx, entered_rx) = oneshot::channel();
    let gate = Arc::new(SettlementGate {
        entered: Mutex::new(Some(entered_tx)),
        terminal_controls: true,
        terminal_control_ack_lost: lose_ack,
        ..SettlementGate::default()
    });
    let runtime = Orchestrator::builder()
        .behavior_fingerprint(crabber_core::InputFingerprint::new("b".repeat(64)).unwrap())
        .store(Arc::new(DelayedTerminalStore {
            inner: inner.clone(),
            gate: gate.clone(),
        }))
        .resolver(Arc::new(FakeProvider::scripted(vec![text_script("done")])))
        .plan_provider(Arc::new(StaticPlanProvider::new(Vec::new(), Vec::new())))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let run_id = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(2), entered_rx)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(
        inner.get_run(&run_id).await.unwrap().unwrap().status,
        RunStatus::Completed
    );
    let mut done = tokio::spawn(handle.done());
    tokio::time::timeout(
        Duration::from_secs(2),
        gate.terminal_control_seen.notified(),
    )
    .await
    .unwrap();
    tokio::select! {
        result = &mut done => panic!("control poll superseded retained completion ACK: {result:?}"),
        () = tokio::time::sleep(Duration::from_millis(50)) => (),
    }
    gate.release.notify_one();
    let result = tokio::time::timeout(Duration::from_secs(2), done)
        .await
        .unwrap()
        .unwrap();
    if lose_ack {
        assert!(
            matches!(result, Err(RuntimeError::AttemptBoundaryUnknown { run_id: id, .. }) if id == run_id)
        );
    } else {
        assert_eq!(result.unwrap().status, RunStatus::Completed);
    }
}

#[tokio::test]
async fn own_terminal_control_refusal_waits_for_original_completion_ack() {
    held_ack(false).await;
}

#[tokio::test]
async fn own_terminal_control_refusal_does_not_replace_unknown_completion_ack() {
    held_ack(true).await;
}
