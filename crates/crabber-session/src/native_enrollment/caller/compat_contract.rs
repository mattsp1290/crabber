//! Real native unit-ACK compatibility paths: exact rollback, after-commit loss and proof exhaustion.
use super::Harness;
use crate::public_session::v2::proof::Proof;
use crabber_core::{
    Clock, EventKind, EventRecord, ManualClock, ToolCallId, ToolCallRecord, ToolCallStatus,
};

pub(crate) async fn run(store: &impl Harness, clock: &ManualClock) {
    for lost in [false, true] {
        legacy(store, clock, lost).await;
        admission(store, clock, lost).await;
    }
}
async fn legacy(store: &impl Harness, clock: &ManualClock, lost: bool) {
    let admitted = crate::public_state_contract::admit(store, clock.now()).await;
    let session = &admitted.session.id;
    let run = &admitted.run.id;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let id = ToolCallId::new();
    let event = EventRecord {
        cursor: None,
        session_id: session.clone(),
        run_id: run.clone(),
        turn_id: None,
        kind: EventKind::ToolCallPending,
        payload: serde_json::json!({"call_id":id}),
        correlation: None,
        live_only: false,
        created_at: clock.now(),
    };
    execution
        .create_tool_call(
            ToolCallRecord {
                id: id.clone(),
                run_id: run.clone(),
                name: "compat".into(),
                arguments: serde_json::json!({}),
                status: ToolCallStatus::Pending,
                retry_safe: false,
                result: None,
            },
            event.clone(),
        )
        .await
        .unwrap();
    let mut running = event;
    // Preserve this accepted legacy input: the Raw witness must not invent a phase-event contract.
    running.kind = EventKind::ExtensionNotice;
    running.cursor = Some(crabber_core::EventCursor(u64::MAX));
    let before = store.retained(session, run, false).await;
    let birth = store.retained(session, run, true).await;
    store.probe().arm_legacy(session, run, &id, true);
    assert!(
        execution
            .claim_tool_call(&id, running.clone())
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, false).await, before);
    let mut measured = Proof::bounded(32 << 20);
    assert!(
        store
            .legacy_with_proof(admitted.fence.clone(), &id, &running, &mut measured)
            .await
            .is_err()
    );
    store.probe().arm_legacy(session, run, &id, false);
    let mut insufficient = Proof::bounded(measured.used() - 1);
    assert!(
        store
            .legacy_with_proof(admitted.fence.clone(), &id, &running, &mut insufficient)
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 0);
    assert_eq!(store.retained(session, run, false).await, before);
    if lost {
        store.probe().lose_ack();
    }
    assert_eq!(
        execution
            .claim_tool_call(&id, running.clone())
            .await
            .is_err(),
        lost
    );
    assert_eq!(store.probe().hits().len(), 1);
    let committed = store.retained(session, run, false).await;
    assert_ne!(committed, before);
    assert_eq!(store.retained(session, run, true).await, birth);
    store.probe().arm_legacy(session, run, &id, true);
    assert!(
        execution
            .claim_tool_call(&id, running.clone())
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 0);
    assert_eq!(store.retained(session, run, false).await, committed);
    let mut stale = admitted.fence;
    stale.claim_token = "stale-original-owner".into();
    assert!(
        store
            .legacy_with_proof(stale, &id, &running, &mut Proof::bounded(32 << 20))
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 0);
}
async fn admission(store: &impl Harness, clock: &ManualClock, lost: bool) {
    let keyed =
        crate::admission_execution_contract::keyed(&crabber_core::SessionId::new(), clock.now());
    let crate::KeyedAdmitOutcome::Started { admitted, .. } =
        store.admit_keyed_run(keyed).await.unwrap()
    else {
        panic!("fresh actual keyed admission")
    };
    let session = &admitted.session.id;
    let run = &admitted.run.id;
    let execution = store.execution(admitted.fence.clone()).await.unwrap();
    let before = store.retained(session, run, false).await;
    let birth = store.retained(session, run, true).await;
    store.probe().arm_begin(session, run, true);
    assert!(execution.begin_admission_execution().await.is_err());
    assert_eq!(store.probe().hits().len(), 1);
    assert_eq!(store.retained(session, run, false).await, before);
    let mut measured = Proof::bounded(32 << 20);
    assert!(
        store
            .begin_with_proof(admitted.fence.clone(), &mut measured)
            .await
            .is_err()
    );
    store.probe().arm_begin(session, run, false);
    assert!(
        store
            .begin_with_proof(
                admitted.fence.clone(),
                &mut Proof::bounded(measured.used() - 1)
            )
            .await
            .is_err()
    );
    assert_eq!(store.probe().hits().len(), 0);
    assert_eq!(store.retained(session, run, false).await, before);
    if lost {
        store.probe().lose_ack();
    }
    assert_eq!(execution.begin_admission_execution().await.is_err(), lost);
    assert_eq!(store.probe().hits().len(), 1);
    let committed = store.retained(session, run, false).await;
    assert_ne!(committed, before);
    assert_eq!(store.retained(session, run, true).await, birth);
    store.probe().arm_begin(session, run, true);
    assert_eq!(
        execution.begin_admission_execution().await.unwrap_err(),
        crate::AdmissionExecutionError::AlreadyStarted
    );
    assert_eq!(store.probe().hits().len(), 0);
    assert_eq!(store.retained(session, run, false).await, committed);
    let mut stale = admitted.fence;
    stale.claim_token = "stale-original-owner".into();
    assert_eq!(
        store
            .begin_with_proof(stale, &mut Proof::bounded(32 << 20))
            .await
            .unwrap_err(),
        crate::AdmissionExecutionError::StaleOwner
    );
    assert_eq!(store.probe().hits().len(), 0);
}
