use super::MAX_BYTES;
use crate::{AdmitOutcome, Store};
use crate::{PublicRunOutcome, PublicRunState};
use crabber_core::{EventKind, EventRecord, Usage};
use crabber_core::{RunStatus, SessionId};

async fn read(store: &dyn Store, session: &SessionId) -> PublicRunState {
    let PublicRunOutcome::State(state) = store.public_run_state(session, MAX_BYTES).await.unwrap()
    else {
        panic!("bounded metadata")
    };
    assert_eq!(state.session_id, *session);
    assert_eq!(
        state.revision,
        store.session_revision(session).await.unwrap()
    );
    state
}
fn event(admitted: &AdmitOutcome, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: admitted.session.id.clone(),
        run_id: admitted.run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: admitted.run.created_at,
    }
}
pub(crate) async fn run(store: &dyn Store) {
    assert!(matches!(
        store.public_run_state(&SessionId::new(), MAX_BYTES).await,
        Err(crate::StoreError::NotFound)
    ));
    let created = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let session = &created.session_id;
    assert!(read(store, session).await.run.is_none());
    assert_eq!(
        store.public_run_state(session, 0).await.unwrap(),
        PublicRunOutcome::Limited
    );
    let mut request = crate::management_contract::admission(&created.summary);
    request.owner = "private-owner-sentinel".into();
    request.config_hash = "private-config-sentinel".into();
    request.plan_fingerprint = "private-plan-sentinel".into();
    let first = store.admit_run(request).await.unwrap();
    let execution = store.execution(first.fence.clone()).await.unwrap();
    let current = read(store, session).await;
    assert_eq!(current.run.as_ref().unwrap().run_id, first.run.id);
    assert_eq!(current.run.unwrap().status, first.run.status);
    let before_lease_update = read(store, session).await;
    let renewed = first.run.lease_until + time::Duration::seconds(60);
    execution.renew_lease(renewed).await.unwrap();
    assert_eq!(
        store
            .get_run(&first.run.id)
            .await
            .unwrap()
            .unwrap()
            .lease_until,
        renewed
    );
    assert_eq!(read(store, session).await, before_lease_update);
    let usage = Usage {
        input_tokens: 13,
        output_tokens: 8,
    };
    execution
        .settle_run(
            RunStatus::Completed,
            None,
            usage.clone(),
            event(&first, EventKind::RunSettled),
        )
        .await
        .unwrap();
    let completed = read(store, session).await;
    assert_eq!(completed.run.as_ref().unwrap().status, RunStatus::Completed);
    assert_eq!(completed.run.unwrap().usage, usage);
    let next = store
        .admit_run(crate::management_contract::admission(&created.summary))
        .await
        .unwrap();
    assert_eq!(read(store, session).await.run.unwrap().run_id, next.run.id);
    let next_execution = store.execution(next.fence.clone()).await.unwrap();
    next_execution
        .pause_run(
            serde_json::json!({"private-checkpoint-sentinel":"x".repeat(100_000)}),
            event(&next, EventKind::RunPaused),
        )
        .await
        .unwrap();
    let paused = read(store, session).await;
    assert_eq!(paused.run.as_ref().unwrap().status, RunStatus::Paused);
    let text = serde_json::to_string(&paused).unwrap();
    for secret in [
        "private-",
        &first.run.claim_token,
        &first.run.owner,
        &first.run.config_hash,
    ] {
        assert!(!text.contains(secret));
    }
}

pub(crate) fn allocation_contract(runtime: &tokio::runtime::Runtime, store: &dyn Store) {
    let session = runtime.block_on(async {
        let created = store
            .manage_session(crate::management_contract::create())
            .await
            .unwrap();
        let admitted = store
            .admit_run(crate::management_contract::admission(&created.summary))
            .await
            .unwrap();
        let execution = store.execution(admitted.fence.clone()).await.unwrap();
        execution
            .pause_run(
                serde_json::json!({"private":"x".repeat(1_048_576)}),
                event(&admitted, EventKind::RunPaused),
            )
            .await
            .unwrap();
        created.session_id
    });
    let measured = allocation_counter::measure(|| {
        let state = runtime
            .block_on(store.public_run_state(&session, MAX_BYTES))
            .unwrap();
        assert!(matches!(state, PublicRunOutcome::State(_)));
    });
    assert!(
        measured.bytes_max < 131_072,
        "private Run decoded/cloned: {measured:?}"
    );
    let measured = allocation_counter::measure(|| {
        let request = crate::PublicSessionRequest {
            session_id: session.clone(),
            continuation: None,
            limits: crate::PublicSessionLimits {
                header_bytes: 65536,
                proof_bytes: 1,
                interactions: 64,
                history: crate::SnapshotLimits {
                    messages: 16,
                    tool_calls: 16,
                    parts: 128,
                    text_bytes: 65536,
                    encoded_bytes: 131_072,
                },
            },
        };
        assert!(matches!(
            runtime
                .block_on(store.public_session_snapshot(request))
                .unwrap(),
            crate::PublicSessionOutcome::Page(_)
        ));
    });
    assert!(
        measured.bytes_max < 262_144,
        "aggregate decoded private Run: {measured:?}"
    );
}
