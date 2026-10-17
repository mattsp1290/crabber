//! Queue's actual factory result stays in its original parked admission task.
use super::*;
use crate::QueueControl;
use crabber_core::{ControlId, QueueId, SessionRevision};
use crabber_session::{
    ManageSession, QueueAction, QueueMutation, QueueOutcome, SessionChange, SessionDefaults,
};
async fn prepared(h: &mut Harness) -> (QueueControl, crabber_core::MessageId) {
    let session = h.input.session_id.clone().unwrap();
    h.memory
        .manage_session(ManageSession {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: h.input.workspace_id.clone(),
                directory: h.input.directory.clone(),
                title: "pending Queue".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    let queue = QueueId::new();
    h.runtime
        .enqueue_queue(h.input.clone(), queue.clone(), ControlId::new(), options())
        .await
        .unwrap();
    h.input.input_envelope = Some(
        h.memory
            .retained_queue(&session, &queue)
            .await
            .unwrap()
            .input
            .input,
    );
    let claim = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: h.memory.session_revision(&session).await.unwrap(),
        action: QueueAction::Claim {
            owner: "queue-controller".into(),
            lease: Duration::from_secs(30),
        },
    };
    let QueueOutcome::Applied {
        authority: Some(authority),
        ..
    } = h.runtime.mutate_queue(claim).await.unwrap()
    else {
        panic!("fresh Queue claim")
    };
    h.runtime
        .prepare_queue(
            h.input.clone(),
            QueueControl {
                id: ControlId::new(),
                authority: authority.clone(),
                expected_revision: h.memory.session_revision(&session).await.unwrap(),
            },
        )
        .await
        .unwrap();
    h.provider.cleanup_started.notified().await;
    h.provider.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(5), async {
        while h.provider.cleanup_completed.load(Ordering::SeqCst) != 1
            || !h.runtime.retained_preparation_status(&session).is_empty()
        {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    let original = h
        .memory
        .retained_queue(&session, &queue)
        .await
        .unwrap()
        .prepared
        .unwrap();
    (
        QueueControl {
            id: ControlId::new(),
            authority,
            expected_revision: h.memory.session_revision(&session).await.unwrap(),
        },
        original.keyed.request.user_message.id,
    )
}
async fn same_task(store: Arc<dyn Store>) {
    let gate = Arc::new(SettlementGate::default());
    gate.release.notify_one();
    let wrapped = Arc::new(DelayedTerminalStore {
        inner: store.clone(),
        gate: gate.clone(),
    });
    let mut h = Harness::on_store(wrapped, store, gate);
    let (control, actual_id) = prepared(&mut h).await;
    h.gate
        .initial_task_diagnostics
        .store(true, Ordering::SeqCst);
    let crate::Admission::Started { receipt, handle } = h
        .runtime
        .admit_queue(h.input.clone(), control.clone(), None)
        .await
        .unwrap()
    else {
        panic!("fresh Queue")
    };
    assert_eq!(receipt.user_message_id, actual_id);
    h.provider.cleanup_release.notify_one();
    handle.done().await.unwrap();
    let session = h.input.session_id.as_ref().unwrap();
    let drained = h
        .runtime
        .drain_session_execution_tasks(session, Duration::from_secs(5))
        .await;
    assert!(
        drained.pending.is_empty() && drained.pending_preparations.is_empty(),
        "{drained:?}"
    );
    let stages = h.gate.initial_task_ids.lock().unwrap().clone();
    assert_eq!(
        stages.iter().map(|(stage, _)| *stage).collect::<Vec<_>>(),
        vec![
            InitialTaskStage::CompletePreparation,
            InitialTaskStage::InspectQueue,
            InitialTaskStage::RegisterDriver,
            InitialTaskStage::BeginAttempt
        ]
    );
    assert!(
        stages[0].1.is_some() && stages.iter().all(|(_, id)| *id == stages[0].1),
        "{stages:?}"
    );
    let calls = h.provider.calls.load(Ordering::SeqCst);
    h.runtime
        .mutate_queue(QueueMutation {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: h.memory.session_revision(session).await.unwrap(),
            action: QueueAction::Cancel {
                queue_id: control.authority.queue_id.clone(),
            },
        })
        .await
        .unwrap();
    h.gate
        .queue_inspection_disabled
        .store(true, Ordering::SeqCst);
    for _ in 0..2 {
        assert!(
            matches!(h.runtime.admit_queue(h.input.clone(), control.clone(), None).await.unwrap(), crate::Admission::Replayed(saved) if saved == receipt)
        );
    }
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), calls);
    assert_eq!(h.fake.requests().len(), 1);
}
#[tokio::test]
async fn pending_queue_runtime_same_task_and_original_replay_memory() {
    same_task(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn pending_queue_runtime_same_task_and_original_replay_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("queue-main.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    same_task(Arc::new(
        crabber_session::SqliteStore::connect(path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pending_queue_runtime_same_task_and_original_replay_postgres() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    same_task(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
#[tokio::test]
async fn pending_queue_runtime_waiter_drop_and_stop_retain_committed_ack_and_original_plan() {
    let mut h = Harness::new();
    let (control, _) = prepared(&mut h).await;
    let baseline_joins = h.gate.owned_join_calls.load(Ordering::SeqCst);
    h.gate.preparation_hold.store(true, Ordering::SeqCst);
    let runtime = h.runtime.clone();
    let input = h.input.clone();
    let waiter = tokio::spawn(async move { runtime.admit_queue(input, control, None).await });
    tokio::time::timeout(
        Duration::from_secs(5),
        h.gate.preparation_entered.notified(),
    )
    .await
    .unwrap();
    waiter.abort();
    assert!(waiter.await.unwrap_err().is_cancelled());
    let session = h.input.session_id.as_ref().unwrap();
    let descriptor = h.runtime.retained_preparation_status(session)[0]
        .preparation
        .clone();
    h.runtime
        .cleanup_retained_preparation(&descriptor)
        .await
        .unwrap();
    tokio::time::timeout(
        Duration::from_secs(3),
        h.provider.cleanup_started.notified(),
    )
    .await
    .unwrap();
    h.provider.cleanup_release.notify_one();
    tokio::time::timeout(Duration::from_secs(3), async {
        while h.provider.cleanup_completed.load(Ordering::SeqCst) != 2 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    assert_eq!(
        h.gate.owned_join_calls.load(Ordering::SeqCst),
        baseline_joins
    );
    assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Active);
    assert_eq!(h.fake.requests().len(), 0);
    h.gate.preparation_hold.store(false, Ordering::SeqCst);
    h.gate.preparation_release.notify_one();
    let drained = h
        .runtime
        .drain_session_execution_tasks(session, Duration::from_secs(5))
        .await;
    assert!(
        drained.pending.is_empty() && drained.pending_preparations.is_empty(),
        "{drained:?}"
    );
    assert_eq!(h.child_state(&descriptor).await, SessionOwnerState::Joined);
    assert_eq!(h.fake.requests().len(), 0);
}
#[tokio::test]
async fn pending_queue_runtime_fresh_notice_survives_registration_and_activation_failure() {
    for activation in [false, true] {
        let mut h = Harness::new();
        let (control, _) = prepared(&mut h).await;
        if activation {
            h.gate.driver_activation_fault.store(1, Ordering::SeqCst);
        } else {
            h.gate.driver_register_fault.store(1, Ordering::SeqCst);
        }
        let crate::Admission::Started { handle, .. } = h
            .runtime
            .admit_queue(h.input.clone(), control, None)
            .await
            .unwrap()
        else {
            panic!("committed Queue needs original handle")
        };
        h.provider.cleanup_release.notify_one();
        assert!(handle.done().await.is_err());
        assert_eq!(h.fake.requests().len(), 0);
    }
}

#[tokio::test]
async fn pending_queue_runtime_unsupported_inspector_refuses_before_factory() {
    let mut h = Harness::new();
    let (control, _) = prepared(&mut h).await;
    let calls = h.provider.calls.load(Ordering::SeqCst);
    h.gate
        .queue_inspection_disabled
        .store(true, Ordering::SeqCst);
    assert!(matches!(
        h.runtime.admit_queue(h.input.clone(), control, None).await,
        Err(RuntimeError::Store(StoreError::HostUnsupported))
    ));
    assert_eq!(h.provider.calls.load(Ordering::SeqCst), calls);
    assert_eq!(h.fake.requests().len(), 0);
}
