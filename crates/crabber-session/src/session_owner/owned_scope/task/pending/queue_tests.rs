use super::*;
use crate::session_owner::preparation_contract::resources;

async fn genuine<S: Store + 'static>(store: Arc<S>) {
    let owner = PendingDriverTasks::default();
    let (context, original, actual_id) = resources::queue::prepared(store.as_ref()).await;
    assert!(
        store
            .inspect_queue_preparation_admission(&original)
            .await
            .unwrap()
            .is_none()
    );
    let replay = original.clone();
    let mut task = PendingQueueTask::register(
        &owner,
        store.clone(),
        context.clone(),
        original,
        |context, completion| async move {
            assert!(context.try_enter_main().is_ok());
            assert!(matches!(
                completion,
                crate::session_owner::preparation::PreparationCompletion::Queue(
                    crate::QueueOutcome::Applied {
                        admitted: Some(_),
                        authority: None,
                        ..
                    }
                )
            ));
            (context, std::cell::Cell::new(7))
        },
    )
    .unwrap();
    let control = task.control();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert!(observed.error().is_none(), "{:?}", observed.error());
    assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
    let (main, value) = observed.value().unwrap();
    assert_eq!(value.get(), 7);
    assert!(main.try_enter_main().is_err());
    assert!(main.activation() == control.bound().await.unwrap().activation());
    let PendingQueueCompletion::Fresh(notice) = control.completion().await.unwrap() else {
        panic!("fresh Queue notice")
    };
    assert_eq!(
        notice
            .receipt()
            .summary
            .admission
            .as_ref()
            .unwrap()
            .user_message_id,
        actual_id
    );
    // Repeated original replay never registers or dispatches a replacement main.
    for _ in 0..2 {
        let adapter = Interposed::new(store.clone(), Stage::Register, false);
        let mut task = PendingQueueTask::register(
            &owner,
            adapter.clone(),
            context.clone(),
            replay.clone(),
            |_, _| async { panic!("Queue replay dispatch") },
        )
        .unwrap();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert!(observed.error().is_none(), "{:?}", observed.error());
        assert_eq!(observed.replayed(), Some(notice.receipt()));
        assert!(observed.bound_main().is_none());
        assert_eq!(adapter.register_calls.load(Ordering::SeqCst), 0);
    }
    owner.wait_for_supervision().await;
    unrelated_revision_refuses_fresh_registration(store.as_ref()).await;
}
#[tokio::test]
async fn pending_queue_same_actual_main_original_ids_and_repeated_replay_memory() {
    Box::pin(genuine(store().0)).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn pending_queue_same_actual_main_original_ids_and_repeated_replay_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pending-queue.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    Box::pin(genuine(Arc::new(
        crate::SqliteStore::connect(&path).await.unwrap(),
    )))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn pending_queue_same_actual_main_original_ids_and_repeated_replay_postgres() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    Box::pin(genuine(Arc::new(
        crate::PostgresStore::connect(&url).await.unwrap(),
    )))
    .await;
}
#[tokio::test]
async fn pending_queue_drop_retains_each_actual_call_and_capacity() {
    for stage in [
        Stage::Complete,
        Stage::Inspect,
        Stage::Register,
        Stage::Activate,
    ] {
        let owner = PendingDriverTasks::default();
        let (store, _) = store();
        let (context, original, _) = resources::queue::prepared(store.as_ref()).await;
        let adapter = Interposed::new(store, stage, false);
        let mut task = PendingQueueTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            original,
            |_, _| async { panic!("stopped Queue dispatch") },
        )
        .unwrap();
        task.start().unwrap();
        adapter.arrived.notified().await;
        drop(task);
        assert!(
            tokio::time::timeout(Duration::from_millis(15), owner.wait_for_supervision())
                .await
                .is_err()
        );
        assert_eq!(owner.available_capacity(), 63);
        adapter.release.notify_one();
        tokio::time::timeout(Duration::from_secs(5), owner.wait_for_supervision())
            .await
            .unwrap();
        assert_eq!(owner.available_capacity(), 64);
    }
}
#[tokio::test]
async fn pending_queue_notice_requires_full_ack_and_inspection_before_failed_registration() {
    for stage in [
        Stage::Complete,
        Stage::Inspect,
        Stage::Register,
        Stage::Activate,
    ] {
        let owner = PendingDriverTasks::default();
        let (store, _) = store();
        let (context, original, _) = resources::queue::prepared(store.as_ref()).await;
        let adapter = Interposed::new(store, stage, true);
        let mut task = PendingQueueTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            original,
            |_, _| async { panic!("unknown Queue ACK") },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        adapter.arrived.notified().await;
        if matches!(stage, Stage::Complete | Stage::Inspect) {
            assert!(
                tokio::time::timeout(Duration::from_millis(15), control.completion())
                    .await
                    .is_err()
            );
        } else {
            assert!(matches!(
                control.completion().await.unwrap(),
                PendingQueueCompletion::Fresh(_)
            ));
        }
        adapter.release.notify_one();
        assert!(task.join().await.unwrap().error().is_some());
        if matches!(stage, Stage::Complete | Stage::Inspect) {
            assert!(control.completion().await.is_err());
        }
    }
    for inspect in [false, true] {
        let owner = PendingDriverTasks::default();
        let (store, _) = store();
        let (context, original, _) = resources::queue::prepared(store.as_ref()).await;
        let adapter = Interposed::new(store, Stage::Complete, false);
        if inspect {
            adapter.corrupt_inspection.store(true, Ordering::SeqCst);
        } else {
            adapter.corrupt_receipt.store(true, Ordering::SeqCst);
        }
        adapter.release.notify_one();
        let mut task = PendingQueueTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            original,
            |_, _| async { panic!("corrupt Queue ACK") },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        assert!(task.join().await.unwrap().error().is_some());
        assert!(control.completion().await.is_err());
        assert_eq!(adapter.register_calls.load(Ordering::SeqCst), 0);
    }
}

async fn unrelated_revision_refuses_fresh_registration(store: &impl Store) {
    let (context, original, _) = resources::queue::prepared(store).await;
    let crate::session_owner::preparation::PreparationCompletion::Queue(
        crate::QueueOutcome::Applied {
            receipt,
            admitted: Some(admitted),
            ..
        },
    ) = store.complete_preparation(original.clone()).await.unwrap()
    else {
        panic!("fresh Queue completion")
    };
    store
        .manage_session(crate::ManageSession {
            session_id: context.session_id().clone(),
            id: crabber_core::ControlId::new(),
            expected_revision: store.session_revision(context.session_id()).await.unwrap(),
            change: crate::SessionChange::Rename {
                title: "later public revision".into(),
            },
        })
        .await
        .unwrap();
    assert!(
        store
            .register_execution_driver(crate::session_owner::driver::RegisterExecutionDriver {
                context: context.clone(),
                fence: admitted.fence.clone()
            })
            .await
            .is_err()
    );
    store
        .mutate_queue(crate::QueueMutation {
            session_id: context.session_id().clone(),
            id: crabber_core::ControlId::new(),
            expected_revision: store.session_revision(context.session_id()).await.unwrap(),
            action: crate::QueueAction::Cancel {
                queue_id: receipt.summary.queue_id.clone(),
            },
        })
        .await
        .unwrap();
    assert_eq!(
        store
            .inspect_queue_preparation_admission(&original)
            .await
            .unwrap(),
        Some(receipt)
    );
}
#[tokio::test]
async fn pending_queue_initial_successor_share_exact_capacity_and_keep_original_observer_on_wrong_tag()
 {
    let owner = PendingDriverTasks::default();
    let (store, clock) = store();
    let (queue_context, queue, _) = resources::queue::prepared(store.as_ref()).await;
    let (initial_context, initial) = super::initial::prepared(store.as_ref(), false).await;
    let (successor_context, successor) = fixture::prepared(store.as_ref(), &clock, true).await;
    let mut queues = Vec::new();
    let mut initials = Vec::new();
    let mut successors = Vec::new();
    for _ in 0..21 {
        queues.push(
            PendingQueueTask::register(
                &owner,
                store.clone(),
                queue_context.clone(),
                queue.clone(),
                |_, _| async {},
            )
            .unwrap(),
        );
        successors.push(
            PendingSuccessorTask::register(
                &owner,
                store.clone(),
                successor_context.clone(),
                successor.clone(),
                |_, _| async {},
            )
            .unwrap(),
        );
    }
    for _ in 0..22 {
        initials.push(
            PendingInitialTask::register(
                &owner,
                store.clone(),
                initial_context.clone(),
                initial.clone(),
                |_, _| async {},
            )
            .unwrap(),
        );
    }
    assert!(matches!(
        PendingQueueTask::register(&owner, store, queue_context, queue, |_, _| async {}),
        Err(Error::Capacity)
    ));
    let first = queues.last_mut().unwrap();
    assert!(matches!(
        first.inner.observation(core::Family::Initial).await,
        Err(Error::SemanticConflict)
    ));
    assert_eq!(owner.available_capacity(), 0);
    assert_eq!(
        first.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert_eq!(owner.available_capacity(), 0);
    drop(queues);
    drop(initials);
    drop(successors);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn pending_queue_rejects_ordinary_source_and_observes_actual_constructor_panic() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, initial) = super::initial::prepared(store.as_ref(), false).await;
    assert!(matches!(
        PendingQueueTask::register(&owner, store.clone(), context, initial, |_, _| async {}),
        Err(Error::Unsupported)
    ));
    let (context, queue, _) = resources::queue::prepared(store.as_ref()).await;
    let mut task = PendingQueueTask::register(
        &owner,
        store,
        context,
        queue,
        |_, _| -> std::future::Ready<()> { panic!("actual Queue business construction panic") },
    )
    .unwrap();
    task.start().unwrap();
    let observed = task.join().await.unwrap();
    assert_eq!(observed.disposition(), OwnedMainDisposition::Panicked);
    assert!(observed.bound_main().is_some());
}

#[tokio::test]
async fn pending_queue_genuine_other_queue_ack_cannot_replace_original_completed_source() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original, _) = resources::queue::prepared(store.as_ref()).await;
    let expected = store.complete_preparation(original.clone()).await.unwrap();
    let (_, other, _) = resources::queue::prepared(store.as_ref()).await;
    let other_ack = store.complete_preparation(other).await.unwrap();
    let adapter = Interposed::new(store.clone(), Stage::Complete, false);
    *adapter.replacement.lock().unwrap() = Some(other_ack);
    adapter.release.notify_one();
    let mut task = PendingQueueTask::register(
        &owner,
        adapter.clone(),
        context,
        original.clone(),
        |_, _| async { panic!("other Queue source") },
    )
    .unwrap();
    let control = task.control();
    task.start().unwrap();
    assert!(matches!(
        task.join().await.unwrap().error(),
        Some(PendingSuccessorError::Owner(Error::SemanticConflict))
    ));
    assert!(control.completion().await.is_err());
    assert_eq!(adapter.register_calls.load(Ordering::SeqCst), 0);
    let crate::session_owner::preparation::PreparationCompletion::Queue(expected) = expected else {
        panic!()
    };
    assert_eq!(
        store
            .inspect_queue_preparation_admission(&original)
            .await
            .unwrap(),
        Some(expected.receipt().clone())
    );
}
