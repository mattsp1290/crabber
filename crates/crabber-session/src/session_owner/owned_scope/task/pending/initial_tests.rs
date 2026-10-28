use super::*;
use crate::session_owner::preparation::{
    JoinedPreparation, PreparationCompletion, PreparedOperation,
};
use crate::session_owner::preparation_contract::resources;
use crabber_core::SessionContext;

pub(super) async fn prepared(
    store: &impl Store,
    keyed: bool,
) -> (SessionContext, JoinedPreparation) {
    let original = if keyed {
        let (root, bound) = resources::declaration::setup(store).await;
        resources::initial_driver_observation(
            store,
            root,
            PreparedOperation::AdmitKeyed(Box::new(bound)),
        )
        .await
    } else {
        resources::initial_unkeyed_observation(store).await
    };
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let context = SessionContext::new(
        binding.session_id.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    (context, original)
}

async fn genuine<S: Store + 'static>(store: Arc<S>) {
    let owner = PendingDriverTasks::default();
    for keyed in [false, true] {
        let (context, original) = prepared(store.as_ref(), keyed).await;
        let replay = original.clone();
        let mut task = PendingInitialTask::register(
            &owner,
            store.clone(),
            context.clone(),
            original,
            |context, completion| async move {
                assert!(context.try_enter_main().is_ok());
                assert!(matches!(
                    completion,
                    PreparationCompletion::Admitted(_)
                        | PreparationCompletion::Keyed(crate::KeyedAdmitOutcome::Started { .. })
                ));
                // Send, deliberately not Sync: the physical observer must not require Sync.
                (context, std::cell::Cell::new(7))
            },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        let actual = task.join().await.unwrap();
        assert!(actual.error().is_none(), "{:?}", actual.error());
        assert_eq!(actual.disposition(), OwnedMainDisposition::Returned);
        let (main_context, value) = actual.value().unwrap();
        assert_eq!(value.get(), 7);
        assert!(main_context.try_enter_main().is_err());
        assert!(main_context.activation() == control.bound().await.unwrap().activation());
        let PendingInitialCompletion::Fresh(notice) = control.completion().await.unwrap() else {
            panic!("fresh notice")
        };
        assert_eq!(notice.session_id(), context.session_id());
        assert!(actual.bound_main().is_some());
        // This exact protected original, not a reconstructed fence, produces replay.
        let mut replay_task = PendingInitialTask::<()>::register(
            &owner,
            store.clone(),
            context,
            replay,
            |_, _| async { panic!("replay dispatch") },
        )
        .unwrap();
        let replay_control = replay_task.control();
        replay_task.start().unwrap();
        let actual = replay_task.join().await.unwrap();
        assert!(actual.error().is_none(), "{:?}", actual.error());
        assert_eq!(actual.replayed(), Some(notice.receipt()));
        assert!(actual.bound_main().is_none());
        assert!(replay_control.bound().await.is_err());
        assert!(matches!(
            replay_control.completion().await.unwrap(),
            PendingInitialCompletion::Replayed(_)
        ));
    }
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}
#[tokio::test]
async fn initial_same_actual_main_and_metadata_replay_memory() {
    genuine(store().0).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn initial_same_actual_main_and_metadata_replay_sqlite() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("pending-initial.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    genuine(Arc::new(crate::SqliteStore::connect(&path).await.unwrap())).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn initial_same_actual_main_and_metadata_replay_postgres() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        genuine(Arc::new(crate::PostgresStore::connect(&url).await.unwrap())).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn initial_drop_retains_original_each_pending_call() {
    for stage in [
        Stage::Complete,
        Stage::Inspect,
        Stage::Register,
        Stage::Activate,
    ] {
        let owner = PendingDriverTasks::default();
        let (store, _) = store();
        let (context, original) = prepared(store.as_ref(), false).await;
        let adapter = Interposed::new(store, stage, false);
        let mut task = PendingInitialTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            original,
            |_, _| async { panic!("stopped dispatch") },
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
async fn initial_notice_only_after_protected_inspection_before_failed_register_or_activate() {
    for stage in [
        Stage::Complete,
        Stage::Inspect,
        Stage::Register,
        Stage::Activate,
    ] {
        let owner = PendingDriverTasks::default();
        let (store, _) = store();
        let (context, original) = prepared(store.as_ref(), false).await;
        let adapter = Interposed::new(store, stage, true);
        let mut task = PendingInitialTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            original,
            |_, _| async { panic!("unknown administrative ACK") },
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
                PendingInitialCompletion::Fresh(_)
            ));
        }
        adapter.release.notify_one();
        let actual = task.join().await.unwrap();
        assert!(actual.error().is_some());
        assert_eq!(actual.disposition(), OwnedMainDisposition::NoDispatch);
        if matches!(stage, Stage::Complete | Stage::Inspect) {
            assert!(control.completion().await.is_err());
        }
    }
}

#[tokio::test]
async fn initial_changed_keyed_message_receipt_has_no_notice() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), true).await;
    let adapter = Interposed::new(store, Stage::Complete, false);
    adapter.corrupt_receipt.store(true, Ordering::SeqCst);
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {
            panic!("changed receipt")
        })
        .unwrap();
    let control = task.control();
    task.start().unwrap();
    assert!(task.join().await.unwrap().error().is_some());
    assert!(control.completion().await.is_err());
}

#[tokio::test]
async fn initial_other_genuine_run_ack_cannot_replace_original_before_commit() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), false).await;
    let (_, other) = prepared(store.as_ref(), false).await;
    let PreparationCompletion::Admitted(mut ack) = store.complete_preparation(other).await.unwrap()
    else {
        panic!()
    };
    // A genuine other fence/Run, coherently re-routed to A's session, still
    // cannot replace the protected original receipt bound to A's preparation.
    ack.session.id = context.session_id().clone();
    ack.run.session_id = context.session_id().clone();
    let check = original.clone();
    let adapter = Interposed::new(store.clone(), Stage::Complete, false);
    *adapter.replacement.lock().unwrap() = Some(PreparationCompletion::Admitted(ack));
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {
            panic!("other run")
        })
        .unwrap();
    let control = task.control();
    task.start().unwrap();
    assert!(task.join().await.unwrap().error().is_some());
    assert!(control.completion().await.is_err());
    assert!(
        store
            .inspect_initial_preparation_admission(&check)
            .await
            .unwrap()
            .is_none()
    );
}

#[tokio::test]
async fn initial_and_successor_share_one_capacity_and_original_tag_observation() {
    let owner = PendingDriverTasks::default();
    let (store, clock) = store();
    let (initial_context, initial) = prepared(store.as_ref(), false).await;
    let (successor_context, successor) = fixture::prepared(store.as_ref(), &clock, true).await;
    let mut tasks = Vec::new();
    for _ in 0..32 {
        tasks.push(
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
    let mut successors = Vec::new();
    for _ in 0..32 {
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
    assert!(matches!(
        PendingInitialTask::register(
            &owner,
            store.clone(),
            initial_context,
            initial,
            |_, _| async {}
        ),
        Err(crate::session_owner::preparation::PreparationCommitError::Owner(Error::Capacity))
    ));
    let first = tasks.last_mut().unwrap();
    // A malformed private facade tag cannot consume/release the raw actual
    // observation. The correct original facade can still observe it afterward.
    assert!(matches!(
        first.inner.observation(core::Family::Successor).await,
        Err(Error::SemanticConflict)
    ));
    assert_eq!(owner.available_capacity(), 0);
    assert_eq!(
        first.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert_eq!(owner.available_capacity(), 0);
    drop(tasks);
    drop(successors);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn initial_genuine_other_driver_permit_cannot_dispatch() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), false).await;
    let grant = super::super::super::driver_tests::grant(store.as_ref()).await;
    let mut other =
        super::super::super::OwnedTaskRegistration::register_driver(&grant, |_| async {}).unwrap();
    let permit = super::super::super::driver_tests::permit(store.as_ref(), &grant, &other).await;
    let adapter = Interposed::new(store, Stage::Activate, false);
    *adapter.wrong_permit.lock().unwrap() = Some(permit);
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {
            panic!("other permit")
        })
        .unwrap();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert!(matches!(
        actual.error(),
        Some(PendingSuccessorError::Owner(Error::SemanticConflict))
    ));
    assert_eq!(actual.disposition(), OwnedMainDisposition::NoDispatch);
    assert!(actual.bound_main().is_some());
    other.join().await.unwrap();
}

#[tokio::test]
async fn initial_constructor_panic_and_unstarted_drop_preserve_actual_dispositions() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), false).await;
    let mut unstarted = PendingInitialTask::<()>::register(
        &owner,
        store.clone(),
        context.clone(),
        original.clone(),
        |_, _| async { panic!("unstarted") },
    )
    .unwrap();
    assert_eq!(
        unstarted.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert!(
        store
            .inspect_initial_preparation_admission(&original)
            .await
            .unwrap()
            .is_none()
    );
    let mut task = PendingInitialTask::<()>::register(
        &owner,
        store,
        context,
        original,
        |_, _| -> std::future::Ready<()> { panic!("actual constructor panic") },
    )
    .unwrap();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert_eq!(actual.disposition(), OwnedMainDisposition::Panicked);
    assert!(actual.bound_main().is_some());
    assert!(actual.error().is_none());
}

#[tokio::test]
async fn initial_coherently_changed_receipt_version_has_no_notice_or_dispatch() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), true).await;
    let adapter = Interposed::new(store, Stage::Complete, false);
    adapter.corrupt_version.store(true, Ordering::SeqCst);
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {})
            .unwrap();
    let control = task.control();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert!(
        actual.error().is_some(),
        "coherently changed ACK+lookup version accepted"
    );
    assert!(control.completion().await.is_err());
    assert!(actual.bound_main().is_none());
}

#[tokio::test]
async fn initial_receipt_changed_original_operation_has_no_notice() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), false).await;
    let adapter = Interposed::new(store, Stage::Inspect, false);
    adapter.corrupt_operation.store(true, Ordering::SeqCst);
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {})
            .unwrap();
    let control = task.control();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert!(
        actual.error().is_some(),
        "changed original operation metadata accepted"
    );
    assert!(control.completion().await.is_err());
    assert!(actual.bound_main().is_none());
}

#[tokio::test]
async fn initial_receipt_malformed_replay_run_has_no_notice() {
    let owner = PendingDriverTasks::default();
    let (store, _) = store();
    let (context, original) = prepared(store.as_ref(), false).await;
    store.complete_preparation(original.clone()).await.unwrap();
    let adapter = Interposed::new(store, Stage::Complete, false);
    adapter.corrupt_replay_run.store(true, Ordering::SeqCst);
    adapter.release.notify_one();
    let mut task =
        PendingInitialTask::<()>::register(&owner, adapter, context, original, |_, _| async {
            panic!("replay")
        })
        .unwrap();
    let control = task.control();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert!(
        actual.error().is_some(),
        "malformed replay routing accepted"
    );
    assert!(control.completion().await.is_err());
    assert!(actual.bound_main().is_none());
}
