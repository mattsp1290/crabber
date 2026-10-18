//! Genuine fixed producer plus independent SQL storage/origin corroboration.
use super::{Error, PreparationCommitError, Proof, SourceKey, storage};
use crate::Store;
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
};
use crate::session_owner::preparation_contract::resources;

#[tokio::test]
async fn sqlite_pending_initial_original_assignment_and_immutable_roster() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-initial-native.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    for keyed in [false, true] {
        let original = if keyed {
            let (root, bound) = resources::declaration::setup(store.as_ref()).await;
            resources::initial_driver_observation(
                store.as_ref(),
                root,
                crate::session_owner::preparation::PreparedOperation::AdmitKeyed(Box::new(bound)),
            )
            .await
        } else {
            resources::initial_unkeyed_observation(store.as_ref()).await
        };
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        let context = crabber_core::SessionContext::new(
            binding.session_id.clone(),
            binding.workspace_id.clone(),
            binding.directory.clone(),
        );
        let session = context.session_id().clone();
        let source = SourceKey::expected(&original, &mut Proof::bounded(32 << 20)).unwrap();
        let owner = PendingDriverTasks::default();
        let mut producer =
            PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        let status = observed.status().clone();
        let failed = observed.observation_failed();
        let failure = format!("{:?}", observed.error());
        owner.wait_for_supervision().await;
        // Observe/retire the actual original producer before any diagnostic assertion.
        assert!(
            matches!(status, PendingInitialProducerStatus::ClosedAssignment),
            "{status:?}: {failure}"
        );
        assert!(!failed);
        verify_assignment(store.as_ref(), &session, &source).await;
        verify_guards(store.as_ref(), &session, &source, keyed).await;
    }
}

#[derive(Default)]
struct CommitBarrier {
    released: std::sync::Mutex<bool>,
    changed: std::sync::Condvar,
}
impl CommitBarrier {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut value = self.released.lock().unwrap();
        while !*value {
            value = self.changed.wait(value).unwrap();
        }
    }
}
struct ReleaseOnDrop(std::sync::Arc<CommitBarrier>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn sqlite_pending_open_commit_stop_and_competing_raw_complete() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-open-commit.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    let original = resources::initial_unkeyed_observation(store.as_ref()).await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let session = binding.session_id.clone();
    let context = crabber_core::SessionContext::new(
        session.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let duplicate = original.clone();
    let barrier = std::sync::Arc::new(CommitBarrier::default());
    let release = ReleaseOnDrop(barrier.clone());
    let (entered, entry) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered);
    let hook = barrier.clone();
    let mut writer = store.writer.acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
                hook.wait();
            }
            true
        });
    drop(writer);
    let owner = PendingDriverTasks::default();
    let mut producer =
        PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
    producer.start().unwrap();
    let entry = tokio::time::timeout(std::time::Duration::from_secs(5), entry).await;
    let control = producer.control();
    control.request_stop();
    let snapshot: Result<i64, _> =
        sqlx::query_scalar("SELECT count(*) FROM pending_initial_epochs WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.readers)
            .await;
    let raw_store = store.clone();
    let raw = tokio::spawn(async move { raw_store.complete_preparation(duplicate).await });
    // Release before observing any failure. The real worker retains the write
    // gate until COMMIT; original producer stop is not SQL worker termination.
    barrier.release();
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    owner.wait_for_supervision().await;
    let raw = raw.await.unwrap();
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_assignments WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    store.writer.close().await;
    store.readers.close().await;
    drop(release);
    assert!(
        entry.is_ok_and(|entry| entry.is_ok()),
        "actual Open COMMIT entered"
    );
    assert_eq!(snapshot.unwrap(), 0, "uncommitted Open remains invisible");
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedNoPublication
    ));
    assert!(
        raw.is_err(),
        "Raw Complete must refuse the committed original source slot before replay"
    );
    assert_eq!(counts, (0, 1, 0));
}

#[tokio::test]
async fn sqlite_pending_complete_worker_commit_lost_ack_precedes_original_seal() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-complete-commit.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    let original = resources::initial_unkeyed_observation(store.as_ref()).await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let session = binding.session_id.clone();
    let context = crabber_core::SessionContext::new(
        session.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let barrier = std::sync::Arc::new(CommitBarrier::default());
    let release = ReleaseOnDrop(barrier.clone());
    let (entered, entry) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered);
    let hook = barrier.clone();
    let mut commits = 0;
    let mut writer = store.writer.acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            commits += 1;
            if commits == 2
                && let Some(entered) = entered.take()
            {
                let _ = entered.send(());
                hook.wait();
            }
            true
        });
    drop(writer);
    let owner = PendingDriverTasks::default();
    let mut producer =
        PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
    let control = producer.control();
    producer.start().unwrap();
    let entry = tokio::time::timeout(std::time::Duration::from_secs(5), entry).await;
    // This is the actual transport's existing five-second deadline, not a new
    // synthetic permission or a SQL-worker stop signal. Observe the original
    // business-task join while its already-submitted COMMIT remains blocked.
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if control.original_join_observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let snapshot:Result<(i64,i64),_>=sqlx::query_as("SELECT (SELECT count(*) FROM pending_initial_epochs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_assignments WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await;
    barrier.release();
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    owner.wait_for_supervision().await;
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_assignments WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    let run: String =
        sqlx::query_scalar("SELECT run_id FROM pending_initial_assignments WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.readers)
            .await
            .unwrap();
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let source = super::super::original(
        &mut tx,
        &session,
        &crabber_core::RunId(run),
        &mut Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    store.writer.close().await;
    store.readers.close().await;
    drop(release);
    assert!(
        entry.is_ok_and(|entry| entry.is_ok()),
        "actual Complete COMMIT entered"
    );
    assert!(
        joined.is_ok(),
        "same original task joined before worker COMMIT released"
    );
    assert_eq!(
        snapshot.unwrap(),
        (1, 0),
        "Open visible, Complete publication still uncommitted"
    );
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedAssignment
    ));
    assert_eq!(counts, (1, 1, 1));
    assert!(matches!(
        source.unwrap(),
        crate::native_enrollment::Status::Pending
    ));
}

#[tokio::test]
async fn sqlite_pending_initial_authentic_raw_already_completed_never_adopts() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("raw-completed.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    for keyed in [false, true] {
        let original = if keyed {
            let (root, bound) = resources::declaration::setup(store.as_ref()).await;
            resources::initial_driver_observation(
                store.as_ref(),
                root,
                crate::session_owner::preparation::PreparedOperation::AdmitKeyed(Box::new(bound)),
            )
            .await
        } else {
            resources::initial_unkeyed_observation(store.as_ref()).await
        };
        let binding = &original
            .observed_grant()
            .original_reservation()
            .owner
            .binding;
        let session = binding.session_id.clone();
        let context = crabber_core::SessionContext::new(
            session.clone(),
            binding.workspace_id.clone(),
            binding.directory.clone(),
        );
        let completed = Store::complete_preparation(store.as_ref(), original.clone())
            .await
            .unwrap();
        let run = match completed {
            crate::session_owner::preparation::PreparationCompletion::Admitted(value) => {
                value.run.id
            }
            crate::session_owner::preparation::PreparationCompletion::Keyed(
                crate::KeyedAdmitOutcome::Started { admitted, .. },
            ) => admitted.run.id,
            _ => panic!("fresh actual Raw completion"),
        };
        let owner = PendingDriverTasks::default();
        let mut producer =
            PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        let status = observed.status().clone();
        owner.wait_for_supervision().await;
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1),(SELECT count(*) FROM pending_initial_assignments WHERE session_id=$1),(SELECT count(*) FROM native_execution_unclassified WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap();
        assert!(
            matches!(status,PendingInitialProducerStatus::SourceAlreadyCompletedRaw{run_id} if run_id==run)
        );
        assert_eq!(
            counts,
            (0, 0, 1),
            "historical Raw metadata never creates a Pending cohort or assignment"
        );
        assert!(
            store.get_run(&run).await.unwrap().is_some(),
            "authentic Raw read behavior preserved"
        );
    }
}

#[tokio::test]
async fn sqlite_pending_pre_run_slot_tombstone_and_scoped_purge() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("pending-pre-run.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = std::sync::Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    let (root, bound) = resources::declaration::setup_session(store.as_ref(), false).await;
    let original = resources::initial_driver_observation(
        store.as_ref(),
        root,
        crate::session_owner::preparation::PreparedOperation::AdmitKeyed(Box::new(bound)),
    )
    .await;
    let binding = &original
        .observed_grant()
        .original_reservation()
        .owner
        .binding;
    let session = binding.session_id.clone();
    let context = crabber_core::SessionContext::new(
        session.clone(),
        binding.workspace_id.clone(),
        binding.directory.clone(),
    );
    let workspace = binding.workspace_id.clone();
    let directory_name = binding.directory.clone();
    let late = original.clone();
    let owner = PendingDriverTasks::default();
    let (adapter, entered, release) = PendingInitialProducer::store_holding_open_ack(store.clone());
    let mut producer =
        PendingInitialProducer::register(&owner, adapter, context, original).unwrap();
    producer.start().unwrap();
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), entered.notified()).await;
    producer.control().request_stop();
    release.notify_one();
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    owner.wait_for_supervision().await;
    assert!(entered.is_ok());
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedNoPublication
    ));
    let before:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM sessions WHERE id=$1),(SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    assert_eq!(
        before,
        (0, 0, 1),
        "pre-Run source is independent of Session and Run rows"
    );
    let mut created = crate::sql_fork_contract::create();
    if let crate::SessionChange::Create {
        workspace_id,
        directory,
        ..
    } = &mut created.change
    {
        workspace_id.clone_from(&workspace);
        directory.clone_from(&directory_name);
    }
    let survivor = store.manage_session(created).await.unwrap().summary;
    let mut fork = crate::sql_fork_contract::request(&survivor);
    fork.target_session_id = session.clone();
    assert!(matches!(
        store.fork_session(fork).await,
        Err(crate::ForkError::TargetExists)
    ));
    let deleted = crate::DeleteSession {
        id: crabber_core::ControlId::new(),
        session_id: session.clone(),
        expected_revision: crabber_core::SessionRevision::INITIAL,
    };
    let mut tx = store.begin_write().await.unwrap();
    sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,0,1,$3,0,1,1)").bind(&session.0).bind(&deleted.id.0).bind(deleted.fingerprint().unwrap().as_str()).execute(&mut *tx).await.unwrap();
    tx.commit().await.unwrap();
    let late = Store::complete_preparation(store.as_ref(), late).await;
    assert!(matches!(
        late,
        Err(PreparationCommitError::Owner(Error::Deleted)
            | PreparationCommitError::Store(crate::StoreError::SessionDeleted))
    ));
    verify_pre_run_purge(&store, &session).await;
    assert!(
        store
            .get_session(&survivor.session_id)
            .await
            .unwrap()
            .is_some(),
        "other scope survives"
    );
}

async fn verify_pre_run_purge(store: &crate::SqliteStore, session: &crabber_core::SessionId) {
    // Test-only physical routing: a tombstone, not this fixture, authorizes the
    // immutable erasure guards. No lifecycle readiness is claimed for pre-Run.
    let mut tx = store.begin_write().await.unwrap();
    let corrupt: bool = sqlx::query_scalar(crate::sql_deletion::CROSS_SCOPE)
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    for statement in crate::sql_deletion::PURGE {
        sqlx::query(statement)
            .bind(&session.0)
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    let remaining: i64 =
        sqlx::query_scalar("SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.readers)
            .await
            .unwrap();
    assert!(!corrupt);
    assert_eq!(remaining, 0);
}

async fn verify_assignment(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
    source: &SourceKey,
) {
    let (epoch,run):(String,String)=sqlx::query_as("SELECT epoch_id,run_id FROM pending_initial_assignments WHERE session_id=$1 AND source_key=$2")
   .bind(&session.0).bind(source.identity.as_str()).fetch_one(&store.readers).await.unwrap();
    let run = crabber_core::RunId(run);
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let mut proof = Proof::bounded(32 << 20);
    let state = storage::load(
        &mut tx,
        source,
        &crabber_core::ControlId(epoch.clone()),
        &mut proof,
    )
    .await
    .unwrap();
    state.view().verify(source, &mut proof).unwrap();
    assert_eq!(state.publications.len(), 3);
    assert!(state.close.is_some());
    assert!(matches!(
        super::super::original(&mut tx, session, &run, &mut proof)
            .await
            .unwrap(),
        crate::native_enrollment::Status::Pending
    ));
    let limited = storage::load(
        &mut tx,
        source,
        &crabber_core::ControlId(epoch),
        &mut Proof::bounded(0),
    )
    .await;
    assert!(limited.is_err());
    tx.rollback().await.unwrap();
    let raw_poison: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM native_execution_unclassified WHERE session_id=$1",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    assert_eq!(
        raw_poison, 0,
        "first birth is Pending, never Raw then relabelled"
    );
    assert!(
        store.get_run(&run).await.is_err(),
        "no public Raw credential loan"
    );
}

async fn verify_guards(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
    source: &SourceKey,
    keyed: bool,
) {
    // Alternate UNIQUE(epoch_id) is independent of the source-key PK.
    let replacement=sqlx::query("INSERT OR REPLACE INTO pending_initial_cohorts(session_id,source_key,epoch_id,epoch_digest) SELECT session_id,$2,epoch_id,epoch_digest FROM pending_initial_cohorts WHERE session_id=$1")
   .bind(&session.0).bind("a".repeat(64)).execute(&store.writer).await;
    assert!(
        replacement.is_err(),
        "different source PK cannot replace original epoch cohort"
    );
    // Immutable independent cohort survives even an attempted all-projection erase.
    let rejected = sqlx::query("DELETE FROM pending_initial_cohorts WHERE session_id=$1")
        .bind(&session.0)
        .execute(&store.writer)
        .await
        .unwrap_err();
    assert!(
        rejected
            .as_database_error()
            .is_some_and(|error| error.message().contains("crabber_pending_initial_")),
        "actual immutable/absorbing-close guard refused"
    );
    let rejected =
        sqlx::query("UPDATE pending_initial_epochs SET data_json='{}' WHERE session_id=$1")
            .bind(&session.0)
            .execute(&store.writer)
            .await
            .unwrap_err();
    assert!(
        rejected
            .as_database_error()
            .is_some_and(|error| error.message().contains("crabber_pending_initial_")),
        "actual immutable/absorbing-close guard refused"
    );
    let rejected = sqlx::query("UPDATE pending_initial_heads SET sequence=1 WHERE session_id=$1")
        .bind(&session.0)
        .execute(&store.writer)
        .await
        .unwrap_err();
    assert!(
        rejected
            .as_database_error()
            .is_some_and(|error| error.message().contains("crabber_pending_initial_")),
        "actual immutable/absorbing-close guard refused"
    );
    if keyed {
        corrupt_projection(store, session, source).await;
    }
}

async fn corrupt_projection(
    store: &crate::SqliteStore,
    session: &crabber_core::SessionId,
    source: &SourceKey,
) {
    const PROJECTIONS: [&str; 7] = [
        "closes",
        "assignments",
        "registered_completes",
        "heads",
        "publications",
        "source_slots",
        "epochs",
    ];
    sqlx::query("PRAGMA foreign_keys=OFF")
        .execute(&store.writer)
        .await
        .unwrap();
    for table in PROJECTIONS {
        for operation in ["insert", "update", "delete"] {
            sqlx::query(&format!("DROP TRIGGER pending_initial_{table}_{operation}"))
                .execute(&store.writer)
                .await
                .unwrap();
        }
        sqlx::query(&format!(
            "DELETE FROM pending_initial_{table} WHERE session_id=$1"
        ))
        .bind(&session.0)
        .execute(&store.writer)
        .await
        .unwrap();
    }
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let lost = storage::load(
        &mut tx,
        source,
        &crabber_core::ControlId::new(),
        &mut Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    sqlx::query("PRAGMA foreign_keys=ON")
        .execute(&store.writer)
        .await
        .unwrap();
    assert!(
        lost.is_err(),
        "independent cohort refuses all-projection loss"
    );
}
