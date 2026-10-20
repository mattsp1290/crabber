//! Genuine fixed producer plus independent SQL storage/origin corroboration.
use super::{Error, PreparationCommitError, Proof, SourceKey, storage};
use crate::Store;
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
};
use crate::session_owner::preparation_contract::resources;

#[tokio::test]
async fn postgres_pending_initial_original_assignment_and_immutable_roster() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
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
        store.purge_pending_initial_fixture(&session).await.unwrap();
    }
}

#[tokio::test]
async fn postgres_pending_initial_authentic_raw_already_completed_never_adopts() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
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
        let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1),(SELECT count(*) FROM pending_initial_assignments WHERE session_id=$1),(SELECT count(*) FROM native_execution_unclassified WHERE session_id=$1)").bind(&session.0).fetch_one(&store.pool).await.unwrap();
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
        store.purge_pending_initial_fixture(&session).await.unwrap();
    }
}

#[tokio::test]
async fn postgres_pending_initial_real_session_gate_original_drop_then_seal() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
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
    // This is the same actual transaction-level advisory gate used by Open,
    // Complete, Seal and public Raw mutations, held before the first native call.
    let gate = store.session_transaction(&session).await.unwrap();
    let owner = PendingDriverTasks::default();
    let mut producer =
        PendingInitialProducer::register(&owner, store.clone(), context, original).unwrap();
    let control = producer.control();
    producer.start().unwrap();
    let original_join = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            if control.original_join_observed() {
                break;
            }
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before: Result<i64, _> =
        sqlx::query_scalar("SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.pool)
            .await;
    // Release the genuine gate and let any canceled original SQL operation and
    // retained same-source Seal finish before inspecting fixture outcomes.
    gate.rollback().await.unwrap();
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    owner.wait_for_supervision().await;
    let counts:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_registered_completes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.pool).await.unwrap();
    assert!(
        original_join.is_ok(),
        "actual original task joined while native Session gate held"
    );
    assert_eq!(before.unwrap(), 0);
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedNoPublication
    ));
    assert_eq!(
        counts,
        (0, 1, 0),
        "Seal-before-Open registers no Complete loan"
    );
    store.purge_pending_initial_fixture(&session).await.unwrap();
}

#[tokio::test]
async fn postgres_pending_pre_run_slot_tombstone_and_scoped_purge() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = std::sync::Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
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
    let before:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM sessions WHERE id=$1),(SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM pending_initial_cohorts WHERE session_id=$1)").bind(&session.0).fetch_one(&store.pool).await.unwrap();
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
    let mut tx = store.session_transaction(&session).await.unwrap();
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

async fn verify_pre_run_purge(store: &crate::PostgresStore, session: &crabber_core::SessionId) {
    // Test-only physical routing: a tombstone, not this fixture, authorizes the
    // immutable erasure guards. No lifecycle readiness is claimed for pre-Run.
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    super::super::super::lock_session_scope(&mut tx, session)
        .await
        .unwrap();
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
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert!(!corrupt);
    assert_eq!(remaining, 0);
}

async fn verify_assignment(
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    source: &SourceKey,
) {
    let (epoch,run):(String,String)=sqlx::query_as("SELECT epoch_id,run_id FROM pending_initial_assignments WHERE session_id=$1 AND source_key=$2")
   .bind(&session.0).bind(source.identity.as_str()).fetch_one(&store.pool).await.unwrap();
    let run = crabber_core::RunId(run);
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
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
    .fetch_one(&store.pool)
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
    store: &crate::PostgresStore,
    session: &crabber_core::SessionId,
    source: &SourceKey,
    keyed: bool,
) {
    // Alternate UNIQUE(epoch_id) is independent of the source-key PK.
    let replacement=sqlx::query("INSERT INTO pending_initial_cohorts(session_id,source_key,epoch_id,epoch_digest) SELECT session_id,$2,epoch_id,epoch_digest FROM pending_initial_cohorts WHERE session_id=$1")
   .bind(&session.0).bind("a".repeat(64)).execute(&store.pool).await;
    assert!(
        replacement.is_err(),
        "different source PK cannot replace original epoch cohort"
    );
    // Immutable independent cohort survives even an attempted all-projection erase.
    let rejected = sqlx::query("DELETE FROM pending_initial_cohorts WHERE session_id=$1")
        .bind(&session.0)
        .execute(&store.pool)
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
            .execute(&store.pool)
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
        .execute(&store.pool)
        .await
        .unwrap_err();
    assert!(
        rejected
            .as_database_error()
            .is_some_and(|error| error.message().contains("crabber_pending_initial_")),
        "actual immutable/absorbing-close guard refused"
    );
    let _ = keyed;
    corrupt_projection(store, session, source).await;
}

async fn corrupt_projection(
    store: &crate::PostgresStore,
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
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    for table in PROJECTIONS {
        sqlx::query(&format!(
            "DROP TRIGGER pending_initial_{table}_guard ON pending_initial_{table}"
        ))
        .execute(&mut *tx)
        .await
        .unwrap();
        sqlx::query(&format!(
            "DELETE FROM pending_initial_{table} WHERE session_id=$1"
        ))
        .bind(&session.0)
        .execute(&mut *tx)
        .await
        .unwrap();
    }
    let lost = storage::load(
        &mut tx,
        source,
        &crabber_core::ControlId::new(),
        &mut Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    assert!(
        lost.is_err(),
        "independent cohort refuses all-projection loss"
    );
}
