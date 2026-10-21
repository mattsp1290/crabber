use crate::attempt_contract::process;

#[tokio::test]
async fn attempt_fault_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        crate::attempt_contract::run_fault_contract(
            &store,
            &clock,
            |point: crate::attempt_contract::FaultPoint| {
                store
                    .attempt_fault
                    .store(point as u8, std::sync::atomic::Ordering::SeqCst);
            },
        )
        .await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn postgres_attempt_contract() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::PostgresStore::connect(&url).await.unwrap();
        crate::attempt_contract::run_contract(|clock| store.with_clock(clock)).await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn process_helper() {
    let Some(fixture) = process::fixture() else {
        return;
    };
    let store = super::super::PostgresStore::connect(
        &crabber_test_support::postgres::TestDatabase::attached_url()
            .await
            .expect("child database required"),
    )
    .await
    .unwrap();
    if std::env::var_os("CRABBER_ATTEMPT_WRITE").is_some() {
        process::write(&store, &fixture).await.unwrap();
    } else {
        process::verify_loss(&store, fixture).await;
    }
}
#[tokio::test]
async fn lost_attempt_operations_ack_reconcile_in_fresh_process() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
        time::OffsetDateTime::UNIX_EPOCH,
    ));
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock);
        for mode in process::Mode::ALL {
            let fixture = process::prepare_loss(&store, mode, |point| {
                store
                    .attempt_fault
                    .store(point, std::sync::atomic::Ordering::SeqCst);
            })
            .await;
            process::spawn(
                "postgres::attempt::tests::process_helper",
                &fixture,
                crabber_test_support::ChildDatabase::Postgres(&url),
                &[],
            )
            .wait()
            .await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn killed_attempt_operations_before_commit_leave_no_partial_transition() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::now_utc(),
        ));
        let store = super::super::PostgresStore::connect(&url)
            .await
            .unwrap()
            .with_clock(clock.clone());
        for mode in process::Mode::ALL {
            crash(&url, &store, &clock, mode).await;
        }
    })
    .await
    .unwrap();
}
async fn crash(
    url: &str,
    store: &super::super::PostgresStore,
    clock: &crabber_core::ManualClock,
    mode: process::Mode,
) {
    let fixture = process::pending(store, mode).await;
    let name = format!("attempt_barrier_{}", uuid::Uuid::new_v4().simple());
    let state = mode.state();
    // Session/SQL identifiers are generated UUIDs and state is a fixed test enum.
    sqlx::query(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='attempt' AND NEW.session_id='{}' AND NEW.data_json::jsonb->'attempt'->>'state'='{state}' THEN PERFORM pg_advisory_xact_lock(751302924); END IF; RETURN NEW; END $$",fixture.session.0))
        .execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON operation_receipts FOR EACH ROW EXECUTE FUNCTION {name}()"))
        .execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302924)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = process::spawn(
        "postgres::attempt::tests::process_helper",
        &fixture,
        crabber_test_support::ChildDatabase::Postgres(url),
        &[("CRABBER_ATTEMPT_WRITE", "1".into())],
    );
    let waiter = tokio::time::timeout(std::time::Duration::from_secs(10),async {
        loop {
            assert!(child.running(),"writer exited before barrier");
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO operation_receipts%'")
                .bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("child must reach receipt insert before COMMIT");
    process::uncommitted(store, &fixture).await;
    child.kill();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND datname=current_database()")
        .bind(waiter)
        .execute(&store.pool)
        .await
        .unwrap();
    held.rollback().await.unwrap();
    sqlx::query(&format!("DROP TRIGGER {name} ON operation_receipts"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {name}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    process::uncommitted(store, &fixture).await;
    process::retry_crash(store, clock, &fixture).await;
}

#[tokio::test]
async fn foreign_locked_attempt_is_not_locked_or_changed_by_another_run() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    use crate::{
        AttemptCommand, AttemptError, AttemptMutation, AttemptMutationOutcome, AttemptTarget, Store,
    };
    use crabber_core::{AttemptGeneration, AttemptId, ControlId};
    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        super::super::PostgresStore::migrate(&url).await.unwrap();
        let store = super::super::PostgresStore::connect(&url).await.unwrap();
        let left =
            crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
        let right =
            crate::public_state_contract::admit(&store, time::OffsetDateTime::now_utc()).await;
        let execution = store.execution(left.fence).await.unwrap();
        let target = AttemptTarget {
            id: AttemptId::new(),
            generation: AttemptGeneration::INITIAL,
        };
        let request = AttemptMutation {
            id: ControlId::new(),
            expected_revision: store.session_revision(&right.session.id).await.unwrap(),
            command: AttemptCommand::Reserve {
                target: target.clone(),
            },
        };
        let other = store.execution(right.fence).await.unwrap();
        assert!(matches!(
            other.mutate_attempt(request, None).await.unwrap(),
            AttemptMutationOutcome::Applied { .. }
        ));
        let expected = store
            .execution_attempt(&right.run.id, &target.id)
            .await
            .unwrap();
        let revision = store.session_revision(&left.session.id).await.unwrap();
        let mut held = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        sqlx::query("SELECT id FROM execution_attempts WHERE run_id=$1 AND id=$2 FOR UPDATE")
            .bind(&right.run.id.0)
            .bind(&target.id.0)
            .fetch_one(&mut *held)
            .await
            .unwrap();
        for command in [
            AttemptCommand::Begin {
                target: target.clone(),
            },
            AttemptCommand::Reserve {
                target: target.clone(),
            },
        ] {
            let expected_error = if matches!(command, AttemptCommand::Begin { .. }) {
                AttemptError::NotFound
            } else {
                AttemptError::SemanticConflict
            };
            let result = tokio::time::timeout(
                std::time::Duration::from_secs(2),
                execution.mutate_attempt(
                    AttemptMutation {
                        id: ControlId::new(),
                        expected_revision: revision,
                        command,
                    },
                    None,
                ),
            )
            .await;
            if result.is_err() {
                held.rollback().await.unwrap();
                panic!("foreign attempt lock must not delay denial");
            }
            assert_eq!(result.unwrap().unwrap_err(), expected_error);
        }
        held.rollback().await.unwrap();
        assert_eq!(
            store
                .execution_attempt(&right.run.id, &target.id)
                .await
                .unwrap(),
            expected
        );
        assert_eq!(
            store.session_revision(&left.session.id).await.unwrap(),
            revision
        );
    })
    .await
    .unwrap();
}
