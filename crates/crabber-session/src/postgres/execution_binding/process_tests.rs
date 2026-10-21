use crate::execution_binding_contract::process;
const HELPER: &str = "postgres::execution_binding::process_tests::process_helper";

#[tokio::test]
async fn process_helper() {
    let Some(target) = process::settings() else {
        return;
    };
    let store = crate::PostgresStore::connect(
        &crabber_test_support::postgres::TestDatabase::attached_url()
            .await
            .expect("child database required"),
    )
    .await
    .unwrap();
    if std::env::var_os("CRABBER_BINDING_WRITE").is_some() {
        process::write(&store, &target, false).await;
    } else {
        process::verify(&store, &target).await;
    }
}
#[tokio::test]
async fn bindings_survive_lost_ack_in_fresh_process() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        for (keyed, typed) in [(false, false), (true, false), (false, true), (true, true)] {
            let target = process::Fixture::new(keyed, typed);
            store
                .binding_fault
                .store(2, std::sync::atomic::Ordering::SeqCst);
            process::write(&store, &target, true).await;
            process::spawn(
                HELPER,
                &target,
                crabber_test_support::ChildDatabase::Postgres(&url),
                false,
                None,
            )
            .wait()
            .await;
        }
    })
    .await
    .unwrap();
}
#[tokio::test]
async fn killed_binding_insert_rolls_back_entire_admission() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        let store = crate::PostgresStore::connect(&url).await.unwrap();
        for (keyed, typed) in [(false, false), (true, false), (false, true), (true, true)] {
            crash(&url, &store, keyed, typed).await;
        }
    })
    .await
    .unwrap();
}
async fn crash(url: &str, store: &crate::PostgresStore, keyed: bool, typed: bool) {
    let target = process::Fixture::new(keyed, typed);
    let name = format!("binding_barrier_{}", uuid::Uuid::new_v4().simple());
    // IDs are generated UUIDs, never caller SQL or data used as an identifier.
    sqlx::query(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN IF EXISTS(SELECT 1 FROM runs WHERE id=NEW.run_id AND session_id='{}') THEN PERFORM pg_advisory_xact_lock(751302926); END IF; RETURN NEW; END $$", target.session.0)).execute(&store.pool).await.unwrap();
    sqlx::query(&format!("CREATE TRIGGER {name} AFTER INSERT ON run_execution_bindings FOR EACH ROW EXECUTE FUNCTION {name}()")).execute(&store.pool).await.unwrap();
    let mut held = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    sqlx::query("SELECT pg_advisory_xact_lock(751302926)")
        .execute(&mut *held)
        .await
        .unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *held)
        .await
        .unwrap();
    let mut child = process::spawn(
        HELPER,
        &target,
        crabber_test_support::ChildDatabase::Postgres(url),
        true,
        None,
    );
    let waiter = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        loop {
            assert!(child.running(), "writer exited before binding insert");
            let pid: Option<i32> = sqlx::query_scalar("SELECT pid FROM pg_stat_activity WHERE datname=current_database() AND $1=ANY(pg_blocking_pids(pid)) AND query LIKE 'INSERT INTO run_execution_bindings%'").bind(blocker).fetch_optional(&store.pool).await.unwrap();
            if let Some(pid) = pid { break pid; }
            tokio::time::sleep(std::time::Duration::from_millis(10)).await;
        }
    }).await.expect("child must reach binding insert before COMMIT");
    process::absent(store, &target).await;
    child.kill();
    sqlx::query("SELECT pg_terminate_backend(pid) FROM pg_stat_activity WHERE pid=$1 AND datname=current_database()")
        .bind(waiter)
        .execute(&store.pool)
        .await
        .unwrap();
    held.rollback().await.unwrap();
    sqlx::query(&format!("DROP TRIGGER {name} ON run_execution_bindings"))
        .execute(&store.pool)
        .await
        .unwrap();
    sqlx::query(&format!("DROP FUNCTION {name}()"))
        .execute(&store.pool)
        .await
        .unwrap();
    process::absent(store, &target).await;
    process::write(store, &target, false).await;
    process::verify(store, &target).await;
}
