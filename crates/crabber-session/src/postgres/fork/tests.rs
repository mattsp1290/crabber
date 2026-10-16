use super::super::PostgresStore;
use crate::{Store, sql_fork_contract as contract};
#[tokio::test]
async fn metadata_guards_and_controller_races() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let cold = PostgresStore::connect(&url).await.unwrap();
    contract::guards(&store, &cold).await;
}
#[tokio::test]
async fn process_helper() {
    if let Some(request) = contract::child_request() {
        let store = PostgresStore::connect(&crate::postgres::tests::test_url().unwrap())
            .await
            .unwrap();
        contract::child(&store, request).await;
    }
}
#[tokio::test]
async fn rollback_and_cold_process_commit() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let source = store
        .manage_session(contract::create())
        .await
        .unwrap()
        .summary;
    let request = contract::request(&source);
    let name = format!("fork_fault_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!("CREATE FUNCTION {name}() RETURNS trigger LANGUAGE plpgsql AS $$BEGIN RAISE EXCEPTION 'injected fork rollback'; END$$; CREATE TRIGGER {name} BEFORE INSERT ON session_fork_audit FOR EACH ROW EXECUTE FUNCTION {name}();")).execute(&store.pool).await.unwrap();
    assert_eq!(
        store.fork_session(request.clone()).await.unwrap_err(),
        crate::ForkError::UnknownStoreOutcome
    );
    assert!(
        store
            .get_session(&request.target_session_id)
            .await
            .unwrap()
            .is_none()
    );
    assert_eq!(
        store
            .lookup_session_fork(&request.target_session_id, &request.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store.session_summary(&source.session_id).await.unwrap(),
        source
    );
    sqlx::raw_sql(&format!(
        "DROP TRIGGER {name} ON session_fork_audit; DROP FUNCTION {name}();"
    ))
    .execute(&store.pool)
    .await
    .unwrap();
    contract::spawn("postgres::fork::tests::process_helper", &request, None).await;
    let cold = PostgresStore::connect(&url).await.unwrap();
    contract::verify_cold(&cold, request).await;
    PostgresStore::migrate(&url).await.unwrap();
}
#[tokio::test]
async fn retained_phase_cleanup_is_unsafe_and_forgery_is_unknown() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url)
        .await
        .unwrap()
        .with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        )));
    let fixture = crate::fork_pending_contract::fixture(&store).await;
    let (call, message, output) = crate::fork_pending_contract::forge(&store, &fixture).await;
    let run = store.get_run(&fixture.run).await.unwrap().unwrap();
    let mut tx = store.pool.begin().await.unwrap();
    let mut phase =
        super::super::phase::load_host_record(&mut tx, &fixture.run, &fixture.target.id)
            .await
            .unwrap()
            .unwrap();
    let accounting = crate::sql_snapshot::call_record(&call).unwrap();
    let original: crabber_core::ToolCallRecord = super::super::decode_record(
        &sqlx::query_scalar::<_, String>("SELECT snapshot_record FROM tool_calls WHERE id=$1")
            .bind(&call.id.0)
            .fetch_one(&mut *tx)
            .await
            .unwrap(),
    )
    .unwrap();
    assert_eq!(
        crate::attempt_control::validate_phase(&run, &original, &phase),
        Err(crate::AttemptError::CleanupBlocked)
    );
    phase.output = Some(output);
    super::super::phase::save_host_record(&mut tx, &phase, false)
        .await
        .unwrap();
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_record=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1").bind(&call.id.0).bind(super::super::json(&call).unwrap()).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    let accounting = crate::sql_snapshot::message_record(&message).unwrap();
    sqlx::query("UPDATE messages SET data=$2,snapshot_record=$3,snapshot_parts=$4,snapshot_text=$5,snapshot_bytes=$6 WHERE id=$1").bind(&message.id.0).bind(super::super::json(&message).unwrap()).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    for part in &message.parts {
        sqlx::query("UPDATE parts SET data=$2 WHERE id=$1")
            .bind(&part.id.0)
            .bind(super::super::json(part).unwrap())
            .execute(&mut *tx)
            .await
            .unwrap();
    }
    tx.commit().await.unwrap();
    assert_eq!(
        store.fork_session(fixture.request).await.unwrap_err(),
        crate::ForkError::UnknownStoreOutcome
    );
}
