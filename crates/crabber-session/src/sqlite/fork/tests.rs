use super::super::SqliteStore;
use crate::{Store, sql_fork_contract as contract};
#[tokio::test]
async fn metadata_guards_and_controller_races() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let cold = SqliteStore::connect(&path).await.unwrap();
    contract::guards(&store, &cold).await;
}
#[tokio::test]
async fn process_helper() {
    if let Some(request) = contract::child_request() {
        let store = SqliteStore::connect(std::env::var("CRABBER_SQLITE_CHILD_PATH").unwrap())
            .await
            .unwrap();
        contract::child(&store, request).await;
    }
}
#[tokio::test]
async fn rollback_and_cold_process_commit() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let source = store
        .manage_session(contract::create())
        .await
        .unwrap()
        .summary;
    let request = contract::request(&source);
    sqlx::raw_sql("CREATE TRIGGER fork_fault BEFORE INSERT ON session_fork_audit BEGIN SELECT RAISE(ABORT,'injected fork rollback'); END;").execute(&store.writer).await.unwrap();
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
    sqlx::query("DROP TRIGGER fork_fault")
        .execute(&store.writer)
        .await
        .unwrap();
    contract::spawn("sqlite::fork::tests::process_helper", &request, Some(&path)).await;
    let cold = SqliteStore::connect(&path).await.unwrap();
    contract::verify_cold(&cold, request).await;
}
#[tokio::test]
async fn retained_phase_cleanup_is_unsafe_and_forgery_is_unknown() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("fork.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path)
        .await
        .unwrap()
        .with_clock(std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        )));
    let fixture = crate::fork_pending_contract::fixture(&store).await;
    let (call, message, output) = crate::fork_pending_contract::forge(&store, &fixture).await;
    let run = store.get_run(&fixture.run).await.unwrap().unwrap();
    let mut tx = store.begin_write().await.unwrap();
    let mut phase =
        super::super::phase::load_host_record(&mut tx, &fixture.run, &fixture.target.id)
            .await
            .unwrap()
            .unwrap();
    let accounting = crate::sql_snapshot::call_record(&call).unwrap();
    let original: crabber_core::ToolCallRecord = serde_json::from_str(
        &sqlx::query_scalar::<_, String>("SELECT data FROM tool_calls WHERE id=$1")
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
    sqlx::query("UPDATE tool_calls SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1").bind(&call.id.0).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    let accounting = crate::sql_snapshot::message_record(&message).unwrap();
    sqlx::query("UPDATE messages SET data=$2,snapshot_parts=$3,snapshot_text=$4,snapshot_bytes=$5 WHERE id=$1").bind(&message.id.0).bind(accounting.record).bind(accounting.parts).bind(accounting.text).bind(accounting.bytes).execute(&mut *tx).await.unwrap();
    for part in &message.parts {
        sqlx::query("UPDATE parts SET data=$2 WHERE id=$1")
            .bind(&part.id.0)
            .bind(serde_json::to_string(part).unwrap())
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
