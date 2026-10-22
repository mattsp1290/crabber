//! Real original Open blocked by the actual native Session transaction gate.
use super::*;
#[tokio::test]
async fn postgres_initial_consumer_real_gate_original_join_then_absorbing_close() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {

    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = Arc::new(crate::PostgresStore::connect(&url).await.unwrap());
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber_providers::Selection {
            provider_id: "library".into(),
            model_id: "text".into(),
        },
        vec!["not activated".into()],
    )
    .unwrap();
    let (context, original) =
        initial_consumer_fixture::prepared(store.as_ref(), false, &source).await;
    let session = context.session_id().clone();
    let mut gate = store.session_transaction(&session).await.unwrap();
    let blocker: i32 = sqlx::query_scalar("SELECT pg_backend_pid()")
        .fetch_one(&mut *gate)
        .await
        .unwrap();
    let owner = PendingDriverTasks::default();
    let mut producer =
        PendingInitialProducer::register_scripted(&owner, store.clone(), context, original, source)
            .unwrap();
    let control = producer.control();
    producer.start().unwrap();
    let waiting=tokio::time::timeout(std::time::Duration::from_secs(5),async{
  loop{
   let waiting_on_gate:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_stat_activity WHERE datname=current_database() AND wait_event_type='Lock' AND $1=ANY(pg_blocking_pids(pid)))").bind(blocker).fetch_one(&store.pool).await.unwrap();
   if waiting_on_gate{break;}tokio::task::yield_now().await;
  }
 }).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !control.original_join_observed() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    let before: Result<i64, _> =
        sqlx::query_scalar("SELECT count(*) FROM initial_consumer_origins WHERE session_id=$1")
            .bind(&session.0)
            .fetch_one(&store.pool)
            .await;
    gate.rollback().await.unwrap(); // Real native gate released BEFORE diagnostic assertions.
    let observed = producer.join().await.unwrap();
    let status = observed.status().clone();
    owner.wait_for_supervision().await;
    let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM initial_consumer_origins WHERE session_id=$1 AND origin_kind='no_opened_member'),(SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1),(SELECT count(*) FROM pending_initial_registered_completes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.pool).await.unwrap();
    store.purge_pending_initial_fixture(&session).await.unwrap();
    assert!(waiting.is_ok(), "actual native worker blocked on this gate");
    assert!(
        joined.is_ok(),
        "actual original join observed while native gate remains held; no worker termination inferred"
    );
    assert_eq!(before.unwrap(), 0);
    assert_eq!(counts, (0, 1, 0, 0));
    assert!(matches!(
        status,
        PendingInitialProducerStatus::ClosedNoPublication
    ));

    }).await.unwrap();
}
