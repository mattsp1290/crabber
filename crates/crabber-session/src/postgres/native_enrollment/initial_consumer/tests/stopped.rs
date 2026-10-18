//! Actual attempted Open refusal, then genuine original join + paired no-member Seal.
use super::*;
#[tokio::test]
async fn postgres_scripted_original_open_refusal_has_absorbing_no_opened_member() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    let _guard = crate::postgres::pending_initial_test_lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = Arc::new(crate::PostgresStore::connect(&url).await.unwrap());

    for keyed in [false, true] {
        let source = crabber_extension::LibraryScriptedSource::new(
            crabber::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec!["never activated".into()],
        )
        .unwrap();
        let (context, original) =
            initial_consumer_fixture::prepared(store.as_ref(), keyed, &source).await;
        let session = context.session_id().clone();
        let owner = PendingDriverTasks::default();
        let adapter = Arc::new(super::forwarding::Forward {
            inner: store.clone(),
            kind: crate::native_enrollment::initial_consumer::CallKind::Conclude,
            armed: Arc::new(std::sync::atomic::AtomicBool::new(false)),
            calls: std::sync::Mutex::new(Vec::new()),
            fail_first_open: std::sync::atomic::AtomicBool::new(true),
        });
        let mut producer =
            PendingInitialProducer::register_scripted(&owner, adapter, context, original, source)
                .unwrap();
        producer.start().unwrap();
        // The SAME original actually calls Open and receives the test refusal before
        // native I/O. Its joined retainer then issues the REAL paired native Seal.
        let observed = producer.join().await.unwrap();
        let status = observed.status().clone();
        let error = format!("{:?}", observed.error());
        owner.wait_for_supervision().await;
        let counts:(i64,i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM runs WHERE session_id=$1),(SELECT count(*) FROM initial_consumer_origins WHERE session_id=$1 AND origin_kind='no_opened_member'),(SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1),(SELECT count(*) FROM pending_initial_registered_completes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.pool).await.unwrap();
        let mut tx = store.session_transaction(&session).await.unwrap();
        let refused=sqlx::query("INSERT INTO initial_consumer_calls(session_id,source_key,epoch_id,record_digest,call_id,kind,input_digest,data_json) SELECT session_id,source_key,epoch_id,$2,$3,'register',$2,'{}' FROM initial_consumer_origins WHERE session_id=$1").bind(&session.0).bind("0".repeat(64)).bind(crabber_core::ControlId::new().0).execute(&mut *tx).await;
        tx.rollback().await.unwrap();
        store.purge_pending_initial_fixture(&session).await.unwrap();
        assert!(
            matches!(status, PendingInitialProducerStatus::ClosedNoPublication),
            "{status:?}: {error}"
        );
        assert_eq!(counts, (0, 1, 0, 0));
        assert!(
            refused
                .as_ref()
                .err()
                .and_then(|error| error.as_database_error())
                .is_some_and(|error| error.code().as_deref() == Some("P0001")),
            "actual absorbing no-opened-member guard"
        );
    }
}
