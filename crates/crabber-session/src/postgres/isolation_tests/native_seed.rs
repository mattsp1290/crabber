//! Keep genuine, fully observed native producer rows for the RLS denial matrix.
use crate::PostgresStore;
use crate::session_owner::owned_scope::{
    PendingDriverTasks, PendingInitialProducer, PendingInitialProducerStatus,
    initial_consumer_fixture,
};
use std::sync::Arc;

pub(super) async fn seed(store: &PostgresStore) {
    let store = Arc::new(store.clone());
    for keyed in [false, true] {
        let source = crabber_extension::LibraryScriptedSource::new(
            crabber_providers::Selection {
                provider_id: "library".into(),
                model_id: "text".into(),
            },
            vec!["native isolation seed".into()],
        )
        .unwrap();
        let (context, original) =
            initial_consumer_fixture::prepared(store.as_ref(), keyed, &source).await;
        let session = context.session_id().clone();
        let tasks = PendingDriverTasks::default();
        let mut producer = PendingInitialProducer::register_scripted(
            &tasks,
            store.clone(),
            context,
            original,
            source,
        )
        .unwrap();
        producer.start().unwrap();
        let observed = producer.join().await.unwrap();
        let status = observed.status().clone();
        let failed = observed.observation_failed();
        let error = format!("{:?}", observed.error());
        // Join the actual producer and its resource supervisor before asserting;
        // durable completion metadata alone is not a task-join observation.
        tasks.wait_for_supervision().await;
        assert!(
            matches!(status, PendingInitialProducerStatus::ClosedAssignment),
            "{status:?}: {error}"
        );
        assert!(!failed, "{error}");
        for table in [
            "pending_initial_assignments",
            "pending_initial_closes",
            "pending_initial_cohorts",
            "pending_initial_epochs",
            "pending_initial_heads",
            "pending_initial_publications",
            "pending_initial_registered_completes",
            "pending_initial_source_slots",
            "initial_consumer_assignments",
            "initial_consumer_calls",
            "initial_consumer_closes",
            "initial_consumer_cohorts",
            "initial_consumer_heads",
            "initial_consumer_origins",
            "initial_consumer_publications",
            "initial_consumer_results",
            "initial_consumer_reverse",
            "initial_consumer_roster",
            "preparation_admission_witnesses",
        ] {
            let count: i64 =
                sqlx::query_scalar(&format!("SELECT count(*) FROM {table} WHERE session_id=$1"))
                    .bind(&session.0)
                    .fetch_one(&store.pool)
                    .await
                    .unwrap();
            assert!(count > 0, "genuine native seed missing: {table}");
        }
        // Retain these authentic rows for the foreign worker's denial checks.
        // Unlike the destructive native fixtures, this seed never purges them.
    }
}
