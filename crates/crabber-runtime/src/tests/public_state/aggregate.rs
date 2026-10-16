//! Public Runtime reads observe actual worker commits and grant no new work.
use super::*;
use crabber_session::{
    PublicSessionLimitsV2, PublicSessionOutcomeV2, PublicSessionPollV2, PublicSessionRequestV2,
    PublicSessionSubscriptionV2, SnapshotLimits,
};
async fn observe(store: Arc<dyn Store>) {
    let provider = FakeProvider::scripted(vec![text_script("persisted")]);
    let runtime = Orchestrator::builder()
        .behavior_version("runtime-public-state-contract.v1")
        .store(store.clone())
        .resolver(Arc::new(provider.clone()))
        .plan_provider(Arc::new(StaticPlanProvider::new(vec![], vec![])))
        .build()
        .unwrap();
    let handle = runtime.start(request()).await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let revision = store.session_revision(&session).await.unwrap();
    let events = store.list_events(&session, None, 128).await.unwrap();
    let limits = PublicSessionLimitsV2 {
        header_bytes: 65_536,
        proof_bytes: 32 << 20,
        approval_interactions: 64,
        tool_requirements: 64,
        queue_entries: 64,
        registered_owners: 1024,
        run_controls: 64,
        history: SnapshotLimits {
            messages: 32,
            tool_calls: 32,
            parts: 128,
            text_bytes: 65_536,
            encoded_bytes: 131_072,
        },
    };
    let PublicSessionOutcomeV2::Page(page) = runtime
        .public_session_snapshot_v2(PublicSessionRequestV2 {
            session_id: session.clone(),
            limits,
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("complete public capture")
    };
    assert_eq!(page.captured_revision, revision);
    assert_eq!(page.header.revision, revision);
    assert_eq!(page.run.as_ref().unwrap().run_id, run);
    assert_eq!(page.run.as_ref().unwrap().status, RunStatus::Completed);
    assert!(contains_text(&page.history.messages, "persisted"));
    let mut subscription = PublicSessionSubscriptionV2::new(session.clone(), limits);
    assert!(matches!(
        runtime
            .poll_public_session_v2(&mut subscription)
            .await
            .unwrap(),
        PublicSessionPollV2::Page { reset: true, .. }
    ));
    assert!(matches!(
        runtime.poll_public_session_v2(&mut subscription).await.unwrap(),
        PublicSessionPollV2::Unchanged { revision: actual, .. } if actual==revision
    ));
    assert_eq!(store.session_revision(&session).await.unwrap(), revision);
    assert_eq!(
        store.list_events(&session, None, 128).await.unwrap(),
        events
    );
    assert_eq!(provider.requests().len(), 1);
}
#[tokio::test]
async fn memory_actual_worker_public_state_is_pure() {
    observe(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_actual_worker_public_state_is_pure() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("public-state.db");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    observe(Arc::new(
        crabber_session::SqliteStore::connect(path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_actual_worker_public_state_is_pure() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none());
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    observe(Arc::new(
        crabber_session::PostgresStore::connect(&url).await.unwrap(),
    ))
    .await;
}
