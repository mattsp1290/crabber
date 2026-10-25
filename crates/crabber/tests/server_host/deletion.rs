//! Real public controls work after close without allocating execution resources.
use super::*;
use crabber::{
    ManageSession, SessionChange, SessionDefaults,
    core::{ControlId, SessionRevision},
    session::{DeleteSession, DeletionOutcome},
};

async fn quiet_deletion(store: Arc<dyn Store>) {
    let factories = Arc::new(AtomicUsize::new(0));
    let provider = FakeProvider::scripted(vec![]);
    let agent = Agent::builder()
        .behavior_version("quiet-deletion.v1")
        .store(store.clone())
        .provider(Arc::new(provider.clone()))
        .config(AgentConfig::new(selection("scripted")))
        .extension_factory(Arc::new(super::ForbiddenFactory(factories.clone())))
        .build()
        .unwrap();
    let session = SessionId::new();
    let created = agent
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: "quiet-deletion".into(),
                directory: "/quiet-deletion".into(),
                title: "private history".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await
        .unwrap();
    agent.close_extensions().await.unwrap();
    let request = DeleteSession {
        id: ControlId::new(),
        session_id: session.clone(),
        expected_revision: created.revision,
    };
    let DeletionOutcome::Applied(receipt) = agent.delete_session(request.clone()).await.unwrap()
    else {
        panic!("quiet managed session")
    };
    assert_eq!(
        agent.delete_session(request).await.unwrap(),
        DeletionOutcome::Replayed(receipt.clone())
    );
    assert_eq!(
        agent
            .lookup_session_deletion(&session, &receipt.operation_id)
            .await
            .unwrap(),
        Some(receipt)
    );
    assert!(store.get_session(&session).await.unwrap().is_none());
    assert_eq!(factories.load(Ordering::SeqCst), 0);
    assert_eq!(provider.requests().len(), 0);
    assert!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .iter()
            .all(|run| run.session_id != session)
    );
}
#[tokio::test]
async fn memory_quiet_deletion_after_close() {
    quiet_deletion(Arc::new(MemoryStore::new())).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_quiet_deletion_after_close() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("delete.db");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    quiet_deletion(Arc::new(
        crabber::session::SqliteStore::connect(&path).await.unwrap(),
    ))
    .await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_quiet_deletion_after_close() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crabber::session::PostgresStore::migrate(&url)
            .await
            .unwrap();
        quiet_deletion(Arc::new(
            crabber::session::PostgresStore::connect(&url)
                .await
                .unwrap(),
        ))
        .await;
    })
    .await
    .unwrap();
}
