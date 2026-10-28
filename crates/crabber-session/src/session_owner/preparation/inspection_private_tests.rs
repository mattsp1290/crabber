//! Private corruption probes stay in the native crate, not source-included adapters.
use super::*;
use crate::{MemoryStore, Store};
use crabber_core::InputFingerprint;
async fn private_binding(store: &impl Store) {
    let (original, admitted) = inspection::contract::fresh(store).await;
    let session = &admitted.session.id;
    let revision = store.session_revision(session).await.unwrap();
    // A coherent private binding substitution still passes original intent;
    // the protected complete original binding hash must reject it independently.
    let mut changed = original.clone();
    let PreparedOperation::Admit(bound) = &mut changed.operation else {
        panic!();
    };
    bound.binding.plan_fingerprint = InputFingerprint::new("c".repeat(64)).unwrap();
    bound.request.plan_fingerprint = bound.binding.plan_fingerprint.as_str().to_owned();
    bound.binding.input_fingerprint = bound.binding.computed_input_fingerprint().unwrap();
    changed.verify_original_intent().unwrap();
    assert!(
        store
            .inspect_initial_preparation_admission(&changed)
            .await
            .is_err()
    );
    let mut wrong = original;
    let PreparedOperation::Admit(bound) = &mut wrong.operation else {
        panic!();
    };
    bound.request.workspace_id = "foreign".into();
    assert!(
        store
            .inspect_initial_preparation_admission(&wrong)
            .await
            .is_err()
    );
    assert_eq!(store.session_revision(session).await.unwrap(), revision);
    let mut oversized = inspection::contract::fresh(store).await.0;
    let PreparedOperation::Admit(bound) = &mut oversized.operation else {
        panic!()
    };
    bound.binding.runtime_semantics =
        serde_json::Value::String("x".repeat(inspection::MAX_PROOF + 1));
    assert!(matches!(
        store
            .inspect_initial_preparation_admission(&oversized)
            .await,
        Err(PreparationCommitError::Store(crate::StoreError::AdmissionLimit(limit)))
            if limit.limit_bytes() == Some(33_554_432)
    ));
}
#[tokio::test]
async fn memory_initial_inspection_private_binding() {
    private_binding(&MemoryStore::new()).await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_initial_inspection_private_binding() {
    let dir = tempfile::tempdir().unwrap();
    let path = dir.path().join("private.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    private_binding(&crate::SqliteStore::connect(&path).await.unwrap()).await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_initial_inspection_private_binding() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        crate::PostgresStore::migrate(&url).await.unwrap();
        private_binding(&crate::PostgresStore::connect(&url).await.unwrap()).await;
    })
    .await
    .unwrap();
}
