//! Actual origin, carried absent scope, and retained legacy marker contracts.
use super::{Error, Origin, Proof, Source};
use crate::Store;
use std::sync::Arc;
mod legacy;
#[tokio::test]
async fn fresh_absent_service_scope_carries_exact_original_into_admitted_session() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let sessions = crate::session_owner_contract::services::handoff::run_sessions(
        Arc::new(store.clone()),
        false,
    )
    .await;
    assert_eq!(sessions.len(), 1);
    for session in sessions {
        let mut tx = crate::postgres::transaction::begin(&store.pool)
            .await
            .unwrap();
        let marker: i64 = sqlx::query_scalar(
            "SELECT CAST(owner_scope_protocol AS BIGINT) FROM sessions WHERE id=$1",
        )
        .bind(&session.0)
        .fetch_one(&mut *tx)
        .await
        .unwrap();
        let origin = super::load(&mut tx, &session, &mut Proof::bounded(32 * 1024 * 1024))
            .await
            .unwrap();
        assert_eq!(marker, 1);
        assert!(matches!(origin.source, Source::Absent { .. }));
        super::require(
            &mut tx,
            &session,
            &origin.workspace,
            &origin.directory,
            &mut Proof::bounded(32 * 1024 * 1024),
        )
        .await
        .unwrap();
        assert_eq!(
            super::require(
                &mut tx,
                &session,
                &origin.workspace,
                &origin.directory,
                &mut Proof::bounded(1)
            )
            .await
            .unwrap_err(),
            Error::Capacity
        );
        tx.rollback().await.unwrap();
    }
}
#[tokio::test]
async fn service_scope_origin_is_immutable_and_required_before_new_generation() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    for erase in [false, true] {
        let (request, receipt, next) =
            crate::session_owner_contract::services::cohort::source_fixture(&store).await;
        let session = &receipt.source.root.session_id;
        let raw: String = sqlx::query_scalar(
            "SELECT data_json FROM session_owner_scope_origins WHERE session_id=$1",
        )
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
        assert!(
            sqlx::query("DELETE FROM session_owner_scope_origins WHERE session_id=$1")
                .bind(&session.0)
                .execute(&store.pool)
                .await
                .is_err()
        );
        let mut origin: Origin = serde_json::from_str(&raw).unwrap();
        origin.workspace = "forged-scope".into();
        legacy::corrupt_origin(&store, session, (!erase).then(|| origin.encoded().unwrap())).await;
        crate::session_owner_contract::services::cohort::source_refuses(
            &store,
            request,
            receipt,
            Error::UnknownStoreOutcome,
            next,
        )
        .await;
    }
}
#[tokio::test]
async fn retained_legacy_context_refuses_service_after_all_owner_rows_erased() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (_request, receipt, next) =
        crate::session_owner_contract::services::cohort::source_fixture(&store).await;
    let session = &receipt.source.root.session_id;
    legacy::erase_old_scope(&store, session).await;
    let mut generation = crate::session_owner_contract::services::generation();
    generation.id = "new-generation-after-legacy-erase".into();
    let request = crate::session_owner::service::RegisterSessionServiceDependency {
        grant: next,
        generation,
    };
    let reserve = request.source().unwrap().identities().unwrap().reserve;
    assert_eq!(
        store
            .register_session_service_dependency(request)
            .await
            .unwrap_err(),
        Error::Unsupported
    );
    assert!(
        store
            .lookup_session_owner(session, &reserve)
            .await
            .unwrap()
            .is_none()
    );
    assert!(
        sqlx::query("UPDATE session_owner_contexts SET scope_protocol=1 WHERE session_id=$1")
            .bind(&session.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
}

#[tokio::test]
async fn untyped_legacy_owner_cannot_hide_an_older_service_generation() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let (original, receipt, next) =
        crate::session_owner_contract::services::cohort::source_fixture(&store).await;
    let reservation = receipt.source.reservation(&original.grant).unwrap();
    super::super::protocol::tests::legacy_fixture(&store, &reservation).await;
    for table in ["session_owner_current", "session_owner_audits"] {
        sqlx::query(&format!(
            "UPDATE {table} SET data_json='{{}}' WHERE session_id=$1 AND owner_id=$2"
        ))
        .bind(&reservation.binding.session_id.0)
        .bind(&reservation.binding.id.0)
        .execute(&store.pool)
        .await
        .unwrap();
    }
    let mut generation = crate::session_owner_contract::services::generation();
    generation.id = "cannot-replace-an-untyped-legacy-owner".into();
    let request = crate::session_owner::service::RegisterSessionServiceDependency {
        grant: next,
        generation,
    };
    assert_eq!(
        store
            .register_session_service_dependency(request)
            .await
            .unwrap_err(),
        Error::Unsupported
    );
}

#[tokio::test]
async fn missing_required_origin_cannot_delete_a_quiet_managed_session() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    crate::PostgresStore::migrate(&url).await.unwrap();
    let store = crate::PostgresStore::connect(&url).await.unwrap();
    let saved = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    legacy::corrupt_origin(&store, &saved.session_id, None).await;
    let mut tx = crate::postgres::transaction::begin(&store.pool)
        .await
        .unwrap();
    assert_eq!(
        super::verify_if_present(
            &mut tx,
            &saved.session_id,
            &mut Proof::bounded(32 * 1024 * 1024)
        )
        .await
        .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    tx.rollback().await.unwrap();
    assert_eq!(
        store
            .delete_session(crate::DeleteSession {
                id: crabber_core::ControlId::new(),
                session_id: saved.session_id.clone(),
                expected_revision: saved.revision,
            })
            .await
            .unwrap_err(),
        crate::DeletionError::UnknownStoreOutcome
    );
    assert_eq!(
        store.session_revision(&saved.session_id).await.unwrap(),
        saved.revision
    );
    assert!(matches!(
        store.session_disposition(&saved.session_id).await.unwrap(),
        crate::SessionDisposition::Present { .. }
    ));
}

mod created;
