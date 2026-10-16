use super::super::PostgresStore;
use crate::{Store, StoreError};
#[tokio::test]
async fn creation_protocol_is_not_retrofitted() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let managed = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let protocol: i16 = sqlx::query_scalar("SELECT ownership_protocol FROM sessions WHERE id=$1")
        .bind(&managed.session_id.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(protocol, 1);
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM session_ownership_origins WHERE session_id=$1")
            .bind(&managed.session_id.0)
            .fetch_one(&store.pool)
            .await
            .unwrap();
    assert_eq!(count, 1);
    let session = crabber_core::SessionId::new();
    store
        .admit_run(crate::deletion::contract::admission(&session))
        .await
        .unwrap();
    let revision = store.session_revision(&session).await.unwrap();
    store
        .manage_session(crate::ManageSession {
            id: crabber_core::ControlId::new(),
            session_id: session.clone(),
            expected_revision: revision,
            change: crate::SessionChange::Rename {
                title: "renamed legacy session".into(),
            },
        })
        .await
        .unwrap();
    let protocol: i16 = sqlx::query_scalar("SELECT ownership_protocol FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(protocol, 0);
    assert!(
        sqlx::query("INSERT INTO session_ownership_origins(session_id,data_json) VALUES($1,'{}')")
            .bind(&session.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE sessions SET ownership_protocol=1 WHERE id=$1")
            .bind(&session.0)
            .execute(&store.pool)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn permanent_identity_guards() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let cold = PostgresStore::connect(&url).await.unwrap();
    let tombstone = crate::deletion::contract::deleted();
    let receipt = &tombstone.receipt;
    let crate::session_owner::SessionOwnerOutcome::Applied {
        authority: Some(authority),
        ..
    } = store
        .reserve_session_owner(
            crate::session_owner_contract::reservation(receipt.session_id.clone()),
            None,
        )
        .await
        .unwrap()
    else {
        panic!("reserved fixture owner")
    };

    insert_tombstone(&store, &tombstone).await;
    crate::deletion::contract::metadata(&cold, &tombstone).await;
    assert_eq!(
        cold.get_session_owner(&receipt.session_id, &crabber_core::SessionOwnerId::new())
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
    assert_eq!(
        cold.lookup_session_owner(
            &receipt.session_id,
            &crabber_core::SessionOwnerOperationId::new()
        )
        .await
        .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
    let key =
        crate::session_owner::preparation::PreparationKey::Unkeyed(crabber_core::ControlId::new());
    assert_eq!(
        cold.lookup_preparation(&receipt.session_id, &key)
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );

    crate::deletion::contract::creation_guards(&store, &tombstone).await;
    cancelled_after_erasure(&cold, authority).await;

    let error = sqlx::query("INSERT INTO sessions(id,data) VALUES($1,'{}'::jsonb)")
        .bind(&receipt.session_id.0)
        .execute(&store.pool)
        .await
        .unwrap_err();
    assert_eq!(super::super::db(error), StoreError::SessionDeleted);
    let error =
        sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json) VALUES($1,'{}')")
            .bind(&receipt.session_id.0)
            .execute(&store.pool)
            .await
            .unwrap_err();
    assert_eq!(super::super::db(error), StoreError::SessionDeleted);
    for query in [
        "UPDATE session_tombstones SET operation_id='changed' WHERE session_id=$1",
        "DELETE FROM session_tombstones WHERE session_id=$1",
    ] {
        assert!(
            sqlx::query(query)
                .bind(&receipt.session_id.0)
                .execute(&store.pool)
                .await
                .is_err()
        );
    }
    assert_eq!(
        cold.lookup_session_deletion(&receipt.session_id, &receipt.operation_id)
            .await
            .unwrap(),
        Some(receipt.clone())
    );
}

async fn cancelled_after_erasure(
    store: &dyn Store,
    authority: crate::session_owner::SessionOwnerAuthority,
) {
    let request = crate::session_owner::RequestSessionOwnerCancel {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: authority.session_id().clone(),
        target: authority.target().clone(),
        expected_revision: None,
        claim: crate::session_owner::SessionOwnerResolver {
            resolver_id: "host".into(),
            authority_claim: "authenticated host".into(),
        },
    };
    assert_eq!(
        store
            .request_session_owner_cancel(request.clone())
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
    let observed = crate::session_owner::ObserveSessionOwnerCancel {
        id: crabber_core::SessionOwnerOperationId::new(),
        session_id: request.session_id,
        target: request.target,
        request_id: request.id,
    };
    assert_eq!(
        store
            .observe_session_owner_cancel(observed, authority.clone())
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
    assert_eq!(
        store
            .pending_session_owner_cancel(&authority)
            .await
            .unwrap_err(),
        crate::session_owner::SessionOwnerError::Deleted
    );
}

async fn insert_tombstone(
    store: &super::super::PostgresStore,
    tombstone: &crate::deletion::Tombstone,
) {
    let receipt = &tombstone.receipt;
    sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
        .bind(&receipt.session_id.0).bind(&receipt.operation_id.0)
        .bind(i64::try_from(tombstone.expected_revision.get()).unwrap())
        .bind(i64::try_from(receipt.final_revision.get()).unwrap())
        .bind(receipt.semantic_digest.as_str()).bind(receipt.deleted_at.unix_timestamp())
        .bind(i32::try_from(receipt.deleted_at.nanosecond()).unwrap())
        .execute(&store.pool).await.unwrap();
}

#[tokio::test]
async fn deleted_payload_guards_precede_retained_rows_without_mutation() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    let cold = PostgresStore::connect(&url).await.unwrap();
    let session = crabber_core::SessionId::new();
    let revision = crate::deletion::contract::seed_payloads(&store, &session).await;
    let before: String = sqlx::query_scalar("SELECT value FROM extension_state WHERE session_id=$1 AND extension_id='extension' AND key='key'")
        .bind(&session.0).fetch_one(&store.pool).await.unwrap();
    insert_tombstone(&store, &crate::deletion::contract::deleted_for(&session)).await;
    crate::deletion::contract::payload_guards(&cold, &session).await;
    let after: String = sqlx::query_scalar("SELECT value FROM extension_state WHERE session_id=$1 AND extension_id='extension' AND key='key'")
        .bind(&session.0).fetch_one(&store.pool).await.unwrap();
    assert_eq!(after, before);
    let inbox: i64 = sqlx::query_scalar("SELECT COUNT(*) FROM inbox WHERE session_id=$1")
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(inbox, 1);
    let current: i64 = sqlx::query_scalar("SELECT public_revision FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&store.pool)
        .await
        .unwrap();
    assert_eq!(current, i64::try_from(revision.get()).unwrap());
}
