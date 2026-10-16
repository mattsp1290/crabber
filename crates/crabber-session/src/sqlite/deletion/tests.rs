use super::super::SqliteStore;
use crate::{Store, StoreError};
#[tokio::test]
async fn creation_protocol_is_not_retrofitted() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("origins.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let managed = store
        .manage_session(crate::management_contract::create())
        .await
        .unwrap();
    let protocol: i64 = sqlx::query_scalar("SELECT ownership_protocol FROM sessions WHERE id=$1")
        .bind(&managed.session_id.0)
        .fetch_one(&store.readers)
        .await
        .unwrap();
    assert_eq!(protocol, 1);
    // An empty pre-protocol session has no dependent rows to make REPLACE
    // fail incidentally. Its immutable cohort must reject the replacement.
    let mut empty = store
        .get_session(&managed.session_id)
        .await
        .unwrap()
        .unwrap();
    empty.id = crabber_core::SessionId::new();
    sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
        .bind(&empty.id.0)
        .bind(serde_json::to_string(&empty).unwrap())
        .execute(&store.writer)
        .await
        .unwrap();
    for query in [
        "INSERT OR REPLACE INTO sessions(id,data,ownership_protocol) SELECT id,data,1 FROM sessions WHERE id=$1",
        "INSERT OR IGNORE INTO sessions(id,data,ownership_protocol) SELECT id,data,1 FROM sessions WHERE id=$1",
    ] {
        assert!(
            sqlx::query(query)
                .bind(&empty.id.0)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
    let count: i64 =
        sqlx::query_scalar("SELECT COUNT(*) FROM session_ownership_origins WHERE session_id=$1")
            .bind(&managed.session_id.0)
            .fetch_one(&store.readers)
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
    let protocol: i64 = sqlx::query_scalar("SELECT ownership_protocol FROM sessions WHERE id=$1")
        .bind(&session.0)
        .fetch_one(&store.readers)
        .await
        .unwrap();
    assert_eq!(protocol, 0);
    assert!(
        sqlx::query("INSERT INTO session_ownership_origins(session_id,data_json) VALUES($1,'{}')")
            .bind(&session.0)
            .execute(&store.writer)
            .await
            .is_err()
    );
    assert!(
        sqlx::query("UPDATE sessions SET ownership_protocol=1 WHERE id=$1")
            .bind(&session.0)
            .execute(&store.writer)
            .await
            .is_err()
    );
}
#[tokio::test]
async fn permanent_identity_guards() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("deleted.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let cold = SqliteStore::connect(&path).await.unwrap();
    let tombstone = crate::deletion::contract::deleted();
    let receipt = &tombstone.receipt;
    sqlx::query("INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES($1,$2,$3,$4,$5,$6,$7,1)")
        .bind(&receipt.session_id.0).bind(&receipt.operation_id.0)
        .bind(i64::try_from(tombstone.expected_revision.get()).unwrap())
        .bind(i64::try_from(receipt.final_revision.get()).unwrap())
        .bind(receipt.semantic_digest.as_str()).bind(receipt.deleted_at.unix_timestamp())
        .bind(i64::from(receipt.deleted_at.nanosecond()))
        .execute(&store.writer).await.unwrap();
    crate::deletion::contract::metadata(&cold, &tombstone).await;
    crate::deletion::contract::creation_guards(&store, &tombstone).await;
    let error = sqlx::query("INSERT INTO sessions(id,data) VALUES($1,'{}')")
        .bind(&receipt.session_id.0)
        .execute(&store.writer)
        .await
        .unwrap_err();
    assert_eq!(super::super::db(error), StoreError::SessionDeleted);
    let error =
        sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json) VALUES($1,'{}')")
            .bind(&receipt.session_id.0)
            .execute(&store.writer)
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
                .execute(&store.writer)
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
