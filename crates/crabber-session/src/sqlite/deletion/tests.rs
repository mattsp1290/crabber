use super::super::SqliteStore;
use crate::{Store, StoreError};
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
