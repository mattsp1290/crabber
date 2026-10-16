//! Exercise the database fence directly, bypassing every Store precheck.
use super::super::super::SqliteStore;
const NATIVE: &str = "INSERT INTO tool_calls(id,run_id,status,data,snapshot_parts,snapshot_text,snapshot_bytes) SELECT ?1,run_id,status,data,snapshot_parts,snapshot_text,snapshot_bytes FROM tool_calls WHERE id=?2";
const IMPORTED: &str = "INSERT INTO imported_tool_calls(id,session_id,ordinal,data_json,snapshot_parts,snapshot_text,snapshot_bytes) VALUES(?1,?2,?3,'{}',0,0,2)";
#[tokio::test]
async fn cross_table_identity_fence_rejects_bypass_and_serializes_races() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("identity.db");
    SqliteStore::migrate(&path).await.unwrap();
    let store = SqliteStore::connect(&path).await.unwrap();
    let cold = SqliteStore::connect(&path).await.unwrap();
    let fixture = crate::fork_pending_contract::fixture(&store).await;
    let native = &fixture.target.call_id.0;
    let session = &fixture.request.source_session_id.0;
    bypass(&store, native, session).await;
    lifecycle(&store, &cold, native, session).await;
}
async fn bypass(store: &SqliteStore, native: &str, session: &str) {
    let imported = format!("imported-{}", uuid::Uuid::new_v4());
    sqlx::query(IMPORTED)
        .bind(&imported)
        .bind(session)
        .bind(1_i64)
        .execute(&store.writer)
        .await
        .unwrap();
    // Native INSERT cannot turn presentation history into executable authority.
    assert!(
        sqlx::query(NATIVE)
            .bind(&imported)
            .bind(native)
            .execute(&store.writer)
            .await
            .is_err()
    );
    // Imported insertion cannot claim a native identity either.
    assert!(
        sqlx::query(IMPORTED)
            .bind(native)
            .bind(session)
            .bind(2_i64)
            .execute(&store.writer)
            .await
            .is_err()
    );
    for policy in ["OR IGNORE", "OR REPLACE"] {
        let query = NATIVE.replacen("INSERT INTO", &format!("INSERT {policy} INTO"), 1);
        assert!(
            sqlx::query(&query)
                .bind(&imported)
                .bind(native)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
    let other = format!("native-{}", uuid::Uuid::new_v4());
    sqlx::query(NATIVE)
        .bind(&other)
        .bind(native)
        .execute(&store.writer)
        .await
        .unwrap();
    assert!(
        sqlx::query("UPDATE tool_calls SET id=?1 WHERE id=?2")
            .bind(&imported)
            .bind(&other)
            .execute(&store.writer)
            .await
            .is_err()
    );
    // Same-row native upserts remain legal; they do not change identity kind.
    sqlx::query(&format!(
        "{NATIVE} ON CONFLICT(id) DO UPDATE SET status=excluded.status"
    ))
    .bind(&other)
    .bind(native)
    .execute(&store.writer)
    .await
    .unwrap();
    for policy in ["OR IGNORE", "OR REPLACE"] {
        let query = IMPORTED.replacen("INSERT INTO", &format!("INSERT {policy} INTO"), 1);
        assert!(
            sqlx::query(&query)
                .bind(native)
                .bind(session)
                .bind(2_i64)
                .execute(&store.writer)
                .await
                .is_err()
        );
    }
}
async fn lifecycle(store: &SqliteStore, cold: &SqliteStore, native: &str, session: &str) {
    // A rolled-back reservation is reusable and a committed record owns its key.
    let reusable = format!("rollback-{}", uuid::Uuid::new_v4());
    let mut tx = store.writer.begin().await.unwrap();
    sqlx::query(IMPORTED)
        .bind(&reusable)
        .bind(session)
        .bind(3_i64)
        .execute(&mut *tx)
        .await
        .unwrap();
    tx.rollback().await.unwrap();
    sqlx::query(NATIVE)
        .bind(&reusable)
        .bind(native)
        .execute(&store.writer)
        .await
        .unwrap();
    let race = format!("race-{}", uuid::Uuid::new_v4());
    let (a, b) = tokio::join!(
        sqlx::query(NATIVE)
            .bind(&race)
            .bind(native)
            .execute(&store.writer),
        sqlx::query(IMPORTED)
            .bind(&race)
            .bind(session)
            .bind(4_i64)
            .execute(&cold.writer)
    );
    assert_ne!(a.is_ok(), b.is_ok());
    let count: i64 = sqlx::query_scalar("SELECT (SELECT count(*) FROM tool_calls WHERE id=?1)+(SELECT count(*) FROM imported_tool_calls WHERE id=?1)").bind(&race).fetch_one(&store.writer).await.unwrap();
    assert_eq!(count, 1);
    // The FK forbids removing the reservation before the record is gone.
    assert!(
        sqlx::query("DELETE FROM tool_call_identities WHERE id=?1")
            .bind(&reusable)
            .execute(&store.writer)
            .await
            .is_err()
    );
    sqlx::query("DELETE FROM tool_calls WHERE id=?1")
        .bind(&reusable)
        .execute(&store.writer)
        .await
        .unwrap();
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM tool_call_identities WHERE id=?1")
        .bind(&reusable)
        .fetch_one(&store.writer)
        .await
        .unwrap();
    assert_eq!(count, 0);
    sqlx::query(IMPORTED)
        .bind(&reusable)
        .bind(session)
        .bind(5_i64)
        .execute(&store.writer)
        .await
        .unwrap();
}
