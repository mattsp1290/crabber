#[tokio::test]
async fn migration_preserves_predecessor_queue_bytes_and_legacy_membership() {
    let Some(url) = crate::postgres::tests::test_url() else {
        return;
    };
    let _guard = crate::postgres::tests::TEST_LOCK.lock().await;
    let pool = sqlx::PgPool::connect(&url).await.unwrap();
    let mut tx = crate::postgres::transaction::begin(&pool).await.unwrap();
    let namespace = format!("queue_migration_{}", uuid::Uuid::new_v4().simple());
    sqlx::raw_sql(&format!(
        "CREATE SCHEMA {namespace}; SET LOCAL search_path TO {namespace}"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql("CREATE TABLE schema_version(version INTEGER PRIMARY KEY); CREATE TABLE session_tombstones(session_id TEXT); CREATE TABLE future_queue(session_id TEXT NOT NULL,queue_id TEXT NOT NULL,sequence BIGINT NOT NULL,admission_key TEXT NOT NULL,data_json TEXT NOT NULL,PRIMARY KEY(session_id,queue_id))").execute(&mut *tx).await.unwrap();
    let original = r#"{"opaque":"unchanged predecessor queue bytes"}"#;
    sqlx::query(
        "INSERT INTO future_queue VALUES('legacy-session','legacy-queue',1,'legacy-admission',$1)",
    )
    .bind(original)
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../migrations/0021_queue_publications.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let saved: String = sqlx::query_scalar("SELECT data_json FROM future_queue")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    let marker: i32 =
        sqlx::query_scalar("SELECT CAST(source_protocol AS INTEGER) FROM future_queue")
            .fetch_one(&mut *tx)
            .await
            .unwrap();
    let sources:i64=sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM queue_publication_sources)+(SELECT COUNT(*) FROM queue_publication_cohorts)").fetch_one(&mut *tx).await.unwrap();
    assert_eq!(saved, original);
    assert_eq!((marker, sources), (0, 0));
    assert!(
        sqlx::query("UPDATE future_queue SET source_protocol=1 WHERE session_id='legacy-session'")
            .execute(&mut *tx)
            .await
            .is_err()
    );
    tx.rollback().await.unwrap();
}
