#[tokio::test]
async fn migration_preserves_predecessor_queue_bytes_and_legacy_membership() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let mut tx = crate::transaction::begin(&pool).await.unwrap();
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
        "../../../migrations/sqlite/0017_queue_publications.sql"
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
