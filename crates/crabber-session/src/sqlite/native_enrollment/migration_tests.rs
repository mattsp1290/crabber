//! The migration preserves actual predecessor fields and never repairs births.
use super::*;

#[tokio::test]
async fn migration_preserves_legacy_bytes_and_refuses_rebirth_without_promoting_sources() {
    let pool = sqlx::sqlite::SqlitePoolOptions::new()
        .max_connections(1)
        .connect("sqlite::memory:")
        .await
        .unwrap();
    let mut tx = crate::transaction::begin(&pool).await.unwrap();
    sqlx::raw_sql("CREATE TABLE schema_version(version INTEGER PRIMARY KEY); CREATE TABLE session_tombstones(session_id TEXT); CREATE TABLE runs(seq BIGINT PRIMARY KEY,id TEXT NOT NULL UNIQUE,session_id TEXT NOT NULL,status TEXT NOT NULL,data TEXT NOT NULL); CREATE TABLE native_run_sources(session_id TEXT NOT NULL,run_id TEXT NOT NULL,sequence BIGINT NOT NULL,issuance_digest TEXT NOT NULL,PRIMARY KEY(session_id,run_id,sequence))")
        .execute(&mut *tx).await.unwrap();
    let bytes = r#" {"opaque" : "exact predecessor bytes"} "#;
    sqlx::query("INSERT INTO runs(seq,id,session_id,status,data) VALUES(1,'legacy-run','legacy-session','completed',$1)").bind(bytes).execute(&mut *tx).await.unwrap();
    sqlx::raw_sql(include_str!(
        "../../../migrations/sqlite/0018_native_execution_enrollment.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let saved: (String,i32,Option<String>) = sqlx::query_as("SELECT data,CAST(execution_enrollment_protocol AS INTEGER),execution_enrollment_origin_id FROM runs WHERE id='legacy-run'").fetch_one(&mut *tx).await.unwrap();
    assert_eq!(saved, (bytes.to_owned(), 0, None));
    let sources: i64 = sqlx::query_scalar("SELECT (SELECT COUNT(*) FROM native_execution_origins)+(SELECT COUNT(*) FROM native_execution_unclassified)").fetch_one(&mut *tx).await.unwrap();
    assert_eq!(sources, 0);
    // The current reader also selects the Pending column; retain the actual
    // raw-migration assertions above before installing its real successor DDL.
    sqlx::raw_sql(include_str!(
        "../../../migrations/sqlite/0019_pending_initial.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    sqlx::raw_sql(include_str!(
        "../../../migrations/sqlite/0020_pending_initial_consumer.sql"
    ))
    .execute(&mut *tx)
    .await
    .unwrap();
    let session = SessionId("legacy-session".into());
    let run = RunId("legacy-run".into());
    assert_eq!(
        original(&mut tx, &session, &run, &mut Proof::bounded(32 << 20))
            .await
            .unwrap(),
        Status::Legacy
    );
    for statement in [
        "DELETE FROM runs WHERE id='legacy-run'",
        "UPDATE runs SET id='rebound-run' WHERE id='legacy-run'",
        "UPDATE runs SET session_id='rebound-session' WHERE id='legacy-run'",
        "UPDATE runs SET execution_enrollment_protocol=1,execution_enrollment_origin_id=id WHERE id='legacy-run'",
        "INSERT INTO runs(seq,id,session_id,status,data,execution_enrollment_protocol) VALUES(2,'missing-origin','legacy-session','completed','{}',1)",
        "INSERT OR REPLACE INTO runs(seq,id,session_id,status,data) VALUES(1,'legacy-run','legacy-session','completed','{}')",
    ] {
        let mut save = tx.begin().await.unwrap();
        assert!(
            sqlx::query(statement).execute(&mut *save).await.is_err(),
            "{statement}"
        );
        save.rollback().await.unwrap();
    }
    // Existing ordinary status publication stays possible and creates no origin.
    sqlx::query("UPDATE runs SET status='failed' WHERE id='legacy-run'")
        .execute(&mut *tx)
        .await
        .unwrap();
    assert_eq!(
        original(&mut tx, &session, &run, &mut Proof::bounded(32 << 20))
            .await
            .unwrap(),
        Status::Legacy
    );
    let saved: String = sqlx::query_scalar("SELECT data FROM runs WHERE id='legacy-run'")
        .fetch_one(&mut *tx)
        .await
        .unwrap();
    assert_eq!(saved, bytes);
    tx.rollback().await.unwrap();
}
