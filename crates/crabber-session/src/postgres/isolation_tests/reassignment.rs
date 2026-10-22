//! Isolate WITH CHECK from the independent immutable-run identity triggers.
use super::*;

pub(super) async fn run(url: &str, owner_pool: &sqlx::PgPool) {
    let schema = format!("crabber_rls_{}", uuid::Uuid::new_v4().simple());
    sqlx::query(&format!("CREATE SCHEMA {schema}"))
        .execute(owner_pool)
        .await
        .unwrap();
    let cleanup = SchemaCleanup {
        pool: owner_pool.clone(),
        name: schema.clone(),
    };
    let mut scoped_url = url::Url::parse(url).unwrap();
    scoped_url
        .query_pairs_mut()
        .append_pair("options", &format!("-c search_path={schema}"));
    PostgresStore::migrate(scoped_url.as_str()).await.unwrap();
    let owner = PostgresStore::connect(scoped_url.as_str()).await.unwrap();
    let worker = ProvisionedRole::worker(scoped_url.as_str(), &owner, &["ws-a"]).await;
    let original = SessionId::new();
    let own_other = SessionId::new();
    let foreign = SessionId::new();
    for (session, workspace) in [
        (&original, "ws-a"),
        (&own_other, "ws-a"),
        (&foreign, "ws-b"),
    ] {
        sqlx::query("INSERT INTO sessions(id,data) VALUES($1,$2)")
            .bind(&session.0)
            .bind(serde_json::json!({"workspace_id": workspace}))
            .execute(&owner.pool)
            .await
            .unwrap();
    }
    let run = RunId::new();
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES($1,$2,'completed','probe',0,'{}')")
        .bind(&run.0).bind(&original.0).execute(&owner.pool).await.unwrap();
    let (login, rls): (String, bool) =
        sqlx::query_as("SELECT current_user::text,row_security_active('runs'::regclass)")
            .fetch_one(&worker.pool)
            .await
            .unwrap();
    assert_eq!(login, worker.name);
    assert!(rls);

    // Only this disposable schema is altered. Shared test tables and production
    // trigger definitions remain intact even if this fixture is cancelled.
    sqlx::raw_sql(&format!(
        "ALTER TABLE {schema}.runs DISABLE TRIGGER runs_execution_identity; ALTER TABLE {schema}.runs DISABLE TRIGGER runs_native_identity"
    ))
    .execute(&owner.pool)
    .await
    .unwrap();
    let positive = sqlx::query("UPDATE runs SET session_id=$1 WHERE id=$2")
        .bind(&own_other.0)
        .bind(&run.0)
        .execute(&worker.pool)
        .await;
    let denied = sqlx::query("UPDATE runs SET session_id=$1 WHERE id=$2")
        .bind(&foreign.0)
        .bind(&run.0)
        .execute(&worker.pool)
        .await;
    // Restore both guards before inspecting either outcome, including failures.
    sqlx::raw_sql(&format!(
        "ALTER TABLE {schema}.runs ENABLE TRIGGER runs_execution_identity; ALTER TABLE {schema}.runs ENABLE TRIGGER runs_native_identity"
    ))
    .execute(&owner.pool)
    .await
    .unwrap();
    let enabled: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_trigger WHERE tgrelid=to_regclass($1) AND tgname IN ('runs_execution_identity','runs_native_identity') AND tgenabled='O'")
        .bind(format!("{schema}.runs")).fetch_one(&owner.pool).await.unwrap();
    assert_eq!(enabled, 2);
    assert_eq!(positive.unwrap().rows_affected(), 1);
    assert_eq!(code(&denied.unwrap_err()).as_deref(), Some("42501"));
    let unchanged: String = sqlx::query_scalar("SELECT session_id FROM runs WHERE id=$1")
        .bind(&run.0)
        .fetch_one(&owner.pool)
        .await
        .unwrap();
    assert_eq!(unchanged, own_other.0);
    worker.cleanup().await;
    owner.pool.close().await;
    cleanup.remove().await;
}

struct SchemaCleanup {
    pool: sqlx::PgPool,
    name: String,
}
impl SchemaCleanup {
    async fn remove(&self) {
        sqlx::query(&format!("DROP SCHEMA IF EXISTS {} CASCADE", self.name))
            .execute(&self.pool)
            .await
            .unwrap();
    }
}
impl Drop for SchemaCleanup {
    fn drop(&mut self) {
        let pool = self.pool.clone();
        let name = self.name.clone();
        tokio::spawn(async move {
            let _ = sqlx::query(&format!("DROP SCHEMA IF EXISTS {name} CASCADE"))
                .execute(&pool)
                .await;
        });
    }
}
