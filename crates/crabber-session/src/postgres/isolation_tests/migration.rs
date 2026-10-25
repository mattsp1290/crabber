use super::*;

#[tokio::test]
async fn migration_is_idempotent_and_marks_25() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        PostgresStore::migrate(&url).await.unwrap();
        let version: i32 = sqlx::query_scalar("SELECT max(version) FROM schema_version")
            .fetch_one(&admin.pool)
            .await
            .unwrap();
        assert_eq!(version, 25);
        let roles: i64 = sqlx::query_scalar("SELECT count(*) FROM pg_roles WHERE rolname IN ('crabber_worker','crabber_host') AND NOT rolcanlogin")
            .fetch_one(&admin.pool).await.unwrap();
        assert_eq!(roles, 2);
        let admitted = admit(&admin, "ws-a").await;
        let workspace: String = sqlx::query_scalar("SELECT workspace_id FROM sessions WHERE id=$1")
            .bind(&admitted.session.id.0)
            .fetch_one(&admin.pool)
            .await
            .unwrap();
        assert_eq!(workspace, "ws-a");
        let session = SessionId::new();
        admin
            .reserve_session_owner(reservation(session.clone(), "ws-a"), None)
            .await
            .unwrap();
        let workspace: String =
            sqlx::query_scalar("SELECT workspace_id FROM session_owner_contexts WHERE session_id=$1")
                .bind(&session.0)
                .fetch_one(&admin.pool)
                .await
                .unwrap();
        assert_eq!(workspace, "ws-a");
        let legacy = SessionId::new();
        sqlx::query("INSERT INTO session_owner_contexts(session_id,data_json) VALUES($1,'{}')")
            .bind(&legacy.0)
            .execute(&admin.pool)
            .await
            .unwrap();
        let workspace: Option<String> =
            sqlx::query_scalar("SELECT workspace_id FROM session_owner_contexts WHERE session_id=$1")
                .bind(&legacy.0)
                .fetch_one(&admin.pool)
                .await
                .unwrap();
        assert_eq!(workspace, None);
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn worker_cannot_migrate_ddl_or_read_grants() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        let role = ProvisionedRole::worker(&url, &admin, &["ws-a"]).await;
        let other = ProvisionedRole::worker(&url, &admin, &["ws-b"]).await;
        PostgresStore::migrate(&role.url).await.unwrap();
        denied(&role.pool, "CREATE TABLE crabber_forbidden(id int)").await;
        denied(
            &role.pool,
            "CREATE FUNCTION crabber_forbidden() RETURNS int LANGUAGE sql AS 'SELECT 1'",
        )
        .await;
        denied(
            &role.pool,
            "INSERT INTO crabber_workspace_grants VALUES(current_user,'ws-b')",
        )
        .await;
        let grants: Vec<String> =
            sqlx::query_scalar("SELECT role_name FROM crabber_workspace_grants")
                .fetch_all(&role.pool)
                .await
                .unwrap();
        assert_eq!(grants, vec![role.name.clone()]);
        sqlx::query("DELETE FROM schema_version WHERE version=25")
            .execute(&admin.pool)
            .await
            .unwrap();
        let error = PostgresStore::migrate(&role.url).await.unwrap_err();
        sqlx::query("INSERT INTO schema_version(version) VALUES(25)")
            .execute(&admin.pool)
            .await
            .unwrap();
        assert_eq!(error, StoreError::SessionIdentityMismatch);
        role.cleanup().await;
        other.cleanup().await;
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn migration_without_createrole_fails_clearly() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
    let admin = admin(&url).await;
    let owner = format!(
        "crabber_test_{}",
        &uuid::Uuid::new_v4().simple().to_string()[..8]
    );
    let schema = owner.clone();
    let password = uuid::Uuid::new_v4().simple().to_string();
    sqlx::raw_sql(&format!(
        "CREATE ROLE {owner} LOGIN PASSWORD '{password}'; CREATE SCHEMA {schema} AUTHORIZATION {owner}"
    ))
    .execute(&admin.pool)
    .await
    .unwrap();
    let mut owner_url = url::Url::parse(&url).unwrap();
    owner_url.set_username(&owner).unwrap();
    owner_url.set_password(Some(&password)).unwrap();
    owner_url
        .query_pairs_mut()
        .append_pair("options", &format!("-c search_path={schema}"));
    let names = RoleNames {
        pool: Some(admin.pool.clone()),
        owner: owner.clone(),
    };
    sqlx::raw_sql("ALTER ROLE crabber_worker RENAME TO crabber_worker_hidden; ALTER ROLE crabber_host RENAME TO crabber_host_hidden")
        .execute(&admin.pool).await.unwrap();
    let error = PostgresStore::migrate(owner_url.as_str()).await;
    names.restore().await;
    assert_eq!(
        error.unwrap_err(),
        StoreError::Validation("migration requires crabber_worker and crabber_host roles".into())
    );
    let mut options = sqlx::postgres::PgConnectOptions::from_str(owner_url.as_str()).unwrap();
    // Re-open as owner to prove failed migration rolled back every schema object.
    options = options.application_name("crabber-isolation-migration-check");
    let pool = sqlx::postgres::PgPoolOptions::new()
        .max_connections(1)
        .connect_with(options)
        .await
        .unwrap();
    let absent: bool = sqlx::query_scalar("SELECT to_regclass('schema_version') IS NULL")
        .fetch_one(&pool)
        .await
        .unwrap();
    assert!(absent);
    pool.close().await;
    PostgresStore::migrate(owner_url.as_str()).await.unwrap();
    sqlx::raw_sql(&format!(
        "DROP SCHEMA {schema} CASCADE; DROP OWNED BY {owner}; DROP ROLE {owner}"
    ))
    .execute(&admin.pool)
    .await
    .unwrap();
    names.disarm();
    })
    .await
    .unwrap();
}
use std::str::FromStr;

struct RoleNames {
    pool: Option<sqlx::PgPool>,
    owner: String,
}
impl RoleNames {
    async fn restore(&self) {
        sqlx::raw_sql("ALTER ROLE crabber_worker_hidden RENAME TO crabber_worker; ALTER ROLE crabber_host_hidden RENAME TO crabber_host").execute(self.pool.as_ref().unwrap()).await.unwrap();
    }
    fn disarm(mut self) {
        self.pool = None;
    }
}
impl Drop for RoleNames {
    fn drop(&mut self) {
        let Some(pool) = self.pool.take() else {
            return;
        };
        let owner = self.owner.clone();
        tokio::spawn(async move {
            let _ = sqlx::raw_sql("ALTER ROLE crabber_worker_hidden RENAME TO crabber_worker; ALTER ROLE crabber_host_hidden RENAME TO crabber_host").execute(&pool).await;
            let _ = sqlx::raw_sql(&format!("DROP SCHEMA IF EXISTS {owner} CASCADE; DROP OWNED BY {owner}; DROP ROLE IF EXISTS {owner}"))
                .execute(&pool).await;
        });
    }
}
