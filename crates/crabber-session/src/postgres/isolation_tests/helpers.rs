use super::*;
use sqlx::postgres::PgPoolOptions;

pub(super) const CONTRACT_WORKSPACES: &[&str] = &[
    "workspace",
    "bound-workspace",
    "workspace/所有者",
    "workspace\nopaque",
    "test",
    "ws-a",
    "ws-b",
];

pub(super) async fn admin() -> Option<(String, PostgresStore)> {
    let url = tests::test_url()?;
    PostgresStore::migrate(&url).await.unwrap();
    let store = PostgresStore::connect(&url).await.unwrap();
    Some((url, store))
}

pub(super) struct ProvisionedRole {
    pub url: String,
    pub name: String,
    pub pool: sqlx::PgPool,
    admin: sqlx::PgPool,
}
impl ProvisionedRole {
    pub async fn worker(url: &str, admin: &PostgresStore, workspaces: &[&str]) -> Self {
        Self::provision(url, admin, "crabber_worker", workspaces).await
    }
    pub async fn host(url: &str, admin: &PostgresStore) -> Self {
        Self::provision(url, admin, "crabber_host", &[]).await
    }
    async fn provision(url: &str, admin: &PostgresStore, group: &str, workspaces: &[&str]) -> Self {
        let name = format!(
            "crabber_test_{}",
            &uuid::Uuid::new_v4().simple().to_string()[..8]
        );
        let password = uuid::Uuid::new_v4().simple().to_string();
        sqlx::query(&format!(
            "CREATE ROLE {name} LOGIN PASSWORD '{password}' IN ROLE {group}"
        ))
        .execute(&admin.pool)
        .await
        .unwrap();
        for workspace in workspaces {
            sqlx::query(
                "INSERT INTO crabber_workspace_grants(role_name,workspace_id) VALUES($1,$2)",
            )
            .bind(&name)
            .bind(workspace)
            .execute(&admin.pool)
            .await
            .unwrap();
        }
        let mut parsed = url::Url::parse(url).unwrap();
        parsed.set_username(&name).unwrap();
        parsed.set_password(Some(&password)).unwrap();
        if workspaces.contains(&"workspace/所有者") {
            sqlx::query(
                "INSERT INTO crabber_workspace_grants(role_name,workspace_id) VALUES($1,$2)",
            )
            .bind(&name)
            .bind("\u{0001}".repeat(512))
            .execute(&admin.pool)
            .await
            .unwrap();
        }
        let url = parsed.to_string();
        let pool = PgPoolOptions::new()
            .max_connections(2)
            .connect(&url)
            .await
            .unwrap();
        Self {
            url,
            name,
            pool,
            admin: admin.pool.clone(),
        }
    }
    pub async fn cleanup(&self) {
        self.pool.close().await;
        cleanup(&self.admin, &self.name).await;
    }
}
impl Drop for ProvisionedRole {
    fn drop(&mut self) {
        let admin = self.admin.clone();
        let pool = self.pool.clone();
        let name = self.name.clone();
        // Best effort on panic; normal paths await cleanup explicitly.
        tokio::spawn(async move {
            pool.close().await;
            cleanup(&admin, &name).await;
        });
    }
}
async fn cleanup(pool: &sqlx::PgPool, name: &str) {
    let _ = sqlx::query("DELETE FROM crabber_workspace_grants WHERE role_name=$1")
        .bind(name)
        .execute(pool)
        .await;
    let _ = sqlx::query(&format!("DROP OWNED BY {name}"))
        .execute(pool)
        .await;
    let _ = sqlx::query(&format!("DROP ROLE IF EXISTS {name}"))
        .execute(pool)
        .await;
}

pub(super) fn code(error: &sqlx::Error) -> Option<String> {
    error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .map(|s| s.to_string())
}
pub(super) async fn denied(pool: &sqlx::PgPool, sql: &str) {
    let error = sqlx::query(sql).execute(pool).await.unwrap_err();
    assert_eq!(
        code(&error).as_deref(),
        Some("42501"),
        "unexpected SQLSTATE"
    );
}
pub(super) async fn admit(store: &PostgresStore, workspace: &str) -> AdmitOutcome {
    let mut request = tests::request(&SessionId::new());
    request.workspace_id = workspace.into();
    store.admit_run(request).await.unwrap()
}
pub(super) async fn tables(pool: &sqlx::PgPool) -> Vec<String> {
    sqlx::query_scalar(
        "SELECT tablename FROM pg_tables WHERE schemaname=current_schema() ORDER BY tablename",
    )
    .fetch_all(pool)
    .await
    .unwrap()
}
pub(super) fn reservation(
    session: SessionId,
    workspace: &str,
) -> crate::session_owner::ReserveSessionOwner {
    let mut request = crate::session_owner_contract::reservation(session);
    request.binding.workspace_id = workspace.into();
    request
}

pub(super) fn tombstone_insert(session: &str) -> String {
    format!(
        "INSERT INTO session_tombstones(session_id,operation_id,expected_revision,final_revision,semantic_digest,deleted_at_seconds,deleted_at_nanos,format_version) VALUES('{session}','isolation',0,1,'{}',0,0,1)",
        "a".repeat(64)
    )
}
