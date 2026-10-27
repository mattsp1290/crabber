//! SQL-level denial and positive native-workload proofs for migration 25.
use super::*;
use crate::Store;
use crabber_core::EventKind;
mod admission_read;
mod foreign_seed;
mod helpers;
mod migration;
mod native_seed;
mod reassignment;
mod scope;
mod workload;
use helpers::*;

#[tokio::test]
async fn rls_coverage_every_table_has_worker_policy() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        let tables = tables(&admin.pool).await;
        assert!(tables.len() >= 80);
        for table in tables {
            if ["schema_version", "snapshot_auth"].contains(&table.as_str()) {
                continue;
            }
            let row = sqlx::query(
                "SELECT rowsecurity FROM pg_tables WHERE schemaname=current_schema() AND tablename=$1",
            )
            .bind(&table)
            .fetch_one(&admin.pool)
            .await
            .unwrap();
            assert!(row.get::<bool, _>("rowsecurity"), "{table}");
            for role in ["crabber_worker", "crabber_host"] {
                let exists: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname=current_schema() AND tablename=$1 AND $2=ANY(roles))")
                    .bind(&table).bind(role).fetch_one(&admin.pool).await.unwrap();
                assert!(exists, "{table}: {role}");
            }
            let missing_check: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_policies WHERE schemaname=current_schema() AND tablename=$1 AND cmd='ALL' AND with_check IS NULL)")
                .bind(&table).fetch_one(&admin.pool).await.unwrap();
            assert!(!missing_check, "{table}");
            let readable: bool =
                sqlx::query_scalar("SELECT has_table_privilege('crabber_worker',$1,'SELECT')")
                    .bind(&table)
                    .fetch_one(&admin.pool)
                    .await
                    .unwrap();
            assert!(readable, "{table}");
        }
        let can_grant: bool = sqlx::query_scalar(
            "SELECT has_table_privilege('crabber_worker','crabber_workspace_grants','INSERT')",
        )
        .fetch_one(&admin.pool)
        .await
        .unwrap();
        assert!(!can_grant);
        let functions: Vec<String> = sqlx::query_scalar("SELECT proname FROM pg_proc WHERE pronamespace=current_schema()::regnamespace AND prorettype='trigger'::regtype")
            .fetch_all(&admin.pool).await.unwrap();
        assert_eq!(functions.len(), 33);
        let definer_functions: Vec<String> = sqlx::query_scalar("SELECT proname FROM pg_proc WHERE pronamespace=current_schema()::regnamespace AND prosecdef")
            .fetch_all(&admin.pool).await.unwrap();
        assert_eq!(definer_functions, vec!["crabber_session_born"]);
        let migration = include_str!("../../migrations/0025_workspace_isolation.sql");
        for function in functions {
            assert!(
                migration
                    .lines()
                    .any(|line| line.starts_with(&format!("-- {function} |"))),
                "{function} missing inventory"
            );
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn host_role_sees_all_workspaces() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    TestDatabase::run_if_available(DatabaseMode::Current, async move |url| {
        let admin = admin(&url).await;
        let workspace_a = format!("host-a-{}", uuid::Uuid::new_v4());
        let workspace_b = format!("host-b-{}", uuid::Uuid::new_v4());
        let a = admit(&admin, &workspace_a).await;
        let b = admit(&admin, &workspace_b).await;
        let role = ProvisionedRole::host(&url, &admin).await;
        let host = PostgresStore::connect(&role.url).await.unwrap();
        assert_eq!(
            host.get_session(&a.session.id).await.unwrap(),
            Some(a.session.clone())
        );
        assert_eq!(
            host.get_session(&b.session.id).await.unwrap(),
            Some(b.session.clone())
        );
        for (workspace, session) in [(&workspace_a, &a.session.id), (&workspace_b, &b.session.id)] {
            let page = host
                .list_sessions(crate::ListSessions {
                    workspace_id: workspace.clone(),
                    include_archived: true,
                    limit: 100,
                    cursor: None,
                })
                .await
                .unwrap();
            assert!(page.sessions.iter().any(|item| &item.session_id == session));
        }
        host.pool.close().await;
        role.cleanup().await;
    })
    .await
    .unwrap();
}
