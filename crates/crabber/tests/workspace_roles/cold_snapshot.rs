use super::{fixture::Fixture, snapshot_journey as journey};
use sqlx::postgres::PgPoolOptions;

#[tokio::test]
async fn worker_snapshot_continuation_in_fresh_process() {
    let Some(fixture) = Fixture::new().await else {
        return;
    };
    let worker = PgPoolOptions::new()
        .max_connections(1)
        .connect(fixture.worker_url())
        .await
        .unwrap();
    let (superuser, bypass_rls, worker_member): (bool, bool, bool) = sqlx::query_as(
        "SELECT rolsuper, rolbypassrls, pg_has_role(current_user,'crabber_worker','member') FROM pg_roles WHERE rolname=current_user",
    )
    .fetch_one(&worker)
    .await
    .unwrap();
    assert!(!superuser && !bypass_rls && worker_member);
    worker.close().await;
    sqlx::query(
        "INSERT INTO crabber_workspace_grants(role_name,workspace_id) VALUES($1,'fixture')",
    )
    .bind(&fixture.roles[1])
    .execute(&fixture.admin)
    .await
    .unwrap();
    {
        let stores = journey::postgres(fixture.worker_url(), false).await;
        journey::smoke(
            stores,
            "postgres",
            &journey::source(false),
            journey::ChildMode::Test(fixture.worker_child_environment().to_vec()),
        )
        .await;
    }
    fixture.cleanup().await;
}
