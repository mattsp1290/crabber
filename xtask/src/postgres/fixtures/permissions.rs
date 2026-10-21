//! A dedicated role without CREATEDB must fail before test execution and leave
//! no ownership schema behind. Run only against the explicitly dedicated admin.

use crabber_test_support::postgres::RunFixture;
use sqlx::{
    ConnectOptions,
    postgres::{PgConnectOptions, PgPoolOptions},
};
use std::str::FromStr;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) async fn verify(run: &RunFixture, admin: &str) -> Result<()> {
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(admin)
        .await
        .map_err(|_| "cannot connect fixture permission controller")?;
    let role = format!("ct_role_{}", run.identity());
    let password = format!("fixture-{}", run.identity());
    sqlx::query(&format!(
        "CREATE ROLE {role} LOGIN NOCREATEDB PASSWORD '{password}'"
    ))
    .execute(&pool)
    .await?;
    let database: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    // Administrative database names come from server metadata. Escape quoted
    // identifiers; unlike fixture names an external admin database is arbitrary.
    let quoted_database = database.replace('"', "\"\"");
    let outcome=async {
        sqlx::query(&format!("GRANT CONNECT, CREATE ON DATABASE \"{quoted_database}\" TO {role}")).execute(&pool).await?;
        let options=PgConnectOptions::from_str(admin).map_err(|_|"invalid permission-check admin options")?
            .username(&role).password(&password);
        // Ensure connection succeeds so missing credentials cannot masquerade
        // as the expected CREATEDB rejection.
        let control=PgPoolOptions::new().max_connections(1).connect_with(options.clone()).await.map_err(|_|"restricted role cannot connect for privilege control")?;
        let allowed:bool=sqlx::query_scalar("SELECT rolcreatedb FROM pg_roles WHERE rolname=current_user").fetch_one(&control).await?;
        control.close().await;
        drop(control);
        if allowed { return Err("restricted role unexpectedly has CREATEDB".into()); }
        match RunFixture::start(options.to_url_lossy().as_str()).await {
            Ok(unexpected)=>{ unexpected.finish().await?; return Err("fixture accepted a role without CREATEDB".into()); }
            Err(error) if error.to_string().contains("cannot create owned PostgreSQL database")=>{},
            Err(_)=>return Err("privilege check failed before reaching CREATE DATABASE".into()),
        }
        let schemas:i64=sqlx::query_scalar("SELECT count(*) FROM pg_namespace WHERE nspowner=(SELECT oid FROM pg_roles WHERE rolname=$1)")
            .bind(&role).fetch_one(&pool).await?;
        if schemas!=0 { return Err("denied CREATE left a run ownership schema behind".into()); }
        Ok(())
    }.await;
    sqlx::query(&format!(
        "REVOKE CONNECT, CREATE ON DATABASE \"{quoted_database}\" FROM {role}"
    ))
    .execute(&pool)
    .await?;
    sqlx::query(&format!("DROP ROLE {role}"))
        .execute(&pool)
        .await?;
    pool.close().await;
    drop(pool);
    outcome
}
