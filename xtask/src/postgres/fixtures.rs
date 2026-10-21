//! Real-database fixture qualification, independent of Store behavior tests.

mod lifecycle;
mod permissions;
mod processes;

use crabber_test_support::postgres::{DatabaseMode, RunFixture, TestDatabase};
use sqlx::postgres::PgPoolOptions;

pub(super) async fn verify(
    run: &RunFixture,
    admin: &str,
) -> Result<(), Box<dyn std::error::Error>> {
    let first = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    let second = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    let blank = TestDatabase::for_run(run, DatabaseMode::Blank).await?;
    assert_ne!(first.identity(), second.identity());
    let a = PgPoolOptions::new()
        .max_connections(1)
        .connect(&first.url())
        .await
        .map_err(|_| "cannot connect first isolated fixture")?;
    let b = PgPoolOptions::new()
        .max_connections(1)
        .connect(&second.url())
        .await
        .map_err(|_| "cannot connect second isolated fixture")?;
    for pool in [&a, &b] {
        sqlx::query("INSERT INTO sessions(id,data) VALUES ('identical-session-id','{}'::jsonb)")
            .execute(pool)
            .await?;
        sqlx::query("CREATE TABLE fixture_isolation (id integer PRIMARY KEY)")
            .execute(pool)
            .await?;
        sqlx::query("INSERT INTO fixture_isolation VALUES (1)")
            .execute(pool)
            .await?;
    }
    sqlx::query("TRUNCATE sessions CASCADE").execute(&a).await?;
    let sessions: i64 =
        sqlx::query_scalar("SELECT count(*) FROM sessions WHERE id='identical-session-id'")
            .fetch_one(&b)
            .await?;
    assert_eq!(sessions, 1);
    sqlx::query("TRUNCATE fixture_isolation")
        .execute(&a)
        .await?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM fixture_isolation")
        .fetch_one(&b)
        .await?;
    assert_eq!(count, 1);
    sqlx::query("ALTER TABLE fixture_isolation ADD COLUMN extra integer")
        .execute(&a)
        .await?;
    let missing: bool=sqlx::query_scalar("SELECT NOT EXISTS(SELECT 1 FROM information_schema.columns WHERE table_name='fixture_isolation' AND column_name='extra')").fetch_one(&b).await?;
    assert!(missing);
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(974123)")
            .fetch_one(&a)
            .await?
    );
    assert!(
        sqlx::query_scalar::<_, bool>("SELECT pg_try_advisory_lock(974123)")
            .fetch_one(&b)
            .await?
    );
    let empty = PgPoolOptions::new()
        .max_connections(1)
        .connect(&blank.url())
        .await
        .map_err(|_| "cannot connect blank fixture")?;
    let tables: i64 = sqlx::query_scalar(
        "SELECT count(*) FROM information_schema.tables WHERE table_schema='public'",
    )
    .fetch_one(&empty)
    .await?;
    assert_eq!(tables, 0);
    empty.close().await;
    a.close().await;
    b.close().await;
    drop(empty);
    drop(a);
    drop(b);
    first.finish().await?;
    second.finish().await?;
    blank.finish().await?;
    processes::verify(run).await?;
    crate::process::check_cancelled()?;
    lifecycle::verify(run, admin).await?;
    crate::process::check_cancelled()?;
    permissions::verify(run, admin).await?;
    println!(
        "fixture isolation: identical IDs, truncate, schema mutation, advisory locks, current/blank modes passed"
    );
    Ok(())
}
