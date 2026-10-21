//! Lifecycle failures must remain failures and leave exact sweep ownership.

use crabber_session::Store;
use crabber_test_support::postgres::{DatabaseMode, RunFixture, TestDatabase};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;

type Result<T> = std::result::Result<T, Box<dyn std::error::Error>>;

pub(super) async fn verify(run: &RunFixture, admin: &str) -> Result<()> {
    if RunFixture::cleanup_run(admin, run.identity()).await.is_ok() {
        return Err("cleanup acquired an active run's ownership lock".into());
    }
    retained_store(run).await?;
    crate::process::check_cancelled()?;
    two_runs(run, admin).await?;
    println!(
        "fixture lifecycle: active-run cleanup refused, retained Arc Store drain failure preserved, two runners isolated"
    );
    Ok(())
}
async fn retained_store(run: &RunFixture) -> Result<()> {
    let normal = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    {
        let store: Arc<dyn Store> = Arc::new(
            crabber_session::PostgresStore::connect(&normal.url())
                .await
                .map_err(|_| "cannot attach Store to normal-drain fixture")?,
        );
        let holder = Arc::clone(&store);
        assert_eq!(Arc::strong_count(&store), 2);
        drop(holder);
    }
    normal.finish().await?;

    let database = TestDatabase::for_run(run, DatabaseMode::Current).await?;
    let identity = database.identity().to_owned();
    let store: Arc<dyn Store> = Arc::new(
        crabber_session::PostgresStore::connect(&database.url())
            .await
            .map_err(|_| "cannot attach Store to retained-drain fixture")?,
    );
    let holder = Arc::clone(&store);
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&database.url())
        .await
        .map_err(|_| "cannot observe retained fixture")?;
    let result = database.finish().await;
    if !result
        .as_ref()
        .is_err_and(|error| error.to_string().contains("did not drain"))
    {
        return Err("a retained Store/pool did not produce a drain failure".into());
    }
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    if actual != identity || !run.registered_databases().await?.contains(&identity) {
        return Err("failed cleanup lost its live database or ownership record".into());
    }
    drop(holder);
    drop(store);
    pool.close().await;
    drop(pool);
    // finish consumed the fixture and correctly failed. The run's final sweep
    // must retain and remove that exact registry row after all processes stop.
    Ok(())
}
async fn two_runs(original: &RunFixture, admin: &str) -> Result<()> {
    let companion = RunFixture::start(admin).await?;
    let mut survivor = None;
    let outcome: Result<()> = async {
        if companion.identity() == original.identity() {
            return Err("two runners share an identity".into());
        }
        crabber_session::PostgresStore::migrate(&companion.template_url())
            .await
            .map_err(|_| "cannot migrate companion template")?;
        let a = TestDatabase::for_run(original, DatabaseMode::Current).await?;
        let b = TestDatabase::for_run(&companion, DatabaseMode::Current).await?;
        let first = PgPoolOptions::new()
            .max_connections(1)
            .connect(&a.url())
            .await
            .map_err(|_| "cannot connect original run")?;
        let second = PgPoolOptions::new()
            .max_connections(1)
            .connect(&b.url())
            .await
            .map_err(|_| "cannot connect companion run")?;
        sqlx::query("SET application_name='fixture-run-first'")
            .execute(&first)
            .await?;
        sqlx::query("SET application_name='fixture-run-second'")
            .execute(&second)
            .await?;
        for pool in [&first, &second] {
            sqlx::query("CREATE TABLE runner_isolation (id integer PRIMARY KEY)")
                .execute(pool)
                .await?;
            sqlx::query("INSERT INTO runner_isolation VALUES (1)")
                .execute(pool)
                .await?;
        }
        sqlx::query("DELETE FROM runner_isolation")
            .execute(&first)
            .await?;
        let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runner_isolation")
            .fetch_one(&second)
            .await?;
        if count != 1 {
            return Err("another runner modified the companion database".into());
        }
        second.close().await;
        drop(second);
        b.finish().await?;
        // Leave the other run's fixture and connection alive across this run's
        // sweep, proving the sweep cannot terminate or drop unowned resources.
        survivor = Some((a, first));
        Ok(())
    }
    .await;
    companion.finish().await?;
    outcome?;
    let (database, pool) = survivor.ok_or("missing surviving run fixture")?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM runner_isolation")
        .fetch_one(&pool)
        .await?;
    if count != 0 {
        return Err("companion cleanup changed the surviving run's data".into());
    }
    pool.close().await;
    drop(pool);
    database.finish().await?;
    Ok(())
}
