//! Private real-process fixture helper; explicit parent-only attachment.

use sqlx::postgres::PgPoolOptions;
use std::{
    env, fs,
    process::Command,
    time::{Duration, Instant},
};

pub(crate) const CHILD_URL: &str = "CRABBER_TEST_FIXTURE_CHILD_URL";
pub(crate) const CHILD_ID: &str = "CRABBER_TEST_FIXTURE_CHILD_ID";
pub(crate) const READY: &str = "CRABBER_TEST_FIXTURE_CHILD_READY";
pub(crate) const DESCENDANT_READY: &str = "CRABBER_TEST_FIXTURE_DESCENDANT_READY";

pub(crate) fn main(mode: Option<&str>) -> i32 {
    let Ok(runtime) = tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        .enable_all()
        .build()
    else {
        return 1;
    };
    match runtime.block_on(run(mode)) {
        Ok(()) => 0,
        Err(error) => {
            eprintln!("fixture helper: {error}");
            1
        }
    }
}
async fn run(mode: Option<&str>) -> Result<(), Box<dyn std::error::Error>> {
    if mode == Some("scoped-history") {
        return scoped_history().await;
    }
    if mode == Some("allocate-panic") {
        let database = crabber_test_support::postgres::TestDatabase::create(
            crabber_test_support::postgres::DatabaseMode::Current,
        )
        .await?;
        let ready = env::var(READY).map_err(|_| "allocated helper requires readiness path")?;
        fs::write(
            ready,
            serde_json::to_vec(
                &serde_json::json!({"database":database.identity(),"pid":std::process::id()}),
            )?,
        )?;
        panic!("intentional fixture panic immediately after CREATE");
    }
    let url = env::var(CHILD_URL).map_err(|_| "fixture helper requires an explicit parent URL")?;
    let identity =
        env::var(CHILD_ID).map_err(|_| "fixture helper requires an explicit parent identity")?;
    let ready = env::var(READY).map_err(|_| "fixture helper requires a readiness path")?;
    let pool = PgPoolOptions::new()
        .max_connections(1)
        .connect(&url)
        .await
        .map_err(|_| "fixture helper cannot connect to parent database")?;
    let actual: String = sqlx::query_scalar("SELECT current_database()")
        .fetch_one(&pool)
        .await?;
    if actual != identity {
        return Err("child attached to the wrong fixture".into());
    }
    if mode == Some("parent") {
        let descendant =
            env::var(DESCENDANT_READY).map_err(|_| "missing descendant readiness path")?;
        let mut command = Command::new(env::current_exe()?);
        command
            .args(["postgres-fixture-helper", "hold"])
            .env(READY, &descendant);
        // Deliberately leave the parent's group: supervision must follow real
        // descendants rather than assume a process-group kill is sufficient.
        #[cfg(unix)]
        {
            use std::os::unix::process::CommandExt;
            command.process_group(0);
        }
        let child = command.spawn()?;
        let deadline = Instant::now() + Duration::from_secs(10);
        while !std::path::Path::new(&descendant).exists() {
            if Instant::now() >= deadline {
                return Err("descendant did not attach to the parent database".into());
            }
            tokio::time::sleep(Duration::from_millis(20)).await;
        }
        // This test intentionally simulates a parent killed without Rust Drop.
        // The outer subreaper owns cleanup of this child.
        drop(child);
    }
    fs::write(
        ready,
        serde_json::to_vec(&serde_json::json!({"database":actual,"pid":std::process::id()}))?,
    )?;
    assert_ne!(
        mode,
        Some("panic"),
        "intentional fixture helper panic after database attachment"
    );
    tokio::time::sleep(Duration::from_secs(60)).await;
    pool.close().await;
    Ok(())
}

async fn scoped_history() -> Result<(), Box<dyn std::error::Error>> {
    let ready = env::var(READY).map_err(|_| "scoped helper requires readiness path")?;
    crabber_test_support::postgres::TestDatabase::run_if_available(
        crabber_test_support::postgres::DatabaseMode::Current,
        async move |url| {
            let store: std::sync::Arc<dyn crabber_session::Store> =
                std::sync::Arc::new(crabber_session::PostgresStore::connect(&url).await.unwrap());
            let holder = std::sync::Arc::clone(&store);
            assert_eq!(std::sync::Arc::strong_count(&holder), 2);
            let pool = PgPoolOptions::new()
                .max_connections(1)
                .connect(&url)
                .await
                .unwrap();
            let actual: String = sqlx::query_scalar("SELECT current_database()")
                .fetch_one(&pool)
                .await
                .unwrap();
            fs::write(
                ready,
                serde_json::to_vec(
                    &serde_json::json!({"database":actual,"pid":std::process::id()}),
                )
                .unwrap(),
            )
            .unwrap();
            pool.close().await;
            // pool, Store and its Arc holder all leave this future's scope
            // before run_if_available attempts the normal database drain.
        },
    )
    .await?;
    Ok(())
}
