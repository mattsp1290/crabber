//! PostgreSQL fixture database implementation.

use super::{
    ADMIN_URL, Config, DatabaseMode, FixtureError, FixtureMetrics, RUN_ID, Result, RunFixture,
};
use sqlx::PgPool;
use std::{env, time::Instant};

/// A database owned by one test history. Drop cannot perform asynchronous cleanup;
/// normal paths must call `finish` after every Store/child has been released.
pub struct TestDatabase {
    config: Config,
    pool: PgPool,
    name: String,
    metrics: Option<FixtureMetrics>,
}
impl TestDatabase {
    /// Preserve Tokio-test shutdown order for histories that intentionally retain
    /// uncertain controllers. Call from a synchronous test with its original
    /// runtime configuration. Runtime Drop stops and joins its actual tasks
    /// before the administrative runtime attempts normal connection drain.
    /// # Errors
    /// Fails on configuration, runtime construction, provisioning or drain.
    pub fn run_with_runtime(
        mode: DatabaseMode,
        mut history: tokio::runtime::Builder,
        journey: impl AsyncFnOnce(String),
    ) -> Result<()> {
        if !Self::available()? {
            return Ok(());
        }
        let admin = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .map_err(|_| FixtureError("cannot create fixture administrative runtime"))?;
        let database = admin.block_on(Self::create(mode))?;
        {
            let runtime = history
                .enable_all()
                .build()
                .map_err(|_| FixtureError("cannot create original test history runtime"))?;
            runtime.block_on(journey(database.url()));
        }
        admin.block_on(database.finish())
    }

    /// Whether a direct test invocation has a runner configuration. Absent
    /// service is an explicit offline skip; required/legacy URL-only mode fails.
    /// # Errors
    /// Fails for incomplete runner configuration or unsupported URL-only setup.
    pub fn available() -> Result<bool> {
        if env::var_os(ADMIN_URL).is_some() && env::var_os(RUN_ID).is_some() {
            return Ok(true);
        }
        if env::var("CRABBER_REQUIRE_POSTGRES").as_deref() == Ok("1")
            || env::var_os(ADMIN_URL).is_some()
            || env::var_os(RUN_ID).is_some()
            || env::var_os("CRABBER_TEST_POSTGRES_URL").is_some()
        {
            return Err(FixtureError(
                "required PostgreSQL tests need check-postgres runner configuration; URL-only mode is unsupported",
            ));
        }
        eprintln!("skipping PostgreSQL test: no check-postgres runner configured");
        Ok(false)
    }

    /// Run one complete test history, then finish outside the history's future
    /// scope. All Stores, pools and child guards must stay inside that scope.
    /// An absent service skips explicitly; required mode never skips.
    /// # Errors
    /// Fails on missing required configuration, provisioning or connection drain.
    pub async fn run_if_available(
        mode: DatabaseMode,
        journey: impl AsyncFnOnce(String),
    ) -> Result<()> {
        if !Self::available()? {
            return Ok(());
        }
        // Keep fixture administration and potentially large contract histories
        // off the caller's async stack without spawning another Tokio task.
        let database = Box::pin(Self::create(mode)).await?;
        // The completed future is dropped at this statement, before finish.
        Box::pin(journey(database.url())).await;
        Box::pin(database.finish()).await
    }

    /// Provision from runner configuration, never from an application URL.
    /// # Errors
    /// Fails without a valid runner or when cloning/registration fails.
    pub async fn create(mode: DatabaseMode) -> Result<Self> {
        let admin = env::var(ADMIN_URL).map_err(|_| {
            FixtureError("run check-postgres to configure isolated PostgreSQL fixtures")
        })?;
        let nonce = env::var(RUN_ID)
            .map_err(|_| FixtureError("missing PostgreSQL fixture run identity"))?;
        Self::on_config(
            Config::new(&admin, &nonce)?,
            mode,
            FixtureMetrics::inherited(),
        )
        .await
    }
    async fn on_config(
        config: Config,
        mode: DatabaseMode,
        metrics: Option<FixtureMetrics>,
    ) -> Result<Self> {
        let started = Instant::now();
        let pool = config.pool().await?;
        let name = config.database();
        config.create(&pool, &name, mode).await?;
        if let Some(metrics) = &metrics {
            metrics.record("created", &name, Some(mode), started.elapsed())?;
        }
        Ok(Self {
            config,
            pool,
            name,
            metrics,
        })
    }
    /// Attach a newly provisioned test to an explicitly held runner (runner tests).
    /// # Errors
    /// Fails when cloning or registration fails.
    pub async fn for_run(run: &RunFixture, mode: DatabaseMode) -> Result<Self> {
        Self::on_config(run.config.clone(), mode, None).await
    }
    /// Endpoint for this database only. Do not log it.
    #[must_use]
    pub fn url(&self) -> String {
        self.config.url(&self.name)
    }
    /// Opaque database identity safe for child handoffs and assertions.
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.name
    }
    /// Wait for normal connection drain and drop this exact registered database.
    /// # Errors
    /// Fails rather than silently terminating a leaked test connection.
    pub async fn finish(self) -> Result<()> {
        let started = Instant::now();
        self.config
            .drop_database(&self.pool, &self.name, false)
            .await?;
        self.pool.close().await;
        if let Some(metrics) = &self.metrics {
            metrics.record("finished", &self.name, None, started.elapsed())?;
        }
        Ok(())
    }
}
