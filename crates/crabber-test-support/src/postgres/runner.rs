//! PostgreSQL fixture runner implementation.

use super::{ADMIN_URL, Config, DatabaseMode, FixtureError, RUN_ID, Result};
use sqlx::{Connection, PgConnection, PgPool};
use uuid::Uuid;

/// Administrative owner. Keep this alive on the outer runner's Tokio runtime.
/// It does not migrate the template: only the runner depends on the Store crate.
pub struct RunFixture {
    pub(super) config: Config,
    pool: PgPool,
    guard: PgConnection,
}
impl RunFixture {
    /// Establish an isolated registry, verify PostgreSQL 14 durability, and create
    /// a blank run template. The caller must migrate and close template pools.
    /// # Errors
    /// Fails on endpoint, version, durability, ownership or create permission errors.
    pub async fn start(admin_url: &str) -> Result<Self> {
        let config = Config::new(admin_url, &Uuid::new_v4().simple().to_string())?;
        eprintln!("PostgreSQL fixture setup run {}", config.nonce);
        let pool = config.pool().await?;
        let mut guard = PgConnection::connect(config.admin.as_str())
            .await
            .map_err(|_| FixtureError("cannot establish runner ownership connection"))?;
        acquire_lock(&mut guard, &config.nonce).await?;
        let settings: (String,String,String,String) = sqlx::query_as("SELECT current_setting('server_version_num'),current_setting('fsync'),current_setting('synchronous_commit'),current_setting('full_page_writes')")
            .fetch_one(&pool).await.map_err(|_| FixtureError("cannot inspect PostgreSQL durability"))?;
        if !settings.0.starts_with("14")
            || settings.0.len() != 6
            || settings.1 != "on"
            || settings.2 != "on"
            || settings.3 != "on"
        {
            return Err(FixtureError(
                "fixture requires PostgreSQL 14 with fsync, synchronous_commit and full_page_writes enabled",
            ));
        }
        // Registry creation is transactional; a failure cannot leave a schema
        // without its ownership table for the sweep to inspect.
        let mut transaction = pool
            .begin()
            .await
            .map_err(|_| FixtureError("cannot begin registry setup"))?;
        sqlx::query(&format!("CREATE SCHEMA {}", config.schema()))
            .execute(&mut *transaction)
            .await
            .map_err(|_| FixtureError("cannot create run ownership schema"))?;
        sqlx::query(&format!(
            "CREATE TABLE {}.databases (name text PRIMARY KEY)",
            config.schema()
        ))
        .execute(&mut *transaction)
        .await
        .map_err(|_| FixtureError("cannot create ownership registry"))?;
        transaction
            .commit()
            .await
            .map_err(|_| FixtureError("cannot commit run ownership registry"))?;
        let fixture = Self {
            config,
            pool,
            guard,
        };
        let setup = fixture
            .config
            .create(
                &fixture.pool,
                &fixture.config.template(),
                DatabaseMode::Blank,
            )
            .await;
        if let Err(error) = setup {
            // Setup errors never leave a successfully-created database unnamed:
            // create intent is persisted first. Preserve the primary failure.
            let identity = fixture.identity().to_owned();
            if let Err(cleanup) = fixture.finish().await {
                eprintln!("PostgreSQL fixture setup cleanup failed for run {identity}: {cleanup}");
            }
            return Err(error);
        }
        Ok(fixture)
    }
    /// Opaque identity suitable for diagnostic output (never a URL).
    #[must_use]
    pub fn identity(&self) -> &str {
        &self.config.nonce
    }
    /// Template endpoint for the runner's migration step; do not log it.
    #[must_use]
    pub fn template_url(&self) -> String {
        self.config.url(&self.config.template())
    }
    /// Explicit child-only environment overrides. Never serialize their values.
    #[must_use]
    pub fn environment(&self) -> [(&'static str, String); 2] {
        [
            (ADMIN_URL, self.config.admin.as_str().to_owned()),
            (RUN_ID, self.config.nonce.clone()),
        ]
    }
    /// Registered database identities, including incomplete create/cleanup work.
    /// # Errors
    /// Fails if the run's ownership registry cannot be read.
    pub async fn registered_databases(&self) -> Result<Vec<String>> {
        sqlx::query_scalar(&format!(
            "SELECT name FROM {}.databases ORDER BY name",
            self.config.schema()
        ))
        .fetch_all(&self.pool)
        .await
        .map_err(|_| FixtureError("cannot enumerate owned databases"))
    }
    /// Force-sweep only registered databases after all child processes stop.
    /// # Errors
    /// Fails if cleanup cannot prove ownership or remove an owned resource.
    pub async fn finish(self) -> Result<()> {
        sweep(&self.config, &self.pool).await?;
        self.pool.close().await;
        // Explicit close releases the run advisory lock only after the sweep.
        self.guard
            .close()
            .await
            .map_err(|_| FixtureError("cannot close run ownership connection"))
    }
    /// Reclaim a stopped external run. A live runner's advisory lock refuses it.
    /// # Errors
    /// Fails for invalid identities, live runs, unowned registries or failed cleanup.
    pub async fn cleanup_run(admin_url: &str, nonce: &str) -> Result<()> {
        let config = Config::new(admin_url, nonce)?;
        let mut guard = PgConnection::connect(config.admin.as_str())
            .await
            .map_err(|_| FixtureError("cannot establish cleanup ownership connection"))?;
        acquire_lock(&mut guard, nonce).await?;
        let pool = config.pool().await?;
        sweep(&config, &pool).await?;
        pool.close().await;
        guard
            .close()
            .await
            .map_err(|_| FixtureError("cannot close cleanup ownership connection"))
    }
}

async fn acquire_lock(connection: &mut PgConnection, nonce: &str) -> Result<()> {
    let acquired: bool = sqlx::query_scalar("SELECT pg_try_advisory_lock(hashtextextended($1, 0))")
        .bind(nonce)
        .fetch_one(connection)
        .await
        .map_err(|_| FixtureError("cannot acquire run ownership lock"))?;
    if !acquired {
        return Err(FixtureError(
            "run is active; refusing cleanup or duplicate ownership",
        ));
    }
    Ok(())
}
async fn sweep(config: &Config, pool: &PgPool) -> Result<()> {
    let owned: bool = sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM pg_namespace WHERE nspname=$1 AND nspowner=(SELECT oid FROM pg_roles WHERE rolname=current_user))")
        .bind(config.schema()).fetch_one(pool).await.map_err(|_| FixtureError("cannot verify registry schema owner"))?;
    if !owned {
        return Err(FixtureError(
            "ownership registry is missing or owned by another role",
        ));
    }
    let names: Vec<String> = sqlx::query_scalar(&format!(
        "SELECT name FROM {}.databases ORDER BY name",
        config.schema()
    ))
    .fetch_all(pool)
    .await
    .map_err(|_| FixtureError("cannot enumerate ownership registry"))?;
    for name in names {
        config.drop_database(pool, &name, true).await?;
    }
    sqlx::query(&format!("DROP SCHEMA {} CASCADE", config.schema()))
        .execute(pool)
        .await
        .map_err(|_| FixtureError("cannot remove run ownership schema"))?;
    Ok(())
}
