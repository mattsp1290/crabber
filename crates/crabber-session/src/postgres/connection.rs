//! Read-only schema verification and explicitly requested migration.
use super::{PostgresStore, db, migration, snapshot};
use crate::StoreError;
use crabber_core::{ByteLimits, Clock, SystemClock};
use sqlx::postgres::PgPoolOptions;
use std::sync::Arc;
use tracing::Instrument;

impl PostgresStore {
    /// Opens an already migrated dedicated database. The URL is never included in errors.
    /// # Errors
    /// Returns an error when connection or schema verification fails.
    pub async fn connect(url: &str) -> Result<Self, StoreError> {
        let span = tracing::info_span!(target: crate::targets::SESSION, "crabber.store.connect", db.system = "postgresql", outcome = tracing::field::Empty, error.kind = tracing::field::Empty, schema.version = tracing::field::Empty, otel.kind = "client", otel.status_code = tracing::field::Empty);
        async {
            let result = Self::connect_inner(url).await;
            let span = tracing::Span::current();
            if result.is_ok() {
                span.record("outcome", "ok");
                span.record("otel.status_code", "OK");
            } else {
                span.record("outcome", "failed");
                span.record("otel.status_code", "ERROR");
            }
            result
        }
        .instrument(span)
        .await
    }
    /// Applies the bundled migrations to a dedicated PostgreSQL 14+ database.
    /// # Errors
    /// Returns a sanitized error on connection or migration failure.
    pub async fn migrate(url: &str) -> Result<(), StoreError> {
        let span = tracing::info_span!(target: crate::targets::SESSION, "crabber.store.migrate", db.system = "postgresql", outcome = tracing::field::Empty, error.kind = tracing::field::Empty, schema.version = tracing::field::Empty, otel.kind = "client", otel.status_code = tracing::field::Empty);
        async {
            let result = Self::migrate_inner(url).await;
            let span = tracing::Span::current();
            if result.is_ok() {
                span.record("outcome", "ok");
                span.record("otel.status_code", "OK");
                span.record("schema.version", crate::POSTGRES_SCHEMA_VERSION);
                tracing::info!(target: crate::targets::SESSION, { db.system = "postgresql", schema.version = crate::POSTGRES_SCHEMA_VERSION }, "schema migrated");
            } else {
                span.record("outcome", "failed");
                span.record("otel.status_code", "ERROR");
            }
            result
        }.instrument(span).await
    }
    async fn migrate_inner(url: &str) -> Result<(), StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(1)
            .connect(url)
            .await
            .map_err(connection_error)?;
        let mut tx = crate::postgres::transaction::begin(&pool)
            .await
            .map_err(connection_error)?;
        sqlx::query("SELECT pg_advisory_xact_lock(751302919)")
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        if migration::is_current(&mut tx).await? {
            tx.commit().await.map_err(connection_error)?;
            pool.close().await;
            return Ok(());
        }
        for statement in concat!(
            include_str!("../../migrations/0001_initial.sql"),
            "\n",
            include_str!("../../migrations/0002_admission_receipts.sql")
        )
        .split(';')
        .map(str::trim)
        .filter(|part| !part.is_empty())
        {
            sqlx::query(statement)
                .execute(&mut *tx)
                .await
                .map_err(connection_error)?;
        }
        snapshot::migrate(&mut tx).await?;
        sqlx::raw_sql(include_str!(
            "../../migrations/0004_abandonment_commits.sql"
        ))
        .execute(&mut *tx)
        .await
        .map_err(connection_error)?;
        sqlx::raw_sql(include_str!(
            "../../migrations/0005_admission_execution.sql"
        ))
        .execute(&mut *tx)
        .await
        .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0006_host_foundation.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0007_typed_admission.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0008_continuations.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0009_session_management.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        // Later migrations widen this constraint for additional metadata kinds.
        // Reapplying the v10 definition would reject already committed v15 forks.
        migrate_legacy_queue(&mut tx).await?;
        sqlx::raw_sql(include_str!("../../migrations/0011_tool_continuations.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0012_run_controls.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0013_session_owners.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!(
            "../../migrations/0014_frontend_continuations.sql"
        ))
        .execute(&mut *tx)
        .await
        .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0015_safe_forks.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        sqlx::raw_sql(include_str!("../../migrations/0016_session_deletion.sql"))
            .execute(&mut *tx)
            .await
            .map_err(connection_error)?;
        migration::apply_sources(&mut tx).await?;
        tx.commit().await.map_err(connection_error)?;
        pool.close().await;
        Ok(())
    }
    async fn connect_inner(url: &str) -> Result<Self, StoreError> {
        let pool = PgPoolOptions::new()
            .max_connections(10)
            .connect(url)
            .await
            .map_err(connection_error)?;
        let mut verification =
            crate::transaction::begin_scoped(&pool, crate::transaction::StoreScope::Verify)
                .await
                .map_err(connection_error)?;
        let versions: Vec<i32> =
            sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
                .fetch_all(&mut *verification)
                .await
                .map_err(connection_error)?;
        verification.rollback().await.map_err(connection_error)?;
        if versions != (1..=crate::POSTGRES_SCHEMA_VERSION).collect::<Vec<_>>() {
            return Err(StoreError::Validation(
                "unsupported PostgreSQL schema version".into(),
            ));
        }
        Ok(Self {
            pool,
            clock: Arc::new(SystemClock),
            limits: ByteLimits::default(),
            #[cfg(test)]
            abandon_fault: Arc::default(),
            #[cfg(test)]
            public_state_fault: Arc::default(),
            #[cfg(test)]
            management_fault: Arc::default(),
            #[cfg(test)]
            attempt_fault: Arc::default(),
            #[cfg(test)]
            native_raw_probe: Arc::default(),
            #[cfg(test)]
            phase_fault: Arc::default(),
            #[cfg(test)]
            approval_fault: Arc::default(),
            #[cfg(test)]
            binding_fault: Arc::default(),
            #[cfg(test)]
            queue_fault: Arc::default(),
            #[cfg(test)]
            deletion_fault: Arc::default(),
            #[cfg(test)]
            tool_continuation_fault: Arc::default(),
            #[cfg(test)]
            control_fault: Arc::default(),
            #[cfg(test)]
            session_owner_fault: Arc::default(),
        })
    }
    /// Replaces the clock, primarily for deterministic lease tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }
    #[must_use]
    pub fn with_limits(mut self, limits: ByteLimits) -> Self {
        self.limits = limits;
        self
    }
}

fn connection_error(error: sqlx::Error) -> StoreError {
    tracing::Span::current().record("error.kind", crate::transaction::sqlx_error_kind(&error));
    db(error)
}

async fn migrate_legacy_queue(
    tx: &mut crate::transaction::Transaction<'_, sqlx::Postgres>,
) -> Result<(), StoreError> {
    let queue_migrated: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM schema_version WHERE version=10)")
            .fetch_one(&mut **tx)
            .await
            .map_err(connection_error)?;
    if !queue_migrated {
        sqlx::raw_sql(include_str!("../../migrations/0010_future_queue.sql"))
            .execute(&mut **tx)
            .await
            .map_err(connection_error)?;
    }
    Ok(())
}
