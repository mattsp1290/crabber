//! File-backed SQLite lifecycle. Migration owns schema and journal changes;
//! connecting only verifies a version-one Crabber database already using WAL.

use crate::StoreError;
use crabber_core::{ByteLimits, Clock, SystemClock};
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
    error::ErrorKind,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteSynchronous},
};
use std::{path::Path, sync::Arc, time::Duration};

const APPLICATION_ID: i32 = 0x4352_4142;
const SCHEMA_VERSION: i64 = 1;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SCHEMA: &str = include_str!("../migrations/sqlite/0001_initial.sql");

/// A dedicated, file-backed SQLite session database.
///
/// Call [`Self::migrate`] before [`Self::connect`]. Clones share a single writer
/// connection and a pool of four query-only reader connections.
#[derive(Clone)]
pub struct SqliteStore {
    writer: SqlitePool,
    #[allow(dead_code)] // Removed in W3 when Store reads use this pool.
    readers: SqlitePool,
    clock: Arc<dyn Clock>,
    limits: ByteLimits,
    #[cfg(test)]
    #[allow(dead_code)] // Removed in W3 when abandonment uses fault injection.
    abandon_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    write_waiters: Arc<std::sync::atomic::AtomicUsize>,
}

impl SqliteStore {
    /// Creates or verifies the dedicated schema and enables WAL journaling.
    /// The parent directory must already exist. Unknown schemas and foreign
    /// databases are refused before any schema or journal-mode changes.
    ///
    /// # Errors
    /// Returns a sanitized error when opening, classifying, or migrating fails.
    pub async fn migrate(path: impl AsRef<Path>) -> Result<(), StoreError> {
        migrate_with(path.as_ref(), SCHEMA).await
    }

    /// Opens an already migrated database without creating a file or changing
    /// its schema or journal mode. The path is never included in errors.
    ///
    /// # Errors
    /// Returns a sanitized error if opening fails or the file is not a supported
    /// Crabber database in WAL mode.
    pub async fn connect(path: impl AsRef<Path>) -> Result<Self, StoreError> {
        connect_with(path.as_ref(), options(path.as_ref())).await
    }

    /// Replaces the clock, primarily for deterministic lease tests.
    #[must_use]
    pub fn with_clock(mut self, clock: Arc<dyn Clock>) -> Self {
        self.clock = clock;
        self
    }

    /// Replaces the limits used to validate persisted records.
    #[must_use]
    pub fn with_limits(mut self, limits: ByteLimits) -> Self {
        self.limits = limits;
        self
    }

    #[allow(dead_code)] // Removed in W3 when Store writes use this helper.
    async fn begin_write(&self) -> Result<Transaction<'static, Sqlite>, StoreError> {
        #[cfg(test)]
        self.write_waiters
            .fetch_add(1, std::sync::atomic::Ordering::SeqCst);
        let result = self.writer.begin_with("BEGIN IMMEDIATE").await;
        #[cfg(test)]
        self.write_waiters
            .fetch_sub(1, std::sync::atomic::Ordering::SeqCst);
        result.map_err(db)
    }
}

fn options(path: &Path) -> SqliteConnectOptions {
    // Do not configure journal_mode here: even opening a rejected file must
    // leave its journal mode unchanged.
    SqliteConnectOptions::new()
        .filename(path)
        .create_if_missing(false)
        .foreign_keys(true)
        .busy_timeout(BUSY_TIMEOUT)
        .synchronous(SqliteSynchronous::Full)
}

async fn connect_with(
    path: &Path,
    options: SqliteConnectOptions,
) -> Result<SqliteStore, StoreError> {
    let options = options.filename(path).create_if_missing(false);
    let writer = SqlitePoolOptions::new()
        .max_connections(1)
        .connect_with(options.clone())
        .await
        .map_err(classification_error)?;
    let result = verify_connection(&writer).await;
    if let Err(error) = result {
        writer.close().await;
        return Err(error);
    }
    let readers = SqlitePoolOptions::new()
        .max_connections(4)
        .connect_lazy_with(options.pragma("query_only", "ON"));
    Ok(SqliteStore {
        writer,
        readers,
        clock: Arc::new(SystemClock),
        limits: ByteLimits::default(),
        #[cfg(test)]
        abandon_fault: Arc::default(),
        #[cfg(test)]
        write_waiters: Arc::default(),
    })
}

async fn verify_connection(writer: &SqlitePool) -> Result<(), StoreError> {
    let mut connection = writer.acquire().await.map_err(db)?;
    if classify(&mut connection).await? != DatabaseState::Current {
        return Err(foreign_database());
    }
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut *connection)
        .await
        .map_err(classification_error)?;
    if mode != "wal" {
        return Err(StoreError::Validation(
            "SQLite database is not in WAL mode; run migrate".into(),
        ));
    }
    Ok(())
}

#[derive(PartialEq, Eq)]
enum DatabaseState {
    Fresh,
    Current,
}

async fn classify(connection: &mut SqliteConnection) -> Result<DatabaseState, StoreError> {
    let application: i32 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(&mut *connection)
        .await
        .map_err(classification_error)?;
    let objects: i64 = sqlx::query_scalar("SELECT count(*) FROM sqlite_schema")
        .fetch_one(&mut *connection)
        .await
        .map_err(classification_error)?;
    if application == APPLICATION_ID {
        let version: Option<i64> = sqlx::query_scalar("SELECT max(version) FROM schema_version")
            .fetch_one(&mut *connection)
            .await
            .map_err(|_| unsupported_version())?;
        if version != Some(SCHEMA_VERSION) {
            return Err(unsupported_version());
        }
        Ok(DatabaseState::Current)
    } else if application == 0 && objects == 0 {
        Ok(DatabaseState::Fresh)
    } else {
        Err(foreign_database())
    }
}

async fn migrate_with(path: &Path, schema: &str) -> Result<(), StoreError> {
    let mut connection = SqliteConnection::connect_with(&options(path).create_if_missing(true))
        .await
        .map_err(classification_error)?;
    let result = migrate_connection(&mut connection, schema).await;
    let closed = connection.close().await.map_err(db);
    result.and(closed)
}

async fn migrate_connection(
    connection: &mut SqliteConnection,
    schema: &str,
) -> Result<(), StoreError> {
    let started = std::time::Instant::now();
    let state = loop {
        // Reclassify on every retry: another process may have initialized the
        // file or installed a different schema while we waited for the lock.
        let state = classify(connection).await?;
        let result = sqlx::query_scalar::<_, String>("PRAGMA journal_mode=WAL")
            .fetch_one(&mut *connection)
            .await;
        match result {
            Ok(mode) if mode == "wal" => break state,
            Ok(_) => {
                return Err(StoreError::Validation(
                    "SQLite WAL mode is unavailable".into(),
                ));
            }
            Err(error) if is_lock_contention(&error) && started.elapsed() < BUSY_TIMEOUT => {
                tokio::time::sleep(Duration::from_millis(50)).await;
            }
            Err(error) => return Err(db(error)),
        }
    };
    if state == DatabaseState::Current {
        return Ok(());
    }
    let mut transaction = connection.begin_with("BEGIN IMMEDIATE").await.map_err(db)?;
    let result = match classify(&mut transaction).await {
        Ok(DatabaseState::Fresh) => initialize(&mut transaction, schema).await,
        Ok(DatabaseState::Current) => return transaction.rollback().await.map_err(db),
        Err(error) => Err(error),
    };
    match result {
        Ok(()) => transaction.commit().await.map_err(db),
        Err(error) => {
            transaction.rollback().await.map_err(db)?;
            Err(error)
        }
    }
}

async fn initialize(
    transaction: &mut Transaction<'_, Sqlite>,
    schema: &str,
) -> Result<(), StoreError> {
    sqlx::query(&format!("PRAGMA application_id={APPLICATION_ID}"))
        .execute(&mut **transaction)
        .await
        .map_err(db)?;
    // Executor::execute exposes SQLx's Send boxed future directly; RawSql's
    // async convenience method cannot prove Send for a spawned migration.
    sqlx::Executor::execute(&mut **transaction, sqlx::raw_sql(schema))
        .await
        .map_err(db)?;
    sqlx::query("INSERT INTO snapshot_auth(singleton,secret) VALUES(1,?1)")
        .bind(uuid::Uuid::new_v4().to_string())
        .execute(&mut **transaction)
        .await
        .map_err(db)?;
    sqlx::query("INSERT INTO schema_version(version) VALUES(?1)")
        .bind(SCHEMA_VERSION)
        .execute(&mut **transaction)
        .await
        .map_err(db)?;
    Ok(())
}

#[allow(clippy::needless_pass_by_value)] // Required by map_err.
fn db(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(detail) = &error {
        match detail.kind() {
            ErrorKind::UniqueViolation => return StoreError::Conflict,
            ErrorKind::ForeignKeyViolation => return StoreError::NotFound,
            _ => {}
        }
    }
    StoreError::Validation("SQLite operation failed".into())
}

fn primary_code(error: &sqlx::Error) -> Option<i32> {
    error
        .as_database_error()?
        .code()?
        .parse::<i32>()
        .ok()
        .map(|code| code & 0xff)
}

fn classification_error(error: sqlx::Error) -> StoreError {
    if primary_code(&error) == Some(26) {
        foreign_database()
    } else {
        db(error)
    }
}

fn is_lock_contention(error: &sqlx::Error) -> bool {
    matches!(primary_code(error), Some(5 | 6))
}

fn foreign_database() -> StoreError {
    StoreError::Validation("not a Crabber SQLite database".into())
}

fn unsupported_version() -> StoreError {
    StoreError::Validation("unsupported SQLite schema version".into())
}

#[allow(dead_code)] // Removed in W3 when Store writes use this helper.
fn text<T: Serialize>(value: &T) -> Result<String, StoreError> {
    crate::storable::storable_text(value)
}

#[allow(dead_code)] // Removed in W3 when Store reads use this helper.
fn decode<T: DeserializeOwned>(text: &str) -> Result<T, StoreError> {
    serde_json::from_str(text)
        .map_err(|_| StoreError::Validation("stored record is invalid".into()))
}

#[cfg(test)]
mod lifecycle_tests;
