//! File-backed SQLite lifecycle. Migration owns schema and journal changes;
//! connecting only verifies a current-version Crabber database already using WAL.
#[path = "sqlite/expired.rs"]
mod expired;
#[path = "sqlite/read_proof.rs"]
mod read_proof;
#[path = "sqlite/run_facts.rs"]
mod run_facts;

#[path = "sqlite/continuation_handoff.rs"]
mod continuation_handoff;
#[path = "sqlite/continuation_input.rs"]
mod continuation_input;

mod abandon;
mod abandon_history;
mod admission_execution;
mod admission_journal;
mod approval;
mod attempt;
mod deletion;
mod execution;
mod execution_binding;
mod fork;
mod frontend;
mod management;
mod native_enrollment;
mod native_parent;
mod ownership_origin;
mod phase;
mod public_header;
mod public_run;
mod public_state;
mod queue;
mod records;
mod run_control;
mod session_owner;
mod snapshot;
mod store_impl;
mod tool_continuation;
mod tool_result;
mod transactions;
use crate::StoreError;
use crabber_core::{ByteLimits, Clock, SystemClock};
use execution::SqliteExecution;
use serde::{Serialize, de::DeserializeOwned};
use sqlx::{
    Connection, Sqlite, SqliteConnection, SqlitePool, Transaction,
    error::ErrorKind,
    sqlite::{SqliteConnectOptions, SqlitePoolOptions, SqliteSynchronous},
};
use std::{path::Path, sync::Arc, time::Duration};

const APPLICATION_ID: i32 = 0x4352_4142;
use crate::SQLITE_SCHEMA_VERSION;
const BUSY_TIMEOUT: Duration = Duration::from_secs(5);
const SCHEMA: &str = concat!(
    include_str!("../migrations/sqlite/0001_initial.sql"),
    "\nINSERT INTO schema_version(version) VALUES(1);\n",
    include_str!("../migrations/sqlite/0002_host_foundation.sql"),
    "\nINSERT INTO schema_version(version) VALUES(2);\n",
    include_str!("../migrations/sqlite/0003_typed_admission.sql"),
    "\nINSERT INTO schema_version(version) VALUES(3);\n",
    include_str!("../migrations/sqlite/0004_continuations.sql"),
    "\nINSERT INTO schema_version(version) VALUES(4);\n",
    include_str!("../migrations/sqlite/0005_session_management.sql"),
    "\nINSERT INTO schema_version(version) VALUES(5);\n",
    include_str!("../migrations/sqlite/0006_future_queue.sql"),
    "\nINSERT INTO schema_version(version) VALUES(6);\n",
    include_str!("../migrations/sqlite/0007_tool_continuations.sql"),
    "\nINSERT INTO schema_version(version) VALUES(7);\n",
    include_str!("../migrations/sqlite/0008_run_controls.sql"),
    "\nINSERT INTO schema_version(version) VALUES(8);\n",
    include_str!("../migrations/sqlite/0009_session_owners.sql"),
    "\nINSERT INTO schema_version(version) VALUES(9);\n",
    include_str!("../migrations/sqlite/0010_frontend_continuations.sql"),
    "\nINSERT INTO schema_version(version) VALUES(10);\n",
    include_str!("../migrations/sqlite/0011_safe_forks.sql"),
    "\nINSERT INTO schema_version(version) VALUES(11);\n",
    include_str!("../migrations/sqlite/0012_session_deletion.sql"),
    "\nINSERT INTO schema_version(version) VALUES(12);\n",
    include_str!("../migrations/sqlite/0013_admission_execution_sources.sql"),
    "\nINSERT INTO schema_version(version) VALUES(13);\n",
    include_str!("../migrations/sqlite/0014_owner_publications.sql"),
    "\nINSERT INTO schema_version(version) VALUES(14);\n",
    include_str!("../migrations/sqlite/0015_native_run_sources.sql"),
    "\nINSERT INTO schema_version(version) VALUES(15);\n",
    include_str!("../migrations/sqlite/0016_continuation_input_sources.sql"),
    "\n",
    "\nINSERT INTO schema_version(version) VALUES(16);\n",
    include_str!("../migrations/sqlite/0017_queue_publications.sql"),
    "\nINSERT INTO schema_version(version) VALUES(17);\n",
    include_str!("../migrations/sqlite/0018_native_execution_enrollment.sql"),
);

/// A dedicated, file-backed SQLite session database.
///
/// Call [`Self::migrate`] before [`Self::connect`]. Clones share a single writer
/// connection and a pool of four query-only reader connections.
#[derive(Clone)]
pub struct SqliteStore {
    writer: SqlitePool,
    readers: SqlitePool,
    clock: Arc<dyn Clock>,
    limits: ByteLimits,
    #[cfg(test)]
    abandon_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    public_state_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    management_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    attempt_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    phase_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    approval_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    binding_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    queue_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    deletion_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    tool_continuation_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    control_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    session_owner_fault: Arc<std::sync::atomic::AtomicU8>,
    #[cfg(test)]
    write_waiters: Arc<std::sync::atomic::AtomicUsize>,
}

async fn has_pending_continuation(
    tx: &mut Transaction<'_, Sqlite>,
    session: &crabber_core::SessionId,
) -> Result<bool, StoreError> {
    Box::pin(approval::verify_session(tx, session))
        .await
        .map_err(|_| StoreError::Validation("approval integrity failure".into()))?;
    let tool = Box::pin(tool_continuation::pending_in_session(tx, session))
        .await
        .map_err(|_| StoreError::Validation("invalid tool continuation evidence".into()))?;
    let approval: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    Ok(approval || tool)
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
        public_state_fault: Arc::default(),
        #[cfg(test)]
        management_fault: Arc::default(),
        #[cfg(test)]
        attempt_fault: Arc::default(),
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
        #[cfg(test)]
        write_waiters: Arc::default(),
    })
}

async fn verify_connection(writer: &SqlitePool) -> Result<(), StoreError> {
    let mut connection = writer.acquire().await.map_err(db)?;
    if classify_snapshot(&mut connection).await? != DatabaseState::Current {
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

#[derive(Debug, PartialEq, Eq)]
enum DatabaseState {
    Fresh,
    Current,
}

// Metadata must come from one snapshot when another process initializes the
// file concurrently. End this read transaction before attempting a WAL change.
async fn classify_snapshot(connection: &mut SqliteConnection) -> Result<DatabaseState, StoreError> {
    let mut transaction = connection.begin().await.map_err(classification_error)?;
    let state = classify(&mut transaction).await;
    transaction.rollback().await.map_err(db)?;
    state
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
        let versions: Vec<i64> =
            sqlx::query_scalar("SELECT version FROM schema_version ORDER BY version")
                .fetch_all(&mut *connection)
                .await
                .map_err(|_| unsupported_version())?;
        if versions != (1..=SQLITE_SCHEMA_VERSION).collect::<Vec<_>>() {
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
        let state = classify_snapshot(connection).await?;
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
        .bind(SQLITE_SCHEMA_VERSION)
        .execute(&mut **transaction)
        .await
        .map_err(db)?;
    Ok(())
}

#[allow(clippy::needless_pass_by_value)] // Required by map_err.
fn db(error: sqlx::Error) -> StoreError {
    if let sqlx::Error::Database(detail) = &error {
        if detail.message() == "crabber_session_deleted" {
            return StoreError::SessionDeleted;
        }
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

fn text<T: Serialize>(value: &T) -> Result<String, StoreError> {
    crate::storable::storable_text(value)
}

fn decode<T: DeserializeOwned>(text: &str) -> Result<T, StoreError> {
    serde_json::from_str(text)
        .map_err(|_| StoreError::Validation("stored record is invalid".into()))
}

#[cfg(test)]
mod lifecycle_tests;

#[cfg(test)]
mod abandon_tests;
#[cfg(test)]
mod tests;

#[cfg(test)]
mod admission_execution_tests;
#[cfg(test)]
mod admission_tests;
#[cfg(test)]
mod barrier_tests;
#[cfg(test)]
mod canonical_tests;
#[cfg(test)]
mod durability_tests;
#[cfg(test)]
mod process_tests;
#[cfg(test)]
mod snapshot_tests;
#[cfg(test)]
mod test_support;
