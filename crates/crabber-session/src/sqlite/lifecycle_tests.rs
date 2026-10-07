use super::*;
use sha2::{Digest, Sha256};
use sqlx::Row;
use std::{collections::BTreeMap, path::PathBuf};
use tempfile::TempDir;

struct Database {
    directory: TempDir,
    path: PathBuf,
}

impl Database {
    fn fresh() -> Self {
        let directory = tempfile::tempdir().unwrap();
        let path = directory.path().join("sessions.sqlite");
        Self { directory, path }
    }

    async fn migrated() -> Self {
        let database = Self::fresh();
        SqliteStore::migrate(&database.path).await.unwrap();
        database
    }

    async fn raw(&self) -> SqliteConnection {
        SqliteConnection::connect_with(&options(&self.path))
            .await
            .unwrap()
    }

    fn hash(&self) -> Vec<u8> {
        Sha256::digest(std::fs::read(&self.path).unwrap()).to_vec()
    }

    async fn backup(&self) -> PathBuf {
        let copy = self.directory.path().join("backup.sqlite");
        let mut connection = self.raw().await;
        sqlx::query("VACUUM INTO ?1")
            .bind(copy.to_str().unwrap())
            .execute(&mut connection)
            .await
            .unwrap();
        connection.close().await.unwrap();
        copy
    }
}

fn assert_validation<T>(result: Result<T, StoreError>, expected: &str, path: &Path) {
    let error = result.err().expect("operation should fail");
    assert!(!error.to_string().contains(path.to_str().unwrap()));
    assert!(
        !error.to_string().contains(
            path.parent()
                .unwrap()
                .file_name()
                .unwrap()
                .to_str()
                .unwrap()
        )
    );
    assert_eq!(error, StoreError::Validation(expected.into()));
}

async fn assert_refused(path: &Path, expected: &str) {
    assert_validation(SqliteStore::migrate(path).await, expected, path);
    assert_validation(SqliteStore::connect(path).await, expected, path);
}

async fn checkpoint(connection: &mut SqliteConnection) {
    let (busy, _, _): (i64, i64, i64) = sqlx::query_as("PRAGMA wal_checkpoint(TRUNCATE)")
        .fetch_one(connection)
        .await
        .unwrap();
    assert_eq!(busy, 0);
}

type SchemaObject = (String, String, String, Option<String>);

#[derive(Debug, PartialEq, Eq)]
struct DatabaseImage {
    application: i32,
    schema: Vec<SchemaObject>,
    rows: BTreeMap<String, Vec<Vec<String>>>,
}

// Compare all tables and rows, including sqlite_sequence, not just the pragmas.
async fn image(connection: &mut SqliteConnection) -> DatabaseImage {
    let application = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(&mut *connection)
        .await
        .unwrap();
    let schema: Vec<SchemaObject> =
        sqlx::query_as("SELECT type,name,tbl_name,sql FROM sqlite_schema ORDER BY type,name")
            .fetch_all(&mut *connection)
            .await
            .unwrap();
    let mut rows = BTreeMap::new();
    for (_, table, _, _) in schema.iter().filter(|object| object.0 == "table") {
        let columns: Vec<String> =
            sqlx::query_scalar("SELECT name FROM pragma_table_info(?1) ORDER BY cid")
                .bind(table)
                .fetch_all(&mut *connection)
                .await
                .unwrap();
        let quote = |name: &str| format!("\"{}\"", name.replace('"', "\"\""));
        let projections = columns
            .iter()
            .map(|name| format!("quote({})", quote(name)))
            .collect::<Vec<_>>()
            .join(",");
        let query = format!("SELECT {projections} FROM {}", quote(table));
        let records = sqlx::query(&query)
            .fetch_all(&mut *connection)
            .await
            .unwrap();
        let mut values = records
            .iter()
            .map(|record| {
                (0..columns.len())
                    .map(|column| record.get::<String, _>(column))
                    .collect::<Vec<_>>()
            })
            .collect::<Vec<_>>();
        values.sort();
        rows.insert(table.clone(), values);
    }
    DatabaseImage {
        application,
        schema,
        rows,
    }
}

async fn seed_session(connection: &mut SqliteConnection) {
    sqlx::query("INSERT INTO sessions(id,data) VALUES('session','{\"sentinel\":true}')")
        .execute(connection)
        .await
        .unwrap();
}

async fn seed_run(connection: &mut SqliteConnection) {
    seed_session(connection).await;
    sqlx::query("INSERT INTO runs(id,session_id,status,claim_token,lease_until,data) VALUES('run','session','pending','claim',0,'{}')")
        .execute(connection)
        .await
        .unwrap();
}

#[tokio::test]
async fn migrate_creates_version_one_wal_database() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    let application: i32 = sqlx::query_scalar("PRAGMA application_id")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(application, APPLICATION_ID);
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(mode, "wal");
    let foreign_keys: i32 = sqlx::query_scalar("PRAGMA foreign_keys")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(foreign_keys, 1);
    let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM schema_version")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    assert_eq!(versions, [1]);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM snapshot_auth WHERE singleton=1")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 1);
    connection.close().await.unwrap();

    let store = SqliteStore::connect(&database.path).await.unwrap();
    assert_eq!(store.writer.options().get_max_connections(), 1);
    assert_eq!(store.readers.options().get_max_connections(), 4);
    for pool in [&store.writer, &store.readers] {
        let mut connection = pool.acquire().await.unwrap();
        for (pragma, expected) in [
            ("PRAGMA foreign_keys", 1),
            ("PRAGMA synchronous", 2),
            ("PRAGMA busy_timeout", 5000),
        ] {
            let value: i64 = sqlx::query_scalar(pragma)
                .fetch_one(&mut *connection)
                .await
                .unwrap();
            assert_eq!(value, expected);
        }
    }
    let query_only: i64 = sqlx::query_scalar("PRAGMA query_only")
        .fetch_one(&store.readers)
        .await
        .unwrap();
    assert_eq!(query_only, 1);
    let error = sqlx::query("INSERT INTO sessions(id,data) VALUES('forbidden','{}')")
        .execute(&store.readers)
        .await
        .unwrap_err();
    assert_eq!(
        db(error),
        StoreError::Validation("SQLite operation failed".into())
    );
    store.writer.close().await;
    store.readers.close().await;
}

#[tokio::test]
async fn migrate_is_idempotent_and_keeps_the_snapshot_secret() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    seed_session(&mut connection).await;
    let before = image(&mut connection).await;
    checkpoint(&mut connection).await;
    let hash = database.hash();
    connection.close().await.unwrap();
    SqliteStore::migrate(&database.path).await.unwrap();
    assert_eq!(database.hash(), hash);
    let mut connection = database.raw().await;
    assert_eq!(image(&mut connection).await, before);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn connect_never_creates_a_file() {
    let database = Database::fresh();
    assert_validation(
        SqliteStore::connect(&database.path).await,
        "SQLite operation failed",
        &database.path,
    );
    assert!(!database.path.exists());
    assert_eq!(
        std::fs::read_dir(database.directory.path())
            .unwrap()
            .count(),
        0
    );
}

#[tokio::test]
async fn unknown_schema_version_is_refused_without_changes() {
    for pending_wal in [false, true] {
        let database = Database::migrated().await;
        let mut connection = database.raw().await;
        if pending_wal {
            sqlx::query("PRAGMA wal_autocheckpoint=0")
                .execute(&mut connection)
                .await
                .unwrap();
        }
        sqlx::query("INSERT INTO schema_version VALUES(2)")
            .execute(&mut connection)
            .await
            .unwrap();
        seed_session(&mut connection).await;
        if !pending_wal {
            checkpoint(&mut connection).await;
        }
        let before = image(&mut connection).await;
        let hash = database.hash();
        assert_refused(&database.path, "unsupported SQLite schema version").await;
        assert_eq!(image(&mut connection).await, before);
        assert_eq!(database.hash(), hash);
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn malformed_version_metadata_is_refused_without_changes() {
    for statement in [
        "DELETE FROM schema_version",
        "DROP TABLE schema_version",
        "UPDATE schema_version SET version=0",
    ] {
        let database = Database::migrated().await;
        let mut connection = database.raw().await;
        sqlx::query(statement)
            .execute(&mut connection)
            .await
            .unwrap();
        checkpoint(&mut connection).await;
        let before = image(&mut connection).await;
        let hash = database.hash();
        assert_refused(&database.path, "unsupported SQLite schema version").await;
        assert_eq!(image(&mut connection).await, before);
        assert_eq!(database.hash(), hash);
        connection.close().await.unwrap();
    }
}

#[tokio::test]
async fn unknown_schema_in_a_backup_is_refused_before_enabling_wal() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    sqlx::query("INSERT INTO schema_version VALUES(2)")
        .execute(&mut connection)
        .await
        .unwrap();
    seed_session(&mut connection).await;
    connection.close().await.unwrap();
    let copy = database.backup().await;
    let mut connection = SqliteConnection::connect_with(&options(&copy))
        .await
        .unwrap();
    let before = image(&mut connection).await;
    let hash = Sha256::digest(std::fs::read(&copy).unwrap());
    assert_refused(&copy, "unsupported SQLite schema version").await;
    assert_eq!(image(&mut connection).await, before);
    assert_eq!(Sha256::digest(std::fs::read(&copy).unwrap()), hash);
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(mode, "delete");
    connection.close().await.unwrap();
}

#[tokio::test]
async fn foreign_database_is_refused_without_changes() {
    let database = Database::fresh();
    let mut connection =
        SqliteConnection::connect_with(&options(&database.path).create_if_missing(true))
            .await
            .unwrap();
    sqlx::raw_sql("CREATE TABLE host_data(value TEXT); INSERT INTO host_data VALUES('keep me');")
        .execute(&mut connection)
        .await
        .unwrap();
    let before = image(&mut connection).await;
    let hash = database.hash();
    assert_refused(&database.path, "not a Crabber SQLite database").await;
    assert_eq!(image(&mut connection).await, before);
    assert_eq!(database.hash(), hash);
    let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(mode, "delete");
    connection.close().await.unwrap();
}

#[tokio::test]
async fn foreign_application_id_is_refused_without_changes() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    sqlx::query("PRAGMA application_id=42")
        .execute(&mut connection)
        .await
        .unwrap();
    checkpoint(&mut connection).await;
    let before = image(&mut connection).await;
    let hash = database.hash();
    assert_refused(&database.path, "not a Crabber SQLite database").await;
    assert_eq!(image(&mut connection).await, before);
    assert_eq!(database.hash(), hash);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn non_sqlite_inputs_are_refused_without_changes() {
    let database = Database::fresh();
    std::fs::write(&database.path, [b'x'; 100]).unwrap();
    let hash = database.hash();
    assert_refused(&database.path, "not a Crabber SQLite database").await;
    assert_eq!(database.hash(), hash);
    assert_eq!(
        std::fs::read_dir(database.directory.path())
            .unwrap()
            .count(),
        1
    );

    let directory = database.directory.path().join("directory");
    std::fs::create_dir(&directory).unwrap();
    assert_refused(&directory, "SQLite operation failed").await;
    assert!(directory.is_dir());
    assert_eq!(std::fs::read_dir(&directory).unwrap().count(), 0);

    let empty = database.directory.path().join("empty.sqlite");
    std::fs::write(&empty, []).unwrap();
    SqliteStore::migrate(&empty).await.unwrap();
    let store = SqliteStore::connect(&empty).await.unwrap();
    store.writer.close().await;
    store.readers.close().await;
}

#[tokio::test]
async fn interrupted_migration_leaves_a_fresh_database() {
    let database = Database::fresh();
    let broken = format!("{SCHEMA}\nTHIS IS NOT SQL;");
    assert_validation(
        migrate_with(&database.path, &broken).await,
        "SQLite operation failed",
        &database.path,
    );
    let mut connection = database.raw().await;
    let state = image(&mut connection).await;
    assert_eq!(state.application, 0);
    assert_eq!(state.schema, Vec::<SchemaObject>::new());
    connection.close().await.unwrap();
    SqliteStore::migrate(&database.path).await.unwrap();
    let store = SqliteStore::connect(&database.path).await.unwrap();
    store.writer.close().await;
    store.readers.close().await;
}

#[tokio::test]
async fn concurrent_migrate_initializes_once() {
    let database = Database::fresh();
    let first_path = database.path.clone();
    let second_path = database.path.clone();
    let start = Arc::new(tokio::sync::Barrier::new(2));
    let first_start = Arc::clone(&start);
    let first = tokio::spawn(async move {
        first_start.wait().await;
        SqliteStore::migrate(first_path).await
    });
    let second = tokio::spawn(async move {
        start.wait().await;
        SqliteStore::migrate(second_path).await
    });
    first.await.unwrap().unwrap();
    second.await.unwrap().unwrap();
    let mut connection = database.raw().await;
    let versions: Vec<i64> = sqlx::query_scalar("SELECT version FROM schema_version")
        .fetch_all(&mut connection)
        .await
        .unwrap();
    assert_eq!(versions, [1]);
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM snapshot_auth")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    assert_eq!(count, 1);
    connection.close().await.unwrap();
}

#[tokio::test]
async fn restored_backup_requires_migrate_then_connects() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    seed_session(&mut connection).await;
    let before = image(&mut connection).await;
    connection.close().await.unwrap();
    let copy = database.backup().await;
    let hash = Sha256::digest(std::fs::read(&copy).unwrap());
    assert_validation(
        SqliteStore::connect(&copy).await,
        "SQLite database is not in WAL mode; run migrate",
        &copy,
    );
    assert_eq!(Sha256::digest(std::fs::read(&copy).unwrap()), hash);
    SqliteStore::migrate(&copy).await.unwrap();
    let store = SqliteStore::connect(&copy).await.unwrap();
    let mut connection = store.readers.acquire().await.unwrap();
    assert_eq!(image(&mut connection).await, before);
    drop(connection);
    store.writer.close().await;
    store.readers.close().await;
}

#[tokio::test]
async fn connect_is_read_only() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    seed_session(&mut connection).await;
    checkpoint(&mut connection).await;
    let before = image(&mut connection).await;
    let cookie: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut connection)
        .await
        .unwrap();
    connection.close().await.unwrap();
    let hash = database.hash();
    let store = SqliteStore::connect(&database.path).await.unwrap();
    let mut reader = store.readers.acquire().await.unwrap();
    assert_eq!(image(&mut reader).await, before);
    let after: i64 = sqlx::query_scalar("PRAGMA schema_version")
        .fetch_one(&mut *reader)
        .await
        .unwrap();
    assert_eq!(after, cookie);
    drop(reader);
    store.writer.close().await;
    store.readers.close().await;
    assert_eq!(database.hash(), hash);
    let wal = database.path.with_file_name("sessions.sqlite-wal");
    match std::fs::metadata(wal) {
        Ok(metadata) => assert_eq!(metadata.len(), 0),
        Err(error) => assert_eq!(error.kind(), std::io::ErrorKind::NotFound),
    }
}

#[tokio::test]
async fn error_text_never_contains_the_path() {
    let database = Database::fresh();
    let missing_parent = database
        .directory
        .path()
        .join("absent")
        .join("sessions.sqlite");
    assert_refused(&missing_parent, "SQLite operation failed").await;
    assert!(!missing_parent.parent().unwrap().exists());
    // All other refusal tests use assert_validation, which also checks that
    // neither the full path nor its temporary directory appears in the error.
    assert_eq!(
        db(sqlx::Error::PoolTimedOut),
        StoreError::Validation("SQLite operation failed".into())
    );
}

#[tokio::test]
async fn error_mapping() {
    let database = Database::migrated().await;
    let store = SqliteStore::connect(&database.path).await.unwrap();
    let mut transaction = store.begin_write().await.unwrap();
    seed_run(&mut transaction).await;
    let error = sqlx::query("INSERT INTO sessions(id,data) VALUES('session','{}')")
        .execute(&mut *transaction)
        .await
        .unwrap_err();
    assert_eq!(db(error), StoreError::Conflict);
    let error = sqlx::query(
        "INSERT INTO parts(id,message_id,ordinal,data) VALUES('part','missing',0,'{}')",
    )
    .execute(&mut *transaction)
    .await
    .unwrap_err();
    assert_eq!(db(error), StoreError::NotFound);
    let error = sqlx::query("INSERT INTO messages(id,session_id,data,snapshot_parts,snapshot_text,snapshot_bytes) VALUES('message','session','{}',0,0,3)")
        .execute(&mut *transaction)
        .await
        .unwrap_err();
    assert_eq!(
        db(error),
        StoreError::Validation("SQLite operation failed".into())
    );
    transaction.rollback().await.unwrap();
    assert_eq!(
        store
            .write_waiters
            .load(std::sync::atomic::Ordering::SeqCst),
        0
    );
    store.writer.close().await;
    store.readers.close().await;
}

#[tokio::test]
async fn accounting_guard_and_revision_triggers() {
    let database = Database::migrated().await;
    let mut connection = database.raw().await;
    seed_run(&mut connection).await;
    for table in ["messages", "tool_calls"] {
        let ownership = if table == "messages" {
            "session_id"
        } else {
            "run_id"
        };
        let parent = if table == "messages" {
            "session"
        } else {
            "run"
        };
        let status = if table == "tool_calls" { ",status" } else { "" };
        let status_value = if table == "tool_calls" {
            ",'pending'"
        } else {
            ""
        };
        // Non-ASCII content proves accounting uses encoded bytes, not characters.
        let insert = format!(
            "INSERT INTO {table}(id,{ownership},data,snapshot_parts,snapshot_text,snapshot_bytes{status}) VALUES(?1,?2,'\"é\"',0,0,?3{status_value})"
        );
        let error = sqlx::query(&insert)
            .bind(table)
            .bind(parent)
            .bind(3)
            .execute(&mut connection)
            .await
            .unwrap_err();
        assert_eq!(
            db(error),
            StoreError::Validation("SQLite operation failed".into())
        );
        sqlx::query(&insert)
            .bind(table)
            .bind(parent)
            .bind(4)
            .execute(&mut connection)
            .await
            .unwrap();
        let before = image(&mut connection).await;
        let error = sqlx::query(&format!("UPDATE {table} SET snapshot_bytes=3"))
            .execute(&mut connection)
            .await
            .unwrap_err();
        assert_eq!(
            db(error),
            StoreError::Validation("SQLite operation failed".into())
        );
        assert_eq!(image(&mut connection).await, before);
        let revision: i64 = sqlx::query_scalar("SELECT snapshot_revision FROM sessions")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        sqlx::query(&format!("UPDATE {table} SET data='\"x\"',snapshot_bytes=3"))
            .execute(&mut connection)
            .await
            .unwrap();
        let after_update: i64 = sqlx::query_scalar("SELECT snapshot_revision FROM sessions")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(after_update, revision + 1);
        sqlx::query(&format!("DELETE FROM {table}"))
            .execute(&mut connection)
            .await
            .unwrap();
        let after_delete: i64 = sqlx::query_scalar("SELECT snapshot_revision FROM sessions")
            .fetch_one(&mut connection)
            .await
            .unwrap();
        assert_eq!(after_delete, revision + 2);
    }
    connection.close().await.unwrap();
}

#[tokio::test]
async fn migrate_waits_for_a_journal_mode_lock() {
    for exceeds_budget in [false, true] {
        let database = Database::migrated().await;
        let copy = database.backup().await;
        let mut connection = SqliteConnection::connect_with(&options(&copy))
            .await
            .unwrap();
        let before = image(&mut connection).await;
        let hash = Sha256::digest(std::fs::read(&copy).unwrap());
        let transaction = connection.begin_with("BEGIN IMMEDIATE").await.unwrap();
        let path = copy.clone();
        let migration = tokio::spawn(async move { SqliteStore::migrate(path).await });
        if exceeds_budget {
            let result = tokio::time::timeout(Duration::from_secs(8), migration)
                .await
                .expect("migration must respect the busy timeout")
                .unwrap();
            assert_validation(result, "SQLite operation failed", &copy);
            assert_eq!(Sha256::digest(std::fs::read(&copy).unwrap()), hash);
            transaction.rollback().await.unwrap();
            assert_eq!(image(&mut connection).await, before);
        } else {
            tokio::time::sleep(Duration::from_millis(500)).await;
            assert!(!migration.is_finished());
            transaction.rollback().await.unwrap();
            migration.await.unwrap().unwrap();
            // Journal mode is cached per connection. Reopen to verify the
            // persisted mode changed by the migrating connection.
            connection.close().await.unwrap();
            connection = SqliteConnection::connect_with(&options(&copy))
                .await
                .unwrap();
            let mode: String = sqlx::query_scalar("PRAGMA journal_mode")
                .fetch_one(&mut connection)
                .await
                .unwrap();
            assert_eq!(mode, "wal");
            assert_eq!(image(&mut connection).await, before);
        }
        connection.close().await.unwrap();
    }
}
