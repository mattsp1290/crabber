//! Backend selection and raw evidence for the shared fresh-process journeys.
use super::*;

pub(super) enum BackendKind {
    #[cfg(feature = "postgres")]
    Postgres(String),
    #[cfg(feature = "sqlite")]
    Sqlite,
}

pub(super) enum Backend {
    #[cfg(feature = "postgres")]
    Postgres(String),
    #[cfg(feature = "sqlite")]
    Sqlite(PathBuf),
}

impl BackendKind {
    pub(super) async fn prepare(&self, dir: &Path) -> Backend {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(url) => {
                let _ = dir;
                Backend::Postgres(url.clone())
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite => {
                let path = dir.join("session.sqlite3");
                crabber::session::SqliteStore::migrate(&path).await.unwrap();
                Backend::Sqlite(path)
            }
        }
    }
}

pub(super) struct Evidence {
    pub(super) runs: i64,
    pub(super) receipts: i64,
    pub(super) capsules: i64,
    pub(super) messages: Vec<String>,
    pub(super) schema: i32,
}

impl Backend {
    pub(super) async fn connect(&self) -> Arc<dyn Store> {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(url) => {
                Arc::new(crabber::session::PostgresStore::connect(url).await.unwrap())
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(path) => {
                Arc::new(crabber::session::SqliteStore::connect(path).await.unwrap())
            }
        }
    }

    pub(super) fn label(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => "postgres",
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => "sqlite",
        }
    }

    pub(super) fn failure(&self) -> &'static str {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(_) => "PostgreSQL operation failed",
            #[cfg(feature = "sqlite")]
            Self::Sqlite(_) => "SQLite operation failed",
        }
    }

    pub(super) fn child_environment(&self, command: &mut Command, dir: &Path, mode: &str) {
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(url) => {
                command
                    .envs(
                        crabber_test_support::postgres::TestDatabase::child_environment(url)
                            .unwrap(),
                    )
                    .env("CRABBER_RECEIPT_CHILD", mode)
                    .env("CRABBER_RECEIPT_HANDOFF", dir);
                // A PostgreSQL child cannot accidentally select an inherited SQLite file.
                command.env_remove("CRABBER_SQLITE_CHILD_PATH");
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(path) => {
                command
                    .env("CRABBER_SQLITE_CHILD_PATH", path)
                    .env("CRABBER_SQLITE_RECEIPT_CHILD", mode)
                    .env("CRABBER_SQLITE_RECEIPT_HANDOFF", dir);
            }
        }
    }

    pub(super) async fn child() -> Option<(Self, PathBuf, String)> {
        #[cfg(feature = "sqlite")]
        if let Some(path) = std::env::var_os("CRABBER_SQLITE_CHILD_PATH") {
            let mode = std::env::var("CRABBER_SQLITE_RECEIPT_CHILD").ok()?;
            let dir = PathBuf::from(std::env::var_os("CRABBER_SQLITE_RECEIPT_HANDOFF").unwrap());
            return Some((Self::Sqlite(PathBuf::from(path)), dir, mode));
        }
        #[cfg(feature = "postgres")]
        {
            let mode = std::env::var("CRABBER_RECEIPT_CHILD").ok()?;
            let dir = PathBuf::from(std::env::var_os("CRABBER_RECEIPT_HANDOFF").unwrap());
            let url = crabber_test_support::postgres::TestDatabase::attached_url()
                .await
                .unwrap();
            Some((Self::Postgres(url), dir, mode))
        }
        #[cfg(not(feature = "postgres"))]
        None
    }

    pub(super) async fn evidence(&self, session: &SessionId) -> Evidence {
        // The queries and bind values are identical, except for canonical record
        // storage and the independent schema-version SQL integer types.
        macro_rules! read_evidence {
            ($pool:expr, $record:literal) => {{
                let pool = $pool;
                let runs = sqlx::query_scalar("SELECT count(*) FROM runs WHERE session_id=$1")
                    .bind(&session.0)
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                let receipts = sqlx::query_scalar(
                    "SELECT count(*) FROM admission_receipts WHERE session_id=$1",
                )
                .bind(&session.0)
                .fetch_one(&pool)
                .await
                .unwrap();
                let capsules = sqlx::query_scalar(
                    "SELECT count(*) FROM admission_executions WHERE session_id=$1",
                )
                .bind(&session.0)
                .fetch_one(&pool)
                .await
                .unwrap();
                let messages = sqlx::query_scalar(concat!(
                    "SELECT ",
                    $record,
                    " FROM messages WHERE session_id=$1 ORDER BY seq"
                ))
                .bind(&session.0)
                .fetch_all(&pool)
                .await
                .unwrap();
                let schema: i32 = sqlx::query_scalar("SELECT max(version) FROM schema_version")
                    .fetch_one(&pool)
                    .await
                    .unwrap();
                pool.close().await;
                Evidence {
                    runs,
                    receipts,
                    capsules,
                    messages,
                    schema,
                }
            }};
        }
        match self {
            #[cfg(feature = "postgres")]
            Self::Postgres(url) => {
                read_evidence!(sqlx::PgPool::connect(url).await.unwrap(), "snapshot_record")
            }
            #[cfg(feature = "sqlite")]
            Self::Sqlite(path) => {
                use sqlx::sqlite::{SqliteConnectOptions, SqlitePoolOptions};
                let options = SqliteConnectOptions::new()
                    .filename(path)
                    .create_if_missing(false);
                let pool = SqlitePoolOptions::new()
                    .connect_with(options)
                    .await
                    .unwrap();
                let evidence = read_evidence!(pool, "data");
                assert_eq!(
                    i64::from(evidence.schema),
                    crabber::session::SQLITE_SCHEMA_VERSION
                );
                evidence
            }
        }
    }
}
