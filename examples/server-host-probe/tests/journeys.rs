use server_host_probe::{Error, run, store::AuditedStore};
use std::sync::Arc;
#[tokio::test]
async fn memory_public_host() -> Result<(), Error> {
    let store = Arc::new(AuditedStore::new(Arc::new(
        crabber::session::MemoryStore::new(),
    )));
    let report = run(store.clone()).await?;
    assert_eq!(report.contexts, 2);
    assert_eq!(report.ordinary_effects, 2);
    assert_eq!(report.interactive_effects, 1);
    assert_eq!(report.resumed_executions, 2);
    assert_eq!(report.terminated_approvals, 1);
    assert_eq!(report.http_support_profiles, 2);
    assert!(store.calls() > 0);
    Ok(())
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_public_host() -> Result<(), Error> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("host.db");
    crabber::session::SqliteStore::migrate(&path).await?;
    run(Arc::new(AuditedStore::new(Arc::new(
        crabber::session::SqliteStore::connect(&path).await?,
    ))))
    .await?;
    Ok(())
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_public_host() -> Result<(), Error> {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert!(std::env::var_os("CRABBER_REQUIRE_POSTGRES").is_none());
        return Ok(());
    };
    crabber::session::PostgresStore::migrate(&url).await?;
    run(Arc::new(AuditedStore::new(Arc::new(
        crabber::session::PostgresStore::connect(&url).await?,
    ))))
    .await?;
    Ok(())
}
