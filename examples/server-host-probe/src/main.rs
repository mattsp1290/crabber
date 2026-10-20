use server_host_probe::{Error, run, store::AuditedStore};
use std::sync::Arc;
#[tokio::main]
async fn main() -> Result<(), Error> {
    let backend = std::env::args().nth(1).unwrap_or_else(|| "memory".into());
    let store: Arc<dyn crabber::session::Store> = match backend.as_str() {
        "memory" => Arc::new(crabber::session::MemoryStore::new()),
        #[cfg(feature = "sqlite")]
        "sqlite" => {
            let path = std::env::args()
                .nth(2)
                .ok_or("sqlite requires an explicit database path")?;
            crabber::session::SqliteStore::migrate(&path).await?;
            Arc::new(crabber::session::SqliteStore::connect(&path).await?)
        }
        #[cfg(feature = "postgres")]
        "postgres" => {
            let url = std::env::var("CRABBER_TEST_POSTGRES_URL")?;
            crabber::session::PostgresStore::migrate(&url).await?;
            Arc::new(crabber::session::PostgresStore::connect(&url).await?)
        }
        _ => return Err("backend unavailable; enable its Cargo feature".into()),
    };
    let store = Arc::new(AuditedStore::new(store));
    let report = run(store.clone()).await?;
    assert!(store.calls() > 0);
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}
