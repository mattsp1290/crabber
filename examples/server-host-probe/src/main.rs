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
    if std::env::args().any(|argument| argument == "--smoke") {
        smoke(store.clone()).await?;
        assert!(store.calls() > 0);
        println!(
            "{}",
            serde_json::json!({"backend": backend, "smoke": true, "provider_requests": 1})
        );
        return Ok(());
    }
    let report = run(store.clone()).await?;
    assert!(store.calls() > 0);
    println!("{}", serde_json::to_string(&report)?);
    Ok(())
}

// The locked public consumer owns the full host journey. This short path keeps
// the actual CLI's backend construction, facade wiring and JSON output covered.
async fn smoke(store: Arc<AuditedStore>) -> Result<(), Error> {
    let provider = Arc::new(crabber::FakeProvider::scripted(vec![vec![
        crabber::StreamDelta::TextDelta("CLI smoke".into()),
        crabber::StreamDelta::Completed,
    ]]));
    let agent = crabber::Agent::builder()
        .behavior_version("server-host-cli-smoke.v1")
        .store(store)
        .provider(provider.clone())
        .config(crabber::AgentConfig::new(crabber::Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .build()?;
    let result = agent.prompt(None, "Check CLI wiring").await?;
    assert_eq!(result.status, crabber::core::RunStatus::Completed);
    assert_eq!(provider.requests().len(), 1);
    agent.close_extensions().await?;
    Ok(())
}
