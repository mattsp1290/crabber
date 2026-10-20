//! A separate Cargo root compiles and executes only exported public interfaces.
use server_host_probe::{Error, store::AuditedStore};
use std::sync::Arc;
#[tokio::main]
async fn main() -> Result<(), Error> {
    let store = Arc::new(AuditedStore::new(Arc::new(
        crabber::session::MemoryStore::new(),
    )));
    let report = server_host_probe::run(store.clone()).await?;
    assert_eq!(report.scoped_instances, 2);
    assert_eq!(report.ordinary_effects, 2);
    assert_eq!(report.interactive_starts, 1);
    assert_eq!(report.interactive_resumes, 2);
    assert_eq!(report.interactive_effects, 1);
    assert_eq!(report.frontend_resumes, 2);
    assert_eq!(report.fixed_sessions, 2);
    assert_eq!(report.shared_service_reads, 1);
    assert_eq!(report.public_source_writes, 4);
    assert_eq!(report.resumed_executions, 2);
    assert_eq!(report.terminated_approvals, 1);
    assert_eq!(report.http_support_profiles, 2);
    assert!(store.calls() > 0);
    println!(
        "public locked consumer: {}",
        serde_json::to_string(&report)?
    );
    Ok(())
}
