//! Public API denial with real PostgreSQL LOGIN roles and row-integrity controls.
#![cfg(feature = "postgres")]
#[path = "workspace_roles/controls.rs"]
mod controls;
#[path = "workspace_roles/fixture.rs"]
mod fixture;
#[path = "workspace_roles/held_run.rs"]
mod held_run;
#[path = "workspace_roles/management.rs"]
mod management;
#[path = "workspace_roles/positive.rs"]
mod positive;
#[path = "run_controls/provider.rs"]
mod provider;
#[path = "workspace_roles/queue.rs"]
mod queue;
#[path = "workspace_roles/reads.rs"]
mod reads;
#[path = "workspace_roles/recovery.rs"]
mod recovery;

#[path = "workspace_roles/cold_snapshot.rs"]
mod cold_snapshot;
#[path = "../../../examples/bounded-snapshot/src/journey.rs"]
#[allow(dead_code)]
mod snapshot_journey;

#[test]
fn postgres_snapshot_child_process() {
    if std::env::var_os("CRABBER_BOUNDED_CHILD").is_none() {
        return;
    }
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    runtime.block_on(async {
        let url = crabber_test_support::postgres::TestDatabase::attached_url()
            .await
            .unwrap();
        snapshot_journey::child(Some(&url)).await;
    });
}
