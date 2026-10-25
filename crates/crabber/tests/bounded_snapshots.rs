//! Public facade acceptance tests use the same code as the runnable example.
#[path = "../../../examples/bounded-snapshot/src/journey.rs"]
#[allow(dead_code)]
mod journey;

fn runtime() -> tokio::runtime::Runtime {
    tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap()
}

#[test]
fn memory_embedding_pages_settled_relations_then_resumes_events() {
    let runtime = runtime();
    let stores = journey::memory();
    let source = journey::source(false);
    runtime.block_on(journey::journey(
        stores.clone(),
        "memory",
        &source,
        journey::ChildMode::None,
    ));
    journey::allocation_proof(&runtime, &stores, "memory", &source);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_embedding_independent_pools_and_fresh_process() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    if !TestDatabase::available().unwrap() {
        return;
    }
    let runtime = runtime();
    let database = runtime
        .block_on(TestDatabase::create(DatabaseMode::Current))
        .unwrap();
    {
        let url = database.url();
        let stores = runtime.block_on(journey::postgres(&url, true));
        let source = journey::source(false);
        runtime.block_on(journey::journey(
            stores.clone(),
            "postgres",
            &source,
            journey::ChildMode::Test(TestDatabase::child_environment(&url).unwrap().to_vec()),
        ));
        journey::allocation_proof(&runtime, &stores, "postgres", &source);
    }
    runtime.block_on(database.finish()).unwrap();
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_snapshot_child_process() {
    if std::env::var("CRABBER_BOUNDED_CHILD").is_ok() {
        let runtime = runtime();
        let url = runtime
            .block_on(crabber_test_support::postgres::TestDatabase::attached_url())
            .unwrap();
        runtime.block_on(journey::child(Some(&url)));
    }
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_embedding_independent_stores_and_fresh_process() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("snapshots.sqlite");
    let runtime = runtime();
    let stores = runtime.block_on(journey::sqlite(&path, true));
    let source = journey::source(false);
    runtime.block_on(journey::journey(
        stores.clone(),
        "sqlite",
        &source,
        journey::ChildMode::SqliteTest(path),
    ));
    journey::allocation_proof(&runtime, &stores, "sqlite", &source);
}

#[cfg(feature = "sqlite")]
#[test]
fn sqlite_snapshot_child_process() {
    if std::env::var_os("CRABBER_SQLITE_CHILD_PATH").is_some() {
        runtime().block_on(journey::child(None));
    }
}
