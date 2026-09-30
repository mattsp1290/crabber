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
    if std::env::var("CRABBER_TEST_POSTGRES_URL").is_err() {
        assert!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").is_err(),
            "required PostgreSQL environment missing"
        );
        eprintln!("PostgreSQL facade journey skipped: CRABBER_TEST_POSTGRES_URL unset");
        return;
    }
    let runtime = runtime();
    let stores = runtime.block_on(journey::postgres(true));
    let source = journey::source(false);
    runtime.block_on(journey::journey(
        stores.clone(),
        "postgres",
        &source,
        journey::ChildMode::Test,
    ));
    journey::allocation_proof(&runtime, &stores, "postgres", &source);
}

#[cfg(feature = "postgres")]
#[test]
fn postgres_snapshot_child_process() {
    if std::env::var("CRABBER_BOUNDED_CHILD").is_ok() {
        runtime().block_on(journey::child());
    }
}
