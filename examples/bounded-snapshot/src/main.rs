#[allow(dead_code)]
mod journey;
#[cfg(feature = "postgres")]
#[path = "../../postgres-fixture.rs"]
mod postgres_fixture;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "--memory".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    #[cfg(feature = "postgres")]
    if mode == "--postgres-child" {
        let url = std::env::var("CRABBER_TEST_POSTGRES_URL").expect("PostgreSQL endpoint required");
        runtime.block_on(postgres_fixture::verify(&url));
        runtime.block_on(journey::child(Some(&url)));
        return;
    }
    #[cfg(feature = "sqlite")]
    if mode == "--sqlite-child" {
        runtime.block_on(journey::child(None));
        return;
    }
    #[cfg(feature = "sqlite")]
    let directory = if mode == "--sqlite" {
        Some(
            tempfile::Builder::new()
                .prefix("crabber-snapshot-")
                .tempdir()
                .unwrap(),
        )
    } else {
        None
    };
    let check = std::env::args().any(|arg| arg == "--check");
    let source = journey::source(!check);
    let (backend, stores, child) = match mode.as_str() {
        "--memory" => ("memory", journey::memory(), journey::ChildMode::None),
        #[cfg(feature = "postgres")]
        "--postgres" => {
            let url =
                std::env::var("CRABBER_TEST_POSTGRES_URL").expect("PostgreSQL endpoint required");
            runtime.block_on(postgres_fixture::verify(&url));
            (
                "postgres",
                runtime.block_on(journey::postgres(&url, true)),
                journey::ChildMode::Demo(
                    url,
                    check,
                    std::env::var("CRABBER_TEST_POSTGRES_CHILD_DATABASE").ok(),
                ),
            )
        }
        #[cfg(feature = "sqlite")]
        "--sqlite" => {
            let path = directory.as_ref().unwrap().path().join("session.sqlite");
            (
                "sqlite",
                runtime.block_on(journey::sqlite(&path, true)),
                journey::ChildMode::SqliteDemo(path, check),
            )
        }
        _ => panic!(
            "usage: bounded-snapshot --memory | --postgres | --sqlite (durable modes require their Cargo feature)"
        ),
    };
    runtime.block_on(journey::journey(stores.clone(), backend, &source, child));
    journey::allocation_proof(&runtime, &stores, backend, &source);
}
