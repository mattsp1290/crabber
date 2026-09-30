#[allow(dead_code)]
mod journey;

fn main() {
    let mode = std::env::args().nth(1).unwrap_or_else(|| "--memory".into());
    let runtime = tokio::runtime::Builder::new_current_thread()
        .enable_all()
        .build()
        .unwrap();
    #[cfg(feature = "postgres")]
    if mode == "--postgres-child" {
        runtime.block_on(journey::child());
        return;
    }
    let check = std::env::args().any(|arg| arg == "--check");
    let source = journey::source(!check);
    let (backend, stores, child) = match mode.as_str() {
        "--memory" => ("memory", journey::memory(), journey::ChildMode::None),
        #[cfg(feature = "postgres")]
        "--postgres" => (
            "postgres",
            runtime.block_on(journey::postgres(true)),
            journey::ChildMode::Demo(check),
        ),
        _ => panic!("usage: bounded-snapshot --memory | --postgres (requires postgres feature)"),
    };
    runtime.block_on(journey::journey(stores.clone(), backend, &source, child));
    journey::allocation_proof(&runtime, &stores, backend, &source);
}
