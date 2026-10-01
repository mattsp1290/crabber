mod journey;

#[tokio::main]
async fn main() {
    let (run, broadcasts, models) =
        tokio::time::timeout(std::time::Duration::from_secs(10), journey::journey())
            .await
            .expect("host tracing journey must complete");
    println!(
        "backend=memory provider=fake run={run} broadcasts={broadcasts} host_models={models} parallel_tools=2 context_bits=128"
    );
}
