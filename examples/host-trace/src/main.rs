mod durable;
mod export;
mod journey;

fn source() {
    let output = std::process::Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()
        .unwrap();
    assert!(output.status.success());
    println!(
        "source={}",
        String::from_utf8(output.stdout).unwrap().trim()
    );
}
#[tokio::main]
async fn main() {
    source();
    if let Ok(mode) = std::env::var("CRABBER_TRACE_WORKER") {
        #[cfg(feature = "postgres")]
        {
            let url = std::env::var("CRABBER_TEST_POSTGRES_URL")
                .expect("CRABBER_TEST_POSTGRES_URL required");
            let store = std::sync::Arc::new(
                crabber::session::PostgresStore::connect(&url)
                    .await
                    .unwrap(),
            );
            let path = std::path::PathBuf::from(std::env::var_os("CRABBER_TRACE_QUEUE").unwrap());
            durable::worker(store, &path, &mode, "postgres").await;
            return;
        }
        #[cfg(not(feature = "postgres"))]
        panic!("postgres feature required for worker mode {mode}");
    }
    let mode = std::env::args().nth(1).unwrap_or_else(|| "memory".into());
    let dir = std::env::temp_dir().join(format!("crabber-trace-{}", crabber::SessionId::new()));
    std::fs::create_dir(&dir).unwrap();
    let path = dir.join("queue.json");
    durable::enqueue(&path, &durable::Envelope::demo());
    match mode.as_str() {
        "memory" => {
            journey::journey().await;
            durable::memory(&path).await;
        }
        "postgres" => {
            #[cfg(feature = "postgres")]
            {
                let url = std::env::var("CRABBER_TEST_POSTGRES_URL")
                    .expect("CRABBER_TEST_POSTGRES_URL required");
                crabber::session::PostgresStore::migrate(&url)
                    .await
                    .unwrap();
                // API acceptance above did not create a Crabber session/run. Each worker
                // below is a fresh executable and only the retained key grants admission.
                for worker_mode in ["pause", "resume", "duplicate"] {
                    let mut child = tokio::process::Command::new(std::env::current_exe().unwrap())
                        .kill_on_drop(true)
                        .env("CRABBER_TRACE_WORKER", worker_mode)
                        .env("CRABBER_TRACE_QUEUE", &path)
                        .spawn()
                        .unwrap();
                    let status =
                        tokio::time::timeout(std::time::Duration::from_secs(30), child.wait())
                            .await
                            .expect("worker timeout")
                            .unwrap();
                    assert!(status.success());
                }
            }
            #[cfg(not(feature = "postgres"))]
            panic!("postgres feature required");
        }
        _ => panic!("expected memory or postgres"),
    }
    std::fs::remove_dir_all(dir).unwrap();
}
