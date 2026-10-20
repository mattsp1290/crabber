mod durable;
mod export;
mod journey;
mod span_capture;
use tracing_subscriber::prelude::*;

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
    let spans = span_capture::SpanCapture::default();
    tracing_subscriber::registry().with(spans.clone()).init();
    if let Ok(mode) = std::env::var("CRABBER_TRACE_WORKER") {
        #[cfg(feature = "sqlite")]
        if let Some(database) = std::env::var_os("CRABBER_SQLITE_CHILD_PATH") {
            let store = std::sync::Arc::new(
                crabber::session::SqliteStore::connect(std::path::PathBuf::from(database))
                    .await
                    .unwrap(),
            );
            let path = std::path::PathBuf::from(std::env::var_os("CRABBER_TRACE_QUEUE").unwrap());
            durable::worker(store, &path, &mode, "sqlite").await;
            return;
        }
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
            spans.assert_trace(journey::host_context().trace_id());
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
                let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
                command.env_remove("CRABBER_SQLITE_CHILD_PATH");
                workers(&path, command).await;
            }
            #[cfg(not(feature = "postgres"))]
            panic!("postgres feature required");
        }
        "sqlite" => {
            #[cfg(feature = "sqlite")]
            {
                let database = dir.join("session.sqlite");
                crabber::session::SqliteStore::migrate(&database)
                    .await
                    .unwrap();
                let mut command = tokio::process::Command::new(std::env::current_exe().unwrap());
                command.env("CRABBER_SQLITE_CHILD_PATH", database);
                workers(&path, command).await;
            }
            #[cfg(not(feature = "sqlite"))]
            panic!("sqlite feature required");
        }
        _ => panic!("expected memory, postgres or sqlite"),
    }
    std::fs::remove_dir_all(dir).unwrap();
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn workers(path: &std::path::Path, mut command: tokio::process::Command) {
    // Queue acceptance creates no Crabber session/run. Each fresh worker uses
    // the retained key and connects without migrating.
    command.kill_on_drop(true).env("CRABBER_TRACE_QUEUE", path);
    for mode in ["pause", "resume", "duplicate"] {
        let mut child = command.env("CRABBER_TRACE_WORKER", mode).spawn().unwrap();
        let status = tokio::time::timeout(std::time::Duration::from_secs(30), child.wait())
            .await
            .expect("worker timeout")
            .unwrap();
        assert!(status.success());
    }
}
