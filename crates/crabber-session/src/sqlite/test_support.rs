//! SQLite-only fixtures: independent connections, bounded children and observable locks.
use super::*;
use std::{
    path::PathBuf,
    process::{Child, Command, Stdio},
    sync::atomic::Ordering,
};

pub(super) static BARRIER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

pub(super) struct TestOptions {
    pub busy_timeout: Duration,
    pub barrier: Option<PathBuf>,
}
impl Default for TestOptions {
    fn default() -> Self {
        Self {
            busy_timeout: BUSY_TIMEOUT,
            barrier: None,
        }
    }
}

pub(super) async fn connect_with_test_options(path: &Path, test: TestOptions) -> SqliteStore {
    let mut configured = options(path).busy_timeout(test.busy_timeout);
    if let Some(marker) = test.barrier {
        configured = configured.collation("crabber_test_barrier", move |_, _| {
            std::fs::write(&marker, b"reached").expect("publish barrier marker");
            // Only the child SQLite worker blocks; killing the child releases its lock.
            loop {
                std::thread::park();
            }
        });
    }
    connect_with(path, configured).await.unwrap()
}

pub(super) async fn raw(path: &Path) -> SqliteConnection {
    SqliteConnection::connect_with(&options(path).busy_timeout(Duration::ZERO))
        .await
        .unwrap()
}

pub(super) async fn wait_writers(store: &SqliteStore, count: usize) {
    tokio::time::timeout(Duration::from_secs(5), async {
        loop {
            if store.write_waiters.load(Ordering::SeqCst) == count
                && store.writer.size() == 1
                && store.writer.num_idle() == 0
            {
                return;
            }
            tokio::task::yield_now().await;
        }
    })
    .await
    .expect("expected queued writers with the first holding the writer connection");
}

pub(super) struct ChildGuard(pub Child);
impl Drop for ChildGuard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
impl ChildGuard {
    pub async fn wait(&mut self) {
        let status = tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    return status;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("fresh-process assertion timed out");
        assert!(status.success(), "fresh-process assertion failed: {status}");
    }
    pub fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}

pub(super) fn child(path: &Path, helper: &str, env: &[(&str, String)]) -> ChildGuard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args(["--exact", helper, "--nocapture"])
        .env("CRABBER_SQLITE_CHILD_PATH", path)
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    for (key, value) in env {
        command.env(key, value);
    }
    ChildGuard(command.spawn().unwrap())
}

pub(super) async fn marker(path: &Path, child: &mut ChildGuard) {
    tokio::time::timeout(Duration::from_secs(10), async {
        while !path.exists() {
            assert!(
                child.0.try_wait().unwrap().is_none(),
                "barrier child exited before marker"
            );
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("child must reach the collation barrier");
}

pub(super) async fn ready_line(child: &mut ChildGuard) {
    use std::io::{BufRead, BufReader};
    let stdout = child.0.stdout.take().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap() == "CRABBER_SQLITE_BARRIER_READY" {
                    return;
                }
            }
            panic!("child exited before announcing its store call");
        }),
    )
    .await
    .expect("child readiness timed out")
    .unwrap();
}

pub(super) async fn assert_busy(connection: &mut SqliteConnection) {
    let error = connection
        .begin_with("BEGIN IMMEDIATE")
        .await
        .expect_err("child must hold write lock");
    assert_eq!(
        primary_code(&error),
        Some(5),
        "expected SQLITE_BUSY, got {error}"
    );
}

pub(super) async fn close(store: &SqliteStore) {
    store.writer.close().await;
    store.readers.close().await;
}
