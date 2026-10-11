use super::*;
use std::{
    io::{BufRead, BufReader, Read},
    process::{Child, Command, Stdio},
};

pub(super) static BARRIER_LOCK: tokio::sync::Mutex<()> = tokio::sync::Mutex::const_new(());

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
        .expect("fresh-process facade assertion timed out");
        let mut output = String::new();
        if let Some(mut stdout) = self.0.stdout.take() {
            stdout.read_to_string(&mut output).unwrap();
        }
        print!("{output}");
        assert!(
            status.success(),
            "fresh-process facade assertion failed: {status}"
        );
    }
    pub fn kill(&mut self) {
        assert!(self.0.try_wait().unwrap().is_none());
        self.0.kill().unwrap();
        assert!(!self.0.wait().unwrap().success());
    }
}
pub(super) fn child(
    path: &Path,
    request: &AbandonRequest,
    expected: Option<&serde_json::Value>,
) -> ChildGuard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command
        .args([
            "--exact",
            "sqlite::abandonment_process_helper",
            "--nocapture",
        ])
        .env("CRABBER_SQLITE_CHILD_PATH", path)
        .env(
            "CRABBER_SQLITE_FACADE_CHILD_REQUEST",
            serde_json::to_string(request).unwrap(),
        )
        .env_remove("CRABBER_SQLITE_FACADE_CHILD_SNAPSHOT")
        .stdout(Stdio::piped())
        .stderr(Stdio::inherit());
    if let Some(expected) = expected {
        command.env("CRABBER_SQLITE_FACADE_CHILD_SNAPSHOT", expected.to_string());
    }
    ChildGuard(command.spawn().unwrap())
}
pub(super) async fn ready_line(child: &mut ChildGuard) {
    let stdout = child.0.stdout.take().unwrap();
    tokio::time::timeout(
        Duration::from_secs(10),
        tokio::task::spawn_blocking(move || {
            for line in BufReader::new(stdout).lines() {
                if line.unwrap() == "CRABBER_SQLITE_FACADE_READY" {
                    return;
                }
            }
            panic!("child exited before announcing its abandonment call");
        }),
    )
    .await
    .expect("facade child readiness timed out")
    .unwrap();
}
pub(super) fn assert_busy_error(error: &sqlx::Error) {
    let code = error
        .as_database_error()
        .and_then(sqlx::error::DatabaseError::code)
        .and_then(|code| code.parse::<i32>().ok())
        .map(|code| code & 0xff);
    assert_eq!(code, Some(5), "expected SQLITE_BUSY, got {error}");
}
