use super::Fixture;
use std::{path::Path, process::Command, time::Duration};

pub(crate) fn fixture() -> Option<Fixture> {
    std::env::var("CRABBER_APPROVAL_CHILD")
        .ok()
        .map(|v| serde_json::from_str(&v).unwrap())
}
pub(crate) struct Child(std::process::Child);
impl Drop for Child {
    fn drop(&mut self) {
        if self.0.try_wait().ok().flatten().is_none() {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
impl Child {
    pub(crate) async fn wait(mut self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "approval child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("approval child timed out");
    }
    pub(crate) fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    pub(crate) fn running(&mut self) -> bool {
        self.0.try_wait().unwrap().is_none()
    }
}
pub(crate) fn spawn(
    helper: &str,
    fixture: &Fixture,
    path: Option<&Path>,
    extra: &[(&str, String)],
) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_APPROVAL_CHILD",
        serde_json::to_string(fixture).unwrap(),
    );
    if let Some(path) = path {
        command.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    for (key, value) in extra {
        command.env(key, value);
    }
    Child(command.spawn().unwrap())
}
