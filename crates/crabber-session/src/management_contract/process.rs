use super::*;
use crabber_test_support::ChildDatabase;
use std::{
    process::{Child, Command},
    time::Duration,
};
pub(crate) fn fixture() -> Option<ManageSession> {
    std::env::var("CRABBER_MANAGEMENT_CHILD")
        .ok()
        .map(|value| serde_json::from_str(&value).unwrap())
}
pub(crate) async fn verify(store: &dyn Store, request: ManageSession) {
    let saved = store
        .lookup_session_management(&request.session_id, &request.id)
        .await
        .unwrap()
        .expect("committed receipt survives process loss");
    assert_eq!(saved.semantic_digest, request.fingerprint().unwrap());
    let before = store.session_summary(&request.session_id).await.unwrap();
    assert_eq!(saved.summary, before);
    assert_eq!(store.manage_session(request.clone()).await.unwrap(), saved);
    assert_eq!(
        store.session_summary(&request.session_id).await.unwrap(),
        before
    );
    assert_eq!(
        store.list_all_messages(&request.session_id).await.unwrap(),
        Vec::<crabber_core::Message>::new()
    );
}
pub(crate) async fn absent(store: &dyn Store, request: &ManageSession) {
    assert_eq!(
        store
            .session_summary(&request.session_id)
            .await
            .unwrap_err(),
        Error::NotFound
    );
    assert_eq!(
        store
            .lookup_session_management(&request.session_id, &request.id)
            .await
            .unwrap(),
        None
    );
}
pub(crate) struct Guard(Child);
impl Guard {
    #[cfg(feature = "postgres")]
    pub(crate) fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    pub(crate) async fn wait(&mut self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "management child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("management child timed out");
    }
}
impl Drop for Guard {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(crate) fn spawn(
    helper: &str,
    request: &ManageSession,
    path: ChildDatabase<'_>,
    extra: &[(&str, String)],
) -> Guard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_MANAGEMENT_CHILD",
        serde_json::to_string(request).unwrap(),
    );
    path.configure(&mut command, "CRABBER_SQLITE_CHILD_PATH");
    for (key, value) in extra {
        command.env(key, value);
    }
    Guard(command.spawn().unwrap())
}
