//! Test-only process death at the actual atomic erasure boundary.
use crate::{DeleteSession, DeletionOutcome, Store};
use crabber_test_support::ChildDatabase;
use std::{
    path::Path,
    process::{Child, Command},
    time::Duration,
};

pub(crate) fn request() -> Option<DeleteSession> {
    std::env::var("CRABBER_DELETE_CHILD")
        .ok()
        .map(|data| serde_json::from_str(&data).unwrap())
}
pub(crate) fn pause(point: &str) {
    if std::env::var("CRABBER_DELETE_KILL_POINT").ok().as_deref() == Some(point) {
        std::fs::write(std::env::var("CRABBER_DELETE_MARKER").unwrap(), b"ready").unwrap();
        loop {
            std::thread::park();
        }
    }
}
pub(crate) async fn execute(store: &impl Store, request: DeleteSession) {
    if std::env::var_os("CRABBER_DELETE_REPLAY").is_some() {
        let saved = store
            .lookup_session_deletion(&request.session_id, &request.id)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(
            store.delete_session(request.clone()).await.unwrap(),
            DeletionOutcome::Replayed(saved)
        );
        assert!(
            store
                .get_session(&request.session_id)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(
            store.list_all_messages(&request.session_id).await.unwrap(),
            []
        );
    } else {
        assert!(matches!(
            store.delete_session(request).await.unwrap(),
            DeletionOutcome::Applied(_)
        ));
    }
}
pub(crate) async fn setup(store: &impl Store) -> DeleteSession {
    let fixture = super::contract::cancelled_queue(store).await;
    DeleteSession {
        id: crabber_core::ControlId::new(),
        expected_revision: store.session_revision(&fixture.session).await.unwrap(),
        session_id: fixture.session,
    }
}
pub(crate) struct Guard(Child);
impl Guard {
    pub fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    pub async fn wait(&mut self) {
        tokio::time::timeout(Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "deletion child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("deletion child timeout");
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
    request: &DeleteSession,
    database: ChildDatabase<'_>,
    kill: Option<(&str, &Path)>,
) -> Guard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_DELETE_CHILD",
        serde_json::to_string(request).unwrap(),
    );
    database.configure(&mut command, "CRABBER_SQLITE_CHILD_PATH");
    if let Some((point, marker)) = kill {
        command
            .env("CRABBER_DELETE_KILL_POINT", point)
            .env("CRABBER_DELETE_MARKER", marker);
    } else {
        command.env("CRABBER_DELETE_REPLAY", "1");
    }
    Guard(command.spawn().unwrap())
}
pub(crate) async fn marker(path: &Path) {
    tokio::time::timeout(Duration::from_secs(30), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("deletion child boundary");
}
