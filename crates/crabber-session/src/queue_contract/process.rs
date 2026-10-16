//! Cold processes prove durable claim/preparation/admission receipts are metadata only.
use super::*;
use std::{
    path::Path,
    process::{Child, Command},
    time::Duration,
};
pub(crate) fn request() -> Option<QueueMutation> {
    std::env::var("CRABBER_QUEUE_CHILD")
        .ok()
        .map(|value| serde_json::from_str(&value).unwrap())
}
pub(crate) async fn verify(store: &dyn Store, request: QueueMutation) {
    let receipt = store
        .lookup_queue(&request.session_id, &request.id)
        .await
        .unwrap()
        .expect("committed queue receipt survives process loss");
    assert_eq!(receipt.semantic_digest, request.fingerprint().unwrap());
    let before = store.session_revision(&request.session_id).await.unwrap();
    assert!(
        matches!(store.mutate_queue(request.clone()).await.unwrap(), QueueOutcome::Replayed(actual) if actual == receipt)
    );
    assert_eq!(
        store.session_revision(&request.session_id).await.unwrap(),
        before
    );
    let retained = store
        .retained_queue(&request.session_id, &receipt.summary.queue_id)
        .await
        .unwrap();
    assert_eq!(retained.summary, receipt.summary);
    if let Some(admission) = &receipt.summary.admission {
        let prepared = retained.prepared.unwrap();
        assert_eq!(
            store
                .lookup_admission(&request.session_id, &prepared.keyed.options.key)
                .await
                .unwrap(),
            Some(admission.clone())
        );
        assert_eq!(
            store
                .execution_binding(&request.session_id, &admission.run_id)
                .await
                .unwrap()
                .unwrap()
                .capsule,
            prepared.binding
        );
        assert_eq!(
            store
                .load_admission_execution(&request.session_id, &prepared.keyed.options.key)
                .await
                .unwrap()
                .unwrap()
                .state,
            crate::AdmissionExecutionState::Unstarted
        );
    }
}
pub(crate) async fn setup(store: &dyn Store, step: u8) -> QueueMutation {
    let f = fixture(store).await;
    store.mutate_queue(f.enqueue.clone()).await.unwrap();
    if step == 0 {
        return mutation(
            store,
            &f,
            QueueAction::Claim {
                owner: "cold-host".into(),
                lease: Duration::from_secs(30),
            },
        )
        .await;
    }
    let authority = claim(store, &f).await;
    let prepare = mutation(
        store,
        &f,
        QueueAction::Prepare {
            authority: authority.clone(),
            prepared: Box::new(f.prepared.clone()),
        },
    )
    .await;
    if step == 1 {
        return prepare;
    }
    store.mutate_queue(prepare).await.unwrap();
    mutation(
        store,
        &f,
        QueueAction::Admit {
            authority,
            owner: "cold-host".into(),
            lease: Duration::from_secs(30),
        },
    )
    .await
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
                    assert!(status.success(), "queue child failed: {status}");
                    break;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("queue child timed out");
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
    request: &QueueMutation,
    path: Option<&Path>,
    extras: &[(&str, String)],
) -> Guard {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_QUEUE_CHILD",
        serde_json::to_string(request).unwrap(),
    );
    if let Some(path) = path {
        command.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    for (key, value) in extras {
        command.env(key, value);
    }
    Guard(command.spawn().unwrap())
}
pub(crate) async fn marker(path: &Path) {
    tokio::time::timeout(Duration::from_secs(15), async {
        while !path.exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("queue child must reach durable boundary");
}
pub(crate) fn committed_marker() {
    if let Ok(path) = std::env::var("CRABBER_QUEUE_COMMITTED") {
        std::fs::write(path, b"committed").unwrap();
        loop {
            std::thread::park();
        }
    }
}
