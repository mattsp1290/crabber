//! Physical commit-boundary qualification; only outbox identity crosses processes.
use super::fixture;
use crate::{BoundAdmitRequest, KeyedAdmitOutcome, Store};
use crabber_core::SessionId;
use serde::{Deserialize, Serialize};
use std::{path::Path, process::Command, time::Duration};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub session: SessionId,
    pub keyed: bool,
    pub typed: bool,
}
impl Fixture {
    pub(crate) fn new(keyed: bool, typed: bool) -> Self {
        Self {
            session: SessionId::new(),
            keyed,
            typed,
        }
    }
}
pub(crate) fn settings() -> Option<Fixture> {
    std::env::var("CRABBER_BINDING_CHILD")
        .ok()
        .map(|v| serde_json::from_str(&v).unwrap())
}
fn original(target: &Fixture) -> crate::BoundKeyedAdmitRequest {
    let now = time::OffsetDateTime::now_utc();
    if target.typed {
        crate::typed_admission_contract::fixture(&target.session, now, "process-backend")
    } else {
        fixture(&target.session, now, "process-backend", 'b')
    }
}
pub(crate) async fn write(store: &impl Store, target: &Fixture, lost_ack: bool) {
    let mut original = original(target);
    if target.keyed {
        let result = store.admit_bound_keyed_run(original).await;
        if lost_ack {
            assert!(result.is_err());
        } else {
            assert!(matches!(result.unwrap(), KeyedAdmitOutcome::Started { .. }));
        }
    } else {
        original.keyed.request.session_id = None;
        let result = store
            .admit_bound_run(BoundAdmitRequest {
                request: original.keyed.request,
                binding: original.binding,
            })
            .await;
        if lost_ack {
            assert!(result.is_err());
        } else {
            result.unwrap();
        }
    }
}
pub(crate) async fn verify(store: &impl Store, target: &Fixture) {
    let original = original(target);
    let runs = store
        .list_unfinished_runs()
        .await
        .unwrap()
        .into_iter()
        .filter(|r| r.session_id == target.session)
        .collect::<Vec<_>>();
    assert_eq!(runs.len(), 1);
    let binding = store
        .execution_binding(&target.session, &runs[0].id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(binding.capsule, original.binding);
    let revision = store.session_revision(&target.session).await.unwrap();
    let receipt = store
        .lookup_admission(&target.session, &original.keyed.options.key)
        .await
        .unwrap();
    if target.keyed {
        let receipt = receipt.unwrap();
        assert!(
            matches!(store.admit_bound_keyed_run(original).await.unwrap(), KeyedAdmitOutcome::Replayed(actual) if actual == receipt)
        );
    } else {
        assert_eq!(receipt, None);
    }
    assert_eq!(
        store.session_revision(&target.session).await.unwrap(),
        revision
    );
}
pub(crate) async fn absent(store: &impl Store, target: &Fixture) {
    assert_eq!(store.get_session(&target.session).await.unwrap(), None);
    let key = crabber_core::AdmissionKey::new("bound-admission");
    // Admission keys are core vocabulary, not generated run identities.
    assert_eq!(
        store
            .lookup_admission(&target.session, &key.unwrap())
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .list_unfinished_runs()
            .await
            .unwrap()
            .into_iter()
            .filter(|r| r.session_id == target.session)
            .count(),
        0
    );
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
                    assert!(status.success(), "binding child failed: {status}");
                    return;
                }
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("binding child timed out");
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
    target: &Fixture,
    path: Option<&Path>,
    write: bool,
    marker: Option<&Path>,
) -> Child {
    let mut command = Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_BINDING_CHILD",
        serde_json::to_string(target).unwrap(),
    );
    if let Some(path) = path {
        command.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    if write {
        command.env("CRABBER_BINDING_WRITE", "1");
    }
    if let Some(marker) = marker {
        command.env("CRABBER_BINDING_BARRIER", marker);
    }
    Child(command.spawn().unwrap())
}
