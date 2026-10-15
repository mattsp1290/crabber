//! Physical writer death and cold receipt recovery for parent controls.
use super::*;
use crate::attempt_control::Control;
use serde::{Deserialize, Serialize};
#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub control: Control,
    pub before: serde_json::Value,
}
pub(crate) async fn evidence(store: &impl Store, control: &Control) -> serde_json::Value {
    let (session, run, _, _, _) = control.scope();
    serde_json::json!({"run":store.get_run(run).await.unwrap(),"attempt":store.latest_execution_attempt(run).await.unwrap(),"revision":store.session_revision(session).await.unwrap(),"messages":store.list_all_messages(session).await.unwrap(),"events":store.list_events(session,None,100).await.unwrap(),"calls":read_calls(store,session).await,"approval":store.approval_snapshot(session,run).await.unwrap()})
}
pub(crate) async fn pending(store: &impl Store, retire: bool) -> Fixture {
    let (admitted, _, _, planned) = crate::phase_contract::fixture(store).await;
    let request = uncertainty(
        &admitted,
        planned.command.target().attempt.clone(),
        store.session_revision(&admitted.session.id).await.unwrap(),
    );
    let control = if retire {
        let marked = fresh(store.mark_attempt_uncertain(request.clone()).await.unwrap());
        Control::Retire(retirement(&request, marked.revision))
    } else {
        Control::Uncertain(request)
    };
    let before = evidence(store, &control).await;
    Fixture { control, before }
}
pub(crate) async fn write(
    store: &impl Store,
    f: &Fixture,
) -> Result<AttemptControlOutcome, AttemptError> {
    match &f.control {
        Control::Uncertain(r) => store.mark_attempt_uncertain(r.clone()).await,
        Control::Retire(r) => store.retire_attempt(r.clone()).await,
    }
}
pub(crate) async fn unchanged(store: &impl Store, f: &Fixture) {
    assert_eq!(evidence(store, &f.control).await, f.before);
    let (session, _, id, _, _) = f.control.scope();
    assert_eq!(
        store.lookup_attempt_mutation(session, id).await.unwrap(),
        None
    );
}
pub(crate) async fn verify(store: &impl Store, f: Fixture) {
    let (session, _, id, _, _) = f.control.scope();
    let saved = store
        .lookup_attempt_mutation(session, id)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(saved.semantic_digest, f.control.fingerprint().unwrap());
    let before = evidence(store, &f.control).await;
    assert_eq!(
        write(store, &f).await.unwrap(),
        AttemptControlOutcome::Replayed(saved)
    );
    assert_eq!(evidence(store, &f.control).await, before);
}
pub(crate) fn fixture() -> Option<Fixture> {
    std::env::var("CRABBER_PARENT_CONTROL_CHILD")
        .ok()
        .map(|v| serde_json::from_str(&v).unwrap())
}
pub(crate) struct Guard(std::process::Child);
impl Guard {
    #[cfg(feature = "postgres")]
    pub(crate) fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
    pub(crate) async fn wait(&mut self) {
        tokio::time::timeout(std::time::Duration::from_secs(30), async {
            loop {
                if let Some(status) = self.0.try_wait().unwrap() {
                    assert!(status.success(), "parent-control child failed: {status}");
                    break;
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .await
        .expect("parent-control child timed out");
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
    f: &Fixture,
    path: Option<&std::path::Path>,
    extra: &[(&str, String)],
) -> Guard {
    let mut cmd = std::process::Command::new(std::env::current_exe().unwrap());
    cmd.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_PARENT_CONTROL_CHILD",
        serde_json::to_string(f).unwrap(),
    );
    if let Some(path) = path {
        cmd.env("CRABBER_SQLITE_CHILD_PATH", path);
    }
    for (k, v) in extra {
        cmd.env(k, v);
    }
    Guard(cmd.spawn().unwrap())
}
