//! Fresh-process reconciliation and crash-test support.
use super::*;
use crabber_core::RunFence;
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
pub(crate) struct Fixture {
    pub(crate) fence: RunFence,
    pub(crate) session: SessionId,
    pub(crate) mutation: PublicStateMutation,
}
pub(crate) async fn pending(store: &impl Store) -> Fixture {
    let admitted = admit(store, OffsetDateTime::now_utc()).await;
    let revision = store
        .public_state(&admitted.session.id)
        .await
        .unwrap()
        .revision;
    Fixture {
        fence: admitted.fence,
        session: admitted.session.id,
        mutation: PublicStateMutation {
            id: ControlId::new(),
            expected_revision: revision,
            contents: contents(),
        },
    }
}
pub(crate) async fn prepare(store: &impl Store, inject: impl Fn(FaultPoint)) -> Fixture {
    let fixture = pending(store).await;
    inject(FaultPoint::AfterCommit);
    assert_eq!(
        store
            .execution(fixture.fence.clone())
            .await
            .unwrap()
            .mutate_public_state(fixture.mutation.clone())
            .await
            .unwrap_err(),
        Error::UnknownStoreOutcome
    );
    inject(FaultPoint::Off);
    fixture
}
pub(crate) async fn uncommitted(store: &impl Store, fixture: &Fixture) {
    let snapshot = store.public_state(&fixture.session).await.unwrap();
    assert_eq!(snapshot.contents, PublicStateContents::default());
    assert_eq!(snapshot.revision, fixture.mutation.expected_revision);
    assert_eq!(
        store
            .lookup_public_state_mutation(&fixture.session, &fixture.mutation.id)
            .await
            .unwrap(),
        None
    );
    assert_eq!(
        store
            .list_events(&fixture.session, None, 100)
            .await
            .unwrap(),
        [] as [EventRecord; 0]
    );
}
pub(crate) async fn write(store: &impl Store, fixture: &Fixture) {
    store
        .execution(fixture.fence.clone())
        .await
        .unwrap()
        .mutate_public_state(fixture.mutation.clone())
        .await
        .unwrap();
}
pub(crate) async fn verify(store: &impl Store, fixture: Fixture) {
    let receipt = store
        .lookup_public_state_mutation(&fixture.session, &fixture.mutation.id)
        .await
        .unwrap()
        .expect("committed operation must be discoverable without its response");
    assert_eq!(
        receipt.semantic_digest,
        fixture
            .mutation
            .fingerprint(&fixture.session, &fixture.fence.run_id)
            .unwrap()
    );
    let before = store.public_state(&fixture.session).await.unwrap();
    assert_eq!(before.contents, fixture.mutation.contents);
    assert_eq!(before.revision, receipt.revision);
    assert_eq!(
        store
            .execution(fixture.fence)
            .await
            .unwrap()
            .mutate_public_state(fixture.mutation)
            .await
            .unwrap(),
        receipt
    );
    assert_eq!(store.public_state(&fixture.session).await.unwrap(), before);
    let events = store
        .list_events(&fixture.session, None, 100)
        .await
        .unwrap();
    assert_eq!(events.len(), 1);
    assert_eq!(events[0].cursor, Some(receipt.event_cursor));
}
pub(crate) struct Child(std::process::Child);
impl Child {
    #[cfg(feature = "postgres")]
    pub(crate) fn kill(&mut self) {
        self.0.kill().unwrap();
        self.0.wait().unwrap();
    }
}
impl Drop for Child {
    fn drop(&mut self) {
        let _ = self.0.kill();
        let _ = self.0.wait();
    }
}
pub(crate) fn spawn(
    helper: &str,
    fixture: &Fixture,
    path: Option<&std::path::Path>,
    extra: &[(&str, String)],
) -> Child {
    let mut command = std::process::Command::new(std::env::current_exe().unwrap());
    command.args(["--exact", helper, "--nocapture"]).env(
        "CRABBER_PUBLIC_STATE_CHILD",
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

pub(crate) async fn run(helper: &str, fixture: &Fixture, path: Option<&std::path::Path>) {
    let mut child = spawn(helper, fixture, path, &[]);
    tokio::time::timeout(Duration::from_secs(30), async {
        loop {
            if let Some(status) = child.0.try_wait().unwrap() {
                assert!(
                    status.success(),
                    "fresh-process reconciliation failed: {status}"
                );
                break;
            }
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await
    .expect("fresh-process reconciliation timed out");
}
pub(crate) fn fixture() -> Option<Fixture> {
    std::env::var("CRABBER_PUBLIC_STATE_CHILD")
        .ok()
        .map(|value| serde_json::from_str(&value).unwrap())
}
