use super::{fixture::*, store::TestStore};
use crabber::{
    RuntimeError,
    core::SessionId,
    session::{Store, StoreError},
};
use std::sync::{Arc, atomic::Ordering};
pub(super) async fn run(native: Arc<dyn Store>) {
    for after_preparation in [false, true] {
        let session = create(native.as_ref(), defaults(Some("before"), Some("before"))).await;
        let wrapped = Arc::new(TestStore::new(native.clone()));
        let probe = Probe::new(1);
        let host = Arc::new(agent(wrapped.clone(), &probe));
        if after_preparation {
            wrapped.hold_admission.store(true, Ordering::SeqCst);
        } else {
            wrapped.hold_summary.store(true, Ordering::SeqCst);
        }
        let key = options();
        let task = {
            let host = host.clone();
            let session = session.clone();
            let key = key.clone();
            tokio::spawn(
                async move { host.prompt_session_keyed(session, "raced", key, None).await },
            )
        };
        wrapped.entered.notified().await;
        update(
            native.as_ref(),
            &session,
            defaults(Some("after"), Some("after")),
        )
        .await;
        wrapped.release.notify_one();
        assert!(matches!(
            task.await.unwrap(),
            Err(RuntimeError::Store(StoreError::AdmissionRevisionConflict))
        ));
        assert!(
            native
                .lookup_admission(&session, &key.key)
                .await
                .unwrap()
                .is_none()
        );
        assert_eq!(probe.fake.requests().len(), 0);
        assert_eq!(probe.resolutions.load(Ordering::SeqCst), 0);
        if !after_preparation {
            assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
        }
    }
    missing_create_race(native.clone()).await;
    super::recovery::run(native.clone(), false).await;
    super::recovery::run(native, true).await;
}
async fn missing_create_race(native: Arc<dyn Store>) {
    let session = SessionId::new();
    let wrapped = Arc::new(TestStore::new(native.clone()));
    let probe = Probe::new(1);
    let host = Arc::new(agent(wrapped.clone(), &probe));
    wrapped.hold_summary.store(true, Ordering::SeqCst);
    let task = {
        let host = host.clone();
        let session = session.clone();
        tokio::spawn(async move {
            host.prompt_session_keyed(session, "new session race", options(), None)
                .await
        })
    };
    wrapped.entered.notified().await;
    native
        .manage_session(crabber::ManageSession {
            id: crabber::core::ControlId::new(),
            session_id: session,
            expected_revision: crabber::core::SessionRevision::INITIAL,
            change: crabber::SessionChange::Create {
                workspace_id: "defaults-workspace".into(),
                directory: "/defaults".into(),
                title: "concurrent create".into(),
                defaults: defaults(Some("winner"), Some("winner")),
            },
        })
        .await
        .unwrap();
    wrapped.release.notify_one();
    assert!(matches!(
        task.await.unwrap(),
        Err(RuntimeError::Store(StoreError::AdmissionRevisionConflict))
    ));
    assert_eq!(probe.fake.requests().len(), 0);
    assert_eq!(probe.factories.load(Ordering::SeqCst), 0);
}
