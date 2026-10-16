//! Real initial Driver grants and activation permits, never synthetic permits.
use super::*;
use crate::session_owner::{
    driver::{ExecutionDriverGrant, ExecutionDriverOutcome, RegisterExecutionDriver},
    owned_scope::{OwnedScopeActivationOutcome, OwnedScopeParentAuthority},
};
use crate::{MemoryStore, Store};
use crabber_core::{ManualClock, SessionContext};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use tokio::sync::Notify;

fn store() -> MemoryStore {
    MemoryStore::with_clock(Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH)))
}
async fn grant(store: &MemoryStore) -> Box<ExecutionDriverGrant> {
    let admitted =
        crate::session_owner::preparation_contract::resources::initial_driver(store).await;
    let request = RegisterExecutionDriver {
        context: SessionContext::new(
            admitted.session.id,
            admitted.session.workspace_id,
            admitted.session.directory,
        ),
        fence: admitted.fence,
    };
    let ExecutionDriverOutcome::Applied { grant, .. } =
        store.register_execution_driver(request).await.unwrap()
    else {
        panic!("fresh genuine Driver grant")
    };
    grant
}
async fn activate<T: Send + 'static>(
    store: &MemoryStore,
    grant: &ExecutionDriverGrant,
    main: &OwnedTaskRegistration<T>,
) -> OwnedScopeActivationOutcome {
    store
        .activate_owned_scope(
            main.activation().clone(),
            grant.authority().clone(),
            Some(OwnedScopeParentAuthority::ExecutionDriver(Box::new(
                grant.clone(),
            ))),
        )
        .await
        .unwrap()
}
async fn permit<T: Send + 'static>(
    store: &MemoryStore,
    grant: &ExecutionDriverGrant,
    main: &OwnedTaskRegistration<T>,
) -> OwnedDispatchPermit {
    let OwnedScopeActivationOutcome::Applied { permit, .. } = activate(store, grant, main).await
    else {
        panic!("actual fresh native activation")
    };
    *permit
}

#[tokio::test]
async fn driver_context_is_delivered_only_inside_actual_fresh_task_gate() {
    let store = store();
    let grant = grant(&store).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let release = Arc::new(Notify::new());
    let unblock = release.clone();
    let original = grant.original_request().clone();
    let (entered, received) = oneshot::channel();
    let mut main = OwnedTaskRegistration::register_driver(&grant, move |context| {
        counted.fetch_add(1, Ordering::SeqCst);
        async move {
            assert_eq!(context.session_id(), original.context.session_id());
            entered
                .send(context.activation().clone())
                .unwrap_or_else(|_| panic!("original context observer retained"));
            unblock.notified().await;
        }
    })
    .unwrap();
    tokio::task::yield_now().await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let permit = permit(&store, &grant, &main).await;
    tokio::task::yield_now().await;
    assert_eq!(
        calls.load(Ordering::SeqCst),
        0,
        "native ACK alone never invokes factory"
    );
    main.dispatch(permit).unwrap();
    assert!(received.await.unwrap() == *main.activation());
    assert!(
        tokio::time::timeout(std::time::Duration::from_millis(20), main.join())
            .await
            .is_err(),
        "actual main is still blocked"
    );
    release.notify_one();
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
}

#[tokio::test]
async fn wrong_genuine_permit_retains_original_driver_task_gate() {
    let store = store();
    let first = grant(&store).await;
    let other = grant(&store).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let mut first_main = OwnedTaskRegistration::register_driver(&first, move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        async {}
    })
    .unwrap();
    let other_calls = Arc::new(AtomicUsize::new(0));
    let other_counted = other_calls.clone();
    let mut other_main = OwnedTaskRegistration::register_driver(&other, move |_| {
        other_counted.fetch_add(1, Ordering::SeqCst);
        async {}
    })
    .unwrap();
    let wrong = permit(&store, &other, &other_main).await;
    assert!(matches!(
        first_main.dispatch(wrong),
        Err(Error::SemanticConflict)
    ));
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    let actual = permit(&store, &first, &first_main).await;
    first_main.dispatch(actual).unwrap();
    assert_eq!(
        first_main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(
        other_main.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert_eq!(other_calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn missing_or_replayed_activation_never_creates_driver_context_factory() {
    let store = store();
    for ack_lost in [false, true] {
        let grant = grant(&store).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let mut main = OwnedTaskRegistration::register_driver(&grant, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            async {}
        })
        .unwrap();
        if ack_lost {
            drop(permit(&store, &grant, &main).await);
            assert!(matches!(
                activate(&store, &grant, &main).await,
                OwnedScopeActivationOutcome::Replayed(_)
            ));
        }
        assert_eq!(
            main.join().await.unwrap().disposition(),
            OwnedMainDisposition::NoDispatch
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
}

#[tokio::test]
async fn driver_context_factory_panic_is_observed_on_original_main() {
    let store = store();
    for constructor_panics in [false, true] {
        let grant = grant(&store).await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counted = calls.clone();
        let mut main = OwnedTaskRegistration::<()>::register_driver(&grant, move |_| {
            counted.fetch_add(1, Ordering::SeqCst);
            assert!(
                !constructor_panics,
                "actual gated factory constructor panicked"
            );
            async { panic!("actual freshly gated Driver future panicked") }
        })
        .unwrap();
        let actual = permit(&store, &grant, &main).await;
        main.dispatch(actual).unwrap();
        assert_eq!(
            main.join().await.unwrap().disposition(),
            OwnedMainDisposition::Panicked
        );
        assert_eq!(
            main.join().await.unwrap().disposition(),
            OwnedMainDisposition::Panicked
        );
        assert_eq!(calls.load(Ordering::SeqCst), 1);
    }
}

#[tokio::test]
async fn mixed_grant_metadata_cannot_relabel_actual_native_context() {
    let store = store();
    let real = grant(&store).await;
    let fake_identity = crabber_core::InputFingerprint::new("f".repeat(64)).unwrap();
    assert_ne!(fake_identity.as_str(), real.source_identity().as_str());
    let fake = ExecutionDriverGrant::from_fresh_begin(
        real.receipt().clone(),
        real.authority().clone(),
        real.original_request().clone(),
        fake_identity,
    )
    .unwrap();
    // Registration metadata is deliberately different from the genuine native
    // parent. Only the actual permit-bound activation may reach the factory.
    let mut main =
        OwnedTaskRegistration::register_driver(&fake, |context| async move { context }).unwrap();
    let actual = permit(&store, &real, &main).await;
    main.dispatch(actual).unwrap();
    let original_activation = main.activation().clone();
    let context = main.take_value().await.unwrap().unwrap();
    assert!(context.activation() == &original_activation);
    assert_eq!(
        context.session_id(),
        real.original_request().context.session_id()
    );
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
}
