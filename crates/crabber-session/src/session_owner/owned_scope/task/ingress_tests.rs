//! Local main closure, using real native Driver grants/activation permits.
use super::driver::DriverMainClosed;
use super::driver_tests::{grant, permit, store};
use super::*;
use crate::{Store, session_owner::SessionOwnerState};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};
use std::time::Duration;
use tokio::sync::Notify;

fn assert_closed(context: &DriverMainContext) {
    assert!(matches!(context.try_enter_main(), Err(DriverMainClosed)));
}

#[tokio::test]
async fn returned_context_is_closed_before_native_join_ack() {
    let store = store();
    let grant = grant(&store).await;
    let mut main =
        OwnedTaskRegistration::register_driver(&grant, |context| async { context }).unwrap();
    let actual = permit(&store, &grant, &main).await;
    main.dispatch(actual).unwrap();
    let context = main.take_value().await.unwrap().unwrap();
    assert_closed(&context);
    // No native Join has even been requested. Active is not local ingress proof.
    let owner = store
        .get_session_owner(context.session_id(), &context.activation().target().id)
        .await
        .unwrap();
    assert_eq!(owner.state, SessionOwnerState::Active);
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
}

#[tokio::test]
async fn held_local_entry_and_cancelled_wait_do_not_extend_main_lifetime() {
    let store = store();
    let grant = grant(&store).await;
    let release = Arc::new(Notify::new());
    let unblock = release.clone();
    let (send, receive) = oneshot::channel();
    let mut main = OwnedTaskRegistration::register_driver(&grant, move |context| async move {
        let context = Arc::new(context);
        send.send(context.clone())
            .unwrap_or_else(|_| panic!("context observer retained"));
        unblock.notified().await;
    })
    .unwrap();
    let actual = permit(&store, &grant, &main).await;
    main.dispatch(actual).unwrap();
    let context = receive.await.unwrap();
    let held = context.try_enter_main().unwrap();
    assert!(
        tokio::time::timeout(Duration::from_millis(20), main.join())
            .await
            .is_err()
    );
    assert!(
        context.try_enter_main().is_ok(),
        "cancelled observation did not cancel actual main"
    );
    release.notify_one();
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
    assert_closed(&context);
    // This is only an association. It neither delays main closure nor proves I/O joined.
    let _ = std::hint::black_box(held);
}

#[tokio::test]
async fn constructor_and_future_panic_close_escaped_context() {
    let store = store();
    for constructor_panics in [false, true] {
        let grant = grant(&store).await;
        let (send, receive) = oneshot::channel();
        let mut main = OwnedTaskRegistration::<()>::register_driver(&grant, move |context| {
            send.send(context)
                .unwrap_or_else(|_| panic!("context observer retained"));
            assert!(
                !constructor_panics,
                "constructor panic after context escape"
            );
            async { panic!("future panic after context escape") }
        })
        .unwrap();
        let actual = permit(&store, &grant, &main).await;
        main.dispatch(actual).unwrap();
        let context = receive.await.unwrap();
        assert_eq!(
            main.join().await.unwrap().disposition(),
            OwnedMainDisposition::Panicked
        );
        assert_closed(&context);
    }
}

#[tokio::test]
async fn actual_main_abort_closes_escaped_context() {
    let store = store();
    let grant = grant(&store).await;
    let (send, receive) = oneshot::channel();
    let mut main =
        OwnedTaskRegistration::<()>::register_driver(&grant, move |context| async move {
            send.send(context)
                .unwrap_or_else(|_| panic!("context observer retained"));
            std::future::pending::<()>().await;
        })
        .unwrap();
    let actual = permit(&store, &grant, &main).await;
    main.dispatch(actual).unwrap();
    let context = receive.await.unwrap();
    assert!(context.try_enter_main().is_ok());
    main.task.as_ref().unwrap().abort();
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Aborted
    );
    assert_closed(&context);
}

#[tokio::test]
async fn unpolled_actual_future_disposal_still_closes_ingress() {
    let store = store();
    let grant = grant(&store).await;
    let calls = Arc::new(AtomicUsize::new(0));
    let counted = calls.clone();
    let mut main = OwnedTaskRegistration::register_driver(&grant, move |_| {
        counted.fetch_add(1, Ordering::SeqCst);
        async {}
    })
    .unwrap();
    let RegistrationKind::Driver(ingress) = &main.kind else {
        panic!("Driver registration")
    };
    let ingress = ingress.clone();
    // No await/poll since spawn, and no activation or context delivery occurred.
    main.task.as_ref().unwrap().abort();
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Aborted
    );
    assert!(ingress.is_closed());
    assert_eq!(calls.load(Ordering::SeqCst), 0);
}

#[tokio::test]
async fn concurrent_entries_observe_actual_task_closure() {
    let store = store();
    let grant = grant(&store).await;
    let release = Arc::new(Notify::new());
    let unblock = release.clone();
    let (send, receive) = oneshot::channel();
    let mut main = OwnedTaskRegistration::register_driver(&grant, move |context| async move {
        send.send(Arc::new(context))
            .unwrap_or_else(|_| panic!("context observer retained"));
        unblock.notified().await;
    })
    .unwrap();
    let actual = permit(&store, &grant, &main).await;
    main.dispatch(actual).unwrap();
    let context = receive.await.unwrap();
    let (ready, ready_rx) = std::sync::mpsc::channel();
    let (closed, closed_rx) = std::sync::mpsc::channel();
    let mut threads = Vec::new();
    for _ in 0..8 {
        let context = context.clone();
        let ready = ready.clone();
        let closed = closed.clone();
        threads.push(std::thread::spawn(move || {
            assert!(context.try_enter_main().is_ok());
            ready.send(()).unwrap();
            while context.try_enter_main().is_ok() {
                std::thread::yield_now();
            }
            assert_closed(&context);
            closed.send(()).unwrap();
        }));
    }
    for _ in 0..8 {
        ready_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    release.notify_one();
    assert_eq!(
        main.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
    assert_closed(&context);
    for _ in 0..8 {
        closed_rx.recv_timeout(Duration::from_secs(2)).unwrap();
    }
    for thread in threads {
        thread.join().unwrap();
    }
}
