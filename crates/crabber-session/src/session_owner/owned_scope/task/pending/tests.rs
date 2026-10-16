use super::*;
use crate::{MemoryStore, Store, session_owner::owned_scope::OwnedDispatchPermit};
use crabber_core::ManualClock;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;
use tokio::sync::Notify;
#[path = "fixture.rs"]
mod fixture;
#[path = "interposed.rs"]
mod interposed;
use interposed::{Interposed, Stage};

fn store() -> (Arc<MemoryStore>, Arc<ManualClock>) {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    (Arc::new(MemoryStore::with_clock(clock.clone())), clock)
}

#[tokio::test]
async fn genuine_first_two_complete_register_activate_and_join_same_main() {
    let owner = PendingSuccessorTasks::default();
    for admission in [true, false] {
        let (store, clock) = store();
        let (context, joined) = fixture::prepared(store.as_ref(), &clock, admission).await;
        let mut task = PendingSuccessorTask::register(
            &owner,
            store.clone(),
            context,
            joined,
            |context, _completion| async move {
                assert!(context.try_enter_main().is_ok());
                context
            },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        let observed = task.join().await.unwrap();
        assert!(observed.error().is_none(), "{:?}", observed.error());
        assert_eq!(observed.disposition(), OwnedMainDisposition::Returned);
        let context = observed.value().unwrap();
        assert!(context.try_enter_main().is_err());
        assert_eq!(
            control
                .bound()
                .await
                .unwrap()
                .activation()
                .fingerprint()
                .unwrap()
                .as_str(),
            context.activation().fingerprint().unwrap().as_str()
        );
        // Physical return supplies no native or resource retirement shortcut.
        assert_eq!(
            store
                .get_session_owner(context.session_id(), &context.activation().target().id)
                .await
                .unwrap()
                .state,
            crate::session_owner::SessionOwnerState::Active
        );
        assert!(observed.bound_main().is_some());
    }
    owner.wait_for_supervision().await;
}

#[tokio::test]
async fn drop_retains_each_original_pending_call_and_disposes_without_dispatch() {
    for stage in [Stage::Complete, Stage::Register, Stage::Activate] {
        let owner = PendingSuccessorTasks::default();
        let (store, clock) = store();
        let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
        let adapter = Interposed::new(store, stage, false);
        let calls = Arc::new(AtomicUsize::new(0));
        let count = calls.clone();
        let mut task = PendingSuccessorTask::register(
            &owner,
            adapter.clone(),
            context,
            joined,
            move |_, _| async move {
                count.fetch_add(1, Ordering::SeqCst);
            },
        )
        .unwrap();
        task.start().unwrap();
        adapter.arrived.notified().await;
        drop(task);
        assert!(
            tokio::time::timeout(Duration::from_millis(15), owner.wait_for_supervision())
                .await
                .is_err()
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        adapter.release.notify_one();
        tokio::time::timeout(Duration::from_secs(3), owner.wait_for_supervision())
            .await
            .unwrap();
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        assert_eq!(owner.available_capacity(), 64);
    }
}

#[tokio::test]
async fn cancelled_join_wait_preserves_actual_blocked_business_and_ingress() {
    let owner = PendingSuccessorTasks::default();
    let (store, clock) = store();
    let (context, joined) = fixture::prepared(store.as_ref(), &clock, false).await;
    let release = Arc::new(Notify::new());
    let ready = Arc::new(Notify::new());
    let (unblock, started) = (release.clone(), ready.clone());
    let mut task = PendingSuccessorTask::register(
        &owner,
        store,
        context,
        joined,
        move |context, _| async move {
            started.notify_one();
            unblock.notified().await;
            context
        },
    )
    .unwrap();
    task.start().unwrap();
    ready.notified().await;
    assert!(
        tokio::time::timeout(Duration::from_millis(15), task.join())
            .await
            .is_err()
    );
    task.control().request_stop(); // Does not abort already-dispatched main.
    release.notify_one();
    let actual = task.join().await.unwrap();
    assert_eq!(actual.disposition(), OwnedMainDisposition::Returned);
    assert!(actual.value().unwrap().try_enter_main().is_err());
}

#[tokio::test]
async fn unknown_ack_never_calls_business_or_reconstructs_context() {
    for stage in [Stage::Complete, Stage::Register, Stage::Activate] {
        let owner = PendingSuccessorTasks::default();
        let (store, clock) = store();
        let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
        let adapter = Interposed::new(store, stage, true);
        let mut task = PendingSuccessorTask::<()>::register(
            &owner,
            adapter.clone(),
            context,
            joined,
            |_, _| async { panic!("unknown ACK must not dispatch") },
        )
        .unwrap();
        let control = task.control();
        task.start().unwrap();
        adapter.arrived.notified().await;
        adapter.release.notify_one();
        let actual = task.join().await.unwrap();
        assert!(actual.error().is_some());
        assert_eq!(actual.disposition(), OwnedMainDisposition::NoDispatch);
        assert_eq!(actual.bound_main().is_some(), stage == Stage::Activate);
        assert_eq!(control.bound().await.is_ok(), stage == Stage::Activate);
    }
}

#[tokio::test]
async fn unstarted_drop_and_business_panic_have_distinct_actual_observations() {
    let owner = PendingSuccessorTasks::default();
    for start in [false, true] {
        let (store, clock) = store();
        let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
        let mut task = PendingSuccessorTask::<()>::register(
            &owner,
            store,
            context,
            joined,
            |_, _| -> std::future::Ready<()> { panic!("factory construction") },
        )
        .unwrap();
        if start {
            task.start().unwrap();
        }
        let observed = task.join().await.unwrap();
        assert_eq!(
            observed.disposition(),
            if start {
                OwnedMainDisposition::Panicked
            } else {
                OwnedMainDisposition::NoDispatch
            }
        );
        assert_eq!(observed.bound_main().is_some(), start);
    }
}

#[tokio::test]
async fn genuine_wrong_permit_and_original_completion_replay_do_not_dispatch() {
    let owner = PendingSuccessorTasks::default();
    let (store, clock) = store();
    let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
    let replay = joined.clone();
    let replay_context = context.clone();
    let adapter = Interposed::new(store.clone(), Stage::Activate, false);
    let grant = super::super::driver_tests::grant(store.as_ref()).await;
    let mut other =
        super::super::OwnedTaskRegistration::register_driver(&grant, |_| async {}).unwrap();
    let permit = super::super::driver_tests::permit(store.as_ref(), &grant, &other).await;
    *adapter.wrong_permit.lock().unwrap() = Some(permit);
    let mut task = PendingSuccessorTask::<()>::register(
        &owner,
        adapter.clone(),
        context,
        joined,
        |_, _| async { panic!("wrong actual permit") },
    )
    .unwrap();
    task.start().unwrap();
    adapter.arrived.notified().await;
    adapter.release.notify_one();
    let actual = task.join().await.unwrap();
    assert!(matches!(
        actual.error(),
        Some(PendingSuccessorError::Owner(Error::SemanticConflict))
    ));
    assert_eq!(actual.disposition(), OwnedMainDisposition::NoDispatch);
    assert!(actual.bound_main().is_some());
    other.join().await.unwrap();
    let mut task =
        PendingSuccessorTask::<()>::register(&owner, store, replay_context, replay, |_, _| async {
            panic!("replay supplies no fresh authority")
        })
        .unwrap();
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert!(actual.error().is_some());
    assert!(actual.bound_main().is_none());
}

#[tokio::test]
async fn concurrent_capacity_stays_retained_until_actual_observation_disposal() {
    let owner = PendingSuccessorTasks::default();
    let (store, clock) = store();
    let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
    let mut tasks = Vec::new();
    for _ in 0..64 {
        tasks.push(
            PendingSuccessorTask::register(
                &owner,
                store.clone(),
                context.clone(),
                joined.clone(),
                |_, _| async {},
            )
            .unwrap(),
        );
    }
    assert!(matches!(
        PendingSuccessorTask::register(&owner, store, context, joined, |_, _| async {}),
        Err(Error::Capacity)
    ));
    let mut first = tasks.pop().unwrap();
    assert_eq!(
        first.join().await.unwrap().disposition(),
        OwnedMainDisposition::NoDispatch
    );
    assert_eq!(owner.available_capacity(), 0); // Cached result still owns capacity.
    drop(first);
    assert_eq!(owner.available_capacity(), 1);
    drop(tasks);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn hostile_returned_value_disposal_cannot_forget_observation_or_capacity() {
    struct Payload;
    impl Drop for Payload {
        fn drop(&mut self) {
            panic!("payload destructor");
        }
    }
    struct Value;
    impl Drop for Value {
        fn drop(&mut self) {
            std::panic::panic_any(Payload);
        }
    }
    let owner = PendingSuccessorTasks::default();
    let (store, clock) = store();
    let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
    let mut task =
        PendingSuccessorTask::register(&owner, store, context, joined, |_, _| async { Value })
            .unwrap();
    task.start().unwrap();
    assert_eq!(
        task.join().await.unwrap().disposition(),
        OwnedMainDisposition::Returned
    );
    drop(task);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}

#[tokio::test]
async fn actual_panic_is_sticky_while_hostile_payload_disposal_is_blocked() {
    struct Payload {
        started: std::sync::mpsc::SyncSender<()>,
        release: Arc<std::sync::Barrier>,
    }
    impl Drop for Payload {
        fn drop(&mut self) {
            self.started.send(()).unwrap();
            self.release.wait();
            panic!("hostile payload disposal");
        }
    }
    let owner = PendingSuccessorTasks::default();
    let (store, clock) = store();
    let (context, joined) = fixture::prepared(store.as_ref(), &clock, true).await;
    let (started, disposing) = std::sync::mpsc::sync_channel(1);
    let release = Arc::new(std::sync::Barrier::new(2));
    let payload = Payload {
        started,
        release: release.clone(),
    };
    let mut task = PendingSuccessorTask::<()>::register(
        &owner,
        store,
        context,
        joined,
        move |_, _| -> std::future::Ready<()> { std::panic::panic_any(payload) },
    )
    .unwrap();
    let control = task.control();
    let retained = owner.clone();
    let observer = std::thread::spawn(move || {
        disposing.recv_timeout(Duration::from_secs(5)).unwrap();
        let disposition = control.finished_disposition();
        let capacity = retained.available_capacity();
        release.wait();
        (disposition, capacity)
    });
    task.start().unwrap();
    let actual = task.join().await.unwrap();
    assert_eq!(actual.disposition(), OwnedMainDisposition::Panicked);
    let (disposition, capacity) = observer.join().unwrap();
    assert_eq!(disposition, Some(OwnedMainDisposition::Panicked));
    assert_eq!(capacity, 63);
    drop(task);
    owner.wait_for_supervision().await;
    assert_eq!(owner.available_capacity(), 64);
}
