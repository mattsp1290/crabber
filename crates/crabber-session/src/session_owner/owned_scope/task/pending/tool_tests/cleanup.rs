//! A returned pending main cannot stand in for its original child cleanup join.
use super::*;
use crate::session_owner::owned_scope::OwnedCleanupTask;
use tokio::sync::Notify;

struct Resource(Arc<AtomicUsize>);
impl Drop for Resource {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
pub(super) async fn blocked<S: Store + 'static>(
    store: Arc<S>,
    clock: &ManualClock,
    frontend: bool,
    recovery: bool,
) {
    let owner = PendingDriverTasks::default();
    let (context, original, _) =
        Box::pin(prepared(store.as_ref(), clock, frontend, recovery, false)).await;
    let ready = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let disposed = Arc::new(AtomicUsize::new(0));
    let child = Arc::new(std::sync::Mutex::new(None));
    let output = child.clone();
    let (started, wait, resource) = (ready.clone(), release.clone(), Resource(disposed.clone()));
    let mut task = PendingSuccessorTask::register(
        &owner,
        store.clone(),
        context,
        original,
        move |_, _| async move {
            let original_child = tokio::spawn(async move {
                let resource = resource;
                started.notify_one();
                wait.notified().await;
                drop(resource);
                Ok(())
            });
            *output.lock().unwrap() = Some(original_child);
        },
    )
    .unwrap();
    let control = task.control();
    task.start().unwrap();
    ready.notified().await;
    let observed = task.join().await.unwrap();
    assert!(observed.error().is_none(), "{:?}", observed.error());
    let main = observed.bound_main().unwrap();
    let bound = control.bound().await.unwrap();
    let mut cleanup = OwnedCleanupTask::from_host_task(
        main.activation().clone(),
        child.lock().unwrap().take().unwrap(),
    );
    assert!(
        tokio::time::timeout(
            std::time::Duration::from_millis(15),
            main.observe_resources(&mut cleanup)
        )
        .await
        .is_err()
    );
    assert_eq!(disposed.load(Ordering::SeqCst), 0);
    assert_eq!(
        store
            .get_session_owner(
                main.activation().session_id(),
                &main.activation().target().id
            )
            .await
            .unwrap()
            .state,
        crate::session_owner::SessionOwnerState::Active
    );
    release.notify_one();
    let joined = main.observe_resources(&mut cleanup).await.unwrap();
    assert_eq!(disposed.load(Ordering::SeqCst), 1);
    store
        .join_owned_scope(joined, bound.grant().authority().clone())
        .await
        .unwrap();
    assert_eq!(
        store
            .get_session_owner(
                main.activation().session_id(),
                &main.activation().target().id
            )
            .await
            .unwrap()
            .state,
        crate::session_owner::SessionOwnerState::Joined
    );
    owner.wait_for_supervision().await;
}
