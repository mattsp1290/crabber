//! Actual selected SQLite worker COMMIT outlives original caller observation.
use super::*;
use crate::native_enrollment::initial_consumer::CallKind;
use std::sync::{
    Condvar, Mutex,
    atomic::{AtomicBool, Ordering},
};
#[derive(Default)]
struct Barrier {
    released: Mutex<bool>,
    changed: Condvar,
}
impl Barrier {
    fn wait(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
}
struct Release(Arc<Barrier>);
impl Drop for Release {
    fn drop(&mut self) {
        self.0.release();
    }
}
#[tokio::test]
async fn sqlite_initial_consumer_actual_conclusion_worker_lost_ack_is_paired_closed_once() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("consumer-commit.sqlite");
    crate::SqliteStore::migrate(&path).await.unwrap();
    let store = Arc::new(crate::SqliteStore::connect(&path).await.unwrap());
    let source = crabber_extension::LibraryScriptedSource::new(
        crabber::Selection {
            provider_id: "library".into(),
            model_id: "text".into(),
        },
        vec!["commit original".into()],
    )
    .unwrap();
    let (context, original) =
        initial_consumer_fixture::prepared(store.as_ref(), false, &source).await;
    let session = context.session_id().clone();
    let barrier = Arc::new(Barrier::default());
    let release = Release(barrier.clone());
    let armed = Arc::new(AtomicBool::new(false));
    let adapter = Arc::new(super::forwarding::Forward {
        inner: store.clone(),
        kind: CallKind::Conclude,
        armed: armed.clone(),
        calls: Mutex::new(Vec::new()),
        fail_first_open: AtomicBool::new(false),
    });
    let (entered, entry) = tokio::sync::oneshot::channel();
    hold_commit(&store, armed, barrier.clone(), entered).await;
    let owner = PendingDriverTasks::default();
    let mut producer = PendingInitialProducer::register_scripted(
        &owner,
        adapter.clone(),
        context,
        original,
        source,
    )
    .unwrap();
    let control = producer.control();
    producer.start().unwrap();
    let entered = tokio::time::timeout(std::time::Duration::from_secs(5), entry).await;
    let joined = tokio::time::timeout(std::time::Duration::from_secs(10), async {
        while !control.original_join_observed() {
            tokio::task::yield_now().await;
        }
    })
    .await;
    // Actual independent reader snapshot while the selected real worker is blocked.
    let before:Result<(i64,i64,i64),_>=sqlx::query_as("SELECT (SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1 AND kind='conclude'),(SELECT count(*) FROM initial_consumer_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await;
    barrier.release();
    let observed = producer.join().await.unwrap();
    let failed = observed.observation_failed();
    let error = matches!(
        observed.error(),
        Some(
            crate::session_owner::owned_scope::PendingSuccessorError::Completion(
                crate::session_owner::preparation::PreparationCommitError::Owner(
                    crate::session_owner::SessionOwnerError::UnknownStoreOutcome
                )
            )
        )
    );
    let status = observed.status().clone();
    let disposition = observed.disposition();
    owner.wait_for_supervision().await;
    let after:(i64,i64,i64)=sqlx::query_as("SELECT (SELECT count(*) FROM initial_consumer_calls WHERE session_id=$1 AND kind='conclude'),(SELECT count(*) FROM initial_consumer_closes WHERE session_id=$1),(SELECT count(*) FROM pending_initial_closes WHERE session_id=$1)").bind(&session.0).fetch_one(&store.readers).await.unwrap();
    let conclusions = adapter
        .calls
        .lock()
        .unwrap()
        .iter()
        .filter(|kind| **kind == CallKind::Conclude)
        .count();
    store.writer.close().await;
    store.readers.close().await;
    drop(release);
    assert!(
        entered.is_ok_and(|value| value.is_ok()),
        "actual selected worker reached COMMIT"
    );
    assert!(
        joined.is_ok(),
        "SAME original task join observed before COMMIT release"
    );
    assert_eq!(before.unwrap(), (0, 0, 0));
    assert_eq!(after, (1, 1, 1));
    assert_eq!(
        conclusions, 1,
        "unknown ACK never retries effect or constructs another original"
    );
    assert_eq!(status, PendingInitialProducerStatus::ClosedAssignment);
    assert_eq!(
        disposition,
        crate::session_owner::owned_scope::OwnedMainDisposition::Returned
    );
    assert!(
        !failed && error,
        "clean literal disposal keeps physical observation proven while the original ACK remains unknown"
    );
}

async fn hold_commit(
    store: &crate::SqliteStore,
    armed: Arc<AtomicBool>,
    barrier: Arc<Barrier>,
    entered: tokio::sync::oneshot::Sender<()>,
) {
    let mut entered = Some(entered);
    let hook = barrier;
    let mut writer = store.writer.acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if armed.swap(false, Ordering::SeqCst)
                && let Some(entered) = entered.take()
            {
                let _ = entered.send(());
                hook.wait();
            }
            true
        });
    drop(writer);
}
