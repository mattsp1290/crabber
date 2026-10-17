//! An actual SQLite worker can commit after its Store caller has been joined.
use super::super::*;
use crate::{KeyedAdmitOutcome, Store};
use std::sync::{Arc, Condvar, Mutex};

#[derive(Default)]
struct CommitRelease {
    released: Mutex<bool>,
    changed: Condvar,
}
impl CommitRelease {
    fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    fn wait(&self) {
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
}
struct ReleaseOnDrop(Arc<CommitRelease>);
impl Drop for ReleaseOnDrop {
    fn drop(&mut self) {
        self.0.release();
    }
}

#[tokio::test]
async fn acknowledged_store_caller_observes_actual_commit_worker() {
    commit_worker(false).await;
}

#[tokio::test]
async fn joined_cancelled_store_caller_does_not_prove_commit_worker_stopped() {
    commit_worker(true).await;
}

async fn commit_worker(cancel_caller: bool) {
    let (store, path, directory) = super::temp_store().await;
    let session = crabber_core::SessionId::new();
    let request =
        crate::admission_execution_contract::keyed(&session, time::OffsetDateTime::now_utc());
    let release = Arc::new(CommitRelease::default());
    let release_on_drop = ReleaseOnDrop(release.clone());
    let entered_rx = install_hook(&store, release.clone()).await;
    let actual_store = store.clone();
    let original_request = request.clone();
    let mut caller = Some(tokio::spawn(async move {
        actual_store.admit_keyed_run(original_request).await
    }));
    let observations =
        held_observations(&store, &path, &mut caller, entered_rx, cancel_caller).await;
    // Release before inspecting results, including failed diagnostic operations.
    // The Drop guard also releases a worker on panic/cancellation of this fixture.
    release.release();
    if (cancel_caller || observations.is_err())
        && let Some(caller) = &caller
    {
        caller.abort();
    }
    let acknowledged = match caller {
        Some(caller) => Some(caller.await),
        None => None,
    };
    if observations.is_err() {
        store.writer.close().await;
        store.readers.close().await;
    }
    let observations = observations.unwrap();
    // Acquiring the independent writer gate observes release of the previous
    // write/commit gate and permits durable receipt corroboration. This does
    // not prove connection disposal or termination of the SQLite worker.
    observe_writer_gate(&path).await;
    let saved = store
        .lookup_admission(&session, &request.options.key)
        .await
        .unwrap()
        .unwrap();
    let retry = store.admit_keyed_run(request).await.unwrap();
    let count: (i64, i64, i64, i64) = sqlx::query_as(
        "SELECT (SELECT count(*) FROM runs WHERE session_id=$1),
         (SELECT count(*) FROM admission_receipts WHERE session_id=$1),
         (SELECT count(*) FROM native_run_issuances WHERE session_id=$1),
         (SELECT count(*) FROM native_execution_origins WHERE session_id=$1)",
    )
    .bind(&session.0)
    .fetch_one(&store.readers)
    .await
    .unwrap();
    let mut tx = crate::transaction::begin(&store.writer).await.unwrap();
    let source = super::super::native_enrollment::original(
        &mut tx,
        &session,
        &saved.run_id,
        &mut crate::public_session::v2::proof::Proof::bounded(32 << 20),
    )
    .await;
    tx.rollback().await.unwrap();
    let next = store
        .admit_run(super::request(&crabber_core::SessionId::new()))
        .await;
    // Close the real pools before assertions, after all worker barriers release.
    store.writer.close().await;
    store.readers.close().await;
    drop(release_on_drop);
    drop(directory);
    assert_eq!(
        observations.sessions, 0,
        "uncommitted admission is invisible"
    );
    assert_eq!(
        observations.receipts, 0,
        "no committed receipt while hook is held"
    );
    assert_eq!(observations.busy_code.as_deref(), Some("5"));
    assert_eq!(observations.caller_joined, cancel_caller);
    if let Some(acknowledged) = acknowledged {
        let KeyedAdmitOutcome::Started { receipt, .. } = acknowledged.unwrap().unwrap() else {
            panic!("fresh acknowledged admission");
        };
        assert_eq!(receipt, saved);
    }
    let KeyedAdmitOutcome::Replayed(replayed) = retry else {
        panic!("retry must not issue another grant");
    };
    assert_eq!(replayed, saved);
    assert_eq!(count, (1, 1, 1, 1));
    assert_eq!(
        source.unwrap(),
        crate::native_enrollment::Status::Unclassified
    );
    assert!(next.is_ok(), "a later actual writer remains usable");
}

struct Observations {
    sessions: i64,
    receipts: i64,
    busy_code: Option<String>,
    caller_joined: bool,
}
async fn held_observations(
    store: &SqliteStore,
    path: &Path,
    caller: &mut Option<tokio::task::JoinHandle<Result<KeyedAdmitOutcome, StoreError>>>,
    entered: tokio::sync::oneshot::Receiver<()>,
    cancel_caller: bool,
) -> Result<Observations, sqlx::Error> {
    tokio::time::timeout(std::time::Duration::from_secs(5), entered)
        .await
        .map_err(|_| sqlx::Error::Protocol("commit hook entry timed out".into()))?
        .map_err(|_| sqlx::Error::Protocol("commit hook not entered".into()))?;
    let caller_joined = if cancel_caller {
        let mut original = caller.take().expect("original caller is retained");
        original.abort();
        if let Ok(joined) =
            tokio::time::timeout(std::time::Duration::from_secs(5), &mut original).await
        {
            joined.is_err_and(|error| error.is_cancelled())
        } else {
            *caller = Some(original);
            return Err(sqlx::Error::Protocol(
                "original caller join timed out".into(),
            ));
        }
    } else {
        false
    };
    // WAL readers see the previous committed snapshot while the real worker is
    // blocked inside COMMIT, even after the original caller's actual join.
    let sessions = sqlx::query_scalar("SELECT count(*) FROM sessions")
        .fetch_one(&store.readers)
        .await?;
    let receipts = sqlx::query_scalar("SELECT count(*) FROM admission_receipts")
        .fetch_one(&store.readers)
        .await?;
    let mut contender =
        SqliteConnection::connect_with(&options(path).busy_timeout(std::time::Duration::ZERO))
            .await?;
    let attempt = sqlx::query("BEGIN IMMEDIATE").execute(&mut contender).await;
    let busy_code = match attempt {
        Err(sqlx::Error::Database(error)) => error.code().map(std::borrow::Cow::into_owned),
        Ok(_) => {
            sqlx::query("ROLLBACK").execute(&mut contender).await?;
            None
        }
        Err(error) => return Err(error),
    };
    contender.close().await?;
    Ok(Observations {
        sessions,
        receipts,
        busy_code,
        caller_joined,
    })
}

async fn install_hook(
    store: &SqliteStore,
    release: Arc<CommitRelease>,
) -> tokio::sync::oneshot::Receiver<()> {
    let (entered, entered_rx) = tokio::sync::oneshot::channel();
    let mut entered = Some(entered);

    let mut writer = store.writer.acquire().await.unwrap();
    writer
        .lock_handle()
        .await
        .unwrap()
        .set_commit_hook(move || {
            if let Some(entered) = entered.take() {
                let _ = entered.send(());
                release.wait();
            }
            true // SQLx's safe hook uses TRUE to allow the actual COMMIT.
        });
    drop(writer);
    entered_rx
}

async fn observe_writer_gate(path: &Path) {
    let mut independent = SqliteConnection::connect_with(&options(path))
        .await
        .unwrap();
    sqlx::query("BEGIN IMMEDIATE")
        .execute(&mut independent)
        .await
        .unwrap();
    sqlx::query("ROLLBACK")
        .execute(&mut independent)
        .await
        .unwrap();
    independent.close().await.unwrap();
}
