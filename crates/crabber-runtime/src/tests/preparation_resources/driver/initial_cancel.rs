//! Explicit stop closes actual factory resources while original Complete ACK stays held.
use super::*;

async fn contract(store: Arc<dyn Store>, backend: &str) {
    let mut observations = Vec::new();
    for unkeyed in [false, true] {
        let gate = Arc::new(SettlementGate::default());
        gate.release.notify_one();
        gate.initial_task_diagnostics.store(true, Ordering::SeqCst);
        gate.preparation_hold.store(true, Ordering::SeqCst);
        let wrapped = Arc::new(DelayedTerminalStore {
            inner: store.clone(),
            gate: gate.clone(),
        });
        let h = Harness::on_store(wrapped, store.clone(), gate);
        let start = start_initial_task(&h, unkeyed);
        let entered = tokio::time::timeout(
            Duration::from_secs(5),
            h.gate.preparation_entered.notified(),
        )
        .await;
        if entered.is_err() {
            h.provider.cleanup_release.notify_one();
            h.gate.preparation_release.notify_one();
            let result = start.await.unwrap();
            let error = match result {
                Ok(handle) => {
                    let _ = handle.done().await;
                    None
                }
                Err(error) => Some(error),
            };
            panic!(
                "genuine initial Complete ACK not reached: backend={backend} unkeyed={unkeyed} error={error:?}"
            );
        }
        let session = h
            .gate
            .initial_preparation_session
            .lock()
            .unwrap()
            .clone()
            .unwrap();
        let descriptor = h.runtime.retained_preparation_status(&session)[0]
            .preparation
            .clone();
        let requested = h.runtime.cleanup_retained_preparation(&descriptor).await;
        let closed_before_ack = tokio::time::timeout(
            Duration::from_millis(150),
            h.provider.cleanup_started.notified(),
        )
        .await
        .is_ok();
        let ack_still_pending = !start.is_finished();
        let stages_before_ack = h.gate.initial_task_ids.lock().unwrap().clone();
        // Always release both real gates before checking the expected RED.
        h.provider.cleanup_release.notify_one();
        let held = while_ack_held(&h, &descriptor, closed_before_ack).await;
        h.gate.preparation_hold.store(false, Ordering::SeqCst);
        h.gate.preparation_release.notify_one();
        let started = start.await.unwrap();
        if let Ok(handle) = started {
            let _ = handle.done().await;
        }
        // A refused old path may only become eligible for cooperative cleanup
        // after the held ACK. Retain and finish that exact original entry too.
        let _ = h.runtime.cleanup_retained_preparation(&descriptor).await;
        let cleaned = tokio::time::timeout(Duration::from_secs(3), async {
            while h.provider.cleanup_completed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await;
        let drained = h
            .runtime
            .drain_session_execution_tasks(&session, Duration::from_secs(3))
            .await;
        let child_after = h.child_state(&descriptor).await;
        let effects = h.fake.requests().len();
        eprintln!(
            "held initial Complete ACK backend={backend} unkeyed={unkeyed} requested={requested:?} cleanup_started_before_ack={closed_before_ack} ack_pending={ack_still_pending} stages={stages_before_ack:?} effects={effects}"
        );
        observations.push(Observation {
            unkeyed,
            requested,
            closed: closed_before_ack,
            pending: ack_still_pending,
            stages: stages_before_ack,
            cleaned,
            drained,
            effects,
            held,
            child_after,
        });
    }
    for observation in observations {
        observation.check(backend);
    }
}
struct Observation {
    unkeyed: bool,
    requested: Result<(), RuntimeError>,
    closed: bool,
    pending: bool,
    stages: Vec<(InitialTaskStage, Option<tokio::task::Id>)>,
    cleaned: Result<(), tokio::time::error::Elapsed>,
    drained: crate::ExecutionTaskDrain,
    effects: usize,
    held: HeldAck,
    child_after: SessionOwnerState,
}
struct HeldAck {
    actual_cleanup_completed: bool,
    join_attempted: bool,
    child: SessionOwnerState,
}
async fn while_ack_held(
    h: &Harness,
    descriptor: &PreparationReconciliation,
    closed: bool,
) -> HeldAck {
    let actual_cleanup_completed = closed
        && tokio::time::timeout(Duration::from_secs(2), async {
            while h.provider.cleanup_completed.load(Ordering::SeqCst) != 1 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
    let join_attempted = actual_cleanup_completed
        && tokio::time::timeout(Duration::from_millis(150), async {
            while h.gate.owned_join_calls.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .is_ok();
    let child = h.child_state(descriptor).await;
    eprintln!(
        "original lease while CompleteACK held actual_cleanup_completed={actual_cleanup_completed} join_attempted={join_attempted} child={child:?}"
    );
    HeldAck {
        actual_cleanup_completed,
        join_attempted,
        child,
    }
}
impl Observation {
    fn check(self, backend: &str) {
        let Self {
            unkeyed,
            requested,
            closed,
            pending,
            stages,
            cleaned,
            drained,
            effects,
            held,
            child_after,
        } = self;
        assert!(cleaned.is_ok(), "{cleaned:?}");
        assert!(
            drained.pending.is_empty() && drained.pending_preparations.is_empty(),
            "{drained:?}"
        );
        assert!(
            pending,
            "original Complete ACK must stay pending during stop"
        );
        assert_eq!(
            stages.iter().map(|(stage, _)| *stage).collect::<Vec<_>>(),
            vec![InitialTaskStage::CompletePreparation]
        );
        assert!(
            requested.is_ok() && closed,
            "explicit initial cleanup must start before held Complete ACK returns: backend={backend} unkeyed={unkeyed} requested={requested:?} closed={closed}"
        );
        assert!(
            held.actual_cleanup_completed,
            "real cleanup must finish while original ACK remains held"
        );
        assert!(
            !held.join_attempted,
            "native child Join preceded original returned plan disposal"
        );
        assert_eq!(
            held.child,
            SessionOwnerState::Active,
            "held original main still owns the actual returned plan lease"
        );
        assert_eq!(
            child_after,
            SessionOwnerState::Joined,
            "child joins only after original main disposal and actual cleanup"
        );
        assert_eq!(
            effects, 0,
            "explicit stop must prevent new business dispatch"
        );
    }
}

#[tokio::test]
async fn memory_held_initial_complete_ack_allows_independent_cleanup() {
    contract(Arc::new(MemoryStore::new()), "memory").await;
}
#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_held_initial_complete_ack_allows_independent_cleanup() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("initial-cancel.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber_session::SqliteStore::connect(&path).await.unwrap();
    contract(Arc::new(store), "sqlite").await;
}
#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_held_initial_complete_ack_allows_independent_cleanup() {
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1")
        );
        return;
    };
    crabber_session::PostgresStore::migrate(&url).await.unwrap();
    let store = crabber_session::PostgresStore::connect(&url).await.unwrap();
    contract(Arc::new(store), "postgres").await;
}

fn start_initial_task(
    h: &Harness,
    unkeyed: bool,
) -> tokio::task::JoinHandle<Result<crate::RunHandle, RuntimeError>> {
    let runtime = h.runtime.clone();
    let mut input = h.input.clone();
    if unkeyed {
        input.session_id = None;
    }
    tokio::spawn(async move {
        if unkeyed {
            runtime.start(input).await
        } else {
            runtime
                .start_keyed(input, options())
                .await
                .map(|admission| match admission {
                    crate::Admission::Started { handle, .. } => handle,
                    crate::Admission::Replayed(_) => panic!("fresh initial operation"),
                })
        }
    })
}
