//! An authentic ACK must still belong to the exact original invocation.
use super::*;
async fn capture(h: &Harness, choice: &Choice) -> crabber_session::ToolContinuationClaimOutcome {
    h.gate.preparation_fault.store(15, Ordering::SeqCst);
    assert!(choice.invoke(&h.runtime).await.is_err());
    let mut captured = h.gate.foreign_grant_ack.lock().unwrap();
    captured.take().unwrap()
}
async fn foreign_run(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    recovery: bool,
) -> Option<String> {
    let (foreign, foreign_choice, _) =
        fixture::paused_none(store.clone(), clock.clone(), frontend).await;
    let ack = capture(&foreign, &foreign_choice).await;
    let (h, original, effects) = fixture::paused_none(store, clock.clone(), frontend).await;
    let choice = if recovery {
        choice_for_recovery(&h, &original, &clock).await
    } else {
        original
    };
    rejected(&h, &choice, effects, ack, "foreign run").await
}
async fn same_run(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    prior_recovery: bool,
) -> Option<String> {
    let (h, original, effects) = fixture::paused_none(store, clock.clone(), frontend).await;
    let prior = if prior_recovery {
        choice_for_recovery(&h, &original, &clock).await
    } else {
        original.clone()
    };
    let ack = capture(&h, &prior).await;
    clock.set(clock.now() + time::Duration::seconds(31));
    let current = original.recovery(&h).await;
    rejected(
        &h,
        &current,
        effects,
        ack,
        if prior_recovery {
            "older recovery"
        } else {
            "older claim"
        },
    )
    .await
}
async fn rejected(
    h: &Harness,
    choice: &Choice,
    effects: Arc<fixture::Effects>,
    ack: crabber_session::ToolContinuationClaimOutcome,
    label: &str,
) -> Option<String> {
    let crabber_session::ToolContinuationClaimOutcome::Applied { receipt, grant } = &ack else {
        panic!("genuine native ACK");
    };
    grant.verify_native_ack(receipt).unwrap();
    *h.gate.foreign_grant_ack.lock().unwrap() = Some(ack);
    h.gate
        .initial_task_diagnostics
        .store(true, Ordering::SeqCst);
    h.gate.initial_task_ids.lock().unwrap().clear();
    h.gate.preparation_fault.store(18, Ordering::SeqCst);
    let result = choice.invoke(&h.runtime).await;
    let accepted = result.is_ok();
    if let Ok(crate::ToolContinuation::Started { handle, .. }) = result {
        let _ = handle.done().await;
    }
    let began = h
        .gate
        .initial_task_ids
        .lock()
        .unwrap()
        .iter()
        .any(|(stage, _)| *stage == InitialTaskStage::BeginAttempt);
    let resumed = effects.resumes.load(Ordering::SeqCst);
    let protected = h.gate.grant_payload_protections.load(Ordering::SeqCst);
    if accepted || began || resumed != 0 || protected != 0 {
        Some(format!(
            "{label} accepted={accepted} begin={began} resumed={resumed} protected={protected}"
        ))
    } else {
        None
    }
}
async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>) {
    let mut failures = Vec::new();
    for frontend in [false, true] {
        for recovery in [false, true] {
            for result in [
                Box::pin(foreign_run(
                    store.clone(),
                    clock.clone(),
                    frontend,
                    recovery,
                ))
                .await,
                Box::pin(same_run(store.clone(), clock.clone(), frontend, recovery)).await,
            ]
            .into_iter()
            .flatten()
            {
                failures.push(format!("frontend={frontend}: {result}"));
            }
        }
    }
    assert!(
        failures.is_empty(),
        "foreign sealed ACK accepted:\n{}",
        failures.join("\n")
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_whole_none_ack_is_bound_to_original_control() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_whole_none_ack_is_bound_to_original_control() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("intent.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    Box::pin(contract(store, clock)).await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_whole_none_ack_is_bound_to_original_control() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let mut history_runtime = tokio::runtime::Builder::new_multi_thread();
    history_runtime.worker_threads(4);
    TestDatabase::run_with_runtime(DatabaseMode::Current, history_runtime, async move |url| {
        crabber_session::PostgresStore::migrate(&url).await.unwrap();
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(
            crabber_session::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        );
        Box::pin(contract(store, clock)).await;
    })
    .unwrap();
}
