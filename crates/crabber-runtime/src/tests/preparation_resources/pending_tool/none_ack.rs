//! Legacy None contracts still require integrity of the actual native grant ACK.
use super::*;
type OriginCase = (bool, bool, &'static [usize]);
const MEMORY_FAULTS: &[usize] = &[8, 10, 11, 12, 16, 17];
const MEMORY_CASES: &[OriginCase] = &[
    (false, false, MEMORY_FAULTS),
    (false, true, MEMORY_FAULTS),
    (true, false, MEMORY_FAULTS),
    (true, true, MEMORY_FAULTS),
];
#[cfg(any(feature = "sqlite", feature = "postgres"))]
const SQL_CASES: &[OriginCase] = &[
    (false, false, &[8, 16, 17]),
    (false, true, &[]),
    (true, false, &[]),
    (true, true, &[8, 16, 17]),
];
async fn choice_for_recovery(h: &Harness, original: &Choice, clock: &ManualClock) -> Choice {
    h.gate.preparation_fault.store(2, Ordering::SeqCst);
    assert!(original.invoke(&h.runtime).await.is_err());
    replay_without_factory(h, original).await;
    clock.set(clock.now() + time::Duration::seconds(31));
    original.recovery(h).await
}
async fn foreign_capabilities(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
) -> super::super::super::grant_ack_faults::Capabilities {
    let (h, choice, _) = fixture::paused_none(store, clock, frontend).await;
    h.gate.preparation_fault.store(15, Ordering::SeqCst);
    assert!(choice.invoke(&h.runtime).await.is_err());
    let mut captured = h.gate.foreign_grant_capabilities.lock().unwrap();
    captured.take().unwrap()
}
async fn mutated(
    store: Arc<dyn Store>,
    clock: Arc<ManualClock>,
    frontend: bool,
    recovery: bool,
    fault: usize,
) -> Option<String> {
    let foreign = if matches!(fault, 16 | 17) {
        Some(Box::pin(foreign_capabilities(store.clone(), clock.clone(), frontend)).await)
    } else {
        None
    };
    let (h, original, effects) = fixture::paused_none(store, clock.clone(), frontend).await;
    let choice = if recovery {
        choice_for_recovery(&h, &original, &clock).await
    } else {
        original.clone()
    };
    *h.gate.foreign_grant_capabilities.lock().unwrap() = foreign;
    h.gate
        .initial_task_diagnostics
        .store(true, Ordering::SeqCst);
    h.gate.initial_task_ids.lock().unwrap().clear();
    h.gate.preparation_fault.store(fault, Ordering::SeqCst);
    let result = choice.invoke(&h.runtime).await;
    let accepted = result.is_ok();
    if let Ok(crate::ToolContinuation::Started { handle, .. }) = result {
        let _ = handle.done().await;
    }
    let resumed = effects.resumes.load(Ordering::SeqCst);
    let protected = h.gate.grant_payload_protections.load(Ordering::SeqCst);
    let began = h
        .gate
        .initial_task_ids
        .lock()
        .unwrap()
        .iter()
        .any(|(stage, _)| *stage == InitialTaskStage::BeginAttempt);
    let registered = h.gate.successor_request.lock().unwrap().is_some();
    replay_without_factory(&h, &choice).await;
    if accepted || resumed != 0 || protected != 0 || began || registered {
        Some(format!(
            "frontend={frontend} recovery={recovery} fault={fault}: accepted={accepted} resumed={resumed} protected={protected} begin={began} registered={registered}"
        ))
    } else {
        None
    }
}
async fn positives(store: Arc<dyn Store>, clock: Arc<ManualClock>, frontend: bool, recovery: bool) {
    let (h, original, effects) = fixture::paused_none(store, clock.clone(), frontend).await;
    let choice = if recovery {
        choice_for_recovery(&h, &original, &clock).await
    } else {
        original.clone()
    };
    let crate::ToolContinuation::Started { handle, receipt } =
        choice.invoke(&h.runtime).await.unwrap()
    else {
        panic!("fresh native None grant");
    };
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(
        effects.resumes.load(Ordering::SeqCst),
        usize::from(!frontend)
    );
    assert_eq!(h.gate.grant_payload_protections.load(Ordering::SeqCst), 1);
    assert_eq!(h.fake.requests().len(), 2);
    assert_eq!(replay_without_factory(&h, &choice).await, receipt);
    replay_without_factory(&h, &original).await;
}
async fn contract(store: Arc<dyn Store>, clock: Arc<ManualClock>, cases: &[OriginCase]) {
    let mut failures = Vec::new();
    for &(frontend, recovery, faults) in cases {
        Box::pin(positives(store.clone(), clock.clone(), frontend, recovery)).await;
        for &fault in faults {
            if let Some(failure) = Box::pin(mutated(
                store.clone(),
                clock.clone(),
                frontend,
                recovery,
                fault,
            ))
            .await
            {
                failures.push(failure);
            }
        }
    }
    assert!(
        failures.is_empty(),
        "native None ACK integrity failures:\n{}",
        failures.join("\n")
    );
}
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn memory_none_grant_ack_integrity() {
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    Box::pin(contract(
        Arc::new(MemoryStore::with_clock(clock.clone())),
        clock,
        MEMORY_CASES,
    ))
    .await;
}
#[cfg(feature = "sqlite")]
#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn sqlite_none_grant_ack_integrity() {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("none-ack.sqlite");
    crabber_session::SqliteStore::migrate(&path).await.unwrap();
    let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
    let store = Arc::new(
        crabber_session::SqliteStore::connect(path)
            .await
            .unwrap()
            .with_clock(clock.clone()),
    );
    Box::pin(contract(store, clock, SQL_CASES)).await;
}
#[cfg(feature = "postgres")]
#[test]
fn postgres_none_grant_ack_integrity() {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};

    let mut history_runtime = tokio::runtime::Builder::new_multi_thread();
    history_runtime.worker_threads(4);
    TestDatabase::run_with_runtime(DatabaseMode::Current, history_runtime, async move |url| {
        let clock = Arc::new(ManualClock::new(time::OffsetDateTime::UNIX_EPOCH));
        let store = Arc::new(
            crabber_session::PostgresStore::connect(&url)
                .await
                .unwrap()
                .with_clock(clock.clone()),
        );
        Box::pin(contract(store, clock, SQL_CASES)).await;
    })
    .unwrap();
}

#[path = "none_ack/intent.rs"]
mod intent;
