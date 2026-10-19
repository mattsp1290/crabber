mod result_transform_support;

use crabber::{
    ExtensionError, Observer, RuntimeError,
    core::{
        ContentBlock, EventKind, EventRecord, RunStatus, SessionId, ToolCallStatus,
        ToolResultStatus,
    },
    extension::{DEFAULT_MOUNT_CLOSE_TIMEOUT, MountCloseTimeout},
    runtime::{INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use result_transform_support::{Probes, RAW_OUTPUT, REDACTED_OUTPUT, agent_builder};
use std::{
    sync::{Arc, Mutex},
    time::Duration,
};
use tokio::time::{Instant, timeout};

const FIXTURE_BOUND: Duration = Duration::from_secs(10);

// The real subprocess is already ready before this helper is entered. Freeze
// only the accepted reducer window; deadline exactness belongs to paused-time
// unit tests, not OS scheduling of a child-process integration fixture.
struct ResumeClock;
impl Drop for ResumeClock {
    fn drop(&mut self) {
        tokio::time::resume();
    }
}
async fn frozen_redaction(operation: impl std::future::Future<Output = ()>) {
    tokio::time::pause();
    let _resume = ResumeClock;
    let started = std::time::Instant::now();
    tokio::pin!(operation);
    loop {
        assert!(
            started.elapsed() < FIXTURE_BOUND,
            "accepted redaction wall-clock watchdog"
        );
        tokio::select! {
            biased;
            () = &mut operation => {
                assert!(started.elapsed() < FIXTURE_BOUND, "accepted redaction wall-clock watchdog");
                return;
            },
            // Remaining runnable prevents Tokio's paused clock from advancing
            // automatically while actual child cancellation/cleanup is polled.
            () = tokio::task::yield_now() => {}
        }
    }
}
async fn interrupt_and_observe(run: &crabber::RunHandle, probes: &Probes, accepted: bool) {
    let operation = async {
        run.interrupt();
        probes.callback_dropped.wait().await;
        probes.kill_started.wait().await;
        // Dropping the callback never proves that its child has been reaped.
        // While the actual wait is held, no native or logical result is ready.
        assert_eq!(probes.permit_count(), 0);
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        if accepted {
            probes.redacted.wait().await;
        }
    };
    if accepted {
        frozen_redaction(operation).await;
    } else {
        operation.await;
    }
}

async fn assert_settlement(
    store: &dyn Store,
    session: &SessionId,
    run: &crabber::core::RunId,
    probes: &Probes,
    expected: &str,
) {
    assert_eq!(
        store.get_run(run).await.unwrap().unwrap().status,
        RunStatus::Interrupted
    );
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 1000,
                text_bytes: 100_000,
                encoded_bytes: 1_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("complete fixture snapshot")
    };
    assert!(page.continuation.is_none());
    assert_eq!(page.tool_calls.len(), 1);
    let call = &page.tool_calls[0];
    assert_eq!(call.status, ToolCallStatus::Interrupted);
    let result = call.result.as_ref().unwrap();
    assert_eq!(result.status, ToolResultStatus::Interrupted);
    assert_eq!(
        result.content,
        vec![ContentBlock::Text {
            text: if expected == INTERRUPTED_RESULT_TEXT {
                expected.into()
            } else {
                serde_json::to_string(expected).unwrap()
            }
        }]
    );
    let messages = store.list_all_messages(session).await.unwrap();
    let tool_results: Vec<_> = messages
        .iter()
        .flat_map(|message| &message.parts)
        .filter_map(|part| {
            if let ContentBlock::ToolResult {
                call_id,
                content,
                is_error,
            } = &part.content
            {
                Some((call_id, content, is_error))
            } else {
                None
            }
        })
        .collect();
    assert_eq!(tool_results.len(), 1);
    assert_eq!(tool_results[0].0, &call.id);
    assert_eq!(tool_results[0].1, &result.content);
    assert!(*tool_results[0].2);
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(events.len() < 1000);
    let settled: Vec<_> = events
        .iter()
        .filter(|event| event.kind == EventKind::ToolCallSettled)
        .collect();
    assert_eq!(settled.len(), 1);
    assert_native_settlement(store, run, &events).await;
    let payload = &settled[0].payload;
    assert_eq!(payload["call_id"], serde_json::json!(call.id));
    assert_eq!(payload["content"], serde_json::json!(result.content));
    assert_eq!(payload["is_error"], true);
    assert_eq!(payload["status"], "interrupted");
    assert!(!settled[0].live_only);
    assert!(settled[0].cursor.is_some());
    let requests = probes.provider.requests();
    assert_eq!(requests.len(), 1);
    for persisted in [
        serde_json::to_string(&page).unwrap(),
        serde_json::to_string(&messages).unwrap(),
        serde_json::to_string(&events).unwrap(),
        format!("{requests:?}"),
    ] {
        assert!(
            !persisted.contains(RAW_OUTPUT),
            "unredacted fixture output escaped"
        );
        if expected == INTERRUPTED_RESULT_TEXT {
            assert!(!persisted.contains(REDACTED_OUTPUT), "seed was accepted");
        }
    }
}

async fn assert_native_settlement(
    store: &dyn Store,
    run: &crabber::core::RunId,
    events: &[crabber::core::EventRecord],
) {
    let source = events
        .iter()
        .rev()
        .find(|event| event.kind == EventKind::ToolPhaseChanged)
        .unwrap();
    let phase: crabber::session::PhaseSnapshot =
        serde_json::from_value(source.payload["phase"].clone()).unwrap();
    let phase = store.tool_phase(run, &phase.target.id).await.unwrap().phase;
    assert_eq!(phase.state, crabber::session::PhaseState::Failed);
    assert_eq!(
        phase.quiescence,
        crabber::session::PhaseQuiescence::Complete
    );
}

async fn assert_held_phase(store: &dyn Store, session: &SessionId, run: &crabber::core::RunId) {
    assert_eq!(
        store.get_run(run).await.unwrap().unwrap().status,
        RunStatus::Running
    );
    let calls = store.list_unfinished_tool_calls(run).await.unwrap();
    assert_eq!(calls.len(), 1);
    assert_eq!(calls[0].status, ToolCallStatus::Running);
    assert!(calls[0].result.is_none());
    let events = store.list_events(session, None, 1000).await.unwrap();
    assert!(
        !events
            .iter()
            .any(|event| event.kind == EventKind::ToolCallSettled)
    );
    let source = events
        .iter()
        .rev()
        .find(|event| event.kind == EventKind::ToolPhaseChanged)
        .unwrap();
    let phase: crabber::session::PhaseSnapshot =
        serde_json::from_value(source.payload["phase"].clone()).unwrap();
    let phase = store.tool_phase(run, &phase.target.id).await.unwrap().phase;
    assert_eq!(phase.state, crabber::session::PhaseState::Started);
    assert_eq!(phase.quiescence, crabber::session::PhaseQuiescence::Pending);
}

async fn assert_drained(probes: &Probes, pid: u32) {
    timeout(FIXTURE_BOUND, async {
        probes.reaped.wait().await;
        probes.pipe_closed.wait().await;
        let permit = probes.permits.acquire().await.unwrap();
        drop(permit);
    })
    .await
    .expect("cleanup drain bound");
    assert_eq!(probes.permit_count(), 1);
    #[cfg(target_os = "linux")]
    assert!(
        !std::path::Path::new(&format!("/proc/{pid}")).exists(),
        "fixture PID {pid} survived reaping"
    );
    println!("fixture PID {pid} reaped; pipes closed; permit restored");
}

async fn interrupt_active_child(store: Arc<dyn Store>, accepted: bool) {
    timeout(FIXTURE_BOUND, async {
        let probes = Probes::new(true);
        let agent = agent_builder(store.clone(), probes.clone(), accepted)
            .build()
            .unwrap();
        let mut run = agent.prompt(None, "reduce fixture output").await.unwrap();
        let session = run.session_id().clone();
        let run_id = run.run_id().clone();
        let mut events = run.events();
        let pid = probes.ready().await;
        assert_ne!(pid, 0);
        assert_eq!(probes.permit_count(), 0);
        interrupt_and_observe(&run, &probes, accepted).await;
        assert_held_phase(store.as_ref(), &session, &run_id).await;
        probes.release_reap();
        timeout(INTERRUPT_SETTLEMENT_BOUND, async {
            assert_eq!(run.done().await.unwrap().status, RunStatus::Interrupted);
        })
        .await
        .expect("durable interrupt settlement after actual cleanup");
        assert_settlement(
            store.as_ref(),
            &session,
            &run_id,
            &probes,
            if accepted {
                REDACTED_OUTPUT
            } else {
                INTERRUPTED_RESULT_TEXT
            },
        )
        .await;
        assert_eq!(probes.redacted.observed(), accepted);
        let mut live_settlements = 0;
        while let Some(event) = events.recv().await.unwrap() {
            let serialized = serde_json::to_string(&event).unwrap();
            assert!(!serialized.contains(RAW_OUTPUT));
            if !accepted {
                assert!(!serialized.contains(REDACTED_OUTPUT));
            }
            if event.kind == EventKind::ToolCallSettled {
                live_settlements += 1;
                assert_eq!(event.payload["is_error"], true);
                assert_eq!(event.payload["status"], "interrupted");
                let text = if accepted {
                    serde_json::to_string(REDACTED_OUTPUT).unwrap()
                } else {
                    INTERRUPTED_RESULT_TEXT.into()
                };
                assert_eq!(
                    event.payload["content"],
                    serde_json::json!([ContentBlock::Text { text }])
                );
            }
        }
        assert_eq!(live_settlements, 1);
        agent.close_extensions().await.unwrap();
        assert_drained(&probes, pid).await;
    })
    .await
    .expect("active child interruption timeout");
}

#[tokio::test]
async fn run_interrupt_drops_reducer_and_settles_fixed_text() {
    interrupt_active_child(Arc::new(MemoryStore::new()), false).await;
}

#[tokio::test]
async fn run_interrupt_persists_only_accepted_final_redaction() {
    interrupt_active_child(Arc::new(MemoryStore::new()), true).await;
}

#[cfg(feature = "postgres")]
async fn postgres_interrupt(accepted: bool) {
    static MIGRATED: tokio::sync::OnceCell<()> = tokio::sync::OnceCell::const_new();
    let Ok(url) = std::env::var("CRABBER_TEST_POSTGRES_URL") else {
        assert_ne!(
            std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref(),
            Ok("1"),
            "CRABBER_TEST_POSTGRES_URL is required"
        );
        eprintln!("PostgreSQL interruption skipped: CRABBER_TEST_POSTGRES_URL unset");
        return;
    };
    // Finish shared DDL before either parallel fixture starts admitting work.
    MIGRATED
        .get_or_init(|| async {
            crabber::session::PostgresStore::migrate(&url)
                .await
                .unwrap();
        })
        .await;
    let store = crabber::session::PostgresStore::connect(&url)
        .await
        .unwrap();
    interrupt_active_child(Arc::new(store), accepted).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_run_interrupt_drops_reducer_and_settles_fixed_text() {
    postgres_interrupt(false).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_run_interrupt_persists_only_accepted_final_redaction() {
    postgres_interrupt(true).await;
}

#[derive(Default)]
struct CloseCapture(Mutex<Vec<MountCloseTimeout>>);
impl Observer for CloseCapture {
    fn emit(&self, _: &EventRecord) {}
    fn mount_close_timed_out(&self, timeout: &MountCloseTimeout) {
        self.0.lock().unwrap().push(timeout.clone());
    }
}

#[tokio::test]
async fn close_timeout_retains_child_reaper_and_terminal_registry() {
    timeout(FIXTURE_BOUND, async {
        let probes = Probes::new(true);
        let store = Arc::new(MemoryStore::new());
        let observer = Arc::new(CloseCapture::default());
        let bound = DEFAULT_MOUNT_CLOSE_TIMEOUT / 100;
        let agent = agent_builder(store.clone(), probes.clone(), false)
            .observer(observer.clone())
            .extension_close_timeout(bound)
            .build()
            .unwrap();
        let run = agent.prompt(None, "reduce fixture output").await.unwrap();
        let session = run.session_id().clone();
        let run_id = run.run_id().clone();
        let pid = probes.ready().await;
        run.interrupt();
        probes.callback_dropped.wait().await;
        probes.kill_started.wait().await;
        assert_held_phase(store.as_ref(), &session, &run_id).await;
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        assert_eq!(probes.permit_count(), 0);
        let error = timeout(FIXTURE_BOUND, run.done())
            .await
            .expect("bounded incomplete native phase")
            .unwrap_err();
        let RuntimeError::PhaseOutcomeUnknown { phase, .. } = error else {
            panic!("held child must leave a native uncertain phase");
        };
        let phase = store.tool_phase(&run_id, &phase.id).await.unwrap().phase;
        assert_eq!(phase.state, crabber::session::PhaseState::Uncertain);
        assert_eq!(
            phase.quiescence,
            crabber::session::PhaseQuiescence::Incomplete
        );
        assert_eq!(
            store.get_run(&run_id).await.unwrap().unwrap().status,
            RunStatus::Running
        );
        let calls = store.list_unfinished_tool_calls(&run_id).await.unwrap();
        assert_eq!(calls.len(), 1);
        assert!(calls[0].result.is_none());
        let retained = agent.drain_execution_tasks(Duration::ZERO).await;
        assert!(retained.resources.iter().any(
            |scope| scope.phase.as_ref() == Some(&phase.target) && scope.resources.pending == 1
        ));
        // Real readiness and kill initiation precede the deterministic close clock.
        tokio::time::pause();
        let started = Instant::now();
        let Err(ExtensionError::MountCloseTimeout { extension }) =
            timeout(INTERRUPT_SETTLEMENT_BOUND, agent.close_extensions())
                .await
                .expect("bounded close timeout")
        else {
            panic!("typed mount close timeout")
        };
        // Tokio rounds timer deadlines up to its millisecond wheel tick.
        assert!(started.elapsed() >= bound);
        assert!(started.elapsed() <= bound + Duration::from_millis(1));
        assert_eq!(extension, "fixture-reducer");
        let captured = observer.0.lock().unwrap().clone();
        assert_eq!(captured.len(), 1);
        let close = &captured[0];
        assert_eq!(close.extension, extension);
        assert_eq!(close.bound, bound);
        // The run worker has returned, but the same actual child remains
        // tracked by both its native phase owner and its extension mount.
        assert_eq!(close.pending_tasks, 1);
        assert!(matches!(
            agent.prompt(None, "closed registry").await,
            Err(RuntimeError::HostClosed)
        ));
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        assert_eq!(probes.permit_count(), 0);
        tokio::time::resume();
        probes.release_reap();
        assert_drained(&probes, pid).await;
        let joined = agent.drain_execution_tasks(FIXTURE_BOUND).await;
        assert!(
            joined
                .resources
                .iter()
                .all(|scope| scope.resources.pending == 0)
        );
        // A later local join cannot retrospectively assert a durable outcome.
        assert_eq!(
            store
                .tool_phase(&run_id, &phase.target.id)
                .await
                .unwrap()
                .phase
                .quiescence,
            crabber::session::PhaseQuiescence::Incomplete
        );
        assert!(matches!(
            agent.prompt(None, "still closed").await,
            Err(RuntimeError::HostClosed)
        ));
    })
    .await
    .expect("close timeout fixture bound");
}

#[tokio::test]
async fn idle_mount_close_joins_and_returns_ok() {
    let probes = Probes::new(false);
    let agent = agent_builder(Arc::new(MemoryStore::new()), probes.clone(), false)
        .build()
        .unwrap();
    let run = agent.prompt(None, "reduce fixture output").await.unwrap();
    let pid = timeout(FIXTURE_BOUND, probes.ready())
        .await
        .expect("idle fixture readiness");
    probes.release_callback();
    timeout(FIXTURE_BOUND, run.done())
        .await
        .expect("idle run completion")
        .unwrap();
    timeout(FIXTURE_BOUND, agent.close_extensions())
        .await
        .expect("idle close bound")
        .unwrap();
    assert_eq!(probes.permit_count(), 1);
    assert!(probes.kill_started.observed());
    assert_drained(&probes, pid).await;
}

#[cfg(feature = "sqlite")]
async fn sqlite_interrupt(accepted: bool) {
    let directory = tempfile::tempdir().unwrap();
    let path = directory.path().join("interrupt.sqlite");
    crabber::session::SqliteStore::migrate(&path).await.unwrap();
    let store = crabber::session::SqliteStore::connect(&path).await.unwrap();
    interrupt_active_child(Arc::new(store), accepted).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_run_interrupt_drops_reducer_and_settles_fixed_text() {
    sqlite_interrupt(false).await;
}

#[cfg(feature = "sqlite")]
#[tokio::test]
async fn sqlite_run_interrupt_persists_only_accepted_final_redaction() {
    sqlite_interrupt(true).await;
}
