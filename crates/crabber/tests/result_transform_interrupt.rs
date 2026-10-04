mod result_transform_support;

use crabber::{
    ExtensionError, Observer, RuntimeError,
    core::{ContentBlock, EventRecord, RunStatus, SessionId, ToolCallStatus, ToolResultStatus},
    extension::{FINAL_REDACTION_DEADLINE, MountCloseTimeout},
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

async fn assert_settlement(
    store: &MemoryStore,
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

async fn interrupt_active_child(accepted: bool) {
    timeout(FIXTURE_BOUND, async {
        let probes = Probes::new(true);
        let store = Arc::new(MemoryStore::new());
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
        let interrupted = Instant::now();
        run.interrupt();
        timeout(INTERRUPT_SETTLEMENT_BOUND, async {
            assert_eq!(run.done().await.unwrap().status, RunStatus::Interrupted);
            assert_settlement(
                &store,
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
        })
        .await
        .expect("durable interrupt settlement bound");
        assert!(interrupted.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
        assert_eq!(probes.redacted.observed(), accepted);
        if accepted {
            assert!(interrupted.elapsed() <= FINAL_REDACTION_DEADLINE);
        }
        while let Some(event) = events.recv().await.unwrap() {
            assert!(!serde_json::to_string(&event).unwrap().contains(RAW_OUTPUT));
        }
        probes.callback_dropped.wait().await;
        probes.kill_started.wait().await;
        // Cleanup outlives the dropped callback and cannot release before wait().
        assert_eq!(probes.permit_count(), 0);
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        probes.release_reap();
        agent.close_extensions().await.unwrap();
        assert_drained(&probes, pid).await;
    })
    .await
    .expect("active child interruption timeout");
}

#[tokio::test]
async fn run_interrupt_drops_reducer_and_settles_fixed_text() {
    interrupt_active_child(false).await;
}

#[tokio::test]
async fn run_interrupt_persists_only_accepted_final_redaction() {
    interrupt_active_child(true).await;
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
        let bound = Duration::from_millis(50);
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
        timeout(INTERRUPT_SETTLEMENT_BOUND, async {
            assert_eq!(run.done().await.unwrap().status, RunStatus::Interrupted);
            assert_settlement(&store, &session, &run_id, &probes, INTERRUPTED_RESULT_TEXT).await;
        })
        .await
        .expect("durable interrupt settlement bound");
        probes.callback_dropped.wait().await;
        probes.kill_started.wait().await;
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        assert_eq!(probes.permit_count(), 0);
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
        assert_eq!(close.pending_tasks, 1);
        assert!(matches!(
            agent.prompt(None, "closed registry").await,
            Err(RuntimeError::Extension(_))
        ));
        assert!(!probes.reaped.observed());
        assert!(!probes.pipe_closed.observed());
        assert_eq!(probes.permit_count(), 0);
        tokio::time::resume();
        probes.release_reap();
        assert_drained(&probes, pid).await;
        assert!(matches!(
            agent.prompt(None, "still closed").await,
            Err(RuntimeError::Extension(_))
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
