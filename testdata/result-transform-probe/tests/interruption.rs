//! Public-facade proof that a real `RunHandle::interrupt` stops result
//! dispatch while a reducer-owned child process is active, settles the call
//! `Interrupted` without unredacted output, and that mount close joins the
//! extension-owned cleanup.

mod interruption_support;

use crabber::{
    core::{ContentBlock, EventKind, RunStatus, SessionId, ToolCallStatus, ToolResultStatus},
    extension::FINAL_REDACTION_DEADLINE,
    runtime::{INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use interruption_support::{Probes, RAW_OUTPUT, REDACTED_OUTPUT, agent_builder};
use std::{sync::Arc, time::Duration};
use tokio::time::{Instant, timeout};

const FIXTURE_BOUND: Duration = Duration::from_secs(10);

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
        let interrupted = Instant::now();
        run.interrupt();
        let status = timeout(INTERRUPT_SETTLEMENT_BOUND, run.done())
            .await
            .expect("durable interrupt settlement bound")
            .unwrap()
            .status;
        // Only the runtime's settlement is timed; the reads below are the
        // probe's own work and must not count against the runtime's bounds.
        let settled_after = interrupted.elapsed();
        assert_eq!(status, RunStatus::Interrupted);
        assert!(settled_after <= INTERRUPT_SETTLEMENT_BOUND);
        if accepted {
            assert!(settled_after <= FINAL_REDACTION_DEADLINE);
        }
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
            let serialized = serde_json::to_string(&*event).unwrap();
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
async fn run_interrupt_with_active_child_settles_interrupted_fixed_text() {
    interrupt_active_child(Arc::new(MemoryStore::new()), false).await;
}

#[tokio::test]
async fn run_interrupt_after_accepted_fallback_persists_only_final_redaction() {
    interrupt_active_child(Arc::new(MemoryStore::new()), true).await;
}

/// `CRABBER_TEST_POSTGRES_URL` selects an existing disposable database.
/// Without it the test starts its own `postgres:14` container. There is no
/// skip path: with neither, the test fails.
#[cfg(feature = "postgres")]
async fn postgres_interrupt(accepted: bool) {
    use testcontainers_modules::{
        postgres::Postgres,
        testcontainers::{ImageExt, runners::AsyncRunner},
    };
    let mut container = None;
    // An empty value counts as unset rather than as a malformed URL.
    let configured = std::env::var("CRABBER_TEST_POSTGRES_URL")
        .ok()
        .filter(|url| !url.is_empty());
    let url = match configured {
        Some(url) => {
            println!("PostgreSQL interruption proof: using CRABBER_TEST_POSTGRES_URL");
            url
        }
        None => {
            let node = Postgres::default().with_tag("14").start().await.expect(
                "set CRABBER_TEST_POSTGRES_URL or run Docker so the probe can start postgres:14",
            );
            // The Docker host is not always this machine (remote DOCKER_HOST).
            let host = node.get_host().await.unwrap();
            let port = node.get_host_port_ipv4(5432).await.unwrap();
            container = Some(node);
            println!("PostgreSQL interruption proof: started postgres:14 on {host}:{port}");
            // Default superuser of the throwaway container, not a credential.
            format!("postgres://postgres:postgres@{host}:{port}/postgres")
        }
    };
    crabber::session::PostgresStore::migrate(&url)
        .await
        .unwrap();
    let store = crabber::session::PostgresStore::connect(&url)
        .await
        .unwrap();
    interrupt_active_child(Arc::new(store), accepted).await;
    // Dropping the handle removes the container.
    drop(container);
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_run_interrupt_with_active_child_settles_interrupted_fixed_text() {
    postgres_interrupt(false).await;
}

#[cfg(feature = "postgres")]
#[tokio::test]
async fn postgres_run_interrupt_after_accepted_fallback_persists_only_final_redaction() {
    postgres_interrupt(true).await;
}
