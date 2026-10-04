//! Runtime settlement checks for crabber-v1we; the full driver race matrix is crabber-38nc.

use super::*;
use crate::{INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT};
use crabber_core::{RunStatus, ToolResultStatus};
use crabber_extension::{EventPublished, ToolSettled, TransformOutput};
use tokio::time::{Instant, timeout};

fn settlement_notifications(
    log: Arc<Mutex<Vec<(String, Value)>>>,
    ready: Arc<Semaphore>,
) -> Arc<ClosureExtension> {
    ClosureExtension::new("settlement-notifications", move |r| {
        for point in [EventPublished::ID, ToolSettled::ID] {
            let log = log.clone();
            let ready = ready.clone();
            r.on_notify(
                point,
                0,
                point,
                Arc::new(move |value| {
                    let log = log.clone();
                    let ready = ready.clone();
                    Box::pin(async move {
                        log.lock().unwrap().push((point.to_owned(), value.clone()));
                        if point == ToolSettled::ID || value["kind"] == "tool_call_settled" {
                            ready.add_permits(1);
                        }
                        Ok(value)
                    })
                }),
            );
        }
    })
}

async fn interrupt_and_finish(harness: &Harness, handle: RunHandle) -> Finished<'_> {
    let session_id = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    let cancelled_at = Instant::now();
    handle.interrupt();
    let result = timeout(INTERRUPT_SETTLEMENT_BOUND, handle.done())
        .await
        .expect("interruption must settle within the named bound")
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert!(cancelled_at.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
    assert_eq!(
        harness
            .store
            .list_unfinished_tool_calls(&run_id)
            .await
            .unwrap(),
        [] as [ToolCallRecord; 0]
    );
    Finished {
        harness,
        session_id,
        run_id,
    }
}

async fn assert_interrupted(done: &Finished<'_>, call: &ToolCallId, expected: &str) {
    let record = done.record(call).await;
    assert_eq!(record.status, ToolCallStatus::Interrupted);
    assert_eq!(
        record.result.as_ref().unwrap().status,
        ToolResultStatus::Interrupted
    );
    assert_eq!(record_text(&record), expected);
    let (_, text, is_error) = done.tool_message(call).await;
    assert_eq!(text, expected);
    assert!(is_error);
    let event = done.settled_event(call).await;
    assert_eq!(event.payload["is_error"], true);
    assert_eq!(event.payload["content"][0]["text"], expected);
    let events = done
        .harness
        .store
        .list_events(&done.session_id, None, 1000)
        .await
        .unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|event| event.kind == EventKind::ToolCallSettled
                && event.correlation.as_deref() == Some(call.to_string().as_str()))
            .count(),
        1
    );
}

#[tokio::test]
async fn blocked_result_transform_interrupts_with_fixed_text_within_bound() {
    let call = ScriptedCall::text(ECHO, "secret");
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(Semaphore::new(0));
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(
            settlement_notifications(notifications.clone(), ready.clone()),
            Scope::Global,
        )
        .build()
        .await;
    let gate = harness.block_results();
    let handle = harness.start().await;
    gate.entered().await;
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &call.id, INTERRUPTED_RESULT_TEXT).await;
    timeout(INTERRUPT_SETTLEMENT_BOUND, ready.acquire_many(1))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let notifications = notifications.lock().unwrap();
    assert!(
        notifications
            .iter()
            .any(|(point, value)| point == EventPublished::ID
                && value["kind"] == "tool_call_settled")
    );
    assert!(
        !notifications
            .iter()
            .any(|(point, _)| point == ToolSettled::ID)
    );
}

#[tokio::test]
async fn accepted_final_redaction_settles_interrupted_with_json_and_notifications() {
    let call = ScriptedCall::text(ECHO, "secret");
    let gate = Gate::all();
    let entered = gate.clone();
    let redactor = ClosureExtension::new("interrupt-redactor", move |r| {
        let gate = gate.clone();
        r.on_final_redaction(
            0,
            "protect",
            Arc::new(move |context, value| {
                let gate = gate.clone();
                Box::pin(async move {
                    gate.pass(&value).await;
                    context.cancellation().cancelled().await;
                    Ok(TransformOutput::new(json!({"redacted": true})))
                })
            }),
        );
    });
    let notifications = Arc::new(Mutex::new(Vec::new()));
    let ready = Arc::new(Semaphore::new(0));
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(redactor, Scope::Global)
        .mount(
            settlement_notifications(notifications.clone(), ready.clone()),
            Scope::Global,
        )
        .build()
        .await;
    let handle = harness.start().await;
    entered.entered().await;
    entered.release_all();
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &call.id, r#"{"redacted":true}"#).await;
    timeout(INTERRUPT_SETTLEMENT_BOUND, ready.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    let notifications = notifications.lock().unwrap();
    assert!(
        notifications
            .iter()
            .any(|(point, value)| point == EventPublished::ID
                && value["kind"] == "tool_call_settled")
    );
    assert_eq!(
        notifications
            .iter()
            .filter(|(point, _)| point == ToolSettled::ID)
            .count(),
        1
    );
}

#[tokio::test]
async fn parallel_interruption_settles_out_of_order_without_deadlock() {
    let first = ScriptedCall::text(ECHO, "first-secret");
    let second = ScriptedCall::text(SHOUT, "second-secret");
    let gate = Gate::all();
    let entered = gate.clone();
    let second_id = second.id.clone();
    let redactor = ClosureExtension::new("parallel-interrupt-redactor", move |r| {
        let gate = gate.clone();
        r.on_final_redaction(
            0,
            "protect",
            Arc::new(move |context, _| {
                let gate = gate.clone();
                Box::pin(async move {
                    gate.pass(&json!(context.call_id())).await;
                    context.cancellation().cancelled().await;
                    Ok(TransformOutput::new(json!("protected")))
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&first, &second]))
        .execution_mode(ExecutionMode::Parallel { max: 2 })
        .mount(redactor, Scope::Global)
        .build()
        .await;
    let handle = harness.start().await;
    entered.entered().await;
    entered.entered().await;
    entered.release_where(|value| value == &json!(second_id));
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &first.id, INTERRUPTED_RESULT_TEXT).await;
    assert_interrupted(&done, &second.id, r#""protected""#).await;
}

fn cancellation_redactor(first_id: ToolCallId, started: Arc<Semaphore>) -> Arc<ClosureExtension> {
    ClosureExtension::new("cancellation-redactor", move |r| {
        let first_id = first_id.clone();
        let started = started.clone();
        r.on_final_redaction(
            0,
            "protect",
            Arc::new(move |context, _| {
                let first_id = first_id.clone();
                let started = started.clone();
                Box::pin(async move {
                    started.add_permits(1);
                    if context.call_id() == &first_id {
                        context.cancellation().cancelled().await;
                    }
                    Ok(TransformOutput::new(json!("protected")))
                })
            }),
        );
    })
}

fn blocked_settlement_notifier(
    first_id: ToolCallId,
    gate: Arc<Gate>,
    blocked_point: &'static str,
    captured: Arc<Mutex<Vec<(String, Value)>>>,
    delivered: Arc<Semaphore>,
) -> Arc<ClosureExtension> {
    ClosureExtension::new("blocked-settlement-notifier", move |r| {
        for point in [EventPublished::ID, ToolSettled::ID] {
            let gate = gate.clone();
            let first_id = first_id.clone();
            let captured = captured.clone();
            let delivered = delivered.clone();
            r.on_notify(
                point,
                0,
                point,
                Arc::new(move |value| {
                    let gate = gate.clone();
                    let first_id = first_id.clone();
                    let captured = captured.clone();
                    let delivered = delivered.clone();
                    Box::pin(async move {
                        if point == ToolSettled::ID || value["kind"] == "tool_call_settled" {
                            if point == blocked_point
                                && value["payload"]["call_id"] == json!(first_id)
                            {
                                gate.pass(&value).await;
                            }
                            captured
                                .lock()
                                .unwrap()
                                .push((point.to_owned(), value.clone()));
                            delivered.add_permits(1);
                        }
                        Ok(value)
                    })
                }),
            );
        }
    })
}

#[tokio::test]
async fn interrupted_notifications_cannot_block_sibling_settlement_and_retain_plan_lease() {
    for (protected, blocked_point) in [
        (true, ToolSettled::ID),
        (true, EventPublished::ID),
        (false, EventPublished::ID),
    ] {
        let first = ScriptedCall::text(ECHO, "first-secret");
        let second = ScriptedCall::text(SHOUT, "second-secret");
        let first_id = first.id.clone();
        let started = Arc::new(Semaphore::new(0));
        let gate = Gate::all();
        let entered = gate.clone();
        let notifications = Arc::new(Mutex::new(Vec::new()));
        let captured = notifications.clone();
        let completed = Arc::new(Semaphore::new(0));
        let delivered = completed.clone();
        let notifier =
            blocked_settlement_notifier(first_id, gate, blocked_point, captured, delivered);
        let mut builder = Harness::builder(one_turn(&[&first, &second]))
            .execution_mode(ExecutionMode::Parallel { max: 2 })
            .mount(notifier, Scope::Global);
        if protected {
            builder = builder.mount(
                cancellation_redactor(first.id.clone(), started.clone()),
                Scope::Global,
            );
        }
        let harness = builder.build().await;
        let ordinary_gate = (!protected).then(|| harness.block_results());
        let handle = harness.start().await;
        if let Some(gate) = ordinary_gate {
            gate.entered().await;
            gate.entered().await;
        } else {
            started.acquire_many(2).await.unwrap().forget();
        }
        let cancelled_at = Instant::now();
        handle.interrupt();
        timeout(INTERRUPT_SETTLEMENT_BOUND, entered.entered())
            .await
            .unwrap();
        let done = interrupt_and_finish(&harness, handle).await;
        assert!(cancelled_at.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
        assert_interrupted(
            &done,
            &first.id,
            if protected {
                r#""protected""#
            } else {
                INTERRUPTED_RESULT_TEXT
            },
        )
        .await;
        assert_interrupted(&done, &second.id, INTERRUPTED_RESULT_TEXT).await;
        let close = harness.registry.close_all();
        tokio::pin!(close);
        assert!(
            timeout(std::time::Duration::from_millis(25), &mut close)
                .await
                .is_err(),
            "blocked callback must retain the mount plan lease"
        );
        entered.release_all();
        timeout(INTERRUPT_SETTLEMENT_BOUND, &mut close)
            .await
            .unwrap()
            .unwrap();
        completed
            .acquire_many(if protected { 3 } else { 2 })
            .await
            .unwrap()
            .forget();
        let notifications = notifications.lock().unwrap();
        assert_eq!(
            notifications
                .iter()
                .filter(|(point, _)| point == EventPublished::ID)
                .count(),
            2
        );
        assert_eq!(
            notifications
                .iter()
                .filter(|(point, _)| point == ToolSettled::ID)
                .count(),
            usize::from(protected)
        );
    }
}

#[tokio::test]
async fn delayed_protected_notifications_read_seeded_state_and_reject_terminal_writes() {
    const EXTENSION: &str = "delayed-state-notifier";
    let call = ScriptedCall::text(ECHO, "secret");
    let gate = Gate::all();
    let entered = gate.clone();
    let observations = Arc::new(Mutex::new(Vec::new()));
    let observed = observations.clone();
    let ready = Arc::new(Semaphore::new(0));
    let delivered = ready.clone();
    let notifier = ClosureExtension::new(EXTENSION, move |r| {
        for point in [EventPublished::ID, ToolSettled::ID] {
            let gate = gate.clone();
            let observed = observed.clone();
            let delivered = delivered.clone();
            r.on_notify(
                point,
                0,
                point,
                Arc::new(move |value| {
                    let gate = gate.clone();
                    let observed = observed.clone();
                    let delivered = delivered.clone();
                    Box::pin(async move {
                        if value["kind"] == "run_started" {
                            crabber_extension::current_state_sink()
                                .unwrap()
                                .apply(EXTENSION, vec![("seed".into(), Some("before".into()))])
                                .await
                                .unwrap();
                        } else if value["kind"] == "tool_call_settled" {
                            gate.pass(&value).await;
                            let sink = crabber_extension::current_state_sink()
                                .expect("detached protected notifications retain the state sink");
                            let snapshot = sink.snapshot(EXTENSION).await.unwrap();
                            let applied = sink
                                .apply(EXTENSION, vec![("seed".into(), Some("after".into()))])
                                .await;
                            observed
                                .lock()
                                .unwrap()
                                .push((point, snapshot, applied.clone()));
                            delivered.add_permits(1);
                            return applied.map(|()| value).map_err(ExtensionError::Tool);
                        }
                        Ok(value)
                    })
                }),
            );
        }
    });
    let started = Arc::new(Semaphore::new(0));
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(notifier, Scope::Global)
        .mount(
            cancellation_redactor(call.id.clone(), started.clone()),
            Scope::Global,
        )
        .build()
        .await;
    let handle = harness.start().await;
    started.acquire().await.unwrap().forget();
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &call.id, r#""protected""#).await;
    for _ in 0..2 {
        timeout(INTERRUPT_SETTLEMENT_BOUND, entered.entered())
            .await
            .unwrap();
        entered.release_all();
        timeout(INTERRUPT_SETTLEMENT_BOUND, ready.acquire())
            .await
            .unwrap()
            .unwrap()
            .forget();
    }
    let persisted = harness
        .store
        .get_extension_state(EXTENSION, &done.session_id)
        .await
        .unwrap();
    let observations = observations.lock().unwrap();
    assert_eq!(observations.len(), 2);
    assert_eq!(observations[0].0, EventPublished::ID);
    assert_eq!(observations[1].0, ToolSettled::ID);
    for (_, snapshot, applied) in observations.iter() {
        assert_eq!(snapshot.get("seed").map(String::as_str), Some("before"));
        assert_eq!(snapshot, &persisted);
        assert_eq!(
            applied,
            &Err(crabber_session::StoreError::Conflict.to_string())
        );
    }
}
