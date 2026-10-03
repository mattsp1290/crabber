//! Runtime settlement checks for crabber-v1we; the full driver race matrix is crabber-38nc.

use super::*;
use crate::{INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT};
use crabber_core::{RunStatus, ToolResultStatus};
use crabber_extension::{EventPublished, ToolSettled, TransformOutput};
use tokio::time::{Instant, timeout};

fn settlement_notifications(log: Arc<Mutex<Vec<(String, Value)>>>) -> Arc<ClosureExtension> {
    ClosureExtension::new("settlement-notifications", move |r| {
        for point in [EventPublished::ID, ToolSettled::ID] {
            let log = log.clone();
            r.on_notify(
                point,
                0,
                point,
                Arc::new(move |value| {
                    let log = log.clone();
                    Box::pin(async move {
                        log.lock().unwrap().push((point.to_owned(), value.clone()));
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
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(
            settlement_notifications(notifications.clone()),
            Scope::Global,
        )
        .build()
        .await;
    let gate = harness.block_results();
    let handle = harness.start().await;
    gate.entered().await;
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &call.id, INTERRUPTED_RESULT_TEXT).await;
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
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(redactor, Scope::Global)
        .mount(
            settlement_notifications(notifications.clone()),
            Scope::Global,
        )
        .build()
        .await;
    let handle = harness.start().await;
    entered.entered().await;
    entered.release_all();
    let done = interrupt_and_finish(&harness, handle).await;
    assert_interrupted(&done, &call.id, r#"{"redacted":true}"#).await;
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
