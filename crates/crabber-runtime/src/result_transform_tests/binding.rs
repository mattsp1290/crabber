//! Independently mounted transforms bind to durable tool identity and normalized input.

use super::*;
use crabber_extension::{ToolInput, ToolOutcomeClass, TransformOutput};
use tokio::sync::mpsc;

async fn exact_binding(mode: ExecutionMode) {
    let first = ScriptedCall::text(ECHO, "leave-alone");
    let matching = ScriptedCall::text(ECHO, "match");
    let other = ScriptedCall::text(SHOUT, "match");
    let calls = [&first, &matching, &other];
    let seen = Arc::new(Mutex::new(Vec::<ToolResultContext>::new()));
    let gate = Gate::all();
    let (completed, mut completions) = mpsc::unbounded_channel();
    let binding = ClosureExtension::new("exact-result-binding", {
        let seen = Arc::clone(&seen);
        let gate = Arc::clone(&gate);
        move |r| {
            let seen = Arc::clone(&seen);
            let gate = Arc::clone(&gate);
            let completed = completed.clone();
            r.on_result_transform(
                1,
                "bind-echo-match",
                Arc::new(move |context, result| {
                    let seen = Arc::clone(&seen);
                    let gate = Arc::clone(&gate);
                    let completed = completed.clone();
                    Box::pin(async move {
                        seen.lock().unwrap().push(context.clone());
                        gate.pass(&json!(context.call_id().to_string())).await;
                        let result = if context.tool_name() == ECHO
                            && context.input() == &ToolInput::Normalized(json!({"text": "match"}))
                        {
                            json!({"bound": "changed"})
                        } else {
                            result
                        };
                        completed.send(context.call_id().clone()).unwrap();
                        Ok(TransformOutput::new(result))
                    })
                }),
            );
        }
    });
    let harness = Harness::builder(one_turn(&calls))
        .mount(binding, Scope::Global)
        .execution_mode(mode)
        .build()
        .await;
    let handle = harness.start().await;
    let session_id = handle.session_id().clone();
    let run_id = handle.run_id().clone();

    match mode {
        ExecutionMode::Parallel { .. } => {
            let mut arrivals = Vec::new();
            for _ in &calls {
                arrivals.push(gate.entered().await);
            }
            assert_eq!(gate.parked(), calls.len());
            for call in &calls {
                assert_eq!(
                    arrivals
                        .iter()
                        .filter(|key| **key == json!(call.id.to_string()))
                        .count(),
                    1,
                    "each call reached the independently mounted transform"
                );
            }
            // A later callback completes before the first, independently of arrival order.
            for call in calls.iter().rev() {
                assert_eq!(
                    gate.release_where(|key| *key == json!(call.id.to_string())),
                    1
                );
                assert_eq!(completions.recv().await.unwrap(), call.id);
            }
        }
        ExecutionMode::Sequential => {
            // Sequential cannot start a later call while the first callback is parked.
            for call in &calls {
                assert_eq!(gate.entered().await, json!(call.id.to_string()));
                assert_eq!(gate.parked(), 1);
                assert_eq!(gate.release_all(), 1);
                assert_eq!(completions.recv().await.unwrap(), call.id);
            }
        }
    }
    assert_eq!(gate.parked(), 0);
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    let done = Finished {
        harness: &harness,
        session_id,
        run_id,
    };
    let run = harness.store.get_run(&done.run_id).await.unwrap().unwrap();
    assert_eq!(run.id, done.run_id);
    assert_eq!(run.session_id, done.session_id);
    let contexts = seen.lock().unwrap().clone();
    assert_eq!(contexts.len(), calls.len());
    assert_eq!(harness.probe.executed().len(), calls.len());
    let expected = [
        json!({"text": "leave-alone"}),
        json!({"bound": "changed"}),
        json!({"shouted": "MATCH"}),
    ];
    for (call, output) in calls.iter().zip(expected) {
        let record = done.record(&call.id).await;
        let matching_contexts: Vec<_> = contexts
            .iter()
            .filter(|context| context.call_id() == &record.id)
            .collect();
        assert_eq!(matching_contexts.len(), 1);
        let context = matching_contexts[0];
        assert_eq!(record.id, call.id);
        assert_eq!(record.name, call.name);
        assert_eq!(
            record.arguments,
            serde_json::from_str::<Value>(&call.arguments).unwrap()
        );
        assert_eq!(context.tool_name(), record.name);
        assert_eq!(
            context.input(),
            &ToolInput::Normalized(record.arguments.clone())
        );
        assert_eq!(context.call_id(), &record.id);
        assert_eq!(context.run_id(), &record.run_id);
        assert_eq!(context.run_id(), &run.id);
        assert_eq!(context.session_id(), &run.session_id);
        assert!(context.resolved());
        assert_eq!(context.class(), ToolOutcomeClass::Succeeded);
        assert!(!context.is_error());
        assert_settled(
            &done,
            &record.id,
            ToolCallStatus::Completed,
            &serde_json::to_string(&output).unwrap(),
            false,
        )
        .await;
        let (message, _, _) = done.tool_message(&record.id).await;
        assert_eq!(message.session_id, run.session_id);
    }
}

#[tokio::test]
async fn parallel_binding_survives_reverse_callback_completion() {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        exact_binding(ExecutionMode::Parallel { max: 3 }),
    )
    .await
    .expect("parallel callbacks and run complete without deadlock");
}

#[tokio::test]
async fn sequential_binding_preserves_each_calls_context_and_output() {
    tokio::time::timeout(
        std::time::Duration::from_secs(10),
        exact_binding(ExecutionMode::Sequential),
    )
    .await
    .expect("sequential callbacks and run complete without deadlock");
}
