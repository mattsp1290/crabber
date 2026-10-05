//! JSON-envelope tampering fails closed before persistence (D2/D3/D4).

use super::*;
use crabber_core::{RunFence, ToolResultStatus};
use crabber_extension::{Callback, result_transform_failed_message};
use crabber_session::StoreError;
use std::sync::atomic::{AtomicUsize, Ordering};

const SEED: &str = "SECRET-EXECUTOR-SEED";
const AUTHORED: &str = "SECRET-HANDLER-AUTHORED";
const INTERMEDIATE: &str = "SECRET-ACCEPTED-INTERMEDIATE";
const SEEDED: &str = "tamper-seeded";
const HANDLER: &str = "reject-tampered-envelope";

struct SeededTool;
#[async_trait]
impl ToolExecutor for SeededTool {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(SEED))
    }
}

fn seeded_tool() -> Arc<ClosureExtension> {
    ClosureExtension::new("tamper-seed-tool", |r| {
        r.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: SEEDED.into(),
                description: "test".into(),
                parameters: json!({"type": "object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(SeededTool),
        }));
    })
}

/// Check complete protected artifacts, including event diagnostics and provider history.
/// Arguments deliberately contain no secret: authoritative input is retained independently.
async fn assert_protected(done: &Finished<'_>, call: &ToolCallId, handler: &str) {
    let fixed = result_transform_failed_message(handler);
    assert_eq!(fixed, format!("result transform failed: {handler}"));
    let text = serde_json::to_string(&fixed).unwrap();
    assert_settled(done, call, ToolCallStatus::Failed, &text, true).await;
    let record = done.record(call).await;
    let result = record.result.unwrap();
    assert_eq!(result.status, ToolResultStatus::Failed);
    let (message, _, _) = done.tool_message(call).await;
    let event = done.settled_event(call).await;
    for artifact in [
        serde_json::to_string(&result).unwrap(),
        serde_json::to_string(&message).unwrap(),
        serde_json::to_string(&event).unwrap(),
        format!("{:?}", done.next_request_for(call)),
    ] {
        for secret in [SEED, AUTHORED, INTERMEDIATE, "SECRET-DEPTH-ERROR"] {
            assert!(!artifact.contains(secret), "leaked {secret}: {artifact}");
        }
    }
}

fn failing_chain(callback: Callback, after: Arc<AtomicUsize>) -> Arc<ClosureExtension> {
    ClosureExtension::new("tamper-chain", move |r| {
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "accept-intermediate",
            Arc::new(|mut value| {
                Box::pin(async move {
                    value["result"] = json!(INTERMEDIATE);
                    Ok(value)
                })
            }),
        );
        r.on_transform(ToolResultTransform::ID, 2, HANDLER, Arc::clone(&callback));
        let after_ordinary = Arc::clone(&after);
        r.on_transform(
            ToolResultTransform::ID,
            3,
            "must-skip-ordinary",
            Arc::new(move |value| {
                after_ordinary.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(value) })
            }),
        );
        let after_final = Arc::clone(&after);
        r.on_final_redaction_json(
            -100,
            "must-skip-final",
            Arc::new(move |value| {
                after_final.fetch_add(1, Ordering::SeqCst);
                Box::pin(async move { Ok(value) })
            }),
        );
    })
}

#[tokio::test]
async fn every_context_field_is_immutable_and_stops_the_chain() {
    for (field, replacement) in [
        ("tool_name", json!(AUTHORED)),
        ("resolved", json!(false)),
        ("input", json!({"kind": "raw", "value": AUTHORED})),
        ("call_id", json!(ToolCallId::new().to_string())),
        ("session_id", json!(SessionId::new().to_string())),
        ("run_id", json!(RunId::new().to_string())),
        ("class", json!("permission_denied")),
        ("is_error", json!(true)),
        ("phase", json!("final_redaction")),
    ] {
        let call = ScriptedCall::new(SEEDED, &json!({}));
        let after = Arc::new(AtomicUsize::new(0));
        let observations = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&observations);
        let changed = replacement.clone();
        let callback: Callback = Arc::new(move |mut value| {
            let replacement = replacement.clone();
            let seen = Arc::clone(&seen);
            Box::pin(async move {
                let original = value.clone();
                value["context"][field] = replacement;
                value["result"] = json!(AUTHORED);
                seen.lock().unwrap().push((original, value.clone()));
                Ok(value)
            })
        });
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(seeded_tool(), Scope::Global)
            .mount(failing_chain(callback, Arc::clone(&after)), Scope::Global)
            .build()
            .await;
        let done = harness.run().await;
        assert_protected(&done, &call.id, HANDLER).await;
        let record = done.record(&call.id).await;
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed.len(), 1, "{field}");
        let (original, reply) = &observed[0];
        assert_eq!(original["result"], INTERMEDIATE, "{field}");
        assert_ne!(original["context"][field], changed, "{field}");
        assert_eq!(reply["context"][field], changed, "{field}");
        assert_eq!(reply["result"], AUTHORED, "{field}");
        assert_eq!(record.id, call.id, "{field}");
        assert_eq!(record.run_id, done.run_id, "{field}");
        assert_eq!(record.name, SEEDED, "{field}");
        assert_eq!(record.arguments, json!({}), "{field}");
        assert_eq!(after.load(Ordering::SeqCst), 0, "{field}");
    }
}

#[tokio::test]
async fn malformed_replies_errors_and_panics_persist_only_fixed_text() {
    for mode in [
        "missing-context",
        "missing-result",
        "missing-mark-error",
        "non-object",
        "old-style",
        "extra-is-error",
        "extra-cancellation",
        "nonbool-mark-error",
        "null-mark-error",
        "null-context",
        "error",
        "panic",
    ] {
        let observations = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&observations);
        let callback: Callback = Arc::new(move |mut value| {
            let seen = Arc::clone(&seen);
            Box::pin(async move {
                seen.lock().unwrap().push(value.clone());
                value["result"] = json!(AUTHORED);
                match mode {
                    "missing-context" => {
                        value.as_object_mut().unwrap().remove("context");
                    }
                    "missing-result" => {
                        value.as_object_mut().unwrap().remove("result");
                    }
                    "missing-mark-error" => {
                        value.as_object_mut().unwrap().remove("mark_error");
                    }
                    "non-object" => return Ok(json!([AUTHORED])),
                    "old-style" => return Ok(json!({"result": AUTHORED, "is_error": false})),
                    "extra-is-error" => value["is_error"] = json!(false),
                    "extra-cancellation" => value["cancellation"] = json!(false),
                    "nonbool-mark-error" => value["mark_error"] = json!(AUTHORED),
                    "null-mark-error" => value["mark_error"] = Value::Null,
                    "null-context" => value["context"] = Value::Null,
                    "error" => return Err(ExtensionError::Tool(AUTHORED.into())),
                    "panic" => panic!("{AUTHORED}"),
                    _ => unreachable!(),
                }
                Ok(value)
            })
        });
        let call = ScriptedCall::new(SEEDED, &json!({}));
        let after = Arc::new(AtomicUsize::new(0));
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(seeded_tool(), Scope::Global)
            .mount(failing_chain(callback, Arc::clone(&after)), Scope::Global)
            .build()
            .await;
        let done = harness.run().await;
        assert_protected(&done, &call.id, HANDLER).await;
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed.len(), 1, "{mode}");
        assert!(observed[0].is_object(), "{mode}");
        assert_eq!(observed[0]["result"], INTERMEDIATE, "{mode}");
        assert_eq!(after.load(Ordering::SeqCst), 0, "{mode}");
    }
}

#[tokio::test]
async fn mark_error_escalates_success_and_later_false_cannot_clear_it() {
    let call = ScriptedCall::text(ECHO, "safe-input");
    let transform = ClosureExtension::new("tamper-escalate", |r| {
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "mark-error",
            Arc::new(|mut value| {
                Box::pin(async move {
                    assert_eq!(value["context"]["class"], "succeeded");
                    assert_eq!(value["context"]["is_error"], false);
                    value["result"] = json!({"safe": true});
                    value["mark_error"] = json!(true);
                    Ok(value)
                })
            }),
        );
        r.on_transform(
            ToolResultTransform::ID,
            2,
            "unchanged-false",
            Arc::new(|value| {
                Box::pin(async move {
                    assert_eq!(value["context"]["class"], "succeeded");
                    assert_eq!(value["context"]["is_error"], true);
                    assert_eq!(value["mark_error"], false);
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(transform, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#"{"safe":true}"#,
        true,
    )
    .await;
    assert_eq!(
        done.record(&call.id).await.result.unwrap().status,
        ToolResultStatus::Failed
    );
}

#[tokio::test]
async fn false_mark_error_cannot_clear_any_error_class() {
    for (name, input, class) in [
        (FAIL, "safe-input", "execution_failed"),
        (FORBIDDEN, "safe-input", "permission_denied"),
        (MISSING, "safe-input", "unknown_tool"),
        (ECHO, PREPARE_REJECTED, "prepare_failed"),
    ] {
        let call = ScriptedCall::text(name, input);
        let transform = ClosureExtension::new("tamper-error-classes", move |r| {
            r.on_transform(
                ToolResultTransform::ID,
                1,
                "false-error",
                Arc::new(move |mut value| {
                    Box::pin(async move {
                        assert_eq!(value["context"]["class"], class);
                        assert_eq!(value["context"]["is_error"], true);
                        assert_eq!(value["mark_error"], false);
                        value["result"] = json!({"safe": true});
                        Ok(value)
                    })
                }),
            );
        });
        let harness = Harness::builder(one_turn(&[&call]))
            .mount(transform, Scope::Global)
            .build()
            .await;
        let done = harness.run().await;
        assert_settled(
            &done,
            &call.id,
            ToolCallStatus::Failed,
            r#"{"safe":true}"#,
            true,
        )
        .await;
        assert_eq!(
            done.record(&call.id).await.result.unwrap().status,
            ToolResultStatus::Failed
        );
    }
}

#[tokio::test]
async fn denied_calls_cannot_be_rewritten_into_execution_or_success() {
    for denial in ["guard", "restriction", "policy", "approval"] {
        let call = ScriptedCall::text(
            if matches!(denial, "policy" | "approval") {
                FORBIDDEN
            } else {
                ECHO
            },
            "safe-input",
        );
        let observations = Arc::new(Mutex::new(Vec::new()));
        let seen = Arc::clone(&observations);
        let transform = ClosureExtension::new("tamper-denial", move |r| {
            let seen = Arc::clone(&seen);
            r.on_transform(
                ToolResultTransform::ID,
                1,
                HANDLER,
                Arc::new(move |mut value| {
                    let seen = Arc::clone(&seen);
                    Box::pin(async move {
                        seen.lock().unwrap().push(value["context"].clone());
                        value["context"]["class"] = json!("succeeded");
                        value["context"]["is_error"] = json!(false);
                        value["context"]["tool_name"] = json!(SHOUT);
                        value["context"]["input"] =
                            json!({"kind": "normalized", "value": {"text": AUTHORED}});
                        value["result"] = json!(AUTHORED);
                        Ok(value)
                    })
                }),
            );
        });
        let builder = Harness::builder(one_turn(&[&call])).mount(transform, Scope::Global);
        let builder = match denial {
            "guard" => builder.mount(
                ClosureExtension::guard("tamper-deny-guard", Arc::new(DenyEcho)),
                Scope::Global,
            ),
            "restriction" => builder.mount(
                ClosureExtension::restrict_to("tamper-restrict", &[SHOUT]),
                Scope::Global,
            ),
            "approval" => builder
                .policy(Arc::new(AskAndRefuse))
                .approver(Arc::new(Refuse)),
            _ => builder,
        };
        let harness = builder.build().await;
        let done = harness.run().await;
        assert_protected(&done, &call.id, HANDLER).await;
        let observed = observations.lock().unwrap().clone();
        assert_eq!(observed.len(), 1, "{denial}");
        assert_eq!(observed[0]["class"], "permission_denied", "{denial}");
        assert_eq!(
            observed[0]["input"],
            json!({"kind": "normalized", "value": {"text": "safe-input"}}),
            "{denial}"
        );
        assert!(harness.probe.executed().is_empty(), "{denial}");
        let record = done.record(&call.id).await;
        assert_eq!(record.name, call.name);
        assert_eq!(record.arguments, json!({"text": "safe-input"}));
    }
}

#[tokio::test]
async fn tampered_run_id_does_not_change_store_fencing() {
    let call = ScriptedCall::new(SEEDED, &json!({}));
    let gate = Gate::all();
    let held = Arc::clone(&gate);
    let transform = ClosureExtension::new("tamper-fence", move |r| {
        let held = Arc::clone(&held);
        r.on_transform(
            ToolResultTransform::ID,
            1,
            HANDLER,
            Arc::new(move |mut value| {
                let held = Arc::clone(&held);
                Box::pin(async move {
                    held.pass(&value).await;
                    value["context"]["run_id"] = json!(RunId::new().to_string());
                    value["result"] = json!(AUTHORED);
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(seeded_tool(), Scope::Global)
        .mount(transform, Scope::Global)
        .build()
        .await;
    let handle = harness.start().await;
    let session_id = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    gate.entered().await;
    let run = harness.store.get_run(&run_id).await.unwrap().unwrap();
    let fence = RunFence {
        run_id: run_id.clone(),
        claim_token: run.claim_token.clone(),
    };
    let execution = harness.store.execution(fence.clone()).await.unwrap();
    assert!(matches!(
        harness
            .store
            .execution(RunFence {
                run_id: run_id.clone(),
                claim_token: "forged-token".into()
            })
            .await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(gate.release_all(), 1);
    handle.done().await.unwrap();
    let done = Finished {
        harness: &harness,
        session_id,
        run_id,
    };
    assert_protected(&done, &call.id, HANDLER).await;
    let settled_run = harness.store.get_run(&done.run_id).await.unwrap().unwrap();
    assert_eq!(settled_run.claim_token, run.claim_token);
    assert!(matches!(
        harness.store.execution(fence).await,
        Err(StoreError::Conflict)
    ));
    let record = done.record(&call.id).await;
    let (message, _, _) = done.tool_message(&call.id).await;
    let event = done.settled_event(&call.id).await;
    assert!(matches!(
        execution
            .settle_tool_call(
                &call.id,
                record.result.clone().unwrap(),
                message,
                event.clone()
            )
            .await,
        Err(StoreError::Conflict)
    ));
    assert_eq!(done.record(&call.id).await, record);
    assert_eq!(done.settled_event(&call.id).await, event);
}

#[tokio::test]
async fn deep_raw_envelope_is_downgraded_before_result_transform() {
    let deep: Value = serde_json::from_str(&("[".repeat(124) + &"]".repeat(124))).unwrap();
    let call = ScriptedCall::new(MISSING, &deep);
    let transform = ClosureExtension::new("tamper-deep-raw", |r| {
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "deep-raw",
            Arc::new(|value| {
                Box::pin(async move {
                    assert_eq!(value["context"]["input"]["kind"], "raw");
                    assert!(value["context"]["input"]["value"].is_string());
                    let decoded: Value =
                        serde_json::from_str(&serde_json::to_string(&value).unwrap()).unwrap();
                    assert_eq!(decoded, value);
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(transform, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#""unknown tool: missing""#,
        true,
    )
    .await;
    let record = done.record(&call.id).await;
    assert_eq!(
        serde_json::from_str::<ToolCallRecord>(&serde_json::to_string(&record).unwrap()).unwrap(),
        record
    );
}

#[tokio::test]
async fn deep_normalized_arguments_are_downgraded_before_result_transform() {
    let deep: Value = serde_json::from_str(&("[".repeat(125) + &"]".repeat(125))).unwrap();
    let call = ScriptedCall::new(OPEN, &json!({"nested": deep}));
    let observations = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&observations);
    let transform = ClosureExtension::new("tamper-deep-normalized", move |r| {
        let seen = Arc::clone(&seen);
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "decode-envelope",
            Arc::new(move |value| {
                let seen = Arc::clone(&seen);
                Box::pin(async move {
                    let text = serde_json::to_string(&value).unwrap();
                    let decode_failed = serde_json::from_str::<Value>(&text).is_err();
                    seen.lock()
                        .unwrap()
                        .push((value["context"]["input"]["kind"].clone(), decode_failed));
                    assert!(!decode_failed);
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(transform, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(
        *observations.lock().unwrap(),
        [(json!("unavailable"), false)],
        "the unstorable parsed envelope must not reach result transforms"
    );
    let record = done.record(&call.id).await;
    assert!(record.arguments.is_object());
    assert_eq!(
        serde_json::from_str::<ToolCallRecord>(&serde_json::to_string(&record).unwrap()).unwrap(),
        record
    );
}
