//! The authoritative `ToolResultContext` `execute_tool` builds on every outcome path (crabber-zv2d).
//!
//! The chain still dispatches the old payload; these tests observe the context through the
//! `#[cfg(test)]` `context_observer` seam on the orchestrator, which the harness installs on
//! every runtime it builds.

use super::*;
use crate::orchestrator::{RecordedCall, read_recorded_call};
use crabber_extension::{InputUnavailable, ToolInput, ToolOutcomeClass};

const RESERVED_TEXT: &str = "reserved argument key";

fn unavailable(reason: InputUnavailable) -> ToolInput {
    ToolInput::Unavailable { reason }
}

/// The one context built for the call, checked against the finished run's IDs.
fn context_for(done: &Finished<'_>, call: &ScriptedCall) -> ToolResultContext {
    let matching: Vec<_> = done
        .harness
        .contexts()
        .into_iter()
        .filter(|context| context.call_id() == &call.id)
        .collect();
    assert_eq!(matching.len(), 1, "exactly one context per executed call");
    let context = matching.into_iter().next().unwrap();
    assert_eq!(context.tool_name(), call.name);
    assert_eq!(context.session_id(), &done.session_id);
    assert_eq!(context.run_id(), &done.run_id);
    context
}

fn assert_context(
    context: &ToolResultContext,
    class: ToolOutcomeClass,
    resolved: bool,
    input: &ToolInput,
) {
    assert_eq!(context.class(), class);
    assert_eq!(context.resolved(), resolved);
    assert_eq!(context.input(), input);
}

fn normalized(value: Value) -> ToolInput {
    ToolInput::Normalized(value)
}

#[tokio::test]
async fn success_and_execution_error_are_normalized() {
    let ok = ScriptedCall::text(ECHO, "hi");
    let fail = ScriptedCall::text(FAIL, "boom");
    let harness = Harness::new(one_turn(&[&ok, &fail])).await;
    let done = harness.run().await;
    assert_context(
        &context_for(&done, &ok),
        ToolOutcomeClass::Succeeded,
        true,
        &normalized(json!({"text": "hi"})),
    );
    assert_context(
        &context_for(&done, &fail),
        ToolOutcomeClass::ExecutionFailed,
        true,
        &normalized(json!({"text": "boom"})),
    );
}

#[tokio::test]
async fn input_is_the_post_tool_prepare_arguments() {
    let call = ScriptedCall::text(ECHO, "original");
    let rewrite = ClosureExtension::new("rewrite-prepare", |r| {
        r.on_transform(
            ToolPrepare::ID,
            1,
            "rewrite",
            Arc::new(|mut value| {
                Box::pin(async move {
                    value["input"]["text"] = json!("rewritten");
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(rewrite, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(
        harness.probe.executed(),
        [(ECHO.to_owned(), json!({"text": "rewritten"}))]
    );
    assert_context(
        &context_for(&done, &call),
        ToolOutcomeClass::Succeeded,
        true,
        &normalized(json!({"text": "rewritten"})),
    );
}

#[tokio::test]
async fn permission_denial_by_policy_and_refused_approval_is_normalized() {
    for denial in [Denial::Policy, Denial::RefusedApproval] {
        let call = ScriptedCall::text(FORBIDDEN, "hi");
        let harness = Harness::with_denial(one_turn(&[&call]), denial).await;
        let done = harness.run().await;
        assert_context(
            &context_for(&done, &call),
            ToolOutcomeClass::PermissionDenied,
            true,
            &normalized(json!({"text": "hi"})),
        );
    }
}

#[tokio::test]
async fn permission_denial_by_guard_and_restriction_is_normalized() {
    let guarded = ScriptedCall::text(ECHO, "a");
    let restricted = ScriptedCall::text(SHOUT, "b");
    let harness = Harness::builder(one_turn(&[&guarded, &restricted]))
        .mount(
            ClosureExtension::guard("deny-echo-guard", Arc::new(DenyEcho)),
            Scope::Global,
        )
        .mount(
            ClosureExtension::restrict_to("only-echo", &[ECHO]),
            Scope::Global,
        )
        .build()
        .await;
    let done = harness.run().await;
    assert_context(
        &context_for(&done, &guarded),
        ToolOutcomeClass::PermissionDenied,
        true,
        &normalized(json!({"text": "a"})),
    );
    assert_context(
        &context_for(&done, &restricted),
        ToolOutcomeClass::PermissionDenied,
        true,
        &normalized(json!({"text": "b"})),
    );
    assert_eq!(harness.probe.executed(), []);
}

#[tokio::test]
async fn unknown_tool_input_is_raw_provider_arguments() {
    let object = ScriptedCall::text(MISSING, "hi");
    let array = ScriptedCall::new(MISSING, &json!([1, "two"]));
    let scalar = ScriptedCall::new(MISSING, &json!(7));
    let unparseable = ScriptedCall {
        id: ToolCallId::new(),
        name: MISSING,
        arguments: "{not json".into(),
    };
    let harness = Harness::new(one_turn(&[&object, &array, &scalar, &unparseable])).await;
    let done = harness.run().await;
    for (call, raw) in [
        (&object, json!({"text": "hi"})),
        (&array, json!([1, "two"])),
        (&scalar, json!(7)),
        (&unparseable, json!("{not json")),
    ] {
        assert_context(
            &context_for(&done, call),
            ToolOutcomeClass::UnknownTool,
            false,
            &ToolInput::Raw(raw.clone()),
        );
        assert_eq!(
            done.record(&call.id).await.arguments,
            json!({"$crabber_unknown_tool": {"raw": raw}})
        );
        // The persisted result is the old unknown-tool text.
        let record = done.record(&call.id).await;
        assert_eq!(record_text(&record), r#""unknown tool: missing""#);
    }
    assert_eq!(harness.probe.executed(), []);
}

struct RejectPrepare;
#[async_trait]
impl ToolPipeline for RejectPrepare {
    async fn prepare(&self, _tool: &ToolInfo, _arguments: Value) -> Result<Value, String> {
        Err("pipeline refused".into())
    }
    async fn transform_result(&self, _tool: &ToolInfo, result: Value) -> Result<Value, String> {
        Ok(result)
    }
}

#[tokio::test]
async fn preparation_failures_have_unavailable_input() {
    // Schema validation.
    let schema = ScriptedCall::new(ECHO, &json!({"text": 5}));
    let harness = Harness::new(one_turn(&[&schema])).await;
    let done = harness.run().await;
    assert_context(
        &context_for(&done, &schema),
        ToolOutcomeClass::PrepareFailed,
        true,
        &unavailable(InputUnavailable::PrepareFailed),
    );
    // ToolPipeline::prepare.
    let pipeline = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::builder(one_turn(&[&pipeline]))
        .tool_pipeline(Arc::new(RejectPrepare))
        .build()
        .await;
    let done = harness.run().await;
    assert_context(
        &context_for(&done, &pipeline),
        ToolOutcomeClass::PrepareFailed,
        true,
        &unavailable(InputUnavailable::PrepareFailed),
    );
    assert_eq!(
        done.record(&pipeline.id).await.arguments,
        json!({"$crabber_prepare_error": "pipeline refused"})
    );
    // A ToolPrepare handler.
    let handler = ScriptedCall::text(ECHO, PREPARE_REJECTED);
    let harness = Harness::new(one_turn(&[&handler])).await;
    let done = harness.run().await;
    assert_context(
        &context_for(&done, &handler),
        ToolOutcomeClass::PrepareFailed,
        true,
        &unavailable(InputUnavailable::PrepareFailed),
    );
    assert_eq!(harness.probe.executed(), []);
}

#[tokio::test]
async fn reserved_sole_key_arguments_are_a_preparation_failure() {
    for arguments in [
        json!({"$crabber_unknown_tool": {"raw": {"text": "x"}}}),
        json!({"$crabber_prepare_error": "x"}),
        json!({"$crabber_prepare_error": 5}),
        json!({"$crabber_other": true}),
    ] {
        let call = ScriptedCall::new(OPEN, &arguments);
        let harness = Harness::new(one_turn(&[&call])).await;
        let done = harness.run().await;
        assert_eq!(harness.probe.executed(), [], "{arguments}");
        assert_eq!(
            done.record(&call.id).await.arguments,
            json!({"$crabber_prepare_error": RESERVED_TEXT})
        );
        assert_context(
            &context_for(&done, &call),
            ToolOutcomeClass::PrepareFailed,
            true,
            &unavailable(InputUnavailable::PrepareFailed),
        );
        let record = done.record(&call.id).await;
        assert_eq!(record.status, ToolCallStatus::Failed);
        assert_eq!(record_text(&record), format!("\"{RESERVED_TEXT}\""));
    }
    // A key that is not reserved, or a reserved key beside others, is ordinary input.
    for arguments in [
        json!({"crabber_unknown_tool": 1}),
        json!({"$crabber_unknown_tool": 1, "other": 2}),
        json!({}),
    ] {
        let call = ScriptedCall::new(OPEN, &arguments);
        let harness = Harness::new(one_turn(&[&call])).await;
        let done = harness.run().await;
        assert_eq!(
            harness.probe.executed(),
            [(OPEN.to_owned(), arguments.clone())]
        );
        assert_context(
            &context_for(&done, &call),
            ToolOutcomeClass::Succeeded,
            true,
            &normalized(arguments),
        );
    }
}

#[tokio::test]
async fn a_tool_prepare_rewrite_into_a_sentinel_is_a_preparation_failure() {
    let call = ScriptedCall::new(OPEN, &json!({"a": 1}));
    let rewrite = ClosureExtension::new("sentinel-prepare", |r| {
        r.on_transform(
            ToolPrepare::ID,
            1,
            "to-sentinel",
            Arc::new(|mut value| {
                Box::pin(async move {
                    value["input"] = json!({"$crabber_unknown_tool": {"raw": 1}});
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(rewrite, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(harness.probe.executed(), []);
    assert_eq!(
        done.record(&call.id).await.arguments,
        json!({"$crabber_prepare_error": RESERVED_TEXT})
    );
    assert_context(
        &context_for(&done, &call),
        ToolOutcomeClass::PrepareFailed,
        true,
        &unavailable(InputUnavailable::PrepareFailed),
    );
}

/// Pauses only when [`ECHO`] is among the staged calls.
struct PauseOnEcho;
impl PermissionPolicy for PauseOnEcho {
    fn decide(&self, _tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, tool: &ToolInfo, _arguments: &Value) -> crate::InterruptPolicy {
        if tool.name == ECHO {
            crate::InterruptPolicy::Pause
        } else {
            crate::InterruptPolicy::Continue
        }
    }
}

#[tokio::test]
async fn resume_from_the_stored_record_derives_the_same_context() {
    let echo = ScriptedCall::text(ECHO, "hi");
    let unknown_array = ScriptedCall::new(MISSING, &json!([1, 2]));
    let unknown_text = ScriptedCall {
        id: ToolCallId::new(),
        name: MISSING,
        arguments: "{broken".into(),
    };
    let schema = ScriptedCall::new(SHOUT, &json!({"text": 5}));
    let harness = Harness::builder(one_turn(&[&echo, &unknown_array, &unknown_text, &schema]))
        .policy(Arc::new(PauseOnEcho))
        .build()
        .await;
    let handle = harness.start().await;
    let (session_id, run_id) = (handle.session_id().clone(), handle.run_id().clone());
    let result = handle.done().await.unwrap();
    assert_eq!(result.status, crabber_core::RunStatus::Paused);
    // Nothing executed before the pause, so no context exists yet.
    assert!(harness.contexts().is_empty());
    // The unknown calls' raw arguments are durable before anything resumes.
    let unfinished = harness
        .store
        .list_unfinished_tool_calls(&run_id)
        .await
        .unwrap();
    let stored = |id: &ToolCallId| {
        unfinished
            .iter()
            .find(|record| &record.id == id)
            .unwrap()
            .arguments
            .clone()
    };
    assert_eq!(
        stored(&unknown_array.id),
        json!({"$crabber_unknown_tool": {"raw": [1, 2]}})
    );
    assert_eq!(
        stored(&unknown_text.id),
        json!({"$crabber_unknown_tool": {"raw": "{broken"}})
    );

    let fresh = harness.fresh_runtime();
    assert_eq!(
        fresh.resume(&run_id).await.unwrap().status,
        crabber_core::RunStatus::Completed
    );
    let done = Finished {
        harness: &harness,
        session_id,
        run_id,
    };
    assert_context(
        &context_for(&done, &echo),
        ToolOutcomeClass::Succeeded,
        true,
        &normalized(json!({"text": "hi"})),
    );
    assert_context(
        &context_for(&done, &unknown_array),
        ToolOutcomeClass::UnknownTool,
        false,
        &ToolInput::Raw(json!([1, 2])),
    );
    assert_context(
        &context_for(&done, &unknown_text),
        ToolOutcomeClass::UnknownTool,
        false,
        &ToolInput::Raw(json!("{broken")),
    );
    assert_context(
        &context_for(&done, &schema),
        ToolOutcomeClass::PrepareFailed,
        true,
        &unavailable(InputUnavailable::PrepareFailed),
    );
    assert_eq!(
        harness.probe.executed(),
        [(ECHO.to_owned(), json!({"text": "hi"}))]
    );
}

// Record reader, row by row (Recorded answer 1).

fn settled(class: ToolOutcomeClass, input: ToolInput, resolved: bool, seed: &str) -> RecordedCall {
    RecordedCall::Settled {
        class,
        input,
        resolved,
        seed: seed.to_owned(),
    }
}

fn read(arguments: Value, resolves: bool) -> RecordedCall {
    read_recorded_call(arguments, "tool", resolves)
}

#[test]
fn reader_unknown_tool_rows() {
    let unknown = "unknown tool: tool";
    for resolves in [true, false] {
        for raw in [json!({"a": 1}), json!([1]), json!("text"), Value::Null] {
            assert_eq!(
                read(json!({"$crabber_unknown_tool": {"raw": raw}}), resolves),
                settled(
                    ToolOutcomeClass::UnknownTool,
                    ToolInput::Raw(raw),
                    false,
                    unknown
                )
            );
        }
        // Malformed shapes lose the raw input but stay an unknown tool.
        for shape in [
            json!(5),
            json!("raw"),
            json!(null),
            json!([]),
            json!({}),
            json!({"other": 1}),
        ] {
            assert_eq!(
                read(json!({"$crabber_unknown_tool": shape}), resolves),
                settled(
                    ToolOutcomeClass::UnknownTool,
                    unavailable(InputUnavailable::Unresolved),
                    false,
                    unknown
                )
            );
        }
    }
}

#[test]
fn reader_preparation_failure_rows() {
    for resolves in [true, false] {
        assert_eq!(
            read(json!({"$crabber_prepare_error": "bad input"}), resolves),
            settled(
                ToolOutcomeClass::PrepareFailed,
                unavailable(InputUnavailable::PrepareFailed),
                true,
                "bad input"
            )
        );
        // Non-string message, or another reserved key: the fixed text.
        for arguments in [
            json!({"$crabber_prepare_error": 5}),
            json!({"$crabber_prepare_error": null}),
            json!({"$crabber_prepare_error": {"a": 1}}),
            json!({"$crabber_something_else": "x"}),
            json!({"$crabber_": 1}),
        ] {
            assert_eq!(
                read(arguments, resolves),
                settled(
                    ToolOutcomeClass::PrepareFailed,
                    unavailable(InputUnavailable::PrepareFailed),
                    true,
                    RESERVED_TEXT
                )
            );
        }
    }
}

#[test]
fn reader_anything_else_rows() {
    for arguments in [
        json!({"text": "hi"}),
        json!({}),
        json!([1]),
        json!("text"),
        json!(3),
        json!(null),
        // A reserved key beside another key is not a sentinel.
        json!({"$crabber_unknown_tool": {"raw": 1}, "text": "hi"}),
        json!({"crabber_prepare_error": "x"}),
    ] {
        assert_eq!(
            read(arguments.clone(), true),
            RecordedCall::Execute(arguments.clone())
        );
        // The defensive arm: the stored arguments are neither Raw nor Normalized.
        assert_eq!(
            read(arguments, false),
            settled(
                ToolOutcomeClass::UnknownTool,
                unavailable(InputUnavailable::Unresolved),
                false,
                "unknown tool: tool"
            )
        );
    }
}
