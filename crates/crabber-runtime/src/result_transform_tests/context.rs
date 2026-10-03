//! The authoritative `ToolResultContext` `execute_tool` builds on every outcome path (crabber-zv2d).
//!
//! The chain still dispatches the old payload; these tests observe the context through the
//! `#[cfg(test)]` `context_observer` seam on the orchestrator, which the harness installs on
//! every runtime it builds.

use super::*;
use crate::orchestrator::{RecordedCall, read_recorded_call, unknown_tool_arguments};
use crabber_extension::{InputUnavailable, ToolInput, ToolOutcomeClass, TransformOutput};

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
    let pipeline = Arc::new(CaptureResult::default());
    let harness = Harness::builder(one_turn(&[&call]))
        .tool_pipeline(pipeline.clone())
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
    let arguments = done.record(&call.id).await.arguments;
    let captured = pipeline.contexts.lock().unwrap();
    let context = &captured[0];
    assert_eq!(captured.len(), 1);
    assert_eq!(context.tool_name(), call.name);
    assert_eq!(context.call_id(), &call.id);
    assert_eq!(context.session_id(), &done.session_id);
    assert_eq!(context.run_id(), &done.run_id);
    assert_context(
        context,
        ToolOutcomeClass::Succeeded,
        true,
        &normalized(arguments),
    );
    assert_eq!(context.phase(), crabber_extension::TransformPhase::Ordinary);
    assert!(!context.is_error());
    assert!(!context.cancellation().is_cancelled());
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
    async fn transform_result(
        &self,
        _context: &ToolResultContext,
        _tool: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String> {
        Ok(TransformOutput::new(result))
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

    // MemoryStore never encodes, so prove the stored arguments survive the encoding a real store
    // uses before the fresh runtime derives the context from them.
    for record in &unfinished {
        let text = serde_json::to_string(&record.arguments).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            record.arguments
        );
    }
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

// Typed classification: the class comes from where the failure happened, never from its text.

#[tokio::test]
async fn executor_error_text_permission_denied_is_still_execution_failed() {
    let call = ScriptedCall::text(SAYS_DENIED, "hi");
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    assert_eq!(harness.probe.executed().len(), 1);
    assert_context(
        &context_for(&done, &call),
        ToolOutcomeClass::ExecutionFailed,
        true,
        &normalized(json!({"text": "hi"})),
    );
}

#[tokio::test]
async fn around_handler_error_and_changed_input_are_execution_failed() {
    let failing = ScriptedCall::text(ECHO, "a");
    let changing = ScriptedCall::text(SHOUT, "b");
    let around = ClosureExtension::new("around-tool", |r| {
        r.on_around(
            crabber_extension::ToolExecute::ID,
            0,
            "around",
            Arc::new(|input, next| {
                Box::pin(async move {
                    if input["text"] == "a" {
                        return Err(ExtensionError::Tool("around exploded".into()));
                    }
                    let mut changed = input;
                    changed["text"] = json!("changed");
                    next.call(changed).await
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&failing, &changing]))
        .mount(around, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(harness.probe.executed(), []);
    for (call, text) in [(&failing, "a"), (&changing, "b")] {
        assert_context(
            &context_for(&done, call),
            ToolOutcomeClass::ExecutionFailed,
            true,
            &normalized(json!({"text": text})),
        );
    }
    let record = done.record(&changing.id).await;
    assert!(record_text(&record).contains("around handler changed immutable tool input"));
}

struct FailResult {
    panic: bool,
}
#[async_trait]
impl ToolPipeline for FailResult {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        _context: &ToolResultContext,
        _tool: &ToolInfo,
        _result: Value,
    ) -> Result<TransformOutput, String> {
        assert!(!self.panic, "private panic text and original output");
        Err("pre-stage failed".into())
    }
}

#[tokio::test]
async fn pre_stage_error_is_sanitized_and_skips_result_chain() {
    for panic in [false, true] {
        let call = ScriptedCall::text(ECHO, "private original output");
        let harness = Harness::builder(one_turn(&[&call]))
            .tool_pipeline(Arc::new(FailResult { panic }))
            .build()
            .await;
        let done = harness.run().await;
        assert_context(
            &context_for(&done, &call),
            ToolOutcomeClass::Succeeded,
            true,
            &normalized(json!({"text": "private original output"})),
        );
        assert!(harness.probe.results().is_empty());
        assert_settled(
            &done,
            &call.id,
            ToolCallStatus::Failed,
            r#""result transform failed: crabber/tool-pipeline""#,
            true,
        )
        .await;
    }
}

struct AskEcho;
impl ToolGuard for AskEcho {
    fn id(&self) -> &'static str {
        "ask-echo"
    }
    fn check(&self, name: &str, _input: &Value) -> crabber_extension::GuardDecision {
        if name == ECHO {
            crabber_extension::GuardDecision::Ask
        } else {
            crabber_extension::GuardDecision::Abstain
        }
    }
}

#[tokio::test]
async fn guard_ask_refused_by_the_approver_is_permission_denied() {
    let call = ScriptedCall::text(ECHO, "hi");
    let harness = Harness::builder(one_turn(&[&call]))
        .mount(
            ClosureExtension::guard("ask-echo-guard", Arc::new(AskEcho)),
            Scope::Global,
        )
        .approver(Arc::new(Refuse))
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(harness.probe.executed(), []);
    assert_context(
        &context_for(&done, &call),
        ToolOutcomeClass::PermissionDenied,
        true,
        &normalized(json!({"text": "hi"})),
    );
}

// Nesting depth (stores decode records with a 128-level limit).

fn nested_value(depth: usize) -> Value {
    serde_json::from_str(&("[".repeat(depth) + &"]".repeat(depth))).unwrap()
}

#[test]
fn unknown_tool_record_always_decodes_and_reads_back_as_raw() {
    for depth in [3, 124, 125, 126, 127] {
        let raw = nested_value(depth);
        let stored = unknown_tool_arguments(&raw);
        let text = serde_json::to_string(&stored).unwrap();
        assert_eq!(
            serde_json::from_str::<Value>(&text).unwrap(),
            stored,
            "depth {depth} must decode"
        );
        let expected = if depth <= 124 {
            raw.clone()
        } else {
            Value::String(serde_json::to_string(&raw).unwrap())
        };
        assert_eq!(
            read(stored, false),
            settled(
                ToolOutcomeClass::UnknownTool,
                ToolInput::Raw(expected),
                false,
                "unknown tool: tool"
            ),
            "depth {depth}"
        );
    }
}

#[tokio::test]
async fn deeply_nested_unknown_tool_call_settles_with_the_usual_result() {
    let call = ScriptedCall {
        id: ToolCallId::new(),
        name: MISSING,
        arguments: "[".repeat(125) + &"]".repeat(125),
    };
    let harness = Harness::new(one_turn(&[&call])).await;
    let done = harness.run().await;
    let record = done.record(&call.id).await;
    assert_eq!(record.status, ToolCallStatus::Failed);
    assert_eq!(record_text(&record), r#""unknown tool: missing""#);
    assert!(matches!(
        context_for(&done, &call).input(),
        ToolInput::Raw(Value::String(_))
    ));
}

#[derive(Default)]
struct CaptureResult {
    contexts: Mutex<Vec<ToolResultContext>>,
    mark_error: bool,
}
#[async_trait]
impl ToolPipeline for CaptureResult {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        context: &ToolResultContext,
        tool: &ToolInfo,
        _result: Value,
    ) -> Result<TransformOutput, String> {
        assert_eq!(context.tool_name(), tool.name);
        self.contexts.lock().unwrap().push(context.clone());
        Ok(TransformOutput {
            result: json!("pipeline output"),
            mark_error: self.mark_error,
        })
    }
}

#[tokio::test]
async fn pre_stage_only_runs_on_success_and_cannot_clear_errors() {
    let success = ScriptedCall::text(ECHO, "ok");
    let failed = ScriptedCall::text(FAIL, "failure");
    let denied = ScriptedCall::text(FORBIDDEN, "denied");
    let unknown = ScriptedCall::text(MISSING, "unknown");
    let prepare = ScriptedCall::text(ECHO, PREPARE_REJECTED);
    let pipeline = Arc::new(CaptureResult::default());
    let harness = Harness::builder(one_turn(&[&success, &failed, &denied, &unknown, &prepare]))
        .tool_pipeline(pipeline.clone())
        .build()
        .await;
    let done = harness.run().await;
    let contexts = pipeline.contexts.lock().unwrap().clone();
    assert_eq!(contexts.len(), 1);
    assert_eq!(contexts[0].call_id(), &success.id);
    assert_settled(
        &done,
        &success.id,
        ToolCallStatus::Completed,
        r#""pipeline output""#,
        false,
    )
    .await;
    for call in [&failed, &denied, &unknown, &prepare] {
        assert_eq!(done.record(&call.id).await.status, ToolCallStatus::Failed);
        assert!(done.tool_message(&call.id).await.2);
    }
}

#[tokio::test]
async fn pre_stage_mark_error_is_monotonic_through_extension_chain() {
    let call = ScriptedCall::text(ECHO, "ok");
    let pipeline = Arc::new(CaptureResult {
        mark_error: true,
        ..CaptureResult::default()
    });
    let reset_error = ClosureExtension::new("reset-error", |r| {
        r.on_transform(
            ToolResultTransform::ID,
            1,
            "reset-error",
            Arc::new(|mut value| {
                Box::pin(async move {
                    value["is_error"] = json!(false);
                    Ok(value)
                })
            }),
        );
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .tool_pipeline(pipeline.clone())
        .mount(reset_error, Scope::Global)
        .build()
        .await;
    let done = harness.run().await;
    assert_eq!(
        pipeline.contexts.lock().unwrap()[0].class(),
        ToolOutcomeClass::Succeeded
    );
    assert_eq!(harness.probe.results()[0]["is_error"], true);
    assert_settled(
        &done,
        &call.id,
        ToolCallStatus::Failed,
        r#""pipeline output""#,
        true,
    )
    .await;
}

struct ParkResult {
    gate: Arc<Gate>,
    context: Mutex<Option<ToolResultContext>>,
}
#[async_trait]
impl ToolPipeline for ParkResult {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        context: &ToolResultContext,
        _tool: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String> {
        *self.context.lock().unwrap() = Some(context.clone());
        self.gate.pass(&result).await;
        Ok(TransformOutput::new(result))
    }
}

#[tokio::test]
async fn cancellation_during_pre_stage_discards_output_and_skips_chain() {
    let call = ScriptedCall::text(ECHO, "private output");
    let pipeline = Arc::new(ParkResult {
        gate: Gate::all(),
        context: Mutex::new(None),
    });
    let harness = Harness::builder(one_turn(&[&call]))
        .tool_pipeline(pipeline.clone())
        .build()
        .await;
    let handle = harness.start().await;
    let session_id = handle.session_id().clone();
    let run_id = handle.run_id().clone();
    pipeline.gate.entered().await;
    handle.interrupt();
    assert_eq!(
        handle.done().await.unwrap().status,
        crabber_core::RunStatus::Interrupted
    );
    assert!(
        pipeline
            .context
            .lock()
            .unwrap()
            .as_ref()
            .unwrap()
            .cancellation()
            .is_cancelled()
    );
    assert!(harness.probe.results().is_empty());
    let done = Finished {
        harness: &harness,
        session_id,
        run_id,
    };
    assert_eq!(
        done.record(&call.id).await.status,
        ToolCallStatus::Interrupted
    );
    assert_eq!(record_text(&done.record(&call.id).await), "interrupted");
}
