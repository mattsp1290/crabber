//! Outcome paths preserve classification and expose only authoritative input (D1/D3/D4).

use super::*;
use crabber_extension::{InputUnavailable, ToolInput, ToolOutcomeClass, TransformOutput};

#[derive(Default)]
struct PathProbe {
    results: Mutex<Vec<(ToolResultContext, Value)>>,
    prepares: Mutex<Vec<Value>>,
    pre_stages: Mutex<Vec<ToolResultContext>>,
    reject_prepare: bool,
}

impl PathProbe {
    fn extension(self: &Arc<Self>) -> Arc<ClosureExtension> {
        let probe = Arc::clone(self);
        ClosureExtension::new("path-observer", move |r| {
            let probe = Arc::clone(&probe);
            r.on_result_transform(
                1,
                "unchanged-path-result",
                Arc::new(move |context, result| {
                    probe
                        .results
                        .lock()
                        .unwrap()
                        .push((context, result.clone()));
                    Box::pin(async move { Ok(TransformOutput::new(result)) })
                }),
            );
        })
    }

    fn builder(self: &Arc<Self>, call: &ScriptedCall) -> HarnessBuilder {
        Harness::builder(one_turn(&[call]))
            .tool_pipeline(self.clone())
            .mount(self.extension(), Scope::Global)
    }

    fn assert_preparation(&self, done: &Finished<'_>, pipeline: &[Value], handlers: usize) {
        assert_eq!(*self.prepares.lock().unwrap(), pipeline);
        assert_eq!(done.harness.probe.prepares().len(), handlers);
    }

    async fn assert_path(
        &self,
        done: &Finished<'_>,
        call: &ScriptedCall,
        class: ToolOutcomeClass,
        input: ToolInput,
        result: Value,
    ) {
        let record = done.record(&call.id).await;
        assert_eq!(
            record.result.as_ref().unwrap().status,
            if class.is_error() {
                crabber_core::ToolResultStatus::Failed
            } else {
                crabber_core::ToolResultStatus::Completed
            }
        );
        let seen = self.results.lock().unwrap().clone();
        assert_eq!(seen.len(), 1, "one mounted callback invocation per call");
        let (context, actual_result) = &seen[0];
        assert_eq!(context.tool_name(), record.name);
        assert_eq!(record.name, call.name);
        assert_eq!(context.call_id(), &record.id);
        assert_eq!(record.id, call.id);
        assert_eq!(context.run_id(), &record.run_id);
        assert_eq!(record.run_id, done.run_id);
        assert_eq!(context.session_id(), &done.session_id);
        assert_eq!(context.class(), class);
        assert_eq!(context.resolved(), class != ToolOutcomeClass::UnknownTool);
        assert_eq!(context.input(), &input);
        assert_eq!(context.is_error(), class.is_error());
        assert_eq!(actual_result, &result);
        let envelopes = done.harness.probe.results();
        assert_eq!(envelopes.len(), 1);
        assert_eq!(envelopes[0]["mark_error"], false);
        assert_eq!(envelopes[0]["context"]["class"], class.as_str());
        assert_eq!(envelopes[0]["result"], result);
        if let ToolInput::Normalized(value) = input {
            assert_eq!(record.arguments, value);
        }
        let stages = self.pre_stages.lock().unwrap().clone();
        if class == ToolOutcomeClass::Succeeded {
            assert_eq!(stages.len(), 1);
            assert_eq!(stages[0].call_id(), context.call_id());
            assert_eq!(stages[0].input(), context.input());
            assert_eq!(stages[0].class(), class);
        } else {
            assert!(
                stages.is_empty(),
                "D1 pre-stage only runs on executed success"
            );
        }
        assert_settled(
            done,
            &call.id,
            if class.is_error() {
                ToolCallStatus::Failed
            } else {
                ToolCallStatus::Completed
            },
            &serde_json::to_string(&result).unwrap(),
            class.is_error(),
        )
        .await;
    }
}

#[async_trait]
impl ToolPipeline for PathProbe {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        self.prepares.lock().unwrap().push(arguments.clone());
        if self.reject_prepare {
            Err("path pipeline rejected".into())
        } else {
            Ok(arguments)
        }
    }

    async fn transform_result(
        &self,
        context: &ToolResultContext,
        _tool: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String> {
        self.pre_stages.lock().unwrap().push(context.clone());
        Ok(TransformOutput::new(result))
    }
}

#[derive(Default)]
struct ObservePolicy(Mutex<Vec<Value>>);
impl PermissionPolicy for ObservePolicy {
    fn decide(&self, _tool: &ToolInfo, arguments: &Value) -> PermissionDecision {
        self.0.lock().unwrap().push(arguments.clone());
        PermissionDecision::Allow
    }
}

#[derive(Default)]
struct ObserveGuard(Mutex<Vec<Value>>);
impl ToolGuard for ObserveGuard {
    fn id(&self) -> &'static str {
        "path-observe-guard"
    }
    fn check(&self, name: &str, input: &Value) -> crabber_extension::GuardDecision {
        assert_eq!(name, ECHO);
        self.0.lock().unwrap().push(input.clone());
        crabber_extension::GuardDecision::Abstain
    }
}

#[tokio::test]
async fn success_uses_post_tool_prepare_input_without_preparing_again() {
    let call = ScriptedCall::text(ECHO, "provider-original");
    let rewritten = json!({"text": "prepared-rewrite"});
    let rewrite_calls = Arc::new(Mutex::new(Vec::new()));
    let seen = Arc::clone(&rewrite_calls);
    let rewrite = ClosureExtension::new("path-rewrite", move |r| {
        let seen = Arc::clone(&seen);
        r.on_transform(
            ToolPrepare::ID,
            1,
            "rewrite-input",
            Arc::new(move |mut value| {
                seen.lock().unwrap().push(value["input"].clone());
                Box::pin(async move {
                    value["input"] = json!({"text": "prepared-rewrite"});
                    Ok(value)
                })
            }),
        );
    });
    let policy = Arc::new(ObservePolicy::default());
    let guard = Arc::new(ObserveGuard::default());
    let probe = Arc::new(PathProbe::default());
    let harness = probe
        .builder(&call)
        .mount(rewrite, Scope::Global)
        .mount(
            ClosureExtension::guard("path-input-guard", guard.clone()),
            Scope::Global,
        )
        .policy(policy.clone())
        .build()
        .await;
    let done = harness.run().await;
    probe
        .assert_path(
            &done,
            &call,
            ToolOutcomeClass::Succeeded,
            ToolInput::Normalized(rewritten.clone()),
            rewritten.clone(),
        )
        .await;
    probe.assert_preparation(&done, &[json!({"text": "provider-original"})], 1);
    assert_eq!(
        *rewrite_calls.lock().unwrap(),
        [json!({"text": "provider-original"})]
    );
    assert_eq!(
        policy.0.lock().unwrap().as_slice(),
        std::slice::from_ref(&rewritten)
    );
    assert_eq!(
        guard.0.lock().unwrap().as_slice(),
        std::slice::from_ref(&rewritten)
    );
    assert_eq!(harness.probe.executed(), [(ECHO.to_owned(), rewritten)]);
}

#[tokio::test]
async fn execution_failure_cannot_be_cleared_by_unchanged_result() {
    let call = ScriptedCall::text(FAIL, "execution-input");
    let input = json!({"text": "execution-input"});
    let probe = Arc::new(PathProbe::default());
    let harness = probe.builder(&call).build().await;
    let done = harness.run().await;
    probe
        .assert_path(
            &done,
            &call,
            ToolOutcomeClass::ExecutionFailed,
            ToolInput::Normalized(input.clone()),
            json!("tool execution failed: executor exploded"),
        )
        .await;
    probe.assert_preparation(&done, std::slice::from_ref(&input), 1);
    assert_eq!(harness.probe.executed(), [(FAIL.to_owned(), input)]);
}

async fn assert_denied(probe: &PathProbe, harness: &Harness, call: &ScriptedCall) {
    let done = harness.run().await;
    let input = json!({"text": "denial-input"});
    probe
        .assert_path(
            &done,
            call,
            ToolOutcomeClass::PermissionDenied,
            ToolInput::Normalized(input.clone()),
            json!("permission denied"),
        )
        .await;
    probe.assert_preparation(&done, &[input], 1);
    assert_eq!(harness.probe.executed(), []);
}

#[tokio::test]
async fn guard_denial_keeps_normalized_input_and_never_executes() {
    let call = ScriptedCall::text(ECHO, "denial-input");
    let probe = Arc::new(PathProbe::default());
    let harness = probe
        .builder(&call)
        .mount(
            ClosureExtension::guard("path-deny-guard", Arc::new(DenyEcho)),
            Scope::Global,
        )
        .build()
        .await;
    assert_denied(&probe, &harness, &call).await;
}

#[tokio::test]
async fn restriction_denial_keeps_normalized_input_and_never_executes() {
    let call = ScriptedCall::text(ECHO, "denial-input");
    let probe = Arc::new(PathProbe::default());
    let harness = probe
        .builder(&call)
        .mount(
            ClosureExtension::restrict_to("path-restriction", &[SHOUT]),
            Scope::Global,
        )
        .build()
        .await;
    assert_denied(&probe, &harness, &call).await;
}

#[tokio::test]
async fn policy_denial_keeps_normalized_input_and_never_executes() {
    let call = ScriptedCall::text(FORBIDDEN, "denial-input");
    let probe = Arc::new(PathProbe::default());
    let harness = probe.builder(&call).build().await;
    assert_denied(&probe, &harness, &call).await;
}

#[derive(Default)]
struct ApprovalReply(Mutex<Vec<Value>>);
#[async_trait]
impl ApprovalRequester for ApprovalReply {
    async fn approve(&self, tool: &ToolInfo, arguments: &Value) -> bool {
        assert_eq!(tool.name, FORBIDDEN);
        self.0.lock().unwrap().push(arguments.clone());
        false
    }
}

#[tokio::test]
async fn refused_approval_keeps_normalized_input_and_never_executes() {
    let call = ScriptedCall::text(FORBIDDEN, "denial-input");
    let probe = Arc::new(PathProbe::default());
    let reply = Arc::new(ApprovalReply::default());
    let harness = probe
        .builder(&call)
        .policy(Arc::new(AskAndRefuse))
        .approver(reply.clone())
        .build()
        .await;
    assert_denied(&probe, &harness, &call).await;
    assert_eq!(*reply.0.lock().unwrap(), [json!({"text": "denial-input"})]);
}

#[tokio::test]
async fn unknown_tool_keeps_raw_provider_values_including_unparseable_text() {
    for (arguments, raw) in [
        (
            r#"{"text":"unknown-input"}"#.to_owned(),
            json!({"text": "unknown-input"}),
        ),
        ("[1,false,null]".to_owned(), json!([1, false, null])),
        (
            "{unparseable provider text".to_owned(),
            json!("{unparseable provider text"),
        ),
    ] {
        let call = ScriptedCall {
            id: ToolCallId::new(),
            name: MISSING,
            arguments,
        };
        let probe = Arc::new(PathProbe::default());
        let harness = probe.builder(&call).build().await;
        let done = harness.run().await;
        probe
            .assert_path(
                &done,
                &call,
                ToolOutcomeClass::UnknownTool,
                ToolInput::Raw(raw.clone()),
                json!("unknown tool: missing"),
            )
            .await;
        assert_eq!(
            done.record(&call.id).await.arguments,
            json!({"$crabber_unknown_tool": {"raw": raw}})
        );
        probe.assert_preparation(&done, &[], 0);
        assert_eq!(harness.probe.executed(), []);
    }
}

async fn assert_prepare_failed(
    probe: &PathProbe,
    done: &Finished<'_>,
    call: &ScriptedCall,
    message: &str,
) {
    probe
        .assert_path(
            done,
            call,
            ToolOutcomeClass::PrepareFailed,
            ToolInput::Unavailable {
                reason: InputUnavailable::PrepareFailed,
            },
            json!(message),
        )
        .await;
    assert_eq!(
        done.record(&call.id).await.arguments,
        json!({"$crabber_prepare_error": message})
    );
    assert_eq!(done.harness.probe.executed(), []);
}

#[tokio::test]
async fn schema_failure_has_unavailable_input_and_never_prepares() {
    let call = ScriptedCall::new(ECHO, &json!({"text": 5}));
    let probe = Arc::new(PathProbe::default());
    let harness = probe.builder(&call).build().await;
    let done = harness.run().await;
    let results = harness.probe.results();
    assert_eq!(results.len(), 1);
    let message = results[0]["result"].as_str().unwrap();
    assert!(
        message.contains("invalid tool arguments:") && message.contains("not of type \"string\""),
        "{message}"
    );
    assert_prepare_failed(&probe, &done, &call, message).await;
    probe.assert_preparation(&done, &[], 0);
}

#[tokio::test]
async fn pipeline_prepare_failure_has_unavailable_input_and_never_runs_handler() {
    let call = ScriptedCall::text(ECHO, "pipeline-input");
    let probe = Arc::new(PathProbe {
        reject_prepare: true,
        ..PathProbe::default()
    });
    let harness = probe.builder(&call).build().await;
    let done = harness.run().await;
    assert_prepare_failed(&probe, &done, &call, "path pipeline rejected").await;
    probe.assert_preparation(&done, &[json!({"text": "pipeline-input"})], 0);
}

#[tokio::test]
async fn tool_prepare_handler_failure_has_unavailable_input_without_reconstruction() {
    let call = ScriptedCall::text(ECHO, PREPARE_REJECTED);
    let probe = Arc::new(PathProbe::default());
    let harness = probe.builder(&call).build().await;
    let done = harness.run().await;
    assert_prepare_failed(
        &probe,
        &done,
        &call,
        "tool execution failed: prepare handler rejected",
    )
    .await;
    probe.assert_preparation(&done, &[json!({"text": PREPARE_REJECTED})], 1);
}
