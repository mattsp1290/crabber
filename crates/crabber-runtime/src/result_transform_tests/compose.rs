//! A host pre-stage, an independently mounted reducer, and a final redactor compose safely.

use super::*;
use crabber_extension::{ToolOutcomeClass, TransformOutput, TransformPhase};

const SECRET: &str = "compose-seeded-secret";
const REDUCED: &str = "protected reduced nonsecret content";

#[derive(Debug)]
struct Observation {
    stage: &'static str,
    context: ToolResultContext,
    result: Value,
}

#[derive(Default)]
struct CompositionProbe(Mutex<Vec<Observation>>);
impl CompositionProbe {
    fn record(&self, stage: &'static str, context: &ToolResultContext, result: &Value) {
        self.0.lock().unwrap().push(Observation {
            stage,
            context: context.clone(),
            result: result.clone(),
        });
    }
}

#[async_trait]
impl ToolPipeline for CompositionProbe {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }

    async fn transform_result(
        &self,
        context: &ToolResultContext,
        _tool: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String> {
        self.record("pre-stage", context, &result);
        Ok(TransformOutput::new(
            json!({"source": result, "secret": SECRET}),
        ))
    }
}

fn assert_observations(
    probe: &CompositionProbe,
    done: &Finished<'_>,
    call: &ScriptedCall,
    class: ToolOutcomeClass,
    seed: Value,
    redactor_first: bool,
    orders: (i32, i32),
) {
    let (reducer_order, redactor_order) = orders;
    let observations = probe.0.lock().unwrap();
    let stages: Vec<_> = observations.iter().map(|seen| seen.stage).collect();
    let expected = if class == ToolOutcomeClass::Succeeded {
        vec!["pre-stage", "reducer", "redactor"]
    } else {
        vec!["reducer", "redactor"]
    };
    assert_eq!(
        stages, expected,
        "{class:?}, redactor first: {redactor_first}, orders: {reducer_order}/{redactor_order}"
    );
    for seen in observations.iter() {
        assert_eq!(seen.context.class(), class);
        assert_eq!(seen.context.call_id(), &call.id);
        assert_eq!(seen.context.session_id(), &done.session_id);
        assert_eq!(seen.context.run_id(), &done.run_id);
        assert_eq!(seen.context.is_error(), class.is_error());
        assert_eq!(
            seen.context.phase(),
            if seen.stage == "redactor" {
                TransformPhase::FinalRedaction
            } else {
                TransformPhase::Ordinary
            }
        );
    }
    let reducer = &observations[observations.len() - 2];
    assert_eq!(
        reducer.result,
        if class == ToolOutcomeClass::Succeeded {
            assert_eq!(observations[0].result, seed);
            json!({"source": seed, "secret": SECRET})
        } else {
            seed
        }
    );
    assert_eq!(
        observations.last().unwrap().result,
        json!({"reduced": REDUCED, "secret": SECRET})
    );
}

async fn assert_protected(done: &Finished<'_>, call: &ScriptedCall, class: ToolOutcomeClass) {
    let protected = serde_json::to_string(&json!({"reduced": REDUCED})).unwrap();
    assert_settled(
        done,
        &call.id,
        if class.is_error() {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        },
        &protected,
        class.is_error(),
    )
    .await;
    let record = done.record(&call.id).await;
    let result = record.result.as_ref().unwrap();
    assert_eq!(
        result.status,
        if class.is_error() {
            crabber_core::ToolResultStatus::Failed
        } else {
            crabber_core::ToolResultStatus::Completed
        }
    );
    let (message, _, _) = done.tool_message(&call.id).await;
    let event = done.settled_event(&call.id).await;
    let request = done.next_request_for(&call.id);
    for artifact in [
        serde_json::to_string(result).unwrap(),
        serde_json::to_string(&message).unwrap(),
        serde_json::to_string(&event).unwrap(),
        format!("{request:?}"),
    ] {
        assert!(
            artifact.contains(REDUCED),
            "reduced content missing: {artifact}"
        );
        assert!(!artifact.contains(SECRET), "secret leaked: {artifact}");
    }
}

fn path_fixture(class: ToolOutcomeClass) -> (&'static str, &'static str, Value) {
    match class {
        ToolOutcomeClass::Succeeded => (ECHO, "verbose source", json!({"text": "verbose source"})),
        ToolOutcomeClass::ExecutionFailed => (
            FAIL,
            "execution input",
            json!("tool execution failed: executor exploded"),
        ),
        ToolOutcomeClass::PermissionDenied => {
            (FORBIDDEN, "denied input", json!("permission denied"))
        }
        ToolOutcomeClass::UnknownTool => (MISSING, "raw input", json!("unknown tool: missing")),
        ToolOutcomeClass::PrepareFailed => (
            ECHO,
            PREPARE_REJECTED,
            json!("tool execution failed: prepare handler rejected"),
        ),
    }
}

async fn composed_path(class: ToolOutcomeClass) {
    for redactor_first in [false, true] {
        for (reducer_order, redactor_order) in [(i32::MAX, i32::MIN), (0, 0), (i32::MIN, i32::MAX)]
        {
            let (name, input, seed) = path_fixture(class);
            // Secrets originate in result processing, never in provider arguments or history.
            let call = ScriptedCall::text(name, input);
            let probe = Arc::new(CompositionProbe::default());
            let reducer = ClosureExtension::new("compose-reducer", {
                let probe = Arc::clone(&probe);
                move |r| {
                    let probe = Arc::clone(&probe);
                    r.on_result_transform(
                        reducer_order,
                        "reduce",
                        Arc::new(move |context, result| {
                            probe.record("reducer", &context, &result);
                            Box::pin(async move {
                                Ok(TransformOutput::new(
                                    json!({"reduced": REDUCED, "secret": SECRET}),
                                ))
                            })
                        }),
                    );
                }
            });
            let redactor = ClosureExtension::new("compose-redactor", {
                let probe = Arc::clone(&probe);
                move |r| {
                    let probe = Arc::clone(&probe);
                    r.on_final_redaction(
                        redactor_order,
                        "redact",
                        Arc::new(move |context, mut result| {
                            probe.record("redactor", &context, &result);
                            Box::pin(async move {
                                if let Some(object) = result.as_object_mut() {
                                    object.remove("secret");
                                }
                                Ok(TransformOutput::new(result))
                            })
                        }),
                    );
                }
            });
            let builder = Harness::builder(one_turn(&[&call]))
                .tool_pipeline(probe.clone())
                .execution_mode(ExecutionMode::Sequential);
            let harness = if redactor_first {
                builder
                    .mount(redactor, Scope::Global)
                    .mount(reducer, Scope::Global)
            } else {
                builder
                    .mount(reducer, Scope::Global)
                    .mount(redactor, Scope::Global)
            }
            .build()
            .await;
            let done = harness.run().await;
            assert_eq!(
                harness.probe.executed(),
                if matches!(
                    class,
                    ToolOutcomeClass::Succeeded | ToolOutcomeClass::ExecutionFailed
                ) {
                    vec![(name.to_owned(), json!({"text": input}))]
                } else {
                    vec![]
                }
            );
            assert_observations(
                &probe,
                &done,
                &call,
                class,
                seed,
                redactor_first,
                (reducer_order, redactor_order),
            );
            assert_protected(&done, &call, class).await;
        }
    }
}

#[tokio::test]
async fn success_runs_pre_stage_then_reducer_then_final_redactor() {
    composed_path(ToolOutcomeClass::Succeeded).await;
}

#[tokio::test]
async fn execution_failure_runs_reducer_then_final_redactor_without_pre_stage() {
    composed_path(ToolOutcomeClass::ExecutionFailed).await;
}

#[tokio::test]
async fn permission_denial_runs_reducer_then_final_redactor_without_pre_stage() {
    composed_path(ToolOutcomeClass::PermissionDenied).await;
}

#[tokio::test]
async fn unknown_tool_runs_reducer_then_final_redactor_without_pre_stage() {
    composed_path(ToolOutcomeClass::UnknownTool).await;
}

#[tokio::test]
async fn preparation_failure_runs_reducer_then_final_redactor_without_pre_stage() {
    composed_path(ToolOutcomeClass::PrepareFailed).await;
}
