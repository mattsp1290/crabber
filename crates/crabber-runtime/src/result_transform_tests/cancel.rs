//! `RunHandle` interruption through the executor pre-stage and the result-chain boundaries.

use super::*;
use crate::{
    INTERRUPT_SETTLEMENT_BOUND, INTERRUPTED_RESULT_TEXT, Observer, OperationKind,
    OperationalObservation, TerminalReason,
};
use crabber_core::{RunStatus, ToolResultStatus};
use crabber_extension::{FINAL_REDACTION_DEADLINE, TransformOutput};
use std::time::Duration;
use tokio::time::{Instant, advance, timeout};

const TARGET: &str = "private-source";
const SEED: &str = "PRIVATE-EXECUTED-SEED";
const ACCEPTED: &str = "PRIVATE-ACCEPTED-VALUE";
const PARTIAL: &str = "PRIVATE-PARTIALLY-REDACTED";
const PROTECTED: &str = "protected-by-both-finals";

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Case {
    PreStage,
    Ordinary,
    BetweenOrdinary,
    FinalInFlight,
    FinalError,
    FinalPanic,
    FinalDeadline,
    NoFinal,
    EveryFinal,
}

struct PrivateSource;
#[async_trait]
impl ToolExecutor for PrivateSource {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Ok(json!(SEED))
    }
}

struct Pipeline {
    gate: Arc<Gate>,
    case: Case,
}
#[async_trait]
impl ToolPipeline for Pipeline {
    async fn prepare(&self, _: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(
        &self,
        context: &ToolResultContext,
        _: &ToolInfo,
        result: Value,
    ) -> Result<TransformOutput, String> {
        if self.case == Case::PreStage {
            self.gate.pass(&json!({"call": context.call_id()})).await;
        }
        Ok(TransformOutput::new(result))
    }
}

// execute_tool reports this measurement after its chain returns, before waiting for index 0.
struct SiblingReturned {
    ready: Semaphore,
    observations: Mutex<Vec<OperationalObservation>>,
}
impl Observer for SiblingReturned {
    fn emit(&self, _: &EventRecord) {}
    fn operational_completed(&self, observation: &OperationalObservation) {
        if observation.kind == (OperationKind::Tool { name: SHOUT.into() }) {
            self.observations.lock().unwrap().push(observation.clone());
            self.ready.add_permits(1);
        }
    }
}

async fn signalled(ready: &Semaphore) {
    timeout(INTERRUPT_SETTLEMENT_BOUND, ready.acquire())
        .await
        .expect("stage must signal within the named bound")
        .unwrap()
        .forget();
}

type StageLog = Arc<Mutex<Vec<(String, &'static str, Value, bool)>>>;

fn transforms(
    case: Case,
    gate: Arc<Gate>,
    finals: Arc<Gate>,
    cancelled: Arc<Semaphore>,
    log: StageLog,
) -> Arc<ClosureExtension> {
    ClosureExtension::new("cancellation-matrix", move |r| {
        r.tool(Arc::new(ToolDefinition {
            info: ToolInfo {
                name: TARGET.into(),
                description: "private result fixture".into(),
                parameters: json!({"type": "object"}),
                retry_safe: true,
                required_permissions: vec![],
            },
            executor: Arc::new(PrivateSource),
        }));
        for (order, stage) in [(-10, "first"), (10, "next")] {
            let gate = gate.clone();
            let log = log.clone();
            r.on_result_transform(
                order,
                stage,
                Arc::new(move |context, value| {
                    let gate = gate.clone();
                    let log = log.clone();
                    Box::pin(async move {
                        log.lock().unwrap().push((
                            context.tool_name().to_owned(),
                            stage,
                            value.clone(),
                            context.cancellation().is_cancelled(),
                        ));
                        // At next entry the predecessor's output has been committed. Hold the
                        // next body before processing it; this does not pin an internal precheck.
                        if (stage == "first" && case == Case::Ordinary)
                            || (stage == "next"
                                && matches!(
                                    case,
                                    Case::BetweenOrdinary | Case::NoFinal | Case::EveryFinal
                                ))
                        {
                            gate.pass(&json!({"call": context.call_id()})).await;
                        }
                        Ok(TransformOutput::new(if stage == "first" {
                            json!(ACCEPTED)
                        } else {
                            value
                        }))
                    })
                }),
            );
        }
        register_finals(r, case, &finals, &cancelled, &log);
    })
}

fn register_finals(
    r: &mut Registrar,
    case: Case,
    finals: &Arc<Gate>,
    cancelled: &Arc<Semaphore>,
    log: &StageLog,
) {
    if !matches!(case, Case::NoFinal | Case::BetweenOrdinary) {
        for (order, stage) in [(-100, "final-one"), (-90, "final-two")] {
            let gate = finals.clone();
            let stages = log.clone();
            let seen = cancelled.clone();
            r.on_final_redaction(
                order,
                stage,
                Arc::new(move |context, value| {
                    let gate = gate.clone();
                    let stages = stages.clone();
                    let seen = seen.clone();
                    Box::pin(async move {
                        stages.lock().unwrap().push((
                            context.tool_name().to_owned(),
                            stage,
                            value,
                            context.cancellation().is_cancelled(),
                        ));
                        if context.tool_name() == TARGET {
                            if stage == "final-one"
                                && matches!(
                                    case,
                                    Case::FinalInFlight
                                        | Case::FinalError
                                        | Case::FinalPanic
                                        | Case::FinalDeadline
                                )
                            {
                                // Keep the final in flight before interrupt, then prove
                                // it survives cancellation when there is an accepted value.
                                gate.pass(&json!({"stage": "before-cancel"})).await;
                                context.cancellation().cancelled().await;
                                seen.add_permits(1);
                                gate.pass(&json!({"stage": stage})).await;
                                match case {
                                    Case::FinalError => {
                                        return Err(ExtensionError::Tool(
                                            "PRIVATE-FINAL-ERROR".into(),
                                        ));
                                    }
                                    Case::FinalPanic => panic!("PRIVATE-FINAL-PANIC"),
                                    _ => {}
                                }
                            } else if case == Case::FinalDeadline && stage == "final-two" {
                                gate.pass(&json!({"stage": stage})).await;
                            }
                        }
                        Ok(TransformOutput::new(json!(if stage == "final-one" {
                            PARTIAL
                        } else {
                            PROTECTED
                        })))
                    })
                }),
            );
        }
    }
}

async fn assert_sibling_waiting(
    done: &Finished<'_>,
    sibling: &ToolCallId,
    returned: &SiblingReturned,
) {
    {
        let observations = returned.observations.lock().unwrap();
        assert_eq!(observations.len(), 1);
        assert_eq!(observations[0].reason, TerminalReason::Success);
    }
    let record = done.record(sibling).await;
    assert_eq!(record.status, ToolCallStatus::Running);
    assert!(
        record.result.is_none(),
        "returned sibling awaits index-0 settlement"
    );
}

fn install_observer(harness: &mut Harness, mode: ExecutionMode, returned: Arc<SiblingReturned>) {
    harness.runtime = Some(
        Orchestrator::builder()
            .store(harness.store.clone())
            .resolver(Arc::new(harness.fake.clone()))
            .plan_provider(harness.registry.clone())
            .policy(harness.policy.clone())
            .tool_pipeline(harness.pipeline.clone().unwrap())
            .execution_mode(mode)
            .observer(returned)
            .build()
            .unwrap(),
    );
}

async fn exercise(case: Case, mode: ExecutionMode) {
    // Private result markers originate in the executor, not provider-authored arguments.
    let target = ScriptedCall::text(TARGET, "public-input");
    let sibling = ScriptedCall::text(SHOUT, "public-sibling-input");
    let target_id = target.id.clone();
    let gate = Gate::when(move |key| key["call"] == target_id.to_string());
    let finals = Gate::all();
    let cancelled = Arc::new(Semaphore::new(0));
    let log = Arc::new(Mutex::new(Vec::new()));
    let transform = transforms(
        case,
        gate.clone(),
        finals.clone(),
        cancelled.clone(),
        log.clone(),
    );
    let pipeline = Arc::new(Pipeline {
        gate: gate.clone(),
        case,
    });
    let parallel = matches!(mode, ExecutionMode::Parallel { .. });
    let mut harness = Harness::builder(one_turn(&[&target, &sibling]))
        .execution_mode(mode)
        .tool_pipeline(pipeline)
        .mount(transform, Scope::Global)
        .build()
        .await;
    let returned = Arc::new(SiblingReturned {
        ready: Semaphore::new(0),
        observations: Mutex::default(),
    });
    install_observer(&mut harness, mode, returned.clone());
    let handle = harness.start().await;
    let done = Finished {
        harness: &harness,
        session_id: handle.session_id().clone(),
        run_id: handle.run_id().clone(),
    };
    if matches!(
        case,
        Case::FinalInFlight | Case::FinalError | Case::FinalPanic | Case::FinalDeadline
    ) {
        assert_eq!(finals.entered().await["stage"], "before-cancel");
        assert_eq!(finals.release_all(), 1);
    } else {
        assert_eq!(gate.entered().await["call"], target.id.to_string());
    }
    if parallel {
        signalled(&returned.ready).await;
        assert_sibling_waiting(&done, &sibling.id, &returned).await;
    }
    if matches!(
        case,
        Case::BetweenOrdinary | Case::NoFinal | Case::EveryFinal
    ) {
        assert!(
            log.lock()
                .unwrap()
                .iter()
                .any(|(name, stage, value, _)| name == TARGET
                    && *stage == "next"
                    && value == &json!(ACCEPTED))
        );
    }
    let interrupted_at = Instant::now();
    handle.interrupt();
    let expected = finish_finals(case, &done, &target.id, &finals, &cancelled).await;
    let result = timeout(INTERRUPT_SETTLEMENT_BOUND, handle.done())
        .await
        .expect("run must settle within the named interrupt bound")
        .unwrap();
    assert_eq!(result.status, RunStatus::Interrupted);
    assert_eq!(
        harness
            .store
            .get_run(&done.run_id)
            .await
            .unwrap()
            .unwrap()
            .status,
        RunStatus::Interrupted
    );
    assert!(interrupted_at.elapsed() <= INTERRUPT_SETTLEMENT_BOUND);
    if case == Case::FinalDeadline {
        assert_eq!(interrupted_at.elapsed(), FINAL_REDACTION_DEADLINE);
    } else {
        assert!(interrupted_at.elapsed() < FINAL_REDACTION_DEADLINE);
    }
    assert_eq!(
        gate.parked(),
        0,
        "dropped ordinary/pre-stage receivers are excluded"
    );
    assert_eq!(gate.release_all(), 0);
    assert_eq!(finals.parked(), 0);
    assert_eq!(finals.release_all(), 0);
    assert_persisted(&done, &target.id, &sibling.id, &expected).await;
    assert_stages(case, &log);
}

async fn finish_finals(
    case: Case,
    done: &Finished<'_>,
    target: &ToolCallId,
    finals: &Gate,
    cancelled: &Semaphore,
) -> String {
    let mut expected = INTERRUPTED_RESULT_TEXT.to_owned();
    if matches!(
        case,
        Case::FinalInFlight | Case::FinalError | Case::FinalPanic | Case::FinalDeadline
    ) {
        signalled(cancelled).await;
        assert_eq!(finals.entered().await["stage"], "final-one");
        assert_eq!(finals.parked(), 1, "in-flight final survives interrupt");
        if case == Case::FinalDeadline {
            advance(FINAL_REDACTION_DEADLINE / 2).await;
            assert_eq!(finals.release_all(), 1);
            assert_eq!(finals.entered().await["stage"], "final-two");
            advance(
                (FINAL_REDACTION_DEADLINE / 2)
                    .checked_sub(Duration::from_nanos(1))
                    .unwrap(),
            )
            .await;
            assert_eq!(done.record(target).await.status, ToolCallStatus::Running);
            advance(Duration::from_nanos(1)).await;
        } else {
            assert_eq!(finals.release_all(), 1);
            if case == Case::FinalInFlight {
                expected = serde_json::to_string(PROTECTED).unwrap();
            }
        }
    } else if case == Case::EveryFinal {
        expected = serde_json::to_string(PROTECTED).unwrap();
    }
    expected
}

async fn assert_persisted(
    done: &Finished<'_>,
    target: &ToolCallId,
    sibling: &ToolCallId,
    expected: &str,
) {
    assert_eq!(
        done.harness
            .store
            .list_unfinished_tool_calls(&done.run_id)
            .await
            .unwrap(),
        [] as [ToolCallRecord; 0]
    );
    for (call, text) in [(target, expected), (sibling, INTERRUPTED_RESULT_TEXT)] {
        let record = done.record(call).await;
        assert_eq!(record.status, ToolCallStatus::Interrupted);
        assert_eq!(
            record.result.as_ref().unwrap().status,
            ToolResultStatus::Interrupted
        );
        assert_eq!(record_text(&record), text);
        let (message, message_text, is_error) = done.tool_message(call).await;
        assert_eq!(message_text, text);
        assert!(is_error);
        let event = done.settled_event(call).await;
        assert_eq!(event.payload["content"][0]["text"], text);
        assert_eq!(event.payload["is_error"], true);
        for persisted in [
            serde_json::to_string(&record.result).unwrap(),
            serde_json::to_string(&message).unwrap(),
            serde_json::to_string(&event).unwrap(),
        ] {
            assert_safe(&persisted);
        }
    }
    // Check every full provider request, including arguments, system messages and earlier turns.
    for request in done.harness.fake.requests() {
        assert_safe(&format!("{request:?}"));
    }
    assert_eq!(
        done.harness.fake.requests().len(),
        1,
        "interruption prevents a later model turn"
    );
}

fn assert_stages(case: Case, log: &StageLog) {
    let stages = log.lock().unwrap();
    if case != Case::PreStage {
        assert!(stages.iter().any(|(name, stage, value, _)| name == TARGET
            && *stage == "first"
            && value == &json!(SEED)));
    }
    let target_finals: Vec<_> = stages
        .iter()
        .filter(|(name, stage, _, _)| name == TARGET && stage.starts_with("final"))
        .collect();
    if matches!(
        case,
        Case::FinalInFlight | Case::EveryFinal | Case::FinalDeadline
    ) {
        assert_eq!(target_finals.len(), 2);
        assert_eq!(target_finals[0].2, json!(ACCEPTED));
        assert_eq!(target_finals[1].2, json!(PARTIAL));
        assert!(
            target_finals[1].3,
            "remaining final receives the cancelled child token"
        );
        if case == Case::EveryFinal {
            assert!(target_finals[0].3, "every final starts after cancellation");
        }
    } else if matches!(case, Case::FinalError | Case::FinalPanic) {
        assert_eq!(target_finals.len(), 1, "failure skips the remaining final");
    } else {
        assert_eq!(
            target_finals.len(),
            0,
            "seed or unprotected value must not reach finals"
        );
    }
}

fn assert_safe(encoded: &str) {
    for private in [
        SEED,
        ACCEPTED,
        PARTIAL,
        "PRIVATE-FINAL-ERROR",
        "PRIVATE-FINAL-PANIC",
    ] {
        assert!(
            !encoded.contains(private),
            "private result leaked: {encoded}"
        );
    }
}

macro_rules! cancellation_test {
    ($name:ident, $case:ident) => {
        #[tokio::test(start_paused = true)]
        async fn $name() {
            for mode in [
                ExecutionMode::Sequential,
                ExecutionMode::Parallel { max: 2 },
            ] {
                exercise(Case::$case, mode).await;
            }
        }
    };
}
cancellation_test!(blocked_pre_stage_is_not_accepted, PreStage);
cancellation_test!(blocked_ordinary_drops_seed, Ordinary);
cancellation_test!(
    accepted_predecessor_at_next_ordinary_entry_is_discarded,
    BetweenOrdinary
);
cancellation_test!(
    in_flight_final_survives_and_remaining_final_protects,
    FinalInFlight
);
cancellation_test!(
    final_error_after_interrupt_uses_fixed_interrupted_text,
    FinalError
);
cancellation_test!(
    final_panic_after_interrupt_uses_fixed_interrupted_text,
    FinalPanic
);
cancellation_test!(two_finals_share_one_cancellation_deadline, FinalDeadline);
cancellation_test!(accepted_value_without_final_is_discarded, NoFinal);
cancellation_test!(
    accepted_value_passes_every_final_after_interrupt,
    EveryFinal
);

#[tokio::test(start_paused = true)]
async fn gate_release_counts_live_receivers_after_a_handler_is_dropped() {
    let gate = Gate::all();
    let held = gate.clone();
    let dropped = tokio::spawn(async move { held.pass(&json!("dropped")).await });
    assert_eq!(gate.entered().await, "dropped");
    let held = gate.clone();
    let live = tokio::spawn(async move { held.pass(&json!("live")).await });
    assert_eq!(gate.entered().await, "live");
    dropped.abort();
    assert!(dropped.await.unwrap_err().is_cancelled());
    // Exercise release's own filtering before parked() has a chance to remove stale entries.
    assert_eq!(gate.release_all(), 1);
    timeout(INTERRUPT_SETTLEMENT_BOUND, live)
        .await
        .unwrap()
        .unwrap();
    assert_eq!(gate.parked(), 0);
}
