//! Ordinary tool work runs only after an acknowledged native phase start.
use super::tool_batch::Batch;
use super::tool_phase::OwnedPhase;
use super::{
    AtomicBool, EventPublished, Measurement, OperationKind, Orchestrator, PendingCall,
    RecordedCall, RuntimeError, TOOL_PIPELINE_HANDLER_ID, TerminalReason, ToolCallRecord,
    ToolInput, ToolOutcomeClass, ToolResultContext, ToolResultOutcome, ToolResultStatus, ToolRun,
    TransformOutput, Value, attempt, ensure_lease, read_recorded_call,
    result_transform_failed_message, runtime_error_output,
};
use crabber_extension::ToolResourceOwner;
use crabber_session::GrantedApproval;
use futures::FutureExt;
use std::sync::atomic::Ordering;
use tokio::sync::watch;

/// A protected output keeps its original class separate from transformation status.
struct ProtectedResult {
    class: ToolOutcomeClass,
    input: ToolInput,
    resolved: bool,
    outcome: ProtectedOutcome,
}
enum ProtectedOutcome {
    Transform(TransformOutput),
    PipelineFailed,
    Interrupted,
}
impl ProtectedResult {
    fn failed(class: ToolOutcomeClass, input: ToolInput, resolved: bool, error: &str) -> Self {
        debug_assert!(class.is_error());
        Self {
            class,
            input,
            resolved,
            outcome: ProtectedOutcome::Transform(TransformOutput {
                result: runtime_error_output(error),
                mark_error: false,
            }),
        }
    }
    // Suspension remains with the caller that owns the original phase/resource scope.
    fn from_run(run: ToolRun, input: ToolInput) -> Result<Self, RuntimeError> {
        let outcome = match run {
            ToolRun::Output(value) => ProtectedOutcome::Transform(value),
            ToolRun::PipelineFailed => ProtectedOutcome::PipelineFailed,
            ToolRun::ProtectionInterrupted => ProtectedOutcome::Interrupted,
            ToolRun::PermissionDenied => {
                return Ok(Self::failed(
                    ToolOutcomeClass::PermissionDenied,
                    input,
                    true,
                    "permission denied",
                ));
            }
            ToolRun::ExecutionFailed(error) => {
                return Ok(Self::failed(
                    ToolOutcomeClass::ExecutionFailed,
                    input,
                    true,
                    &error,
                ));
            }
            ToolRun::Suspended(_) | ToolRun::SuspensionInvalid => {
                return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome.into());
            }
        };
        Ok(Self {
            class: ToolOutcomeClass::Succeeded,
            input,
            resolved: true,
            outcome,
        })
    }
}
struct Work<'a> {
    scope: Batch<'a>,
    call: PendingCall,
    record: ToolCallRecord,
    authorization: GrantedApproval,
    settlement: Option<(usize, watch::Receiver<usize>, watch::Sender<usize>)>,
    owner: &'a OwnedPhase,
    running: crabber_core::EventRecord,
    resources: &'a ToolResourceOwner,
    settled: &'a AtomicBool,
}
impl Orchestrator {
    pub(super) async fn execute_tool(
        &self,
        scope: Batch<'_>,
        record: ToolCallRecord,
        authorization: GrantedApproval,
        settlement: Option<(usize, watch::Receiver<usize>, watch::Sender<usize>)>,
    ) -> Result<(), RuntimeError> {
        ensure_lease(scope.lease_lost)?;
        if scope.cancellation.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        if super::frontend::declarations(scope.plan)?
            .iter()
            .any(|tool| tool.name == record.name)
            && !self
                .frontend_resumes
                .lock()
                .expect("frontend resumes poisoned")
                .contains_key(&record.id)
        {
            // Initial frontend calls wait for the host. They acquire no native
            // phase and never enter the backend executor or its middleware.
            self.store
                .frontend_call_origin(&scope.session.id, scope.run, &record.id)
                .await?
                .ok_or(crabber_session::ToolContinuationError::UnknownStoreOutcome)?;
            return Ok(());
        }
        let (owner, running) = self
            .begin_tool_phase(
                scope.execution,
                &scope.session.id,
                scope.run,
                &record,
                &authorization,
                scope.turn.clone(),
            )
            .await?;
        let worker = self
            .worker_resources
            .as_ref()
            .filter(|worker| worker.matches(&scope.session.id, scope.run))
            .ok_or(RuntimeError::PreparationRecoveryRequired)?;
        let resources = worker.tool_resources(record.id.clone(), Some(owner.target.clone()));
        let guard = resources.guard();
        let callbacks = crabber_extension::CallbackInvocationOwner::attach(resources.resources())
            .map_err(|_| RuntimeError::PreparationRecoveryRequired)?;
        let settled = AtomicBool::new(false);
        let mut call = PendingCall::from_record(&record);
        call.turn_id.clone_from(&scope.turn);
        let result = std::panic::AssertUnwindSafe(crabber_extension::with_callback_invocations(
            callbacks.capability(),
            self.execute_tool_body(Work {
                scope: scope.clone(),
                call,
                record,
                authorization,
                settlement,
                owner: &owner,
                running,
                resources: &resources,
                settled: &settled,
            }),
        ))
        .catch_unwind()
        .await;
        drop(callbacks);
        drop(guard);
        let result = match result {
            Ok(result) => result,
            Err(payload) => {
                if let Err(nested) =
                    std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| drop(payload)))
                {
                    std::mem::forget(nested);
                }
                if settled.load(Ordering::SeqCst) {
                    // Notification failure cannot revoke an acknowledged
                    // completion or invent a second reconciliation operation.
                    return Err(RuntimeError::Extension(
                        "tool settlement notification failed".into(),
                    ));
                }
                return Err(self
                    .stop_phase_after_error(&scope, &owner, &resources)
                    .await);
            }
        };
        match result {
            Ok(()) => Ok(()),
            Err(RuntimeError::LeaseLost) => {
                resources.drain(attempt::RESOURCE_JOIN_BOUND).await;
                Err(RuntimeError::LeaseLost)
            }
            Err(RuntimeError::Interrupted) if settled.load(Ordering::SeqCst) => {
                Err(RuntimeError::Interrupted)
            }
            Err(error @ RuntimeError::PhaseOutcomeUnknown { .. }) => {
                resources.drain(attempt::RESOURCE_JOIN_BOUND).await;
                Err(error)
            }
            Err(_) => Err(self
                .stop_phase_after_error(&scope, &owner, &resources)
                .await),
        }
    }
    async fn stop_phase_after_error(
        &self,
        scope: &Batch<'_>,
        owner: &OwnedPhase,
        resources: &ToolResourceOwner,
    ) -> RuntimeError {
        let cleanup = resources.drain(attempt::RESOURCE_JOIN_BOUND).await;
        let complete = cleanup.sealed && cleanup.pending == 0 && cleanup.failures.is_empty();
        self.stopped_tool_phase(
            scope.execution,
            &scope.session.id,
            scope.run,
            owner,
            complete,
        )
        .await
    }
    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    async fn execute_tool_body(&self, work: Work<'_>) -> Result<(), RuntimeError> {
        let Work {
            scope,
            call,
            record,
            authorization,
            settlement,
            owner,
            running,
            resources,
            settled: acknowledged,
        } = work;
        let Batch {
            execution,
            session,
            run: run_id,
            plan,
            turn: _,
            cancellation,
            lease_lost,
            ..
        } = scope;
        let session_id = &session.id;
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let frontend = self
            .frontend_resumes
            .lock()
            .expect("frontend resumes poisoned")
            .get(&record.id)
            .cloned();
        let recorded =
            read_recorded_call(record.arguments.clone(), &call.name, definition.is_some());
        ensure_lease(lease_lost)?;
        let tool_name = call.name.clone();
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            session_id,
            run_id,
            OperationKind::Tool {
                name: tool_name.clone(),
            },
            cancellation,
            lease_lost,
        );
        let tool_started_at = self.monotonic_clock.now();
        self.observer.emit(&running);
        let running_projection = serde_json::to_value(&running).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(running_projection.clone())
            .await;
        plan.dispatcher
            .notify::<crabber_extension::ToolStarted>(running_projection)
            .await;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await
            .inspect_err(|_| {
                measurement.observation.reason = TerminalReason::RuntimeError;
            })?;
        ensure_lease(lease_lost)?;
        let child_cancellation = cancellation.child_token();
        let protected = if let Some(frontend) = frontend {
            if frontend.target != owner.target
                || frontend.frozen.arguments != record.arguments
                || frontend.frozen.descriptor.name != record.name
                || frontend.frozen.call_binding != owner.call_binding
                || definition.is_some()
            {
                return Err(crabber_session::ToolContinuationError::SemanticConflict.into());
            }
            crabber_session::validate_frontend_result(
                &frontend.frozen.descriptor,
                &frontend.result,
            )?;
            let input = ToolInput::Normalized(record.arguments.clone());
            let context = ToolResultContext::new(
                call.name.clone(),
                true,
                input.clone(),
                record.id.clone(),
                session_id.clone(),
                run_id.clone(),
                ToolOutcomeClass::Succeeded,
            )
            .with_cancellation(child_cancellation.clone())
            .with_cleanup(resources.cleanup_tracker());
            let descriptor = frontend.frozen.descriptor;
            let info = crabber_core::ToolInfo {
                name: descriptor.name,
                description: descriptor.description,
                parameters: descriptor.input_parameters,
                retry_safe: false,
                required_permissions: Vec::new(),
            };
            let protected = self
                .protect_tool_output(
                    &info,
                    frontend.result.output().clone(),
                    &context,
                    cancellation,
                    lease_lost,
                )
                .await?;
            // Frontend values enter only protection, never backend middleware/execution.
            if !matches!(
                &protected,
                ToolRun::Output(_) | ToolRun::PipelineFailed | ToolRun::ProtectionInterrupted
            ) {
                return Err(crabber_session::ToolContinuationError::UnknownStoreOutcome.into());
            }
            ProtectedResult::from_run(protected, input)?
        } else {
            match recorded {
                RecordedCall::Execute(arguments) => {
                    let tool =
                        definition.expect("the record reader executes only calls that resolve");
                    let input = ToolInput::Normalized(arguments.clone());
                    let pipeline_context = ToolResultContext::new(
                        call.name.clone(),
                        true,
                        input.clone(),
                        record.id.clone(),
                        session_id.clone(),
                        run_id.clone(),
                        ToolOutcomeClass::Succeeded,
                    )
                    .with_cancellation(child_cancellation.clone())
                    .with_cleanup(resources.cleanup_tracker());
                    let task_resources = resources.resources();
                    let run = self
                        .permit_and_execute(
                            execution,
                            session,
                            run_id,
                            &call.id,
                            plan,
                            &tool,
                            authorization.decision,
                            arguments,
                            cancellation,
                            lease_lost,
                            &pipeline_context,
                            &task_resources,
                            owner,
                        )
                        .await
                        .inspect_err(|error| {
                            measurement.observation.reason = match error {
                                RuntimeError::LeaseLost => TerminalReason::LeaseLost,
                                RuntimeError::Interrupted => TerminalReason::Cancelled,
                                _ => TerminalReason::RuntimeError,
                            };
                        })?;
                    match run {
                        ToolRun::Suspended(proposal) => {
                            let identity =
                                super::tool_invoke::frozen_identity(plan, &tool.info.name)?.ok_or(
                                    crabber_session::ToolContinuationError::SchemaMismatch,
                                )?;
                            self.retain_tool_suspension(
                                run_id,
                                &record,
                                owner,
                                identity,
                                proposal,
                                resources,
                                cancellation,
                            )
                            .await?;
                            return Ok(());
                        }
                        ToolRun::SuspensionInvalid => {
                            return Err(
                                crabber_session::ToolContinuationError::SchemaMismatch.into()
                            );
                        }
                        run => ProtectedResult::from_run(run, input)?,
                    }
                }
                RecordedCall::Settled {
                    class,
                    input,
                    resolved,
                    seed,
                } => ProtectedResult::failed(class, input, resolved, &seed),
            }
        };
        ensure_lease(lease_lost)?;
        let ProtectedResult {
            class,
            input,
            resolved,
            outcome,
        } = protected;
        // Built from the stored record only, so a resumed call derives the same context.
        let result_context = ToolResultContext::new(
            call.name.clone(),
            resolved,
            input,
            record.id.clone(),
            session_id.clone(),
            run_id.clone(),
            class,
        )
        .with_cancellation(child_cancellation)
        .with_cleanup(resources.cleanup_tracker());
        #[cfg(test)]
        if let Some(observer) = &self.context_observer {
            observer(&result_context);
        }
        let outcome = match outcome {
            ProtectedOutcome::Interrupted => ToolResultOutcome::Interrupted { redacted: None },
            ProtectedOutcome::PipelineFailed => ToolResultOutcome::Failed {
                handler: TOOL_PIPELINE_HANDLER_ID.into(),
            },
            ProtectedOutcome::Transform(seed) => {
                plan.dispatcher
                    .transform_tool_result(result_context, seed)
                    .await
            }
        };
        let mut interrupted = matches!(outcome, ToolResultOutcome::Interrupted { .. });
        let mut fixed_interruption =
            matches!(outcome, ToolResultOutcome::Interrupted { redacted: None });
        let (mut status, output, is_error) = match outcome {
            ToolResultOutcome::Completed { result, is_error } => (
                if is_error {
                    ToolResultStatus::Failed
                } else {
                    ToolResultStatus::Completed
                },
                result,
                is_error,
            ),
            ToolResultOutcome::Failed { handler } => (
                ToolResultStatus::Failed,
                runtime_error_output(&result_transform_failed_message(&handler)),
                true,
            ),
            ToolResultOutcome::Interrupted {
                redacted: Some(value),
            } => (ToolResultStatus::Interrupted, value, true),
            // The executor/middleware already returned. Protection discarded its
            // value, so only the fixed runtime cancellation result may persist.
            ToolResultOutcome::Interrupted { redacted: None } => (
                ToolResultStatus::Interrupted,
                crabber_core::ToolOutput::from_content_blocks(
                    vec![crabber_core::ContentBlock::Text {
                        text: super::INTERRUPTED_RESULT_TEXT.into(),
                    }],
                    true,
                )
                .expect("fixed runtime interruption is valid"),
                true,
            ),
        };
        measurement.observation.reason = if interrupted {
            TerminalReason::Cancelled
        } else if is_error {
            TerminalReason::ToolError
        } else {
            TerminalReason::Success
        };
        drop(measurement);
        let cleanup = resources.drain(attempt::RESOURCE_JOIN_BOUND).await;
        if !cleanup.sealed || cleanup.pending != 0 || !cleanup.failures.is_empty() {
            return Err(RuntimeError::AttemptCleanupIncomplete {
                run_id: run_id.clone(),
            });
        }
        let mut output = if is_error {
            output.with_error()
        } else {
            output
        };
        // Protected interruptions may settle out of order; the batch dispatcher
        // still lets already-started siblings reach their own safe boundary.
        if !interrupted && let Some((index, receiver, _)) = &settlement {
            let mut receiver = receiver.clone();
            while *receiver.borrow_and_update() != *index {
                tokio::select! {
                    biased;
                    () = cancellation.cancelled() => {
                        // This sibling returned before cancellation but its
                        // result had not committed. Discard that stale value;
                        // do not rerun protection or drop an active executor.
                        output = crabber_core::ToolOutput::from_content_blocks(
                            vec![crabber_core::ContentBlock::Text {
                                text: super::INTERRUPTED_RESULT_TEXT.into(),
                            }], true,
                        ).expect("fixed runtime interruption is valid");
                        status = ToolResultStatus::Interrupted;
                        interrupted = true;
                        fixed_interruption = true;
                        break;
                    }
                    changed = receiver.changed() => {
                        changed.map_err(|_| RuntimeError::TaskStopped)?;
                    }
                }
            }
        }
        ensure_lease(lease_lost)?;
        let duration_ms = u64::try_from(
            self.monotonic_clock
                .now()
                .saturating_sub(tool_started_at)
                .as_millis(),
        )
        .unwrap_or(u64::MAX);
        let settled = self
            .settle_owned_phase(
                execution,
                session_id,
                run_id,
                &record,
                owner,
                &output,
                status,
                call.turn_id.clone(),
                duration_ms,
            )
            .await?;
        acknowledged.store(true, Ordering::SeqCst);
        // This phase is already sealed and joined. All post-settlement
        // callbacks, including the observer, belong to the still-live worker.
        let notifications = async {
            self.observer.emit(&settled);
            if interrupted {
                self.notify_interrupted_settlement(plan, &settled, !fixed_interruption)?;
                return Err(RuntimeError::Interrupted);
            }
            let settled_projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
            plan.dispatcher
                .notify::<EventPublished>(settled_projection.clone())
                .await;
            plan.dispatcher
                .notify::<crabber_extension::ToolSettled>(settled_projection)
                .await;
            Ok(())
        };
        if let Some(capability) = &self.worker_callbacks {
            crabber_extension::with_callback_invocations(capability.clone(), notifications).await
        } else {
            notifications.await
        }
    }
}
