use super::{
    Arc, AtomicBool, CancellationToken, CheckpointRequest, ContentBlock, ContextAssemble,
    EventKind, EventPublished, ExecutionStore, HeartbeatGuard, InboxKind, InitialEntry,
    InitialRequest, InterruptPolicy, Measurement, OperationKind, Orchestrator, Ordering, Request,
    RequestIdentity, Role, RunBeforeExecute, RunFence, RunHandle, RunId, RunPlan, RunResult,
    RunSession, RunSettled, RunStateSink, RunStatus, RuntimeError, Session, SessionId, StateSink,
    StoreError, TurnId, TurnPrepare, TurnSnapshot, Usage, Value, WorkspaceContext, driver,
    ensure_lease, frontend, json, result_reason, terminal, tool_batch, worker,
};
use crate::telemetry_targets::targets;
use tracing::Instrument;

impl Orchestrator {
    /// Every admitted run is bound to the workspace identity of the persisted
    /// `session` it was admitted into; there is no other way to start one.
    pub(super) fn spawn_admitted(
        &self,
        request: Request,
        plan: RunPlan,
        fence: RunFence,
        session: &Session,
        entry: InitialEntry,
        bound: Option<crabber_providers::BoundProvider>,
    ) -> Result<RunHandle, RuntimeError> {
        let work_session = RunSession::from(session);
        let driver = driver::registration::for_entry(
            &entry,
            plan.retains_preparation_resources(),
            crabber_core::SessionContext::new(
                session.id.clone(),
                session.workspace_id.clone(),
                session.directory.clone(),
            ),
            fence.clone(),
        );
        self.spawn_worker_inner(
            session.id.clone(),
            fence.run_id.clone(),
            driver,
            move |runtime, cancellation| async move {
                Box::pin(runtime.run(
                    fence,
                    work_session,
                    InitialRequest {
                        request,
                        entry,
                        bound,
                    },
                    plan,
                    cancellation,
                ))
                .await
            },
        )
    }

    pub(super) fn spawn_worker<F, W>(
        &self,
        session_id: SessionId,
        run_id: RunId,
        work: F,
    ) -> Result<RunHandle, RuntimeError>
    where
        F: FnOnce(Self, CancellationToken) -> W + Send + 'static,
        W: std::future::Future<Output = Result<RunResult, RuntimeError>> + Send + 'static,
    {
        self.spawn_worker_inner(session_id, run_id, None, work)
    }

    pub(super) fn spawn_worker_inner<F, W>(
        &self,
        session_id: SessionId,
        run_id: RunId,
        driver: Option<driver::Request>,
        work: F,
    ) -> Result<RunHandle, RuntimeError>
    where
        F: FnOnce(Self, CancellationToken) -> W + Send + 'static,
        W: std::future::Future<Output = Result<RunResult, RuntimeError>> + Send + 'static,
    {
        let worker::WorkerLaunch {
            handle,
            business,
            resources,
            complete,
        } = self.worker_launch(session_id.clone(), run_id.clone())?;
        let store = self.store.clone();
        let cancellation = handle.cancellation.clone();
        let parent = self.root_scope.as_ref().ok_or(RuntimeError::HostClosed)?;
        let original = parent.clone();
        let original_run = run_id.clone();
        let task = self.execution_tasks.spawn_reporting_in(
            parent,
            session_id,
            Some(run_id),
            move |_scope| async move {
                if let Some(request) = driver {
                    if let Err(error) = driver::run(
                        store,
                        resources,
                        request,
                        cancellation,
                        complete.clone(),
                        move || business.run(work),
                    )
                    .await
                    {
                        original.record_business_failure(&original_run, &error);
                        complete.publish(Err(error));
                    }
                } else {
                    business.run(work).await;
                    let _ = resources.finish_background().await;
                }
                complete.termination()
            },
        )?;
        Ok(handle.finish(task))
    }

    pub(super) async fn run(
        &self,
        fence: RunFence,
        session: RunSession,
        initial: InitialRequest,
        plan: RunPlan,
        cancellation: CancellationToken,
    ) -> Result<RunResult, RuntimeError> {
        let span = self.attempt_span(false, &session.id, &fence.run_id);
        let lost = AtomicBool::new(false);
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            &session.id,
            &fence.run_id,
            OperationKind::Run,
            &cancellation,
            &lost,
        );
        async {
            self.lifecycle(&session.id, &fence.run_id, None);
            let result = Box::pin(self.run_inner(
                fence.clone(),
                session.clone(),
                initial,
                plan,
                cancellation.clone(),
                &lost,
            ))
            .await;
            if result.is_err() && self.verified_lease_loss(&fence).await {
                lost.store(true, Ordering::SeqCst);
            }
            measurement.observation.reason = result_reason(&result);
            let outcome = super::telemetry::record_result(&span, &result);
            self.lifecycle(&session.id, &fence.run_id, Some(outcome));
            result
        }
        .instrument(span.clone())
        .await
    }

    #[allow(clippy::too_many_lines)]
    async fn run_inner(
        &self,
        fence: RunFence,
        session: RunSession,
        initial: InitialRequest,
        plan: RunPlan,
        cancellation: CancellationToken,
        observed_lost: &AtomicBool,
    ) -> Result<RunResult, RuntimeError> {
        let session_id = session.id.clone();
        let InitialRequest {
            request,
            entry,
            bound: prebound,
        } = initial;
        let binding = self.stored_binding(&session_id, &fence.run_id).await?;
        self.verify_binding_request(&request, &session_id, &binding)?;
        let mut request = request;
        request.input_envelope.clone_from(&binding.input_envelope);
        if plan.fingerprint().to_string() != binding.plan_fingerprint.as_str() {
            return Err(RuntimeError::PlanChanged);
        }
        let execution: Arc<dyn ExecutionStore> = self.store.execution(fence.clone()).await?.into();
        if !matches!(entry, InitialEntry::Unkeyed) {
            execution.begin_admission_execution().await?;
        }
        let mut worker = self.clone();
        worker.observation_lifetime = Arc::default();
        worker.tool_suspensions = Arc::default();
        worker.tool_resumes = Arc::default();
        worker.frontend_resumes = Arc::default();
        worker.owned_attempt = Some(Arc::new(
            self.begin_owned_attempt(execution.as_ref(), &session_id, &fence.run_id, false)
                .await?,
        ));
        worker.source_delivery = Some(worker.new_source_delivery(&session_id, &fence.run_id)?);
        let self_ = &worker;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await?;
        let heartbeat = HeartbeatGuard::start(
            Arc::clone(&execution),
            Arc::clone(&self.store),
            fence.clone(),
            Arc::clone(&self.clock),
            self.heartbeat_interval,
            Arc::clone(&worker.observation_lifetime),
            worker
                .worker_resources
                .as_ref()
                .ok_or(RuntimeError::PreparationRecoveryRequired)?,
        )?;
        let run_id = fence.run_id.clone();
        let run_started_at = self.clock.now();
        let mut signal = heartbeat.signal.clone();
        let lost = Arc::clone(&heartbeat.lost);
        let state_sink: Arc<dyn StateSink> = Arc::new(RunStateSink {
            store: Arc::clone(&self.store),
            execution: Arc::clone(&execution),
            session_id: session_id.clone(),
            lost: Arc::clone(&lost),
        });
        let current_turn = std::sync::Mutex::new(None);
        let public_source = self_.public_source_for_attempt(&session_id);
        let outcome = {
            let run_future = crabber_extension::with_state_sink(state_sink, async {
                let bound = if let Some(bound) = prebound {
                    if bound.descriptor() != &binding.provider {
                        return Err(crabber_providers::ProviderBindingError::VersionConflict.into());
                    }
                    bound
                } else {
                    self.restore_provider(&plan, &binding.provider).await?
                };
                self_
                    .run_loop(
                        execution.as_ref(),
                        &run_id,
                        &session,
                        &request,
                        &plan,
                        &bound,
                        &cancellation,
                        true,
                        Usage::default(),
                        0,
                        lost.as_ref(),
                        &current_turn,
                    )
                    .await
            });
            let run_future = crabber_extension::with_public_source(public_source, run_future);
            tokio::pin!(run_future);
            loop {
                tokio::select! {
                    biased;
                    update = signal.changed() => match update {
                        Ok(()) if *signal.borrow_and_update() => { tracing::warn!(target: targets::RUNTIME, { "error.kind" = "lease_lost", source = "run" }, "run attempt lost its lease"); break Err(RuntimeError::LeaseLost); },
                        Ok(()) => (),
                        Err(_) => break run_future.as_mut().await,
                    },
                    control = self_.wait_durable_cancel(execution.as_ref(), &fence) => {
                        match control {
                            Ok(()) => { tracing::info!(target: targets::RUNTIME, "durable cancel accepted"); cancellation.cancel(); break run_future.as_mut().await; }
                            Err(error) => {
                                if matches!(error, RuntimeError::LeaseLost) { lost.store(true, Ordering::SeqCst); }
                                break Err(error);
                            }
                        }
                    },
                    result = &mut run_future => break result,
                }
            }
        };
        drop(heartbeat);
        let result = match outcome {
            Ok(usage) => Ok(RunResult {
                session_id,
                run_id,
                status: RunStatus::Completed,
                usage,
            }),
            Err(error) => {
                let turn = current_turn.lock().expect("turn context poisoned").clone();
                self_
                    .finish_failed_run(
                        terminal::FailureContext {
                            execution: execution.as_ref(),
                            fence: &fence,
                            session: &session_id,
                            plan: &plan,
                            started_at: run_started_at,
                            lost: lost.as_ref(),
                            observed_lost,
                        },
                        error,
                        turn.as_ref(),
                    )
                    .await
            }
        };
        plan.release();
        result
    }

    #[allow(clippy::too_many_lines, clippy::too_many_arguments)]
    pub(super) async fn run_loop(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session: &RunSession,
        request: &Request,
        plan: &RunPlan,
        bound: &crabber_providers::BoundProvider,
        cancellation: &CancellationToken,
        initial: bool,
        mut usage: Usage,
        mut turns_used: usize,
        lease_lost: &AtomicBool,
        current_turn: &std::sync::Mutex<Option<TurnId>>,
    ) -> Result<Usage, RuntimeError> {
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        let session_id = &session.id;
        let run_started_at = self.clock.now();
        if initial {
            plan.dispatcher
                .gate::<RunBeforeExecute>(json!({"run_id": run_id.to_string()}))
                .await
                .map_err(|e| RuntimeError::Extension(e.to_string()))?;
            self.emit_durable(execution, session_id, run_id, plan, EventKind::RunAdmitted)
                .await?;
            self.emit_durable(execution, session_id, run_id, plan, EventKind::RunStarted)
                .await?;
        }
        while turns_used < self.max_turns {
            turns_used += 1;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Interrupted);
            }
            let turn_id = TurnId::new();
            *current_turn.lock().expect("turn context poisoned") = Some(turn_id.clone());
            self.publish_event(
                execution,
                plan,
                self.turn_event(session_id, run_id, &turn_id, EventKind::TurnStarted),
            )
            .await?;
            if self.store.supports_run_controls() {
                let boundary = execution.observe_run_controls(Some(&turn_id)).await?;
                if boundary.cancel_observed {
                    return Err(RuntimeError::Interrupted);
                }
            }
            let mut snapshot = self.snapshot(run_id, session, request, plan).await?;
            snapshot.identity.turn_id = turn_id.clone();
            let mut compacted = false;
            if let Some(context_limit) = bound.descriptor().limits().context_tokens {
                let history_bytes =
                    serde_json::to_vec(&snapshot.messages).map_or(0, |bytes| bytes.len());
                let context_bytes = if snapshot.context.is_empty() {
                    0
                } else {
                    serde_json::to_vec(&snapshot.context).map_or(0, |bytes| bytes.len())
                };
                let estimated = history_bytes.saturating_add(context_bytes) / 4;
                #[allow(clippy::cast_precision_loss)]
                if snapshot.messages.len() > self.compaction.keep_tail_messages
                    && context_limit > 0
                    && (estimated as f64) >= (context_limit as f64 * self.compaction.trigger_ratio)
                {
                    self.compact(
                        execution,
                        run_id,
                        session_id,
                        &snapshot,
                        plan,
                        bound,
                        false,
                        cancellation,
                        lease_lost,
                    )
                    .await?;
                    snapshot.messages = self.store.list_messages(session_id, None).await?;
                    compacted = true;
                }
            }
            plan.dispatcher
                .hook::<TurnPrepare>(json!({"run_id":run_id.to_string()}))
                .await
                .map_err(|e| RuntimeError::Extension(e.to_string()))?;
            let (calls, turn_usage) = Box::pin(self.model_turn_with_retry(
                execution,
                run_id,
                session_id,
                &snapshot,
                plan,
                &session.workspace,
                bound,
                compacted,
                cancellation,
                lease_lost,
            ))
            .await?;
            if cancellation.is_cancelled() {
                return Err(RuntimeError::Interrupted);
            }
            usage.input_tokens += turn_usage.input_tokens;
            usage.output_tokens += turn_usage.output_tokens;
            let had_tools = !calls.is_empty();
            let mut staged = Vec::with_capacity(calls.len());
            for call in calls {
                staged.push(
                    self.stage_tool(execution, session_id, run_id, plan, call, lease_lost)
                        .await?,
                );
            }
            let permissions = self
                .authorize_tools(
                    execution,
                    session,
                    run_id,
                    plan,
                    &staged,
                    &usage,
                    turns_used,
                    Some(turn_id.clone()),
                    lease_lost,
                )
                .await?;
            if staged.iter().any(|call| {
                plan.tools
                    .iter()
                    .find(|tool| tool.info.name == call.name)
                    .is_some_and(|tool| {
                        self.policy.interrupt_policy(&tool.info, &call.arguments)
                            == InterruptPolicy::Pause
                    })
            }) {
                let event = self.conclude_owned_attempt(execution, run_id,
                    crabber_session::RunBoundary::Suspended {
                        checkpoint: json!({"version":4,"turns_used":turns_used,"source_turn_id":turn_id,"pending_calls": staged.iter().map(|call| call.id.to_string()).collect::<Vec<_>>(),
                            "request": CheckpointRequest::new(request), "usage": usage}),
                        usage: usage.clone(),
                    }, Some(turn_id.clone()), run_started_at).await?;
                self.observer.emit(&event);
                return Err(RuntimeError::Paused);
            }
            self.execute_prepared_tools(
                tool_batch::Batch {
                    execution,
                    session,
                    run: run_id,
                    plan,
                    turn: Some(turn_id.clone()),
                    usage: &usage,
                    turns_used,
                    cancellation,
                    lease_lost,
                },
                staged,
                permissions,
            )
            .await?;
            let mut claimed = self.claim_input(execution, InboxKind::Steer).await?;
            if !had_tools && claimed == 0 {
                claimed += self.claim_input(execution, InboxKind::FollowUp).await?;
            }
            self.publish_event(
                execution,
                plan,
                self.turn_event(session_id, run_id, &turn_id, EventKind::TurnCompleted),
            )
            .await?;
            if !had_tools && claimed == 0 {
                ensure_lease(lease_lost)?;
                match self
                    .conclude_owned_attempt(
                        execution,
                        run_id,
                        crabber_session::RunBoundary::Terminal {
                            status: RunStatus::Completed,
                            error: None,
                            usage: usage.clone(),
                        },
                        None,
                        run_started_at,
                    )
                    .await
                {
                    Ok(settled) => {
                        self.observer.emit(&settled);
                        let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                        plan.dispatcher
                            .notify::<EventPublished>(projection.clone())
                            .await;
                        plan.dispatcher.notify::<RunSettled>(projection).await;
                        return Ok(usage);
                    }
                    Err(RuntimeError::Attempt(crabber_session::AttemptError::PendingInput)) => {
                        if self.claim_input(execution, InboxKind::Steer).await?
                            + self.claim_input(execution, InboxKind::FollowUp).await?
                            == 0
                            && !self.store.supports_run_controls()
                        {
                            return Err(RuntimeError::Store(StoreError::PendingInput));
                        }
                    }
                    Err(error) => return Err(error),
                }
            }
        }
        Err(RuntimeError::TurnLimit)
    }

    pub(super) async fn snapshot(
        &self,
        run_id: &RunId,
        session: &RunSession,
        request: &Request,
        plan: &RunPlan,
    ) -> Result<TurnSnapshot, RuntimeError> {
        let session_id = &session.id;
        let mut system = request.system_prompt.clone().unwrap_or_default();
        for prompt in &plan.prompts {
            if !system.is_empty() {
                system.push('\n');
            }
            system.push_str(&prompt.text);
        }
        let mut messages = self.store.list_messages(session_id, None).await?;
        let turn_index = messages
            .iter()
            .filter(|message| message.role == Role::Assistant)
            .count();
        // Workspace keys are runtime-owned: pinned so no handler can change
        // them for a later handler. An unavailable field is JSON null.
        let workspace = &session.workspace;
        let contributions = plan
            .dispatcher
            .transform_pinned::<ContextAssemble>(
                json!({
                    "system_prelude":[], "user_suffix":[], "prompt_sections":[],
                    "run_id":run_id.to_string(), "session_id":session_id.to_string(),
                    "provider_id":request.selection.provider_id, "model_id":request.selection.model_id,
                    "turn_index":turn_index, "message_count":messages.len(),
                    "has_system_prompt":request.system_prompt.is_some()
                }),
                &[
                    (
                        WorkspaceContext::WORKSPACE_ID_KEY,
                        json!(workspace.workspace_id()),
                    ),
                    (
                        WorkspaceContext::DIRECTORY_KEY,
                        json!(workspace.directory()),
                    ),
                ],
            )
            .await
            .map_err(|e| RuntimeError::Extension(e.to_string()))?;
        if let Some(sections) = contributions
            .get("prompt_sections")
            .and_then(Value::as_array)
        {
            for section in sections.iter().filter_map(Value::as_str) {
                if !system.is_empty() {
                    system.push('\n');
                }
                system.push_str(section);
            }
        }
        if let Some(prelude) = contributions
            .get("system_prelude")
            .and_then(Value::as_array)
        {
            for line in prelude.iter().filter_map(Value::as_str) {
                system = format!("{line}\n{system}");
            }
        }
        if let Some(suffixes) = contributions.get("user_suffix").and_then(Value::as_array)
            && let Some(message) = messages
                .iter_mut()
                .rev()
                .find(|message| message.role == Role::User)
            && let Some(text) =
                message
                    .parts
                    .iter_mut()
                    .rev()
                    .find_map(|part| match &mut part.content {
                        ContentBlock::Text { text } => Some(text),
                        _ => None,
                    })
        {
            for suffix in suffixes.iter().filter_map(Value::as_str) {
                text.push('\n');
                text.push_str(suffix);
            }
        }
        Ok(TurnSnapshot {
            context: request
                .input_envelope
                .as_ref()
                .map_or_else(Vec::new, |input| input.context().to_vec()),
            identity: RequestIdentity {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                turn_id: TurnId::new(),
            },
            selection: request.selection.clone(),
            messages,
            system: (!system.is_empty()).then_some(system),
            max_output_tokens: request.max_output_tokens,
            tools: frontend::model_tools(plan)?,
        })
    }
}
