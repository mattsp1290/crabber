use super::{
    AdmissionExecutionError, Arc, AtomicBool, CancellationToken, CheckpointRequest, ContentBlock,
    EventKind, ExecutionStore, HashSet, HeartbeatGuard, INTERRUPTED_RESULT_TEXT, InboxKind,
    Measurement, Message, MessageId, OperationKind, Orchestrator, Ordering, Part, PartId, PartKind,
    PendingCall, RecoverReport, RecoverySkipReason, Request, Role, RunHandle, RunId, RunPlan,
    RunResult, RunSession, RunStateSink, RunStatus, RuntimeError, SessionId, SkippedRun, StateSink,
    StoreError, ToolCallId, ToolCallRecord, ToolCallStatus, ToolResult, ToolResultStatus,
    TraceContext, TurnId, Usage, Value, admission_config_hash, continuation, ensure_lease,
    event_payload, json, preparation, result_reason, terminal, tool_batch,
};

impl Orchestrator {
    /// Rebuilds the latest assistant batch when a resumed process died after
    /// committing its message but before staging every call.
    async fn reconcile_committed_assistant(
        &self,
        execution: &dyn ExecutionStore,
        run: &crabber_core::Run,
        plan: &RunPlan,
        lease_lost: &AtomicBool,
    ) -> Result<Option<bool>, RuntimeError> {
        if run.status != RunStatus::Paused {
            return Ok(None);
        }
        let Some(checkpoint) = &run.checkpoint else {
            return Ok(None);
        };
        let paused_calls: HashSet<&str> = checkpoint
            .get("pending_calls")
            .and_then(Value::as_array)
            .into_iter()
            .flatten()
            .filter_map(Value::as_str)
            .collect();
        if paused_calls.is_empty() {
            return Ok(None);
        }
        let history = self.store.list_all_messages(&run.session_id).await?;
        let Some(anchor) = history.iter().rposition(|message| message.parts.iter().any(|part|
            matches!(&part.content, ContentBlock::ToolCall { call_id, .. } if paused_calls.contains(call_id.0.as_str())))) else {
            return Ok(None);
        };
        let latest = history[anchor + 1..].iter().rev().find(|message| {
            message.run_id.as_ref() == Some(&run.id)
                && message.role == Role::Assistant
                && message
                    .parts
                    .iter()
                    .any(|part| part.kind != PartKind::CompactionSummary)
        });
        let Some(latest) = latest else {
            return Ok(None);
        };
        let settled: HashSet<ToolCallId> = history
            .iter()
            .flat_map(|message| &message.parts)
            .filter_map(|part| match &part.content {
                ContentBlock::ToolResult { call_id, .. } => Some(call_id.clone()),
                _ => None,
            })
            .collect();
        let unfinished: HashSet<ToolCallId> = self
            .store
            .list_unfinished_tool_calls(&run.id)
            .await?
            .into_iter()
            .map(|call| call.id)
            .collect();
        let mut has_calls = false;
        for part in &latest.parts {
            if let ContentBlock::ToolCall {
                call_id,
                name,
                arguments,
            } = &part.content
            {
                has_calls = true;
                if !settled.contains(call_id) && !unfinished.contains(call_id) {
                    ensure_lease(lease_lost)?;
                    self.stage_tool(
                        execution,
                        &run.session_id,
                        &run.id,
                        plan,
                        PendingCall {
                            message_id: Some(latest.id.clone()),
                            turn_id: None,
                            args_completed: true,
                            id: call_id.clone(),
                            name: name.clone(),
                            raw: String::new(),
                            arguments: Some(arguments.clone()),
                        },
                        lease_lost,
                    )
                    .await?;
                }
            }
        }
        Ok(Some(has_calls))
    }

    /// Reclaims a paused or expired run and finishes its persisted tool calls.
    ///
    /// # Errors
    ///
    /// Returns a conflict for a live lease, or `PlanChanged` if registered
    /// tools and handlers no longer match the admitted plan.
    pub async fn resume(&self, run_id: &RunId) -> Result<RunResult, RuntimeError> {
        self.resume_with_context(run_id, None).await
    }

    /// Reclaims one run with explicit current attempt correlation identity.
    /// The host persists predecessor identity separately from execution authority.
    /// # Errors
    /// Returns the same fencing and plan errors as `resume`.
    pub async fn resume_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunResult, RuntimeError> {
        self.resume_execution_with_context(run_id, context)
            .await?
            .done()
            .await
    }

    /// Observe/control a resumed worker using the same handle as initial admission.
    /// Eligibility, frozen-plan restoration and claim execute under retained task
    /// ownership. A handle is observation, never a grant of execution authority.
    /// # Errors
    /// Returns missing/invalid identity; `done` reports subsequent eligibility,
    /// binding, claim, execution and terminal-persistence failures.
    pub async fn resume_execution(&self, run_id: &RunId) -> Result<RunHandle, RuntimeError> {
        self.resume_execution_with_context(run_id, None).await
    }
    /// Resume with explicit correlation, excluded from persisted execution authority.
    /// # Errors
    /// Returns the same failures as `resume_execution`.
    pub async fn resume_execution_with_context(
        &self,
        run_id: &RunId,
        context: Option<TraceContext>,
    ) -> Result<RunHandle, RuntimeError> {
        if let Some(provider) = &self.contextual_provider {
            provider.check_ready_allowed()?;
        }
        let run_id = run_id.clone();
        self.own_root(None, move |runtime| async move {
            let runtime = runtime.with_context(context);
            runtime.initialize_owned().await?;
            let run = runtime
                .store
                .get_run(&run_id)
                .await?
                .ok_or(StoreError::NotFound)?;
            let validated = runtime.validated_session(&run).await?;
            let binding = runtime.stored_binding(&run.session_id, &run.id).await?;
            if crabber_session::session_owner::preparation::resource_contract(
                &binding.runtime_semantics,
            )?
            .is_some()
            {
                return Box::pin(runtime.spawn_prepared_expired(run, validated, binding)).await;
            }
            let session_id = validated.0.id.clone();
            runtime.spawn_worker(
                session_id,
                run_id,
                move |runtime, cancellation| async move {
                    Box::pin(runtime.resume_loaded(run, validated, cancellation, None)).await
                },
            )
        })
        .await
    }

    /// Loads the run's persisted session and compares it with the request saved
    /// in the run checkpoint, when there is one.
    pub(super) async fn validated_session(
        &self,
        run: &crabber_core::Run,
    ) -> Result<(RunSession, Option<Request>), StoreError> {
        let session = self
            .store
            .get_session(&run.session_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let saved = CheckpointRequest::from_checkpoint(run.checkpoint.as_ref())?;
        if let Some(saved) = &saved {
            session.ensure_identity(&saved.workspace_id, &saved.directory)?;
        }
        let saved = if let Some(saved) = saved {
            let binding = self
                .store
                .execution_binding(&session.id, &run.id)
                .await?
                .ok_or(StoreError::HostUnsupported)?
                .capsule;
            let input = binding
                .input_envelope
                .as_ref()
                .ok_or(StoreError::HostUnsupported)?;
            if saved.version != 2
                || saved.input_digest.as_deref()
                    != Some(admission_config_hash(&serde_json::json!(input)).as_str())
            {
                return Err(StoreError::HostUnsupported);
            }
            let mut request = saved.into_request(&session.id);
            request.input_envelope = Some(input.clone());
            Some(request)
        } else {
            None
        };
        Ok((RunSession::from(&session), saved))
    }

    pub(super) async fn resume_loaded(
        &self,
        run: crabber_core::Run,
        validated: (RunSession, Option<Request>),
        cancellation: CancellationToken,
        approval: Option<continuation::ApprovalResume>,
    ) -> Result<RunResult, RuntimeError> {
        Box::pin(self.resume_loaded_inner(run, validated, cancellation, approval, None)).await
    }
    #[allow(clippy::too_many_lines)]
    pub(super) async fn resume_loaded_inner(
        &self,
        run: crabber_core::Run,
        (session, saved): (RunSession, Option<Request>),
        cancellation: CancellationToken,
        mut approval: Option<continuation::ApprovalResume>,
        prepared: Option<preparation::expired_resume::ExpiredResume>,
    ) -> Result<RunResult, RuntimeError> {
        let run_id = &run.id.clone();
        if self.store.admission_execution_state(run_id).await?
            == Some(crabber_session::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired.into());
        }
        if approval.is_none()
            && (run.status.is_terminal()
                || (run.status != RunStatus::Paused && run.lease_until > self.clock.now()))
        {
            return Err(StoreError::Conflict.into());
        }
        if approval.is_none()
            && run.checkpoint.as_ref().is_some_and(|checkpoint| {
                checkpoint.get("approval").is_some()
                    || checkpoint.get("tool_continuation").is_some()
            })
        {
            return Err(RuntimeError::DurableContinuationRequired {
                run_id: run.id.clone(),
            });
        }
        let binding = self.stored_binding(&session.id, run_id).await?;
        if let Some(saved) = &saved {
            self.verify_binding_request(saved, &session.id, &binding)?;
        }
        let original = Request::from_binding(&binding);
        let (plan, bound, prepared_authority) = if let Some(prepared) = prepared {
            (prepared.plan, prepared.bound, Some(prepared.authority))
        } else {
            let (plan, bound) =
                Box::pin(self.prepare_resume_resources(&run, &binding, &mut approval)).await?;
            (plan, bound, None)
        };
        if !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(StoreError::HostUnsupported.into());
        }
        let resume_turns_used = match run.checkpoint.as_ref() {
            Some(checkpoint) => checkpoint
                .get("turns_used")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|count| *count > 0 && *count <= self.max_turns)
                .ok_or(StoreError::HostUnsupported)?,
            None if run.status == RunStatus::Running => {
                match self.store.latest_execution_attempt(run_id).await? {
                    None => 0,
                    Some(previous)
                        if previous.attempt.state == crabber_session::AttemptState::Unstarted =>
                    {
                        0
                    }
                    Some(_) => return Err(crabber_session::AttemptError::Busy.into()),
                }
            }
            None => return Err(StoreError::HostUnsupported.into()),
        };
        if approval.is_none() && run.checkpoint.is_some() {
            tool_batch::restore_paused_calls(
                &run,
                self.store.list_unfinished_tool_calls(run_id).await?,
            )?;
        }
        let acquisition = if let Some(authority) = prepared_authority {
            Some(authority)
        } else {
            self.acquire_resume(&run, approval).await?
        };
        let Some(acquisition) = acquisition else {
            plan.release();
            return Ok(RunResult {
                session_id: run.session_id,
                run_id: run.id,
                status: run.status,
                usage: run.usage,
            });
        };
        let continuation::ResumeAuthority {
            fence,
            native_unstarted,
            continuation: resumed_continuation,
            granted,
            approved,
            tool_phases,
            frontend_phases,
        } = acquisition;
        let measurement_lost = Arc::new(AtomicBool::new(false));
        let measurement_cancellation = CancellationToken::new();
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            &run.session_id,
            run_id,
            OperationKind::Run,
            &measurement_cancellation,
            &measurement_lost,
        );
        let execution = match self.store.execution(fence.clone()).await {
            Ok(execution) => execution,
            Err(error) => {
                if self.verified_lease_loss(&fence).await {
                    measurement_lost.store(true, Ordering::SeqCst);
                }
                return Err(error.into());
            }
        };
        let execution: Arc<dyn ExecutionStore> = execution.into();
        let mut worker = self.clone();
        worker.observation_lifetime = Arc::default();
        worker.tool_suspensions = Arc::default();
        worker.tool_resumes = Arc::default();
        worker.frontend_resumes = Arc::default();
        worker.owned_attempt = Some(Arc::new(match granted {
            Some(owner) => {
                self.begin_granted_attempt(execution.as_ref(), &run.session_id, owner)
                    .await?
            }
            None => {
                self.begin_owned_attempt(execution.as_ref(), &run.session_id, run_id, true)
                    .await?
            }
        }));
        worker.source_delivery = Some(worker.new_source_delivery(&run.session_id, run_id)?);
        worker.tool_resumes = Arc::new(std::sync::Mutex::new(
            tool_phases
                .into_iter()
                .map(|phase| (phase.target.call_id.clone(), phase))
                .collect(),
        ));
        worker.frontend_resumes = Arc::new(std::sync::Mutex::new(
            frontend_phases
                .into_iter()
                .map(|phase| (phase.target.call_id.clone(), phase))
                .collect(),
        ));
        let self_ = &worker;
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
        let mut signal = heartbeat.signal.clone();
        let lost = Arc::clone(&heartbeat.lost);
        let work_cancellation = cancellation.clone();
        let state_sink: Arc<dyn StateSink> = Arc::new(RunStateSink {
            store: Arc::clone(&self.store),
            execution: Arc::clone(&execution),
            session_id: run.session_id.clone(),
            lost: Arc::clone(&lost),
        });
        let failure_execution = execution.clone();
        let failure_plan = plan.clone();
        let failure_session = run.session_id.clone();
        let failure_started_at = run.created_at;
        let failure_lost = lost.clone();
        let current_turn = Arc::new(std::sync::Mutex::new(None));
        let work_turn = current_turn.clone();
        let approval_turn = approved.as_ref().and_then(|batch| batch.turn.clone());
        let resumed_turn = approval_turn.clone().or_else(|| {
            run.checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("source_turn_id"))
                .and_then(Value::as_str)
                .map(|id| TurnId(id.to_owned()))
        });
        let approval_usage = approved.as_ref().map(|batch| batch.usage.clone());
        if approved
            .as_ref()
            .is_some_and(|batch| batch.turns_used != resume_turns_used)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome.into());
        }
        let was_approval = approved.is_some();
        let public_source = self_.public_source_for_attempt(&run.session_id);
        let source_fence = fence.clone();
        let resumed_work = async move {
            let resumed = self_
                .committed_resume_event(
                    &source_fence,
                    &run.session_id,
                    native_unstarted && run.checkpoint.is_none(),
                    resumed_turn.clone(),
                    resumed_continuation,
                )
                .await?;
            execution.append_event(resumed.clone()).await?;
            self.observer.emit(&resumed);
            if native_unstarted && run.checkpoint.is_none() {
                let usage = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &original,
                        &plan,
                        &bound,
                        &work_cancellation,
                        true,
                        run.usage.clone(),
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
                    )
                    .await?;
                return Ok(RunResult {
                    session_id: run.session_id.clone(),
                    run_id: run_id.clone(),
                    status: RunStatus::Completed,
                    usage,
                });
            }
            let committed_turn = if run
                .checkpoint
                .as_ref()
                .and_then(|checkpoint| checkpoint.get("version"))
                .and_then(Value::as_u64)
                .is_some_and(|version| matches!(version, 4 | 5))
            {
                // Native v4 boundaries already committed the complete prepared
                // batch. Later model history cannot add or reorder its calls.
                None
            } else {
                self.reconcile_committed_assistant(execution.as_ref(), &run, &plan, lost.as_ref())
                    .await?
            };
            // A stopped attempt cannot manufacture an outcome for a call whose
            // body may already have caused an external effect.
            let pending_calls = self.store.list_unfinished_tool_calls(run_id).await?;
            let pending_calls = if approved.is_none() && run.checkpoint.is_some() {
                tool_batch::restore_paused_calls(&run, pending_calls)?
            } else {
                pending_calls
            };
            let permissions = if let Some(batch) = approved {
                batch.calls
            } else {
                self_
                    .authorize_tools(
                        execution.as_ref(),
                        &session,
                        run_id,
                        &plan,
                        &pending_calls,
                        &run.usage,
                        resume_turns_used,
                        resumed_turn.clone(),
                        lost.as_ref(),
                    )
                    .await?
            };
            if permissions.len() != pending_calls.len() {
                return Err(crabber_session::ApprovalError::SemanticConflict.into());
            }
            self_
                .execute_prepared_tools(
                    tool_batch::Batch {
                        execution: execution.as_ref(),
                        session: &session,
                        run: run_id,
                        plan: &plan,
                        turn: resumed_turn.clone(),
                        usage: &run.usage,
                        turns_used: resume_turns_used,
                        cancellation: &work_cancellation,
                        lease_lost: lost.as_ref(),
                    },
                    pending_calls,
                    permissions,
                )
                .await?;
            if was_approval {
                if let Some(turn) = &approval_turn {
                    self_
                        .publish_event(
                            execution.as_ref(),
                            &plan,
                            self_.turn_event(
                                &run.session_id,
                                run_id,
                                turn,
                                EventKind::TurnCompleted,
                            ),
                        )
                        .await?;
                }
                let usage = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &original,
                        &plan,
                        &bound,
                        &work_cancellation,
                        false,
                        approval_usage.unwrap_or_else(|| run.usage.clone()),
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
                    )
                    .await?;
                plan.release();
                return Ok(RunResult {
                    session_id: run.session_id.clone(),
                    run_id: run_id.clone(),
                    status: RunStatus::Completed,
                    usage,
                });
            }
            if let Some(checkpoint) = &run.checkpoint
                && let Some(request) = saved
            {
                let usage: Usage = checkpoint
                    .get("usage")
                    .cloned()
                    .and_then(|value| serde_json::from_value(value).ok())
                    .unwrap_or_default();
                if committed_turn == Some(false) {
                    match self_
                        .conclude_owned_attempt(
                            execution.as_ref(),
                            run_id,
                            crabber_session::RunBoundary::Terminal {
                                status: RunStatus::Completed,
                                error: None,
                                usage: usage.clone(),
                            },
                            None,
                            run.created_at,
                        )
                        .await
                    {
                        Ok(event) => {
                            self.observer.emit(&event);
                            plan.release();
                            return Ok(RunResult {
                                session_id: run.session_id,
                                run_id: run_id.clone(),
                                status: RunStatus::Completed,
                                usage,
                            });
                        }
                        Err(RuntimeError::Attempt(crabber_session::AttemptError::PendingInput)) => {
                            self.claim_input(execution.as_ref(), InboxKind::Steer)
                                .await?;
                            self.claim_input(execution.as_ref(), InboxKind::FollowUp)
                                .await?;
                        }
                        Err(error) => return Err(error),
                    }
                }
                if let Some(turn) = checkpoint.get("source_turn_id").and_then(Value::as_str) {
                    self_
                        .publish_event(
                            execution.as_ref(),
                            &plan,
                            self_.turn_event(
                                &run.session_id,
                                run_id,
                                &TurnId(turn.to_owned()),
                                EventKind::TurnCompleted,
                            ),
                        )
                        .await?;
                } else {
                    self.emit_durable(
                        execution.as_ref(),
                        &run.session_id,
                        run_id,
                        &plan,
                        EventKind::TurnCompleted,
                    )
                    .await?;
                }
                let outcome = self_
                    .run_loop(
                        execution.as_ref(),
                        run_id,
                        &session,
                        &request,
                        &plan,
                        &bound,
                        &work_cancellation,
                        false,
                        usage,
                        resume_turns_used,
                        lost.as_ref(),
                        &work_turn,
                    )
                    .await;
                let result = match outcome {
                    Ok(usage) => Ok(RunResult {
                        session_id: run.session_id.clone(),
                        run_id: run_id.clone(),
                        status: RunStatus::Completed,
                        usage,
                    }),
                    Err(RuntimeError::Paused) => Ok(RunResult {
                        session_id: run.session_id.clone(),
                        run_id: run_id.clone(),
                        status: RunStatus::Paused,
                        usage: Usage::default(),
                    }),
                    Err(error) => Err(error),
                };
                plan.release();
                return result;
            }
            let status = RunStatus::Completed;
            let event = self_
                .conclude_owned_attempt(
                    execution.as_ref(),
                    run_id,
                    crabber_session::RunBoundary::Terminal {
                        status,
                        error: None,
                        usage: run.usage.clone(),
                    },
                    None,
                    run.created_at,
                )
                .await?;
            self.observer.emit(&event);
            plan.release();
            Ok(RunResult {
                session_id: run.session_id,
                run_id: run_id.clone(),
                status,
                usage: run.usage,
            })
        };
        let resumed_work = crabber_extension::with_public_source(
            public_source,
            crabber_extension::with_state_sink(state_sink, resumed_work),
        );
        tokio::pin!(resumed_work);
        let result = tokio::select! {
            biased;
            update = signal.changed() => match update {
                Ok(()) if *signal.borrow_and_update() => {
                    cancellation.cancel();
                    Err(RuntimeError::LeaseLost)
                }
                Ok(()) | Err(_) => resumed_work.as_mut().await,
            },
            control = self_.wait_durable_cancel(failure_execution.as_ref(), &fence) => {
                match control {
                    Ok(()) => { cancellation.cancel(); resumed_work.as_mut().await }
                    Err(error) => {
                        if matches!(error, RuntimeError::LeaseLost) { failure_lost.store(true, Ordering::SeqCst); }
                        Err(error)
                    }
                }
            },
            result = &mut resumed_work => result,
        };
        drop(heartbeat);
        let result = match result {
            Ok(result) => Ok(result),
            Err(error) => {
                let turn = current_turn.lock().expect("turn context poisoned").clone();
                self_
                    .finish_failed_run(
                        terminal::FailureContext {
                            execution: failure_execution.as_ref(),
                            fence: &fence,
                            session: &failure_session,
                            plan: &failure_plan,
                            started_at: failure_started_at,
                            lost: failure_lost.as_ref(),
                            observed_lost: measurement_lost.as_ref(),
                        },
                        error,
                        turn.as_ref(),
                    )
                    .await
            }
        };
        failure_plan.release();
        if result.is_err() && self.verified_lease_loss(&fence).await {
            measurement_lost.store(true, Ordering::SeqCst);
        }
        measurement.observation.reason = result_reason(&result);
        result
    }

    /// Reclaims expired unfinished runs. Live leases are left to their owners.
    /// The report also lists each expired run left unfinished, and why.
    ///
    /// # Errors
    ///
    /// Returns a store or execution error for a run it successfully claims.
    pub async fn recover(&self) -> Result<RecoverReport, RuntimeError> {
        self.recover_with_context(|_| None).await
    }

    /// Selects fresh correlation independently for each eligible run.
    /// The selector cannot grant a lease; live leases and competing claims stay fenced.
    /// Only an expired Unstarted reservation, a pristine bound admission or a
    /// fully quiescent suspended attempt can enter the driver. Started, unknown
    /// and incomplete attempts stay unfinished and are reported for reconciliation.
    /// # Errors
    /// Returns the same store and execution errors as `recover`.
    pub async fn recover_with_context<F>(
        &self,
        mut context_for: F,
    ) -> Result<RecoverReport, RuntimeError>
    where
        F: FnMut(&crabber_core::Run) -> Option<TraceContext> + Send,
    {
        let mut report = RecoverReport::default();
        if let Some(provider) = &self.contextual_provider {
            provider.check_ready_allowed()?;
        }
        let runs = self
            .own_root(None, |runtime| async move {
                runtime.initialize_owned().await?;
                runtime
                    .store
                    .list_unfinished_runs()
                    .await
                    .map_err(RuntimeError::from)
            })
            .await?;
        for run in runs {
            if self.is_closed() {
                return Err(RuntimeError::HostClosed);
            }
            if run.lease_until > self.clock.now() {
                continue;
            }
            let (session_id, run_id) = (run.session_id.clone(), run.id.clone());
            let context = context_for(&run);
            // A run whose session is missing, undecodable or drifted, or that is
            // claimed elsewhere or awaits admission recovery, stays unfinished
            // and is reported; it must not stop the sweep.
            let outcome = self.resume_with_context(&run_id, context).await;
            match outcome {
                Ok(result) => report.recovered.push(result),
                Err(RuntimeError::Store(
                    reason @ (StoreError::NotFound
                    | StoreError::Validation(_)
                    | StoreError::SessionIdentityMismatch
                    | StoreError::Conflict
                    | StoreError::AdmissionRecoveryRequired),
                )) => {
                    report.skipped.push(SkippedRun {
                        session_id,
                        run_id,
                        reason: RecoverySkipReason::Store(reason),
                    });
                }
                Err(RuntimeError::Attempt(
                    reason @ (crabber_session::AttemptError::Busy
                    | crabber_session::AttemptError::LiveOwner
                    | crabber_session::AttemptError::CleanupBlocked
                    | crabber_session::AttemptError::InvalidTransition
                    | crabber_session::AttemptError::Unsupported
                    | crabber_session::AttemptError::StaleOwner),
                )) => report.skipped.push(SkippedRun {
                    session_id,
                    run_id,
                    reason: RecoverySkipReason::Attempt(reason),
                }),
                Err(
                    RuntimeError::ProviderBinding(
                        crabber_providers::ProviderBindingError::Unavailable
                        | crabber_providers::ProviderBindingError::VersionConflict,
                    )
                    | RuntimeError::AdmissionExecution(
                        AdmissionExecutionError::MissingEvidence
                        | AdmissionExecutionError::Unsupported,
                    ),
                ) => {
                    report.skipped.push(SkippedRun {
                        session_id,
                        run_id,
                        reason: RecoverySkipReason::Store(StoreError::HostUnsupported),
                    });
                }
                Err(error) => return Err(error),
            }
        }
        Ok(report)
    }

    /// Fixed interruption settlement deliberately runs no pre-stage or result chain;
    /// unfinished calls carry no tool-authored payload into persistence.
    pub(super) async fn settle_unfinished_calls(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        include_pending: bool,
        turn_id: Option<&TurnId>,
    ) -> Result<(), RuntimeError> {
        // execute_tool may already have settled a redacted Interrupted result.
        // Listing only unfinished calls keeps both live and resumed cleanup single-write.
        for call in self.store.list_unfinished_tool_calls(run_id).await? {
            if call.status == ToolCallStatus::Pending && !include_pending {
                continue;
            }
            if call.status == ToolCallStatus::Pending {
                let mut running = self.event(session_id, run_id, EventKind::ToolCallRunning);
                running.turn_id = turn_id.cloned();
                execution.claim_tool_call(&call.id, running).await?;
            }
            self.settle_interrupted_call(execution, session_id, run_id, plan, &call, turn_id)
                .await?;
        }
        Ok(())
    }

    /// Writes only the bare runtime-authored interrupted text, with no pre-stage,
    /// transform or final redactor. Fixed settlements notify `EventPublished` only.
    async fn settle_interrupted_call(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: &ToolCallRecord,
        turn_id: Option<&TurnId>,
    ) -> Result<(), RuntimeError> {
        let content = vec![ContentBlock::Text {
            text: INTERRUPTED_RESULT_TEXT.into(),
        }];
        let message_id = MessageId::new();
        let message = Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Tool,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::FunctionToolResult,
                content: ContentBlock::ToolResult {
                    call_id: call.id.clone(),
                    content: content.clone(),
                    is_error: true,
                },
            }],
            created_at: self.clock.now(),
        };
        let mut event = self.event(session_id, run_id, EventKind::ToolCallSettled);
        event.turn_id = turn_id.cloned();
        event.payload = json!(crate::event_payload::Settlement {
            result: event_payload::CallSettled {
                call_id: call.id.clone(),
                message_id: message.id.clone(),
                content: std::borrow::Cow::Borrowed(&content),
                is_error: true,
            },
            diagnostics: json!({"name": call.name, "status": "interrupted"}),
        });
        event.correlation = Some(call.id.to_string());
        execution
            .settle_tool_call(
                &call.id,
                ToolResult {
                    status: ToolResultStatus::Interrupted,
                    content,
                },
                message,
                event.clone(),
            )
            .await?;
        self.observer.emit(&event);
        self.notify_interrupted_settlement(plan, &event, false)?;
        Ok(())
    }
}
