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
use crate::telemetry_targets::targets;
use tracing::Instrument;

mod acquired;
use acquired::AcquiredResume;

pub(super) enum PreparedResumeKind {
    Expired,
    Continuation,
}
pub(super) enum ResumeRequest {
    Expired,
    Continuation(Box<continuation::ApprovalResume>),
}
enum ResumeEntry {
    Acquire(ResumeRequest),
    Prepared {
        resources: Box<preparation::expired_resume::ExpiredResume>,
        kind: PreparedResumeKind,
    },
}
enum ResumeAcquisition {
    Acquire(ResumeRequest),
    Prepared(Box<continuation::ResumeAuthority>),
}
impl ResumeEntry {
    fn is_expired(&self) -> bool {
        matches!(
            self,
            Self::Acquire(ResumeRequest::Expired)
                | Self::Prepared {
                    kind: PreparedResumeKind::Expired,
                    ..
                }
        )
    }
}

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
                    Box::pin(runtime.resume_loaded(
                        run,
                        validated,
                        cancellation,
                        ResumeRequest::Expired,
                    ))
                    .await
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
        request: ResumeRequest,
    ) -> Result<RunResult, RuntimeError> {
        Box::pin(self.resume_entry(run, validated, cancellation, ResumeEntry::Acquire(request)))
            .await
    }
    pub(super) async fn resume_prepared(
        &self,
        run: crabber_core::Run,
        validated: (RunSession, Option<Request>),
        cancellation: CancellationToken,
        resources: preparation::expired_resume::ExpiredResume,
        kind: PreparedResumeKind,
    ) -> Result<RunResult, RuntimeError> {
        Box::pin(self.resume_entry(
            run,
            validated,
            cancellation,
            ResumeEntry::Prepared {
                resources: Box::new(resources),
                kind,
            },
        ))
        .await
    }
    async fn resume_entry(
        &self,
        run: crabber_core::Run,
        (session, saved): (RunSession, Option<Request>),
        cancellation: CancellationToken,
        entry: ResumeEntry,
    ) -> Result<RunResult, RuntimeError> {
        let span = self.attempt_span(true, &session.id, &run.id);
        let session_id = session.id.clone();
        let trace_run = run.id.clone();
        let result = async {
            self.lifecycle(&session_id, &trace_run, None);
            self.resume_entry_inner(run, (session, saved), cancellation, entry)
                .await
        }
        .instrument(span.clone())
        .await;
        let outcome = super::telemetry::record_result(&span, &result);
        span.in_scope(|| self.lifecycle(&session_id, &trace_run, Some(outcome)));
        result
    }
    async fn resume_entry_inner(
        &self,
        run: crabber_core::Run,
        (session, saved): (RunSession, Option<Request>),
        cancellation: CancellationToken,
        entry: ResumeEntry,
    ) -> Result<RunResult, RuntimeError> {
        let run_id = &run.id.clone();
        if self.store.admission_execution_state(run_id).await?
            == Some(crabber_session::AdmissionExecutionState::Unstarted)
        {
            return Err(StoreError::AdmissionRecoveryRequired.into());
        }
        if entry.is_expired()
            && (run.status.is_terminal()
                || (run.status != RunStatus::Paused && run.lease_until > self.clock.now()))
        {
            return Err(StoreError::Conflict.into());
        }
        if entry.is_expired()
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
        let expired = entry.is_expired();
        let (plan, bound, acquisition) = match entry {
            ResumeEntry::Prepared { resources, .. } => (
                resources.plan,
                resources.bound,
                ResumeAcquisition::Prepared(Box::new(resources.authority)),
            ),
            ResumeEntry::Acquire(ResumeRequest::Expired) => {
                let (plan, bound) = self.restore_resume_resources(&run, &binding).await?;
                (
                    plan,
                    bound,
                    ResumeAcquisition::Acquire(ResumeRequest::Expired),
                )
            }
            ResumeEntry::Acquire(ResumeRequest::Continuation(mut control)) => {
                let (plan, bound) =
                    Box::pin(self.prepare_resume_resources(&run, &binding, &mut control)).await?;
                (
                    plan,
                    bound,
                    ResumeAcquisition::Acquire(ResumeRequest::Continuation(control)),
                )
            }
        };
        if !self.store.supports_owned_attempts()
            || !self.store.supports_owned_tool_phases()
            || !self.store.supports_pre_effect_approvals()
        {
            return Err(StoreError::HostUnsupported.into());
        }
        let resume_turns_used = self.resume_turns_used(&run).await?;
        if expired && run.checkpoint.is_some() {
            tool_batch::restore_paused_calls(
                &run,
                self.store.list_unfinished_tool_calls(run_id).await?,
            )?;
        }
        let acquisition = match acquisition {
            ResumeAcquisition::Prepared(authority) => Some(*authority),
            ResumeAcquisition::Acquire(request) => self.acquire_resume(&run, request).await?,
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
        Box::pin(self.execute_acquired_resume(
            AcquiredResume {
                run,
                session,
                saved,
                original,
                plan,
                bound,
                turns_used: resume_turns_used,
                authority: acquisition,
            },
            cancellation,
        ))
        .await
    }

    async fn resume_turns_used(&self, run: &crabber_core::Run) -> Result<usize, RuntimeError> {
        Ok(match run.checkpoint.as_ref() {
            Some(checkpoint) => checkpoint
                .get("turns_used")
                .and_then(Value::as_u64)
                .and_then(|value| usize::try_from(value).ok())
                .filter(|count| *count > 0 && *count <= self.max_turns)
                .ok_or(StoreError::HostUnsupported)?,
            None if run.status == RunStatus::Running => {
                match self.store.latest_execution_attempt(&run.id).await? {
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
        })
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
                    report.record_skipped(SkippedRun {
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
                )) => report.record_skipped(SkippedRun {
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
                    report.record_skipped(SkippedRun {
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

impl RecoverReport {
    fn record_skipped(&mut self, run: SkippedRun) {
        let reason = match run.reason {
            RecoverySkipReason::Store(_) => "store",
            RecoverySkipReason::Attempt(_) => "attempt",
        };
        tracing::info!(target: targets::RUNTIME, { outcome = "skipped", "skip.reason" = reason }, "recovery skipped run");
        self.skipped.push(run);
    }
}
