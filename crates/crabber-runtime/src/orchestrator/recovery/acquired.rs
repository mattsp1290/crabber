//! Execution accepts only an actual acquired native authority and restored resources.
use super::{
    Arc, AtomicBool, CancellationToken, EventKind, ExecutionStore, HeartbeatGuard, InboxKind,
    Measurement, OperationKind, Orchestrator, Ordering, Request, RunPlan, RunResult, RunSession,
    RunStateSink, RunStatus, RuntimeError, StateSink, TurnId, Usage, Value, continuation,
    result_reason, terminal, tool_batch,
};

pub(super) struct AcquiredResume {
    pub run: crabber_core::Run,
    pub session: RunSession,
    pub saved: Option<Request>,
    pub original: Request,
    pub plan: RunPlan,
    pub bound: crabber_providers::BoundProvider,
    pub turns_used: usize,
    pub authority: continuation::ResumeAuthority,
}
enum ResumeMode {
    NativeUnstarted,
    Approved(continuation::ApprovedCalls),
    Continued,
}
enum AfterTools {
    Approved { turn: Option<TurnId>, usage: Usage },
    Continued,
}
impl Orchestrator {
    #[allow(clippy::too_many_lines)]
    pub(in crate::orchestrator::recovery) async fn execute_acquired_resume(
        &self,
        acquired: AcquiredResume,
        cancellation: CancellationToken,
        measurement_lost: Arc<AtomicBool>,
    ) -> Result<RunResult, RuntimeError> {
        let AcquiredResume {
            run,
            session,
            saved,
            original,
            plan,
            bound,
            turns_used: resume_turns_used,
            authority: acquisition,
        } = acquired;
        let run_id = &run.id.clone();
        let continuation::ResumeAuthority {
            fence,
            native_unstarted,
            continuation: resumed_continuation,
            granted,
            approved,
            tool_phases,
            frontend_phases,
        } = acquisition;
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
        let mut heartbeat = HeartbeatGuard::start(
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
        if approved
            .as_ref()
            .is_some_and(|batch| batch.turns_used != resume_turns_used)
        {
            return Err(crabber_session::ApprovalError::UnknownStoreOutcome.into());
        }
        let mode = if native_unstarted && run.checkpoint.is_none() {
            ResumeMode::NativeUnstarted
        } else if let Some(batch) = approved {
            ResumeMode::Approved(batch)
        } else {
            ResumeMode::Continued
        };
        let public_source = self_.public_source_for_attempt(&run.session_id);
        let source_fence = fence.clone();
        let resumed_work = async move {
            let resumed = self_
                .committed_resume_event(
                    &source_fence,
                    &run.session_id,
                    matches!(mode, ResumeMode::NativeUnstarted),
                    resumed_turn.clone(),
                    resumed_continuation,
                )
                .await?;
            execution.append_event(resumed.clone()).await?;
            self.observer.emit(&resumed);
            if matches!(mode, ResumeMode::NativeUnstarted) {
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
            let pending_calls = if matches!(mode, ResumeMode::Continued) && run.checkpoint.is_some()
            {
                tool_batch::restore_paused_calls(&run, pending_calls)?
            } else {
                pending_calls
            };
            let (permissions, after_tools) = match mode {
                ResumeMode::Approved(batch) => (
                    batch.calls,
                    AfterTools::Approved {
                        turn: batch.turn,
                        usage: batch.usage,
                    },
                ),
                ResumeMode::Continued => (
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
                        .await?,
                    AfterTools::Continued,
                ),
                ResumeMode::NativeUnstarted => {
                    unreachable!("unstarted work returns before tool continuation")
                }
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
            if let AfterTools::Approved { turn, usage } = after_tools {
                if let Some(turn) = &turn {
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
                        usage,
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
                    tracing::warn!(target: crate::telemetry_targets::targets::RUNTIME, { "error.kind" = "lease_lost", source = "run" }, "run attempt lost its lease");
                    cancellation.cancel();
                    Err(RuntimeError::LeaseLost)
                }
                Ok(()) | Err(_) => resumed_work.as_mut().await,
            },
            control = heartbeat.durable_cancel() => {
                match control {
                    Ok(()) => { tracing::info!(target: crate::telemetry_targets::targets::RUNTIME, "durable cancel accepted"); cancellation.cancel(); resumed_work.as_mut().await }
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
}
