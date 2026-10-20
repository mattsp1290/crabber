use super::{
    Arc, AtomicBool, AttemptWorkspaceReaderResolver, CancellationToken, ContentBlock, ContextEpoch,
    Duration, EpochId, EventKind, ExecutionStore, InboxKind, Measurement, Message, MessageId,
    ModelPurpose, ModelRequest, OperationKind, Orchestrator, Part, PartId, PartKind, PendingCall,
    PromptAttemptContext, PromptContributionOutcome, RequestIdentity, Role, RunId, RunPlan,
    RuntimeError, SessionId, StoreError, StreamDelta, StreamExt, TerminalReason, TurnId,
    TurnSnapshot, Usage, WorkspaceContext, WorkspaceReaderResolver,
    collect_prompt_contributions_with_resolver, ensure_lease, invalid_provider, json,
    prompt_contribution_failed_message, request_error, user_message,
};
use crate::telemetry_targets::targets;
use tracing::Instrument;

impl Orchestrator {
    pub(super) async fn claim_input(
        &self,
        execution: &dyn ExecutionStore,
        kind: InboxKind,
    ) -> Result<usize, RuntimeError> {
        Ok(execution.claim_inbox_into_history(kind).await?.len())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn model_turn_with_retry(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        workspace: &WorkspaceContext,
        bound: &crabber_providers::BoundProvider,
        mut compacted: bool,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let streamer = bound.streamer();
        let mut snapshot = snapshot.clone();
        let mut retries = 0u32;
        let mut attempt = 0u32;
        loop {
            attempt += 1;
            let system = Self::attempt_system(
                plan,
                &snapshot,
                workspace,
                attempt,
                compacted,
                cancellation,
                plan.workspace_reader_resolver()
                    .cloned()
                    .or_else(|| self.workspace_reader_resolver.clone()),
            )
            .await?;
            if !plan.prompt_contributors.is_empty() {
                ensure_lease(lease_lost)?;
            }
            let span = tracing::info_span!(target: targets::RUNTIME, "crabber.model_call", "session.id" = %session_id, "run.id" = %run_id, "provider.id" = %snapshot.selection.provider_id, "model.id" = %snapshot.selection.model_id, purpose = "turn", "retry.attempt" = attempt - 1, "tokens.input" = tracing::field::Empty, "tokens.output" = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
            let result = self
                .model_turn(
                    execution,
                    run_id,
                    session_id,
                    &snapshot,
                    system,
                    plan,
                    Arc::clone(&streamer),
                    cancellation,
                    lease_lost,
                )
                .instrument(span.clone())
                .await;
            match &result {
                Ok((_, usage)) => {
                    span.record("tokens.input", usage.input_tokens);
                    span.record("tokens.output", usage.output_tokens);
                    span.record("outcome", "completed");
                    span.record("reason", "success");
                    span.record("otel.status_code", "OK");
                }
                Err(error) => {
                    super::telemetry::record_model_error(&span, error);
                }
            }
            drop(span);
            match result {
                Ok(result) => return Ok(result),
                Err(RuntimeError::Provider(error)) => {
                    let overflow =
                        error.kind == crabber_providers::ProviderErrorKind::ContextOverflow;
                    let retry = !overflow && error.retryable && retries < 2;
                    let delay = if retry {
                        100u64.saturating_mul(1u64 << retries)
                    } else {
                        0
                    };
                    let decision = request_error(plan, &error, retries + 1, retry, delay).await?;
                    if overflow && !compacted && decision.compaction_requested {
                        self.compact(
                            execution,
                            run_id,
                            session_id,
                            &snapshot,
                            plan,
                            bound,
                            true,
                            cancellation,
                            lease_lost,
                        )
                        .await?;
                        snapshot.messages = self.store.list_messages(session_id, None).await?;
                        compacted = true;
                        continue;
                    }
                    if retry && decision.retry {
                        tracing::warn!(target: targets::RUNTIME, { "error.kind" = super::telemetry::provider_error_kind(&error), "retry.attempt" = retries + 1, "retry.delay_ms" = decision.delay_ms, compaction_requested = decision.compaction_requested }, "model request failed, retry scheduled");
                        retries += 1;
                        tokio::select! {
                            biased;
                            () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
                            () = tokio::time::sleep(Duration::from_millis(decision.delay_ms)) => (),
                        }
                        continue;
                    }
                    return Err(RuntimeError::Provider(error));
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn attempt_system(
        plan: &RunPlan,
        snapshot: &TurnSnapshot,
        workspace: &WorkspaceContext,
        attempt: u32,
        compacted: bool,
        cancellation: &CancellationToken,
        workspace_reader_resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
    ) -> Result<Option<String>, RuntimeError> {
        if plan.prompt_contributors.is_empty() {
            return Ok(snapshot.system.clone());
        }
        let context = PromptAttemptContext::new(
            snapshot.identity.session_id.clone(),
            snapshot.identity.run_id.clone(),
            snapshot.identity.turn_id.clone(),
            workspace.clone(),
            snapshot.selection.provider_id.clone(),
            snapshot.selection.model_id.clone(),
            attempt,
            compacted,
        )
        .with_cancellation(cancellation.clone());
        let workspace_reader_resolver = workspace_reader_resolver.map(|host| {
            Arc::new(AttemptWorkspaceReaderResolver {
                admitted: workspace.clone(),
                host,
            }) as Arc<dyn WorkspaceReaderResolver>
        });
        match collect_prompt_contributions_with_resolver(
            &plan.prompt_contributors,
            context,
            workspace_reader_resolver,
        )
        .await
        {
            PromptContributionOutcome::Completed { sections } => {
                let mut system = snapshot.system.clone().unwrap_or_default();
                for section in sections {
                    if !system.is_empty() {
                        system.push('\n');
                    }
                    system.push_str(&section.text);
                }
                Ok((!system.is_empty()).then_some(system))
            }
            PromptContributionOutcome::Failed { contributor } => Err(RuntimeError::Extension(
                prompt_contribution_failed_message(&contributor),
            )),
            PromptContributionOutcome::Interrupted => Err(RuntimeError::Interrupted),
        }
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    pub(super) async fn compact(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        bound: &crabber_providers::BoundProvider,
        overflow: bool,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        let run = self
            .store
            .get_run(run_id)
            .await?
            .ok_or(StoreError::NotFound)?;
        let messages = &snapshot.messages;
        let streamer = bound.streamer();
        let limits = bound.descriptor().limits();
        let context_limit = limits.context_tokens.unwrap_or(0);
        let summary_output_limit = snapshot
            .max_output_tokens
            .unwrap_or(1024)
            .min(limits.output_tokens.unwrap_or(1024))
            .min(1024);
        let budget_chars = usize::try_from(context_limit)
            .ok()
            .filter(|limit| *limit > 0)
            .unwrap_or(2048)
            .min(4096);
        let mut tail_start = messages
            .len()
            .saturating_sub(self.compaction.keep_tail_messages);
        if overflow && !messages.is_empty() {
            tail_start = tail_start.max(1);
            while tail_start < messages.len()
                && serde_json::to_vec(&messages[tail_start..])
                    .map_or(usize::MAX, |bytes| bytes.len())
                    > budget_chars
            {
                tail_start += 1;
            }
        }
        let summary_range = if tail_start == 0 {
            None
        } else {
            messages
                .first()
                .zip(messages.get(tail_start - 1))
                .map(|(first, last)| (first.id.clone(), last.id.clone()))
        };
        let epoch = ContextEpoch {
            id: EpochId::new(),
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            parent: Some(run.epoch_id),
            summarized_range: summary_range,
            summary_message_id: None,
            tail_start_message_id: messages.get(tail_start).map(|message| message.id.clone()),
            provider_id: snapshot.selection.provider_id.clone(),
            model_id: snapshot.selection.model_id.clone(),
            reason: if overflow {
                "context_overflow"
            } else {
                "proactive"
            }
            .into(),
            next_policy: None,
        };
        ensure_lease(lease_lost)?;
        execution.start_epoch(epoch.clone()).await?;
        self.emit_durable(
            execution,
            session_id,
            run_id,
            plan,
            EventKind::ContextEpochStarted,
        )
        .await?;
        let summary_prompt = "Summarize this context for continuation. Preserve every standing instruction and unresolved task from the previous summary and new context.";
        let raw: Vec<char> = serde_json::to_string(&messages[..tail_start])
            .map_err(|error| invalid_provider(&format!("cannot serialize context: {error}")))?
            .chars()
            .collect();
        let mut offset = 0;
        let mut text = String::new();
        while offset < raw.len() {
            // Every character in the replaced range reaches a summary request.
            // Reserve room for the prior summary instead of silently taking a suffix.
            let capacity = budget_chars.saturating_sub(text.chars().count());
            if capacity == 0 {
                return Err(invalid_provider("compaction summary exceeded input budget").into());
            }
            let end = (offset + capacity).min(raw.len());
            let chunk: String = raw[offset..end].iter().collect();
            let input = format!("Previous summary:\n{text}\nNew context:\n{chunk}");
            let request = ModelRequest {
                context: snapshot.context.clone(),
                identity: RequestIdentity {
                    session_id: session_id.clone(),
                    run_id: run_id.clone(),
                    turn_id: TurnId::new(),
                },
                selection: snapshot.selection.clone(),
                system: Some(summary_prompt.into()),
                messages: vec![user_message(session_id.clone(), input, self.clock.now())],
                tools: Vec::new(),
                temperature: None,
                max_tokens: Some(summary_output_limit),
                tool_choice: None,
            };
            let mut measurement = Measurement::new(
                self.observer.as_ref(),
                self.monotonic_clock.as_ref(),
                session_id,
                run_id,
                OperationKind::Model {
                    purpose: ModelPurpose::Compaction,
                    provider: snapshot.selection.provider_id.clone(),
                    model: snapshot.selection.model_id.clone(),
                },
                cancellation,
                lease_lost,
            );
            let summary_started = self.monotonic_clock.now();
            let observe_summary = |status: &str| {
                let mut observed = self.event(
                    session_id,
                    run_id,
                    EventKind::Custom {
                        name: "model_call".into(),
                    },
                );
                observed.payload = json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,
                    "status":status,"input_tokens":0,"output_tokens":0,
                    "latency_ms":self.monotonic_clock.now().saturating_sub(summary_started).as_millis(),"purpose":"compaction"});
                self.observer.model_completed(&observed);
            };
            // Compaction is an internal model request with a dedicated prompt. System-prompt
            // middleware is intentionally scoped to agent-turn attempts and is not collected
            // here; the next agent-turn attempt refreshes middleware after compaction.
            let span = tracing::info_span!(target: targets::RUNTIME, "crabber.model_call", "session.id" = %session_id, "run.id" = %run_id, "provider.id" = %snapshot.selection.provider_id, "model.id" = %snapshot.selection.model_id, purpose = "compaction", "retry.attempt" = 0, "tokens.input" = tracing::field::Empty, "tokens.output" = tracing::field::Empty, outcome = tracing::field::Empty, reason = tracing::field::Empty, "error.kind" = tracing::field::Empty, "otel.status_code" = tracing::field::Empty);
            let result = async {
            let mut stream = tokio::select! {
                () = cancellation.cancelled() => { observe_summary("error"); return Err(RuntimeError::Interrupted); },
                result = streamer.stream(request) => match result {
                    Ok(stream) => stream,
                    Err(error) => { observe_summary("error"); return Err(error.into()); }
                },
            };
            let mut next_text = String::new();
            let mut completed = false;
            loop {
                let delta = tokio::select! {
                    () = cancellation.cancelled() => { observe_summary("error"); return Err(RuntimeError::Interrupted); },
                    result = stream.next() => result,
                };
                let Some(delta) = delta else {
                    break;
                };
                match delta {
                    StreamDelta::TextDelta(fragment) => {
                        measurement.first_text(&fragment);
                        next_text.push_str(&fragment);
                    }
                    StreamDelta::Usage(usage) => {
                        span.record("tokens.input", usage.input_tokens);
                        span.record("tokens.output", usage.output_tokens);
                    }
                    StreamDelta::Completed => {
                        completed = true;
                        break;
                    }
                    StreamDelta::Error(error) => {
                        observe_summary("error");
                        return Err(error.into());
                    }
                    _ => {}
                }
            }
            if cancellation.is_cancelled() {
                observe_summary("error");
                return Err(RuntimeError::Interrupted);
            }
            observe_summary(if completed { "ok" } else { "error" });
            if !completed || next_text.is_empty() {
                return Err(invalid_provider("compaction summary was empty").into());
            }
            measurement.observation.reason = TerminalReason::Success;
            drop(measurement);
            Ok::<_, RuntimeError>(next_text)
            }.instrument(span.clone()).await;
            match &result {
                Ok(_) => {
                    span.record("outcome", "completed");
                    span.record("reason", "success");
                    span.record("otel.status_code", "OK");
                }
                Err(error) => super::telemetry::record_model_error(&span, error),
            }
            let next_text = result?;
            drop(span);
            text = next_text;
            offset = end;
        }
        let message_id = MessageId::new();
        let summary = Message {
            id: message_id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            created_at: self.clock.now(),
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::CompactionSummary,
                content: ContentBlock::Text { text },
            }],
        };
        if cancellation.is_cancelled() {
            return Err(RuntimeError::Interrupted);
        }
        ensure_lease(lease_lost)?;
        execution.finish_epoch(&epoch.id, summary).await?;
        self.emit_durable(
            execution,
            session_id,
            run_id,
            plan,
            EventKind::ContextEpochFinished,
        )
        .await?;
        Ok(())
    }
}
