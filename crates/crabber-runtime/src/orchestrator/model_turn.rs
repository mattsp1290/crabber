use super::{
    Arc, AtomicBool, AtomicUsize, Callback, CancellationToken, ContentBlock, EventKind,
    EventPublished, ExecutionStore, Measurement, Message, MessageId, ModelCompleted, ModelPurpose,
    ModelRequest, ModelRequested, OperationKind, Orchestrator, Ordering, PREPARE_ERROR_KEY,
    PartKind, PendingCall, RESERVED_ARGUMENT_KEY_TEXT, Role, RunId, RunPlan, RuntimeError,
    SessionId, SingleUseStreamer, StreamDelta, StreamExt, StreamOutcome, Streamer, TerminalReason,
    ToolCallRecord, ToolCallStatus, ToolPrepare, TurnSnapshot, UNSTORABLE_ARGUMENTS_TEXT, Usage,
    Value, ensure_lease, event_payload, frontend, invalid_provider, is_reserved_arguments, json,
    push_part, reasoning, storable_call_arguments, unknown_tool_arguments, validate_arguments,
};

impl Orchestrator {
    #[allow(clippy::too_many_arguments)]
    pub(super) async fn model_turn(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        system: Option<String>,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        self.with_model_callback_owner(run_id, || {
            self.model_turn_inner(
                execution,
                run_id,
                session_id,
                snapshot,
                system,
                plan,
                streamer,
                cancellation,
                lease_lost,
            )
        })
        .await
    }

    #[allow(clippy::too_many_lines)] // Streaming assembly and durable assistant commit form one turn boundary.
    #[allow(clippy::too_many_arguments)]
    async fn model_turn_inner(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        system: Option<String>,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        cancellation: &CancellationToken,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let mut measurement = Measurement::new(
            self.observer.as_ref(),
            self.monotonic_clock.as_ref(),
            session_id,
            run_id,
            OperationKind::Model {
                purpose: ModelPurpose::Turn,
                provider: snapshot.selection.provider_id.clone(),
                model: snapshot.selection.model_id.clone(),
            },
            cancellation,
            lease_lost,
        );
        let model_started = self.monotonic_clock.now();
        let request = ModelRequest {
            context: snapshot.context.clone(),
            identity: snapshot.identity.clone(),
            selection: snapshot.selection.clone(),
            system,
            messages: snapshot.messages.clone(),
            tools: snapshot.tools.clone(),
            temperature: None,
            max_tokens: snapshot.max_output_tokens,
            reasoning_effort: snapshot.model_controls.reasoning_effort,
            tool_choice: None,
        };
        let calls_to_next = Arc::new(AtomicUsize::new(0));
        let next: Arc<dyn Streamer> = Arc::new(SingleUseStreamer {
            inner: streamer,
            calls: Arc::clone(&calls_to_next),
        });
        plan.dispatcher.notify::<ModelRequested>(json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"message_count":snapshot.messages.len()})).await;
        let stream_slot = Arc::new(std::sync::Mutex::new(None));
        let error_slot = Arc::new(std::sync::Mutex::new(None));
        let terminal: Callback = {
            let model_stream = Arc::clone(&self.model_stream);
            let stream_slot = Arc::clone(&stream_slot);
            let error_slot = Arc::clone(&error_slot);
            Arc::new(move |input| {
                let model_stream = Arc::clone(&model_stream);
                let next = Arc::clone(&next);
                let request = request.clone();
                let stream_slot = Arc::clone(&stream_slot);
                let error_slot = Arc::clone(&error_slot);
                Box::pin(async move {
                    let mut request = request;
                    if let Some(value) = input.get("temperature") {
                        request.temperature = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_f64()
                                    .filter(|v| v.is_finite() && (0.0..=2.0).contains(v))
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model temperature".into(),
                                        )
                                    })?,
                            )
                        };
                    }
                    if let Some(value) = input.get("max_tokens") {
                        request.max_tokens = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_u64()
                                    .and_then(|v| u32::try_from(v).ok())
                                    .filter(|v| *v > 0)
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model max_tokens".into(),
                                        )
                                    })?,
                            )
                        };
                    }
                    if let Some(value) = input.get("tool_choice") {
                        request.tool_choice = if value.is_null() {
                            None
                        } else {
                            Some(
                                value
                                    .as_str()
                                    .ok_or_else(|| {
                                        crabber_extension::ExtensionError::Plan(
                                            "invalid model tool_choice".into(),
                                        )
                                    })?
                                    .to_owned(),
                            )
                        };
                    }
                    match model_stream.stream(request, next).await {
                        Ok(stream) => {
                            *stream_slot.lock().unwrap() = Some(stream);
                            Ok(Value::Null)
                        }
                        Err(error) => {
                            *error_slot.lock().unwrap() = Some(error);
                            Err(crabber_extension::ExtensionError::Plan(
                                "model stream failed".into(),
                            ))
                        }
                    }
                })
            })
        };
        let dispatched = self.dispatch_model_callbacks(
            plan,
            cancellation,
            json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"session_id":snapshot.identity.session_id,"run_id":snapshot.identity.run_id,"turn_id":snapshot.identity.turn_id,"message_count":snapshot.messages.len(),"temperature":null,"max_tokens":snapshot.max_output_tokens,"reasoning_effort":snapshot.model_controls.reasoning_effort,"tool_choice":null}),
            terminal,
        ).await?;
        let provider_error = error_slot.lock().unwrap().take();
        if let Some(error) = provider_error {
            return Err(error.into());
        }
        dispatched.map_err(|e| {
            measurement.observation.reason = TerminalReason::RuntimeError;
            RuntimeError::Extension(e.to_string())
        })?;
        let mut stream = stream_slot.lock().unwrap().take().ok_or_else(|| {
            measurement.observation.reason = TerminalReason::RuntimeError;
            RuntimeError::Extension("model stream did not produce a stream".into())
        })?;
        if calls_to_next.load(Ordering::SeqCst) != 1 {
            return Err(invalid_provider(
                "model stream wrapper did not call provider exactly once",
            )
            .into());
        }
        let id = MessageId::new();
        self.live_payload(
            snapshot,
            EventKind::MessageStarted,
            event_payload::MessageIdentity {
                message_id: id.clone(),
            },
        );
        let mut presentation = crate::event_payload::Presentation::new(self, snapshot, id.clone());
        let mut text = String::new();
        let mut reasoning = reasoning::ReasoningBuffer::default();
        let mut calls: Vec<PendingCall> = Vec::new();
        let mut usage = Usage::default();
        let mut finish_reason = None;
        let observe_model = |status: &str, usage: &Usage| {
            let mut event = self.event(
                session_id,
                run_id,
                EventKind::Custom {
                    name: "model_call".into(),
                },
            );
            event.payload = json!({"provider": snapshot.selection.provider_id,
                "model": snapshot.selection.model_id, "status": status,
                "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
                "latency_ms": self.monotonic_clock.now().saturating_sub(model_started).as_millis()});
            self.observer.model_completed(&event);
        };
        let stream_result: Result<(), RuntimeError> = async {
            let mut completed = false;
            loop {
                let delta = tokio::select! {
                    () = cancellation.cancelled() => return Err(RuntimeError::Interrupted),
                    delta = stream.next() => delta,
                };
                let Some(delta) = delta else {
                    break;
                };
                match delta {
                    StreamDelta::TextDelta(fragment) => {
                        measurement.first_text(&fragment);
                        text.push_str(&fragment);
                        self.live_payload(
                            snapshot,
                            EventKind::TextDelta,
                            event_payload::MessageDelta {
                                message_id: id.clone(),
                                text: fragment,
                            },
                        );
                    }
                    StreamDelta::ReasoningDelta(fragment) => {
                        reasoning.push(
                            None,
                            crabber_core::ReasoningProvenance::Unspecified,
                            fragment,
                        )?;
                    }
                    StreamDelta::Reasoning {
                        item_id,
                        provenance,
                        text,
                    } => {
                        reasoning.push(Some(item_id.clone()), provenance, text.clone())?;
                        if provenance == crabber_core::ReasoningProvenance::PublicSummary {
                            self.live_payload(
                                snapshot,
                                EventKind::ReasoningDelta,
                                crabber_core::event_payload::ReasoningDelta {
                                    message_id: id.clone(),
                                    item_id,
                                    provenance,
                                    text,
                                },
                            );
                        }
                    }
                    StreamDelta::ReasoningSignature { item_id, signature } => {
                        reasoning.signature(&item_id, &signature)?;
                    }
                    StreamDelta::ToolCallStart { call_id, name } => {
                        if calls.iter().any(|call| call.id == call_id) {
                            return Err(invalid_provider("duplicate tool call start").into());
                        }
                        self.live_payload(
                            snapshot,
                            EventKind::ToolCallStarted,
                            event_payload::CallStarted {
                                message_id: id.clone(),
                                call_id: call_id.clone(),
                                name: name.clone(),
                            },
                        );
                        presentation.start_call(call_id.clone());
                        calls.push(PendingCall {
                            message_id: Some(id.clone()),
                            turn_id: Some(snapshot.identity.turn_id.clone()),
                            args_completed: false,
                            id: call_id,
                            name,
                            raw: String::new(),
                            arguments: None,
                        });
                    }
                    StreamDelta::ToolCallArgsDelta { call_id, text } => {
                        let Some(call) = calls
                            .iter_mut()
                            .find(|call| call.id == call_id && !call.args_completed)
                        else {
                            return Err(invalid_provider("tool arguments without open call").into());
                        };
                        call.raw.push_str(&text);
                        self.live_payload(
                            snapshot,
                            EventKind::ToolCallArgsDelta,
                            event_payload::CallDelta {
                                message_id: id.clone(),
                                call_id,
                                text,
                            },
                        );
                    }
                    StreamDelta::ToolCallDone { call_id } => {
                        let Some(call) = calls
                            .iter_mut()
                            .find(|call| call.id == call_id && !call.args_completed)
                        else {
                            return Err(
                                invalid_provider("tool completion without open call").into()
                            );
                        };
                        call.args_completed = true;
                        call.arguments = Some(match serde_json::from_str(&call.raw) {
                            Ok(parsed) => storable_call_arguments(parsed, &call.raw),
                            Err(_) => Value::String(call.raw.clone()),
                        });
                        presentation.complete_call(&call_id);
                    }
                    StreamDelta::ProviderState { codec_id, payload } => {
                        reasoning.opaque(codec_id, payload)?;
                    }
                    StreamDelta::Usage(delta) => {
                        usage.input_tokens += delta.input_tokens;
                        usage.output_tokens += delta.output_tokens;
                    }
                    StreamDelta::FinishReason(reason) => {
                        finish_reason = Some(match finish_reason {
                            Some(previous) if previous != reason => {
                                crabber_core::ModelFinishReason::Other
                            }
                            _ => reason,
                        });
                    }
                    StreamDelta::Completed => {
                        completed = true;
                        break;
                    }
                    StreamDelta::Error(error) => {
                        return Err(error.into());
                    }
                }
            }
            if !completed {
                return Err(invalid_provider("model stream ended without completion").into());
            }
            let mut event = self.turn_event(
                session_id,
                run_id,
                &snapshot.identity.turn_id,
                EventKind::ModelResponseCompleted,
            );
            event.payload = serde_json::to_value(event_payload::ModelResponseCompleted {
                finish_reason,
                usage: usage.clone(),
                has_visible_text: !text.trim().is_empty(),
                tool_call_count: calls.len(),
                stream_completed: true,
            })
            .expect("event payload serializes");
            ensure_lease(lease_lost)?;
            self.publish_event(execution, plan, event).await?;
            match finish_reason {
                Some(crabber_core::ModelFinishReason::Length) => {
                    return Err(RuntimeError::OutputLimit {
                        tool_call_count: calls.len(),
                    });
                }
                Some(
                    reason @ (crabber_core::ModelFinishReason::ContentFilter
                    | crabber_core::ModelFinishReason::Other),
                ) => return Err(RuntimeError::ModelResponseRejected(reason)),
                _ => {}
            }
            if text.is_empty() && reasoning.is_empty() && calls.is_empty() {
                return Err(invalid_provider("empty model response").into());
            }
            Ok(())
        }
        .await;
        let outcome = match &stream_result {
            Ok(()) => StreamOutcome::Completed,
            Err(RuntimeError::Interrupted) => StreamOutcome::Interrupted,
            Err(_) => StreamOutcome::Failed,
        };
        presentation.finish(outcome);
        if matches!(&stream_result, Err(RuntimeError::Interrupted))
            && (!text.is_empty() || !reasoning.is_empty())
        {
            let mut parts = Vec::new();
            if !text.is_empty() {
                push_part(
                    &mut parts,
                    &id,
                    PartKind::AssistantText,
                    ContentBlock::Text { text: text.clone() },
                );
            }
            for block in reasoning.interrupted_blocks() {
                let kind = reasoning::ReasoningBuffer::part_kind(&block);
                push_part(&mut parts, &id, kind, block);
            }
            ensure_lease(lease_lost)?;
            execution
                .append_message(Message {
                    id: id.clone(),
                    session_id: session_id.clone(),
                    run_id: Some(run_id.clone()),
                    role: Role::Assistant,
                    parent_id: None,
                    parts,
                    created_at: self.clock.now(),
                })
                .await?;
            self.committed_message(execution, plan, snapshot, &id)
                .await?;
        }
        if stream_result.is_err() {
            observe_model("error", &usage);
        }
        stream_result?;
        measurement.observation.reason = TerminalReason::Success;
        drop(measurement);
        observe_model("ok", &usage);
        let mut parts = Vec::new();
        if !text.is_empty() {
            push_part(
                &mut parts,
                &id,
                PartKind::AssistantText,
                ContentBlock::Text { text },
            );
        }
        for block in reasoning.into_blocks() {
            let kind = reasoning::ReasoningBuffer::part_kind(&block);
            push_part(&mut parts, &id, kind, block);
        }
        for call in &calls {
            push_part(
                &mut parts,
                &id,
                PartKind::FunctionToolCall,
                ContentBlock::ToolCall {
                    call_id: call.id.clone(),
                    name: call.name.clone(),
                    arguments: call
                        .arguments
                        .clone()
                        .unwrap_or_else(|| Value::String(call.raw.clone())),
                },
            );
        }
        let message = Message {
            id: id.clone(),
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            parts,
            created_at: self.clock.now(),
        };
        ensure_lease(lease_lost)?;
        execution.append_message(message).await?;
        self.committed_message(execution, plan, snapshot, &id)
            .await?;
        plan.dispatcher
            .notify::<ModelCompleted>(json!({"usage":usage,"finish_reason":finish_reason}))
            .await;
        Ok((calls, usage))
    }

    #[allow(clippy::too_many_arguments)]
    pub(super) async fn stage_tool(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: PendingCall,
        lease_lost: &AtomicBool,
    ) -> Result<ToolCallRecord, RuntimeError> {
        if let Some(descriptor) = frontend::declarations(plan)?
            .into_iter()
            .find(|tool| tool.name == call.name)
        {
            return self
                .stage_frontend(
                    execution, session_id, run_id, plan, call, descriptor, lease_lost,
                )
                .await;
        }
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let raw = call.arguments.unwrap_or(Value::String(call.raw));
        let arguments = if let Some(tool) = &definition {
            let prepared = match validate_arguments(&tool.info, &raw) {
                Ok(()) => match self.tool_pipeline.prepare(&tool.info, raw.clone()).await {
                    Ok(value) => match plan.dispatcher.transform::<ToolPrepare>(json!({"name":tool.info.name,"call_id":call.id.to_string(),"input":value})).await {
                        Ok(output) => {
                            let input = output.get("input").cloned().unwrap_or(output);
                            validate_arguments(&tool.info, &input).map(|()| input)
                        }
                        Err(error) => Err(error.to_string()),
                    },
                    Err(error) => Err(error),
                },
                Err(error) => Err(error),
            };
            match prepared {
                Ok(value) if is_reserved_arguments(&value) => {
                    json!({PREPARE_ERROR_KEY: RESERVED_ARGUMENT_KEY_TEXT})
                }
                Ok(value) => value,
                Err(error) => json!({PREPARE_ERROR_KEY: error}),
            }
        } else {
            unknown_tool_arguments(&raw)
        };
        ensure_lease(lease_lost)?;
        let mut event = self.event(session_id, run_id, EventKind::ToolCallPending);
        event.turn_id = call.turn_id.clone();
        event.payload = json!({"call_id":call.id,"name":call.name,"status":"pending", "message_id":call.message_id});
        event.correlation = Some(call.id.to_string());
        let mut record = ToolCallRecord {
            id: call.id,
            run_id: run_id.clone(),
            name: call.name,
            arguments,
            status: ToolCallStatus::Pending,
            retry_safe: definition.as_ref().is_some_and(|tool| tool.info.retry_safe),
            result: None,
        };
        if crabber_session::ensure_storable(&record).is_err() {
            record.arguments = json!({PREPARE_ERROR_KEY: UNSTORABLE_ARGUMENTS_TEXT});
        }
        execution
            .create_tool_call(record.clone(), event.clone())
            .await?;
        self.observer.emit(&event);
        plan.dispatcher
            .notify::<EventPublished>(serde_json::to_value(&event).unwrap_or(Value::Null))
            .await;
        Ok(record)
    }
}
