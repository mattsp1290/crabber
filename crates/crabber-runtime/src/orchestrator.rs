use crate::policy::{
    ApprovalRequester, DefaultDenyApprover, IdentityToolPipeline, PermissionDecision,
    PermissionPolicy, StaticPolicy, ToolPipeline,
};
use async_trait::async_trait;
use crabber_core::{
    Clock, ContentBlock, EventKind, EventRecord, Message, MessageId, Part, PartId, PartKind, Role,
    RunFence, RunId, RunStatus, SessionId, SystemClock, ToolCallId, ToolCallRecord, ToolCallStatus,
    ToolInfo, ToolResult, ToolResultStatus, TurnId, Usage,
};
use crabber_extension::{RunPlan, RunPlanProvider, ToolDefinition};
use crabber_providers::{
    DeltaStream, ModelRequest, ProviderError, RequestIdentity, Resolver, Selection, StreamDelta,
    Streamer,
};
use crabber_session::{AdmitRequest, ExecutionStore, InboxKind, Store, StoreError};
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::oneshot;

#[derive(Debug, Clone)]
pub struct Request {
    pub session_id: Option<SessionId>,
    pub workspace_id: String,
    pub directory: String,
    pub title: String,
    pub text: String,
    pub selection: Selection,
    pub system_prompt: Option<String>,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct RunResult {
    pub session_id: SessionId,
    pub run_id: RunId,
    pub status: RunStatus,
    pub usage: Usage,
}

#[derive(Debug, thiserror::Error)]
pub enum RuntimeError {
    #[error("session is busy")]
    SessionBusy,
    #[error("store: {0}")]
    Store(#[from] StoreError),
    #[error("provider: {0}")]
    Provider(#[from] ProviderError),
    #[error("extension: {0}")]
    Extension(String),
    #[error("turn limit reached")]
    TurnLimit,
    #[error("run task stopped")]
    TaskStopped,
    #[error("orchestrator is missing {0}")]
    Missing(&'static str),
}

#[derive(Debug, Clone)]
pub struct TurnSnapshot {
    pub identity: RequestIdentity,
    pub selection: Selection,
    pub messages: Vec<Message>,
    pub system: Option<String>,
    pub tools: Vec<ToolInfo>,
}

pub trait Observer: Send + Sync {
    fn emit(&self, event: &EventRecord);
}

pub struct NoopObserver;
impl Observer for NoopObserver {
    fn emit(&self, _event: &EventRecord) {}
}

#[async_trait]
pub trait ModelStream: Send + Sync {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError>;
}

pub struct DirectModelStream;
#[async_trait]
impl ModelStream for DirectModelStream {
    async fn stream(
        &self,
        request: ModelRequest,
        next: Arc<dyn Streamer>,
    ) -> Result<DeltaStream, ProviderError> {
        next.stream(request).await
    }
}

struct SingleUseStreamer {
    inner: Arc<dyn Streamer>,
    calls: Arc<AtomicUsize>,
}

#[async_trait]
impl Streamer for SingleUseStreamer {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(invalid_provider(
                "model stream wrapper called provider more than once",
            ));
        }
        self.inner.stream(request).await
    }
}

#[derive(Clone)]
pub struct Orchestrator {
    store: Arc<dyn Store>,
    resolver: Arc<dyn Resolver>,
    plan_provider: Arc<dyn RunPlanProvider>,
    clock: Arc<dyn Clock>,
    observer: Arc<dyn Observer>,
    model_stream: Arc<dyn ModelStream>,
    policy: Arc<dyn PermissionPolicy>,
    approver: Arc<dyn ApprovalRequester>,
    tool_pipeline: Arc<dyn ToolPipeline>,
    max_turns: usize,
}

#[derive(Default)]
pub struct OrchestratorBuilder {
    store: Option<Arc<dyn Store>>,
    resolver: Option<Arc<dyn Resolver>>,
    plan_provider: Option<Arc<dyn RunPlanProvider>>,
    clock: Option<Arc<dyn Clock>>,
    observer: Option<Arc<dyn Observer>>,
    model_stream: Option<Arc<dyn ModelStream>>,
    policy: Option<Arc<dyn PermissionPolicy>>,
    approver: Option<Arc<dyn ApprovalRequester>>,
    tool_pipeline: Option<Arc<dyn ToolPipeline>>,
    max_turns: Option<usize>,
}

impl OrchestratorBuilder {
    #[must_use]
    pub fn store(mut self, value: Arc<dyn Store>) -> Self {
        self.store = Some(value);
        self
    }
    #[must_use]
    pub fn resolver(mut self, value: Arc<dyn Resolver>) -> Self {
        self.resolver = Some(value);
        self
    }
    #[must_use]
    pub fn plan_provider(mut self, value: Arc<dyn RunPlanProvider>) -> Self {
        self.plan_provider = Some(value);
        self
    }
    #[must_use]
    pub fn clock(mut self, value: Arc<dyn Clock>) -> Self {
        self.clock = Some(value);
        self
    }
    #[must_use]
    pub fn observer(mut self, value: Arc<dyn Observer>) -> Self {
        self.observer = Some(value);
        self
    }
    #[must_use]
    pub fn model_stream(mut self, value: Arc<dyn ModelStream>) -> Self {
        self.model_stream = Some(value);
        self
    }
    #[must_use]
    pub fn policy(mut self, value: Arc<dyn PermissionPolicy>) -> Self {
        self.policy = Some(value);
        self
    }
    #[must_use]
    pub fn approver(mut self, value: Arc<dyn ApprovalRequester>) -> Self {
        self.approver = Some(value);
        self
    }
    #[must_use]
    pub fn tool_pipeline(mut self, value: Arc<dyn ToolPipeline>) -> Self {
        self.tool_pipeline = Some(value);
        self
    }
    #[must_use]
    pub fn max_turns(mut self, value: usize) -> Self {
        self.max_turns = Some(value);
        self
    }

    /// Builds an orchestrator from its required dependencies.
    ///
    /// # Errors
    ///
    /// Returns `Missing` if the store, resolver, or plan provider is absent.
    pub fn build(self) -> Result<Orchestrator, RuntimeError> {
        Ok(Orchestrator {
            store: self.store.ok_or(RuntimeError::Missing("store"))?,
            resolver: self.resolver.ok_or(RuntimeError::Missing("resolver"))?,
            plan_provider: self
                .plan_provider
                .ok_or(RuntimeError::Missing("plan provider"))?,
            clock: self.clock.unwrap_or_else(|| Arc::new(SystemClock)),
            observer: self.observer.unwrap_or_else(|| Arc::new(NoopObserver)),
            model_stream: self
                .model_stream
                .unwrap_or_else(|| Arc::new(DirectModelStream)),
            policy: self
                .policy
                .unwrap_or_else(|| Arc::new(StaticPolicy::default())),
            approver: self
                .approver
                .unwrap_or_else(|| Arc::new(DefaultDenyApprover)),
            tool_pipeline: self
                .tool_pipeline
                .unwrap_or_else(|| Arc::new(IdentityToolPipeline)),
            max_turns: self.max_turns.unwrap_or(64),
        })
    }
}

pub struct RunHandle {
    session_id: SessionId,
    run_id: RunId,
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
}

impl RunHandle {
    #[must_use]
    pub fn session_id(&self) -> &SessionId {
        &self.session_id
    }
    #[must_use]
    pub fn run_id(&self) -> &RunId {
        &self.run_id
    }
    /// Waits for the run's terminal result.
    ///
    /// # Errors
    ///
    /// Returns the run failure or `TaskStopped` if its task ended unexpectedly.
    pub async fn done(self) -> Result<RunResult, RuntimeError> {
        self.done.await.map_err(|_| RuntimeError::TaskStopped)?
    }
    /// Queues input for the next model request.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn steer(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.enqueue(InboxKind::Steer, text.into()).await
    }
    /// Queues input after pending tool work finishes.
    ///
    /// # Errors
    ///
    /// Returns a store error if the inbox write fails.
    pub async fn follow_up(&self, text: impl Into<String>) -> Result<(), RuntimeError> {
        self.enqueue(InboxKind::FollowUp, text.into()).await
    }
    async fn enqueue(&self, kind: InboxKind, text: String) -> Result<(), RuntimeError> {
        let message = user_message(self.session_id.clone(), text, self.clock.now());
        self.store
            .enqueue_inbox(&self.session_id, kind, message)
            .await?;
        Ok(())
    }
}

impl Orchestrator {
    #[must_use]
    pub fn builder() -> OrchestratorBuilder {
        OrchestratorBuilder::default()
    }

    /// Admits a run and starts its turn loop in the background.
    ///
    /// # Errors
    ///
    /// Returns `SessionBusy` for a session with an active run, or a plan/store error.
    pub async fn start(&self, request: Request) -> Result<RunHandle, RuntimeError> {
        let now = self.clock.now();
        let session_id = request.session_id.clone().unwrap_or_default();
        let plan = self
            .plan_provider
            .acquire_plan(&session_id)
            .await
            .map_err(|error| RuntimeError::Extension(error.to_string()))?;
        let config = format!(
            "{}:{}:{:?}",
            request.selection.provider_id, request.selection.model_id, request.system_prompt
        );
        let config_hash = format!("{:x}", Sha256::digest(config.as_bytes()));
        let admitted = self
            .store
            .admit_run(AdmitRequest {
                session_id: request.session_id.clone(),
                workspace_id: request.workspace_id.clone(),
                directory: request.directory.clone(),
                title: request.title.clone(),
                user_message: user_message(session_id, request.text.clone(), now),
                config_hash,
                plan_fingerprint: plan.fingerprint().to_string(),
                owner: RunId::new().to_string(),
                lease: Duration::from_secs(30),
            })
            .await
            .map_err(|error| {
                if error == StoreError::Busy {
                    RuntimeError::SessionBusy
                } else {
                    RuntimeError::Store(error)
                }
            })?;
        let (sender, done) = oneshot::channel();
        let handle = RunHandle {
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            done,
        };
        let runtime = self.clone();
        tokio::spawn(async move {
            let result = runtime
                .run(admitted.fence, admitted.session.id, request, plan)
                .await;
            let _ = sender.send(result);
        });
        Ok(handle)
    }

    async fn run(
        &self,
        fence: RunFence,
        session_id: SessionId,
        request: Request,
        plan: RunPlan,
    ) -> Result<RunResult, RuntimeError> {
        let execution = self.store.execution(fence.clone()).await?;
        let run_id = fence.run_id.clone();
        let outcome = self
            .run_loop(execution.as_ref(), &run_id, &session_id, &request, &plan)
            .await;
        plan.release();
        match outcome {
            Ok(usage) => Ok(RunResult {
                session_id,
                run_id,
                status: RunStatus::Completed,
                usage,
            }),
            Err(error) => {
                let _ = execution
                    .settle_run(
                        RunStatus::Failed,
                        Some(error.to_string()),
                        Usage::default(),
                        self.event(&session_id, &run_id, EventKind::RunSettled),
                    )
                    .await;
                Err(error)
            }
        }
    }

    async fn run_loop(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        request: &Request,
        plan: &RunPlan,
    ) -> Result<Usage, RuntimeError> {
        self.emit_durable(execution, session_id, run_id, EventKind::RunAdmitted)
            .await?;
        self.emit_durable(execution, session_id, run_id, EventKind::RunStarted)
            .await?;
        let streamer = self.resolver.resolve(&request.selection).await?;
        let mut usage = Usage::default();
        for _ in 0..self.max_turns {
            self.emit_durable(execution, session_id, run_id, EventKind::TurnStarted)
                .await?;
            let snapshot = self.snapshot(run_id, session_id, request, plan).await?;
            let (calls, turn_usage) = self
                .model_turn(
                    execution,
                    run_id,
                    session_id,
                    &snapshot,
                    Arc::clone(&streamer),
                )
                .await?;
            usage.input_tokens += turn_usage.input_tokens;
            usage.output_tokens += turn_usage.output_tokens;
            let had_tools = !calls.is_empty();
            for call in calls {
                self.execute_tool(execution, session_id, run_id, plan, call)
                    .await?;
            }
            let mut claimed = self
                .claim_input(execution, run_id, InboxKind::Steer)
                .await?;
            if !had_tools && claimed == 0 {
                claimed += self
                    .claim_input(execution, run_id, InboxKind::FollowUp)
                    .await?;
            }
            self.emit_durable(execution, session_id, run_id, EventKind::TurnCompleted)
                .await?;
            if !had_tools && claimed == 0 {
                match execution
                    .settle_run(
                        RunStatus::Completed,
                        None,
                        usage.clone(),
                        self.event(session_id, run_id, EventKind::RunSettled),
                    )
                    .await
                {
                    Ok(()) => {
                        self.observer
                            .emit(&self.event(session_id, run_id, EventKind::RunSettled));
                        return Ok(usage);
                    }
                    Err(StoreError::PendingInput) => {
                        if self
                            .claim_input(execution, run_id, InboxKind::Steer)
                            .await?
                            + self
                                .claim_input(execution, run_id, InboxKind::FollowUp)
                                .await?
                            == 0
                        {
                            return Err(RuntimeError::Store(StoreError::PendingInput));
                        }
                    }
                    Err(error) => return Err(error.into()),
                }
            }
        }
        Err(RuntimeError::TurnLimit)
    }

    async fn snapshot(
        &self,
        run_id: &RunId,
        session_id: &SessionId,
        request: &Request,
        plan: &RunPlan,
    ) -> Result<TurnSnapshot, RuntimeError> {
        let mut system = request.system_prompt.clone().unwrap_or_default();
        for prompt in &plan.prompts {
            if !system.is_empty() {
                system.push('\n');
            }
            system.push_str(&prompt.text);
        }
        Ok(TurnSnapshot {
            identity: RequestIdentity {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                turn_id: TurnId::new(),
            },
            selection: request.selection.clone(),
            messages: self.store.list_messages(session_id, None).await?,
            system: (!system.is_empty()).then_some(system),
            tools: plan.tools.iter().map(|tool| tool.info.clone()).collect(),
        })
    }

    async fn claim_input(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        kind: InboxKind,
    ) -> Result<usize, RuntimeError> {
        let input = execution.claim_inbox(kind).await?;
        let count = input.len();
        for mut message in input {
            // Claimed rows become durable model context for the next turn.
            message.run_id = Some(run_id.clone());
            execution.append_message(message).await?;
        }
        Ok(count)
    }

    fn event(&self, session_id: &SessionId, run_id: &RunId, kind: EventKind) -> EventRecord {
        EventRecord {
            cursor: None,
            session_id: session_id.clone(),
            run_id: run_id.clone(),
            turn_id: None,
            kind,
            payload: Value::Null,
            correlation: None,
            live_only: false,
            created_at: self.clock.now(),
        }
    }

    async fn emit_durable(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        kind: EventKind,
    ) -> Result<(), RuntimeError> {
        let event = self.event(session_id, run_id, kind);
        execution.append_event(event.clone()).await?;
        self.observer.emit(&event);
        Ok(())
    }
}

fn user_message(session_id: SessionId, text: String, now: OffsetDateTime) -> Message {
    let id = MessageId::new();
    Message {
        id: id.clone(),
        session_id,
        run_id: None,
        role: Role::User,
        parent_id: None,
        parts: vec![Part {
            id: PartId::new(),
            message_id: id,
            ordinal: 0,
            kind: PartKind::UserInputText,
            content: ContentBlock::Text { text },
        }],
        created_at: now,
    }
}

struct PendingCall {
    id: ToolCallId,
    name: String,
    raw: String,
    arguments: Option<Value>,
}

impl Orchestrator {
    #[allow(clippy::too_many_lines)] // Streaming assembly and durable assistant commit form one turn boundary.
    async fn model_turn(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        streamer: Arc<dyn Streamer>,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let request = ModelRequest {
            identity: snapshot.identity.clone(),
            selection: snapshot.selection.clone(),
            system: snapshot.system.clone(),
            messages: snapshot.messages.clone(),
            tools: snapshot.tools.clone(),
        };
        let calls_to_next = Arc::new(AtomicUsize::new(0));
        let next: Arc<dyn Streamer> = Arc::new(SingleUseStreamer {
            inner: streamer,
            calls: Arc::clone(&calls_to_next),
        });
        let mut stream = self.model_stream.stream(request, next).await?;
        if calls_to_next.load(Ordering::SeqCst) != 1 {
            return Err(invalid_provider(
                "model stream wrapper did not call provider exactly once",
            )
            .into());
        }
        let mut text = String::new();
        let mut reasoning = String::new();
        let mut provider_state = Vec::new();
        let mut calls: Vec<PendingCall> = Vec::new();
        let mut usage = Usage::default();
        while let Some(delta) = stream.next().await {
            match delta {
                StreamDelta::TextDelta(fragment) => {
                    text.push_str(&fragment);
                    let mut event = self.event(session_id, run_id, EventKind::TextDelta);
                    event.live_only = true;
                    event.payload = json!({"text": fragment});
                    self.observer.emit(&event);
                }
                StreamDelta::ReasoningDelta(fragment) => {
                    reasoning.push_str(&fragment);
                    let mut event = self.event(session_id, run_id, EventKind::ReasoningDelta);
                    event.live_only = true;
                    self.observer.emit(&event);
                }
                StreamDelta::ToolCallStart { call_id, name } => calls.push(PendingCall {
                    id: call_id,
                    name,
                    raw: String::new(),
                    arguments: None,
                }),
                StreamDelta::ToolCallArgsDelta { call_id, text } => {
                    let call = calls
                        .iter_mut()
                        .find(|call| call.id == call_id)
                        .ok_or_else(|| invalid_provider("tool arguments without call start"))?;
                    call.raw.push_str(&text);
                }
                StreamDelta::ToolCallDone { call_id } => {
                    let call = calls
                        .iter_mut()
                        .find(|call| call.id == call_id)
                        .ok_or_else(|| invalid_provider("tool completion without call start"))?;
                    call.arguments = Some(
                        serde_json::from_str(&call.raw)
                            .unwrap_or_else(|_| Value::String(call.raw.clone())),
                    );
                }
                StreamDelta::ProviderState { codec_id, payload } => {
                    provider_state.push((codec_id, payload));
                }
                StreamDelta::Usage(delta) => {
                    usage.input_tokens += delta.input_tokens;
                    usage.output_tokens += delta.output_tokens;
                }
                StreamDelta::Completed => break,
                StreamDelta::Error(error) => return Err(error.into()),
            }
        }
        if text.is_empty() && reasoning.is_empty() && calls.is_empty() && provider_state.is_empty()
        {
            return Err(invalid_provider("empty model response").into());
        }
        let id = MessageId::new();
        let mut parts = Vec::new();
        if !text.is_empty() {
            push_part(
                &mut parts,
                &id,
                PartKind::AssistantText,
                ContentBlock::Text { text },
            );
        }
        if !reasoning.is_empty() {
            push_part(
                &mut parts,
                &id,
                PartKind::Reasoning,
                ContentBlock::Reasoning {
                    text: reasoning,
                    provider_state: None,
                },
            );
        }
        for (codec_id, payload) in provider_state {
            push_part(
                &mut parts,
                &id,
                PartKind::ProviderState,
                ContentBlock::ProviderState { codec_id, payload },
            );
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
            id,
            session_id: session_id.clone(),
            run_id: Some(run_id.clone()),
            role: Role::Assistant,
            parent_id: None,
            parts,
            created_at: self.clock.now(),
        };
        execution.append_message(message).await?;
        self.emit_durable(execution, session_id, run_id, EventKind::MessageCommitted)
            .await?;
        Ok((calls, usage))
    }

    async fn execute_tool(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: PendingCall,
    ) -> Result<(), RuntimeError> {
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let raw = call.arguments.unwrap_or(Value::String(call.raw));
        let prepared = if let Some(tool) = &definition {
            match validate_arguments(&tool.info, &raw) {
                Ok(()) => self
                    .tool_pipeline
                    .prepare(&tool.info, raw.clone())
                    .await
                    .and_then(|value| validate_arguments(&tool.info, &value).map(|()| value)),
                Err(error) => Err(error),
            }
        } else {
            Err(format!("unknown tool: {}", call.name))
        };
        let pending = self.event(session_id, run_id, EventKind::ToolCallPending);
        execution
            .create_tool_call(
                ToolCallRecord {
                    id: call.id.clone(),
                    run_id: run_id.clone(),
                    name: call.name,
                    arguments: prepared.clone().unwrap_or(raw),
                    status: ToolCallStatus::Pending,
                    retry_safe: definition.as_ref().is_some_and(|tool| tool.info.retry_safe),
                    result: None,
                },
                pending.clone(),
            )
            .await?;
        self.observer.emit(&pending);
        let running = self.event(session_id, run_id, EventKind::ToolCallRunning);
        execution.claim_tool_call(&call.id, running.clone()).await?;
        self.observer.emit(&running);
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await?;
        let outcome = match (definition, prepared) {
            (Some(tool), Ok(arguments)) => {
                self.permit_and_execute(execution, session_id, run_id, &tool, arguments)
                    .await
            }
            (_, Err(error)) => Err(error),
            (None, Ok(_)) => Err("unknown tool".into()),
        };
        let (status, output, is_error) = match outcome {
            Ok(value) => (ToolResultStatus::Completed, value, false),
            Err(error) => (ToolResultStatus::Failed, Value::String(error), true),
        };
        let text = serde_json::to_string(&output).expect("JSON value serializes");
        let content = vec![ContentBlock::Text { text }];
        let result = ToolResult {
            status,
            content: content.clone(),
        };
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
                    content,
                    is_error,
                },
            }],
            created_at: self.clock.now(),
        };
        let settled = self.event(session_id, run_id, EventKind::ToolCallSettled);
        execution
            .settle_tool_call(&call.id, result, message, settled.clone())
            .await?;
        self.observer.emit(&settled);
        Ok(())
    }

    async fn permit_and_execute(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        tool: &ToolDefinition,
        arguments: Value,
    ) -> Result<Value, String> {
        let allowed = match self.policy.decide(&tool.info, &arguments) {
            PermissionDecision::Allow => true,
            PermissionDecision::Deny => false,
            PermissionDecision::Ask => {
                self.emit_durable(
                    execution,
                    session_id,
                    run_id,
                    EventKind::PermissionRequested,
                )
                .await
                .map_err(|error| error.to_string())?;
                let approved = self.approver.approve(&tool.info, &arguments).await;
                self.emit_durable(execution, session_id, run_id, EventKind::PermissionDecided)
                    .await
                    .map_err(|error| error.to_string())?;
                approved
            }
        };
        if !allowed {
            return Err("permission denied".into());
        }
        let output = tool
            .executor
            .execute(arguments)
            .await
            .map_err(|error| error.to_string())?;
        self.tool_pipeline
            .transform_result(&tool.info, output)
            .await
    }
}

fn push_part(parts: &mut Vec<Part>, message_id: &MessageId, kind: PartKind, content: ContentBlock) {
    parts.push(Part {
        id: PartId::new(),
        message_id: message_id.clone(),
        ordinal: u32::try_from(parts.len()).expect("assistant part count fits u32"),
        kind,
        content,
    });
}

fn invalid_provider(message: &str) -> ProviderError {
    ProviderError {
        kind: crabber_providers::ProviderErrorKind::Invalid,
        message: message.into(),
        retryable: false,
    }
}

fn validate_arguments(tool: &ToolInfo, arguments: &Value) -> Result<(), String> {
    let schema = &tool.parameters;
    if schema
        .get("type")
        .and_then(Value::as_str)
        .is_some_and(|kind| kind != "object")
    {
        return Err("tool schema must describe an object".into());
    }
    let object = arguments
        .as_object()
        .ok_or_else(|| "tool arguments must be an object".to_owned())?;
    if let Some(required) = schema.get("required").and_then(Value::as_array) {
        for name in required.iter().filter_map(Value::as_str) {
            if !object.contains_key(name) {
                return Err(format!("missing required argument: {name}"));
            }
        }
    }
    if let Some(properties) = schema.get("properties").and_then(Value::as_object) {
        for (name, value) in object {
            if let Some(expected) = properties
                .get(name)
                .and_then(|property| property.get("type"))
                .and_then(Value::as_str)
            {
                let valid = match expected {
                    "string" => value.is_string(),
                    "number" => value.is_number(),
                    "integer" => value.as_i64().is_some() || value.as_u64().is_some(),
                    "boolean" => value.is_boolean(),
                    "object" => value.is_object(),
                    "array" => value.is_array(),
                    "null" => value.is_null(),
                    _ => false,
                };
                if !valid {
                    return Err(format!("invalid type for argument: {name}"));
                }
            }
        }
    }
    Ok(())
}
