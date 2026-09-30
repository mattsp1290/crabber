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
use crabber_extension::{
    ApprovalFacade, Callback, ContextAssemble, EventPublished, GuardDecision, HostServices,
    ModelCompleted, ModelRequestError, ModelRequested, ModelStream as ExtensionModelStream,
    RunAdmitted, RunBeforeExecute, RunPlan, RunPlanProvider, RunSettled, RunStarted, StateSink,
    ToolContext, ToolDefinition, ToolExecute, ToolPrepare, ToolResultTransform, TurnCompleted,
    TurnPrepare, TurnStarted,
};
use crabber_providers::{
    DeltaStream, ModelRequest, ProviderError, RequestIdentity, Resolver, Selection, StreamDelta,
    Streamer,
};
use crabber_session::{AdmitRequest, ExecutionStore, InboxKind, Store, StoreError};
use futures::StreamExt;
use serde_json::{Value, json};
use sha2::{Digest, Sha256};
use std::{
    collections::BTreeMap,
    sync::{
        Arc,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    time::Duration,
};
use time::OffsetDateTime;
use tokio::sync::{oneshot, watch};
use tokio_util::sync::CancellationToken;

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
    #[error("invalid orchestrator configuration: {0}")]
    InvalidConfiguration(&'static str),
    #[error("run lease ownership was lost")]
    LeaseLost,
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
    fn model_completed(&self, _event: &EventRecord) {}
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
    host_services: HostServices,
    max_turns: usize,
    heartbeat_interval: Duration,
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
    host_services: Option<HostServices>,
    max_turns: Option<usize>,
    heartbeat_interval: Option<Duration>,
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
    pub fn host_services(mut self, value: HostServices) -> Self {
        self.host_services = Some(value);
        self
    }
    #[must_use]
    pub fn max_turns(mut self, value: usize) -> Self {
        self.max_turns = Some(value);
        self
    }
    #[must_use]
    pub fn heartbeat_interval(mut self, value: Duration) -> Self {
        self.heartbeat_interval = Some(value);
        self
    }

    /// Builds an orchestrator from its required dependencies.
    ///
    /// # Errors
    ///
    /// Returns `Missing` if the store, resolver, or plan provider is absent.
    pub fn build(self) -> Result<Orchestrator, RuntimeError> {
        let heartbeat_interval = self.heartbeat_interval.unwrap_or(Duration::from_secs(5));
        if heartbeat_interval.is_zero() || heartbeat_interval >= Duration::from_secs(15) {
            return Err(RuntimeError::InvalidConfiguration(
                "heartbeat interval must be positive and below 15 seconds",
            ));
        }
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
            host_services: self.host_services.unwrap_or_default(),
            max_turns: self.max_turns.unwrap_or(64),
            heartbeat_interval,
        })
    }
}

pub struct RunHandle {
    session_id: SessionId,
    run_id: RunId,
    store: Arc<dyn Store>,
    clock: Arc<dyn Clock>,
    done: oneshot::Receiver<Result<RunResult, RuntimeError>>,
    completion: watch::Receiver<bool>,
}

struct RunStateSink {
    store: Arc<dyn Store>,
    execution: Arc<dyn ExecutionStore>,
    session_id: SessionId,
    lost: Arc<AtomicBool>,
}

#[async_trait]
impl StateSink for RunStateSink {
    fn session_id(&self) -> &SessionId {
        &self.session_id
    }

    async fn snapshot(&self, extension_id: &str) -> Result<BTreeMap<String, String>, String> {
        if self.lost.load(Ordering::Acquire) {
            return Err("run lease lost".into());
        }
        self.store
            .get_extension_state(extension_id, &self.session_id)
            .await
            .map_err(|error| error.to_string())
    }

    async fn apply(
        &self,
        extension_id: &str,
        changes: Vec<(String, Option<String>)>,
    ) -> Result<(), String> {
        if self.lost.load(Ordering::Acquire) {
            return Err("run lease lost".into());
        }
        self.execution
            .put_extension_state(extension_id, changes)
            .await
            .map_err(|error| error.to_string())
    }
}

struct HeartbeatGuard {
    task: tokio::task::JoinHandle<()>,
    lost: Arc<AtomicBool>,
    signal: watch::Receiver<bool>,
    _signal_sender: watch::Sender<bool>,
}

impl HeartbeatGuard {
    fn start(
        execution: Arc<dyn ExecutionStore>,
        store: Arc<dyn Store>,
        fence: RunFence,
        clock: Arc<dyn Clock>,
        period: Duration,
    ) -> Self {
        let lost = Arc::new(AtomicBool::new(false));
        let lost_in_task = Arc::clone(&lost);
        let (sender, signal) = watch::channel(false);
        let task_sender = sender.clone();
        let task = tokio::spawn(async move {
            let mut ticker = tokio::time::interval_at(tokio::time::Instant::now() + period, period);
            ticker.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);
            loop {
                ticker.tick().await;
                if execution
                    .renew_lease(clock.now() + time::Duration::seconds(30))
                    .await
                    .is_err()
                {
                    let settled_by_this_owner = store
                        .get_run(&fence.run_id)
                        .await
                        .ok()
                        .flatten()
                        .is_some_and(|run| {
                            run.status.is_terminal() && run.claim_token == fence.claim_token
                        });
                    if settled_by_this_owner {
                        break;
                    }
                    lost_in_task.store(true, Ordering::SeqCst);
                    let _ = task_sender.send(true);
                    break;
                }
            }
        });
        Self {
            task,
            lost,
            signal,
            _signal_sender: sender,
        }
    }
}

impl Drop for HeartbeatGuard {
    fn drop(&mut self) {
        self.task.abort();
    }
}

impl RunHandle {
    /// Signals when the run task has returned, whether it succeeded or failed.
    #[must_use]
    pub fn completion_signal(&self) -> watch::Receiver<bool> {
        self.completion.clone()
    }
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
        let (completion_sender, completion) = watch::channel(false);
        let handle = RunHandle {
            session_id: admitted.session.id.clone(),
            run_id: admitted.run.id.clone(),
            store: Arc::clone(&self.store),
            clock: Arc::clone(&self.clock),
            done,
            completion,
        };
        let runtime = self.clone();
        tokio::spawn(async move {
            let result = runtime
                .run(admitted.fence, admitted.session.id, request, plan)
                .await;
            let _ = sender.send(result);
            let _ = completion_sender.send(true);
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
        let execution: Arc<dyn ExecutionStore> = self.store.execution(fence.clone()).await?.into();
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await?;
        let heartbeat = HeartbeatGuard::start(
            Arc::clone(&execution),
            Arc::clone(&self.store),
            fence.clone(),
            Arc::clone(&self.clock),
            self.heartbeat_interval,
        );
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
        let outcome = {
            let run_future = crabber_extension::with_state_sink(
                state_sink,
                self.run_loop(
                    execution.as_ref(),
                    &run_id,
                    &session_id,
                    &request,
                    &plan,
                    lost.as_ref(),
                ),
            );
            tokio::pin!(run_future);
            loop {
                tokio::select! {
                    biased;
                    update = signal.changed() => match update {
                        Ok(()) if *signal.borrow_and_update() => break Err(RuntimeError::LeaseLost),
                        Ok(()) => (),
                        Err(_) => break run_future.as_mut().await,
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
                let settled = self.run_settled_event(
                    &session_id,
                    &run_id,
                    RunStatus::Failed,
                    &Usage::default(),
                    run_started_at,
                );
                if execution
                    .settle_run(
                        RunStatus::Failed,
                        Some(error.to_string()),
                        Usage::default(),
                        settled.clone(),
                    )
                    .await
                    .is_ok()
                {
                    self.observer.emit(&settled);
                    let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                    plan.dispatcher
                        .notify::<EventPublished>(projection.clone())
                        .await;
                    plan.dispatcher.notify::<RunSettled>(projection).await;
                }
                Err(error)
            }
        };
        plan.release();
        result
    }

    async fn run_loop(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        request: &Request,
        plan: &RunPlan,
        lease_lost: &AtomicBool,
    ) -> Result<Usage, RuntimeError> {
        let run_started_at = self.clock.now();
        plan.dispatcher
            .gate::<RunBeforeExecute>(json!({"run_id": run_id.to_string()}))
            .await
            .map_err(|e| RuntimeError::Extension(e.to_string()))?;
        self.emit_durable(execution, session_id, run_id, plan, EventKind::RunAdmitted)
            .await?;
        self.emit_durable(execution, session_id, run_id, plan, EventKind::RunStarted)
            .await?;
        let streamer = if let Some(provider) = plan
            .providers
            .iter()
            .find(|p| p.info().id == request.selection.provider_id)
        {
            provider.build(&request.selection).await?
        } else {
            self.resolver.resolve(&request.selection).await?
        };
        let mut usage = Usage::default();
        for _ in 0..self.max_turns {
            self.emit_durable(execution, session_id, run_id, plan, EventKind::TurnStarted)
                .await?;
            let snapshot = self.snapshot(run_id, session_id, request, plan).await?;
            plan.dispatcher
                .hook::<TurnPrepare>(json!({"run_id":run_id.to_string()}))
                .await
                .map_err(|e| RuntimeError::Extension(e.to_string()))?;
            let (calls, turn_usage) = self
                .model_turn(
                    execution,
                    run_id,
                    session_id,
                    &snapshot,
                    plan,
                    Arc::clone(&streamer),
                    lease_lost,
                )
                .await?;
            usage.input_tokens += turn_usage.input_tokens;
            usage.output_tokens += turn_usage.output_tokens;
            let had_tools = !calls.is_empty();
            for call in calls {
                self.execute_tool(execution, session_id, run_id, plan, call, lease_lost)
                    .await?;
            }
            let mut claimed = self.claim_input(execution, InboxKind::Steer).await?;
            if !had_tools && claimed == 0 {
                claimed += self.claim_input(execution, InboxKind::FollowUp).await?;
            }
            self.emit_durable(
                execution,
                session_id,
                run_id,
                plan,
                EventKind::TurnCompleted,
            )
            .await?;
            if !had_tools && claimed == 0 {
                ensure_lease(lease_lost)?;
                let settled = self.run_settled_event(
                    session_id,
                    run_id,
                    RunStatus::Completed,
                    &usage,
                    run_started_at,
                );
                match execution
                    .settle_run(RunStatus::Completed, None, usage.clone(), settled.clone())
                    .await
                {
                    Ok(()) => {
                        self.observer.emit(&settled);
                        let projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
                        plan.dispatcher
                            .notify::<EventPublished>(projection.clone())
                            .await;
                        plan.dispatcher.notify::<RunSettled>(projection).await;
                        return Ok(usage);
                    }
                    Err(StoreError::PendingInput) => {
                        if self.claim_input(execution, InboxKind::Steer).await?
                            + self.claim_input(execution, InboxKind::FollowUp).await?
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
        let contributions = plan
            .dispatcher
            .transform::<ContextAssemble>(json!({"system_prelude":[],"user_suffix":[]}))
            .await
            .map_err(|e| RuntimeError::Extension(e.to_string()))?;
        if let Some(prelude) = contributions
            .get("system_prelude")
            .and_then(Value::as_array)
        {
            for line in prelude.iter().filter_map(Value::as_str) {
                system = format!("{line}\n{system}");
            }
        }
        let mut messages = self.store.list_messages(session_id, None).await?;
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
            identity: RequestIdentity {
                session_id: session_id.clone(),
                run_id: run_id.clone(),
                turn_id: TurnId::new(),
            },
            selection: request.selection.clone(),
            messages,
            system: (!system.is_empty()).then_some(system),
            tools: plan.tools.iter().map(|tool| tool.info.clone()).collect(),
        })
    }

    async fn claim_input(
        &self,
        execution: &dyn ExecutionStore,
        kind: InboxKind,
    ) -> Result<usize, RuntimeError> {
        Ok(execution.claim_inbox_into_history(kind).await?.len())
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

    fn run_settled_event(
        &self,
        session_id: &SessionId,
        run_id: &RunId,
        status: RunStatus,
        usage: &Usage,
        started_at: OffsetDateTime,
    ) -> EventRecord {
        let mut event = self.event(session_id, run_id, EventKind::RunSettled);
        let duration = event.created_at - started_at;
        event.payload = json!({"status": status, "usage": usage,
            "input_tokens": usage.input_tokens, "output_tokens": usage.output_tokens,
            "duration_ms": duration.whole_milliseconds(),
            "duration_ns": duration.whole_nanoseconds()});
        event.correlation = Some(run_id.to_string());
        event
    }

    async fn emit_durable(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        kind: EventKind,
    ) -> Result<(), RuntimeError> {
        let event = self.event(session_id, run_id, kind);
        execution.append_event(event.clone()).await?;
        self.observer.emit(&event);
        let projection = serde_json::to_value(&event).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(projection.clone())
            .await;
        match event.kind {
            EventKind::RunAdmitted => plan.dispatcher.notify::<RunAdmitted>(projection).await,
            EventKind::RunStarted => plan.dispatcher.notify::<RunStarted>(projection).await,
            EventKind::TurnStarted => plan.dispatcher.notify::<TurnStarted>(projection).await,
            EventKind::TurnCompleted => plan.dispatcher.notify::<TurnCompleted>(projection).await,
            EventKind::RunSettled => plan.dispatcher.notify::<RunSettled>(projection).await,
            _ => {}
        }
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
    #[allow(clippy::too_many_arguments)]
    async fn model_turn(
        &self,
        execution: &dyn ExecutionStore,
        run_id: &RunId,
        session_id: &SessionId,
        snapshot: &TurnSnapshot,
        plan: &RunPlan,
        streamer: Arc<dyn Streamer>,
        lease_lost: &AtomicBool,
    ) -> Result<(Vec<PendingCall>, Usage), RuntimeError> {
        let request = ModelRequest {
            identity: snapshot.identity.clone(),
            selection: snapshot.selection.clone(),
            system: snapshot.system.clone(),
            messages: snapshot.messages.clone(),
            tools: snapshot.tools.clone(),
            temperature: None,
            max_tokens: None,
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
        let dispatched = plan.dispatcher.around::<ExtensionModelStream>(json!({"provider":snapshot.selection.provider_id,"model":snapshot.selection.model_id,"session_id":snapshot.identity.session_id,"run_id":snapshot.identity.run_id,"turn_id":snapshot.identity.turn_id,"message_count":snapshot.messages.len(),"temperature":null,"max_tokens":null,"tool_choice":null}),terminal).await;
        let provider_error = error_slot.lock().unwrap().take();
        if let Some(error) = provider_error {
            return Err(request_error(plan, error).await);
        }
        dispatched.map_err(|e| RuntimeError::Extension(e.to_string()))?;
        let mut stream = stream_slot.lock().unwrap().take().ok_or_else(|| {
            RuntimeError::Extension("model stream did not produce a stream".into())
        })?;
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
        let model_started = self.clock.now();
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
                "latency_ms": (self.clock.now() - model_started).whole_milliseconds()});
            self.observer.model_completed(&event);
        };
        let mut completed = false;
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
                    let Some(call) = calls.iter_mut().find(|call| call.id == call_id) else {
                        observe_model("error", &usage);
                        return Err(invalid_provider("tool arguments without call start").into());
                    };
                    call.raw.push_str(&text);
                }
                StreamDelta::ToolCallDone { call_id } => {
                    let Some(call) = calls.iter_mut().find(|call| call.id == call_id) else {
                        observe_model("error", &usage);
                        return Err(invalid_provider("tool completion without call start").into());
                    };
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
                StreamDelta::Completed => {
                    completed = true;
                    break;
                }
                StreamDelta::Error(error) => {
                    observe_model("error", &usage);
                    return Err(request_error(plan, error).await);
                }
            }
        }
        if !completed {
            observe_model("error", &usage);
            return Err(invalid_provider("model stream ended without completion").into());
        }
        if text.is_empty() && reasoning.is_empty() && calls.is_empty() && provider_state.is_empty()
        {
            observe_model("error", &usage);
            return Err(invalid_provider("empty model response").into());
        }
        observe_model("ok", &usage);
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
        ensure_lease(lease_lost)?;
        execution.append_message(message).await?;
        self.emit_durable(
            execution,
            session_id,
            run_id,
            plan,
            EventKind::MessageCommitted,
        )
        .await?;
        plan.dispatcher
            .notify::<ModelCompleted>(json!({"usage":usage}))
            .await;
        Ok((calls, usage))
    }

    #[allow(clippy::too_many_lines)]
    async fn execute_tool(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        plan: &RunPlan,
        call: PendingCall,
        lease_lost: &AtomicBool,
    ) -> Result<(), RuntimeError> {
        let definition = plan
            .tools
            .iter()
            .find(|tool| tool.info.name == call.name)
            .cloned();
        let raw = call.arguments.unwrap_or(Value::String(call.raw));
        let prepared = if let Some(tool) = &definition {
            match validate_arguments(&tool.info, &raw) {
                Ok(()) => {
                    let piped = self.tool_pipeline.prepare(&tool.info, raw.clone()).await;
                    match piped {
                        Ok(value) => match plan.dispatcher.transform::<ToolPrepare>(json!({"name":tool.info.name,"call_id":call.id.to_string(),"input":value})).await {
                            Ok(output) => { let input=output.get("input").cloned().unwrap_or(output); validate_arguments(&tool.info,&input).map(|()| input) },
                            Err(error) => Err(error.to_string()),
                        },
                        Err(error) => Err(error),
                    }
                }
                Err(error) => Err(error),
            }
        } else {
            Err(format!("unknown tool: {}", call.name))
        };
        ensure_lease(lease_lost)?;
        let tool_started_at = self.clock.now();
        let tool_name = call.name.clone();
        let mut pending = self.event(session_id, run_id, EventKind::ToolCallPending);
        pending.payload = json!({"call_id": call.id, "name": tool_name, "status": "pending"});
        pending.correlation = Some(call.id.to_string());
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
        plan.dispatcher
            .notify::<EventPublished>(serde_json::to_value(&pending).unwrap_or(Value::Null))
            .await;
        let mut running = self.event(session_id, run_id, EventKind::ToolCallRunning);
        running.payload = json!({"call_id": call.id, "name": tool_name, "status": "running"});
        running.correlation = Some(call.id.to_string());
        execution.claim_tool_call(&call.id, running.clone()).await?;
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
            .await?;
        ensure_lease(lease_lost)?;
        let outcome: Result<Value, String> = match (definition, prepared) {
            (Some(tool), Ok(arguments)) => {
                self.permit_and_execute(
                    execution, session_id, run_id, &call.id, plan, &tool, arguments, lease_lost,
                )
                .await?
            }
            (_, Err(error)) => Err(error),
            (None, Ok(_)) => Err("unknown tool".into()),
        };
        ensure_lease(lease_lost)?;
        let original_error = outcome.is_err();
        let seed = match outcome {
            Ok(value) => value,
            Err(error) => Value::String(error),
        };
        let outcome = match plan
            .dispatcher
            .transform::<ToolResultTransform>(json!({"result":seed,"is_error":original_error}))
            .await
        {
            Ok(output) => {
                let result = output
                    .get("result")
                    .cloned()
                    .unwrap_or_else(|| output.clone());
                if original_error || output.get("is_error").and_then(Value::as_bool) == Some(true) {
                    Err(result
                        .as_str()
                        .map_or_else(|| result.to_string(), str::to_owned))
                } else {
                    Ok(result)
                }
            }
            Err(error) => Err(error.to_string()),
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
        let mut settled = self.event(session_id, run_id, EventKind::ToolCallSettled);
        settled.payload = json!({"call_id": call.id, "name": tool_name,
            "status": status, "is_error": is_error,
            "tool": tool_name, "tool_id": call.id.to_string(),
            "duration_ms": (self.clock.now()-tool_started_at).whole_milliseconds()});
        settled.correlation = Some(call.id.to_string());
        execution
            .settle_tool_call(&call.id, result, message, settled.clone())
            .await?;
        self.observer.emit(&settled);
        let settled_projection = serde_json::to_value(&settled).unwrap_or(Value::Null);
        plan.dispatcher
            .notify::<EventPublished>(settled_projection.clone())
            .await;
        plan.dispatcher
            .notify::<crabber_extension::ToolSettled>(settled_projection)
            .await;
        Ok(())
    }

    #[allow(clippy::too_many_arguments, clippy::too_many_lines)]
    async fn permit_and_execute(
        &self,
        execution: &dyn ExecutionStore,
        session_id: &SessionId,
        run_id: &RunId,
        call_id: &ToolCallId,
        plan: &RunPlan,
        tool: &ToolDefinition,
        arguments: Value,
        lease_lost: &AtomicBool,
    ) -> Result<Result<Value, String>, RuntimeError> {
        let guard_denied = plan
            .guards
            .iter()
            .any(|guard| guard.check(&tool.info.name, &arguments) == GuardDecision::Deny);
        let restricted = plan
            .restrictions
            .iter()
            .any(|set| !set.iter().any(|name| name == &tool.info.name));
        let allowed = if guard_denied || restricted {
            false
        } else {
            match self.policy.decide(&tool.info, &arguments) {
                PermissionDecision::Allow => true,
                PermissionDecision::Deny => false,
                PermissionDecision::Ask => {
                    self.emit_durable(
                        execution,
                        session_id,
                        run_id,
                        plan,
                        EventKind::PermissionRequested,
                    )
                    .await?;
                    let approved = self.approver.approve(&tool.info, &arguments).await;
                    self.emit_durable(
                        execution,
                        session_id,
                        run_id,
                        plan,
                        EventKind::PermissionDecided,
                    )
                    .await?;
                    approved
                }
            }
        };
        if !allowed {
            return Ok(Err("permission denied".into()));
        }
        ensure_lease(lease_lost)?;
        execution
            .renew_lease(self.clock.now() + time::Duration::seconds(30))
            .await
            .map_err(|_| RuntimeError::LeaseLost)?;
        ensure_lease(lease_lost)?;
        let observer = Arc::clone(&self.observer);
        let session_for_progress = session_id.clone();
        let run_for_progress = run_id.clone();
        let clock = Arc::clone(&self.clock);
        let progress = Arc::new(move |content: ContentBlock| {
            let event = EventRecord {
                cursor: None,
                session_id: session_for_progress.clone(),
                run_id: run_for_progress.clone(),
                turn_id: None,
                kind: EventKind::Custom {
                    name: "tool_progress".into(),
                },
                payload: json!({"content":content}),
                correlation: None,
                live_only: true,
                created_at: clock.now(),
            };
            observer.emit(&event);
        });
        let context = ToolContext::new(
            session_id.clone(),
            run_id.clone(),
            call_id.clone(),
            CancellationToken::new(),
            self.host_services.clone(),
            progress,
            Some(Arc::new(HostApproval {
                approver: Arc::clone(&self.approver),
                tool: tool.info.clone(),
                arguments: arguments.clone(),
            })),
        );
        let executor = Arc::clone(&tool.executor);
        let authorized_arguments = arguments.clone();
        let terminal: Callback = Arc::new(move |input| {
            let executor = Arc::clone(&executor);
            let context = context.clone();
            let authorized_arguments = authorized_arguments.clone();
            Box::pin(async move {
                if input != authorized_arguments {
                    return Err(crabber_extension::ExtensionError::Tool(
                        "around handler changed immutable tool input".into(),
                    ));
                }
                executor
                    .execute_with_context(context, authorized_arguments)
                    .await
            })
        });
        let output = match plan
            .dispatcher
            .around::<ToolExecute>(arguments, terminal)
            .await
        {
            Ok(output) => output,
            Err(error) => return Ok(Err(error.to_string())),
        };
        ensure_lease(lease_lost)?;
        Ok(self
            .tool_pipeline
            .transform_result(&tool.info, output)
            .await)
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

fn ensure_lease(lost: &AtomicBool) -> Result<(), RuntimeError> {
    if lost.load(Ordering::SeqCst) {
        Err(RuntimeError::LeaseLost)
    } else {
        Ok(())
    }
}

fn validate_arguments(tool: &ToolInfo, arguments: &Value) -> Result<(), String> {
    let validator = jsonschema::validator_for(&tool.parameters)
        .map_err(|error| format!("invalid tool schema: {error}"))?;
    validator
        .validate(arguments)
        .map_err(|error| format!("invalid tool arguments: {error}"))
}

struct HostApproval {
    approver: Arc<dyn ApprovalRequester>,
    tool: ToolInfo,
    arguments: Value,
}
#[async_trait]
impl ApprovalFacade for HostApproval {
    async fn request(&self, _reason: &str) -> bool {
        self.approver.approve(&self.tool, &self.arguments).await
    }
}

async fn request_error(plan: &RunPlan, error: ProviderError) -> RuntimeError {
    let class = format!("{:?}", error.kind);
    let seed =
        json!({"class":class,"attempt":1,"retry":false,"delay_ms":0,"compaction_requested":false});
    match plan.dispatcher.transform::<ModelRequestError>(seed).await {
        Ok(decision)
            if decision.get("class").and_then(Value::as_str) != Some(class.as_str())
                || decision.get("attempt").and_then(Value::as_u64) != Some(1) =>
        {
            RuntimeError::Extension("request-error handler changed immutable metadata".into())
        }
        Ok(decision) if decision.get("retry").and_then(Value::as_bool) != Some(false) => {
            RuntimeError::Extension("request-error handler cannot enable retry".into())
        }
        Ok(decision) if decision.get("delay_ms").and_then(Value::as_u64) != Some(0) => {
            RuntimeError::Extension("request-error handler cannot lengthen delay".into())
        }
        Ok(decision)
            if decision
                .get("compaction_requested")
                .and_then(Value::as_bool)
                == Some(true)
                && error.kind != crabber_providers::ProviderErrorKind::ContextOverflow =>
        {
            RuntimeError::Extension("compaction is only valid for context overflow".into())
        }
        Ok(_) => RuntimeError::Provider(error),
        Err(handler) => RuntimeError::Extension(handler.to_string()),
    }
}
