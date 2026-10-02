use axum::{
    Router,
    body::{Body, Bytes, to_bytes},
    extract::{Request, State},
    http::StatusCode,
    response::{IntoResponse, Response},
    routing::post,
};
use crabber::{
    Agent, RunHandle, RuntimeError,
    core::{RunStatus, SessionId},
};
use crabber_agui::ag_ui_core::{
    event::Event,
    types::{Message, RunAgentInput},
};
use crabber_agui::{Completion, ProjectionConfig, ProjectionError, Projector, encode_sse};
use futures::Stream;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore, mpsc, oneshot};
use tokio_util::{sync::CancellationToken, task::TaskTracker};

pub type AgentFactory = Arc<dyn Fn() -> Result<Agent, crabber::BuildError> + Send + Sync>;

/// Host policy lives here, outside the reusable adapter.
#[derive(Clone)]
pub struct Host {
    factory: AgentFactory,
    capacity: Arc<Semaphore>,
    shutdown: CancellationToken,
    tasks: TaskTracker,
    pub active: Arc<AtomicUsize>,
    pub unresolved: Arc<AtomicUsize>,
    pub faults: Arc<AtomicUsize>,
    pub config: ProjectionConfig,
    pub deadline: Duration,
    pub cleanup: Duration,
}
impl Host {
    #[must_use]
    pub fn new(factory: AgentFactory) -> Self {
        Self {
            factory,
            capacity: Arc::new(Semaphore::new(8)),
            shutdown: CancellationToken::new(),
            tasks: TaskTracker::new(),
            active: Arc::new(AtomicUsize::new(0)),
            unresolved: Arc::new(AtomicUsize::new(0)),
            faults: Arc::new(AtomicUsize::new(0)),
            config: ProjectionConfig::default(),
            deadline: Duration::from_secs(30),
            cleanup: Duration::from_secs(5),
        }
    }

    pub fn router(&self) -> Router {
        Router::new()
            .route("/run", post(run))
            .with_state(self.clone())
    }
    /// Cancel and join workers. An expired join deadline reports remaining work.
    ///
    /// # Errors
    /// Returns an error while unresolved tasks remain registered and supervised.
    pub async fn shutdown(&self) -> Result<(), &'static str> {
        self.shutdown.cancel();
        self.tasks.close();
        tokio::time::timeout(self.cleanup, self.tasks.wait())
            .await
            .map_err(|_| "host workers remain unresolved")
    }
}

fn error(status: StatusCode, code: &str) -> Response {
    (status, axum::Json(json!({"error":{"code":code}}))).into_response()
}
fn valid_id(id: &str) -> bool {
    !id.is_empty() && id.len() <= 256 && !id.chars().any(char::is_control)
}
fn empty(value: &Value) -> bool {
    value.is_null() || value.as_object().is_some_and(serde_json::Map::is_empty)
}

fn validate(value: Value) -> Result<(RunAgentInput, String), ()> {
    let object = value.as_object().ok_or(())?;
    if object.keys().any(|k| {
        ![
            "threadId",
            "runId",
            "messages",
            "tools",
            "context",
            "state",
            "forwardedProps",
            "protocolVersion",
        ]
        .contains(&k.as_str())
    }) {
        return Err(());
    }
    let input: RunAgentInput = serde_json::from_value(value).map_err(|_| ())?;
    if !valid_id(input.thread_id.as_str())
        || !valid_id(input.run_id.as_str())
        || input.messages.len() != 1
        || input.tools.as_ref().is_some_and(|v| !v.is_empty())
        || input.context.as_ref().is_some_and(|v| !v.is_empty())
        || !empty(&input.state)
        || !empty(&input.forwarded_props)
        || input
            .protocol_version
            .as_deref()
            .is_some_and(|v| v != "1.0")
    {
        return Err(());
    }
    let Message::User { content, .. } = &input.messages[0] else {
        return Err(());
    };
    let text = content.as_text().ok_or(())?;
    if text.len() > 65_536 {
        return Err(());
    }
    Ok((input.clone(), text.to_owned()))
}

async fn run(State(host): State<Host>, request: Request) -> Response {
    let request_started = tokio::time::Instant::now();
    if host.shutdown.is_cancelled() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_shutdown");
    }
    let bytes =
        match tokio::time::timeout(host.deadline, to_bytes(request.into_body(), 131_072)).await {
            Ok(Ok(bytes)) => bytes,
            Ok(Err(_)) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body_limit"),
            Err(_) => return error(StatusCode::REQUEST_TIMEOUT, "request_timeout"),
        };
    let Ok(value) = serde_json::from_slice(&bytes) else {
        return error(StatusCode::BAD_REQUEST, "invalid_json");
    };
    let Ok((input, text)) = validate(value) else {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "unsupported_input");
    };
    let Ok(permit) = host.capacity.clone().try_acquire_owned() else {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_capacity");
    };
    let Ok(agent) = (host.factory)() else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "startup_failed");
    };
    let handle = match agent
        .prompt(Some(SessionId::from(input.thread_id.to_string())), text)
        .await
    {
        Ok(handle) => handle,
        Err(RuntimeError::SessionBusy) => return error(StatusCode::CONFLICT, "thread_busy"),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "startup_failed"),
    };
    let Ok(projector) = Projector::new(
        handle.session_id().clone(),
        handle.run_id().clone(),
        input.thread_id.to_string(),
        input.run_id.to_string(),
        host.config.clone(),
    ) else {
        handle.interrupt();
        // Admission has happened: keep responsibility for completion even on host config error.
        host.tasks.spawn(async move {
            let _permit = permit;
            let _ = handle.done().await;
        });
        return error(
            StatusCode::INTERNAL_SERVER_ERROR,
            "projection_configuration",
        );
    };
    let (sender, receiver) = mpsc::channel(32);
    let (terminal_sender, terminal) = oneshot::channel();
    let disconnect = CancellationToken::new();
    let output = Output {
        receiver,
        terminal,
        terminal_frames: Vec::new().into_iter(),
        disconnect: disconnect.clone(),
        data_closed: false,
        terminal_closed: false,
    };
    host.active.fetch_add(1, Ordering::SeqCst);
    let mut worker_host = host.clone();
    worker_host.deadline = host.deadline.saturating_sub(request_started.elapsed());
    host.tasks.spawn(worker(
        worker_host,
        handle,
        projector,
        sender,
        terminal_sender,
        disconnect,
        permit,
    ));
    (
        [
            ("content-type", "text/event-stream"),
            ("cache-control", "no-cache"),
        ],
        Body::from_stream(output),
    )
        .into_response()
}

struct Frame {
    bytes: Vec<u8>,
    _permit: OwnedSemaphorePermit,
}
struct Output {
    receiver: mpsc::Receiver<Frame>,
    terminal: oneshot::Receiver<Vec<Vec<u8>>>,
    terminal_frames: std::vec::IntoIter<Vec<u8>>,
    disconnect: CancellationToken,
    data_closed: bool,
    terminal_closed: bool,
}
impl Drop for Output {
    fn drop(&mut self) {
        self.disconnect.cancel();
    }
}
impl Stream for Output {
    type Item = Result<Bytes, std::io::Error>;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.data_closed {
            match self.receiver.poll_recv(cx) {
                Poll::Ready(Some(frame)) => return Poll::Ready(Some(Ok(frame.bytes.into()))),
                Poll::Ready(None) => self.data_closed = true,
                Poll::Pending => return Poll::Pending,
            }
        }
        if let Some(frame) = self.terminal_frames.next() {
            return Poll::Ready(Some(Ok(frame.into())));
        }
        if self.terminal_closed {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.terminal).poll(cx) {
            Poll::Ready(Ok(frames)) => {
                self.terminal_closed = true;
                self.terminal_frames = frames.into_iter();
                Poll::Ready(self.terminal_frames.next().map(|frame| Ok(frame.into())))
            }
            Poll::Ready(Err(_)) => {
                self.terminal_closed = true;
                Poll::Ready(None)
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

fn frames(events: &[Event], limit: usize) -> Result<Vec<Vec<u8>>, ProjectionError> {
    events.iter().map(|e| encode_sse(e, limit)).collect()
}

fn enqueue(
    projector: &mut Projector,
    record: &crabber::EventRecord,
    sender: &mpsc::Sender<Frame>,
    budget: &Arc<Semaphore>,
    limit: usize,
) -> Result<(), ProjectionError> {
    let mut candidate = projector.clone();
    let batch = frames(&candidate.push(record)?, limit)?;
    let slots = sender
        .try_reserve_many(batch.len())
        .map_err(|_| ProjectionError::Transport)?;
    let mut reserved = Vec::with_capacity(batch.len());
    for bytes in batch {
        let count = u32::try_from(bytes.len()).map_err(|_| ProjectionError::Limit)?;
        let permit = budget
            .clone()
            .try_acquire_many_owned(count)
            .map_err(|_| ProjectionError::Transport)?;
        reserved.push(Frame {
            bytes,
            _permit: permit,
        });
    }
    for (slot, frame) in slots.zip(reserved) {
        slot.send(frame);
    }
    *projector = candidate;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // The worker exclusively owns runtime and delivery lifecycle.
async fn worker(
    host: Host,
    mut handle: RunHandle,
    mut projector: Projector,
    sender: mpsc::Sender<Frame>,
    terminal: oneshot::Sender<Vec<Vec<u8>>>,
    disconnect: CancellationToken,
    _capacity: OwnedSemaphorePermit,
) {
    let mut receiver = handle.events();
    let budget = Arc::new(Semaphore::new(2 * 1024 * 1024));
    let deadline = tokio::time::sleep(host.deadline);
    tokio::pin!(deadline);
    let mut failed = false;
    let mut cleanup_deadline = None;
    let mut unresolved = false;
    loop {
        let next_event = if failed {
            if let Some(until) = cleanup_deadline {
                tokio::select! {
                    result = receiver.recv() => Some(result),
                    () = tokio::time::sleep_until(until) => {
                        unresolved = true; host.unresolved.fetch_add(1, Ordering::SeqCst);
                        eprintln!("run {} remains running after cleanup deadline", handle.run_id());
                        cleanup_deadline = None; None
                    }
                }
            } else {
                Some(receiver.recv().await)
            }
        } else {
            tokio::select! {
                result = receiver.recv() => Some(result),
                () = disconnect.cancelled() => None,
                () = host.shutdown.cancelled() => None,
                () = &mut deadline => None,
            }
        };
        match next_event {
            Some(Ok(None)) => break,
            Some(Ok(Some(record))) if !failed => {
                // Commit projection state only after reserving the entire delivery batch.
                // This prevents a rejected start batch from producing an orphaned closing event.
                let result = enqueue(
                    &mut projector,
                    &record,
                    &sender,
                    &budget,
                    host.config.max_event_bytes,
                );
                if let Err(error) = result {
                    projector.fail(error);
                    failed = true;
                }
            }
            Some(Err(_)) => {
                projector.fail(ProjectionError::Lagged);
                failed = true;
            }
            None if !failed => {
                projector.fail(ProjectionError::Transport);
                failed = true;
            }
            Some(Ok(Some(_))) | None => {}
        }
        if failed && cleanup_deadline.is_none() && !unresolved {
            host.faults.fetch_add(1, Ordering::SeqCst);
            handle.interrupt();
            cleanup_deadline = Some(tokio::time::Instant::now() + host.cleanup);
        }
    }
    // Receiver completion watches the runtime task; consuming done here preserves interrupt ownership.
    let completion = match handle.done().await {
        Ok(result) => match result.status {
            RunStatus::Completed => Completion::Completed,
            RunStatus::Interrupted => Completion::Cancelled,
            RunStatus::Paused => Completion::Paused,
            _ => Completion::Failed,
        },
        Err(RuntimeError::LeaseLost) => Completion::LeaseLost,
        Err(_) => Completion::Failed,
    };
    let final_frames = projector
        .finish(completion)
        .and_then(|batch| frames(&batch, host.config.max_event_bytes));
    drop(sender);
    if let Ok(frames) = final_frames {
        let _ = terminal.send(frames);
    }
    if unresolved {
        host.unresolved.fetch_sub(1, Ordering::SeqCst);
    }
    host.active.fetch_sub(1, Ordering::SeqCst);
}
