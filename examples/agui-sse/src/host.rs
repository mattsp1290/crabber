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
    event::{BaseEvent, Event, RunErrorEvent},
    types::{Message, RunAgentInput},
};
use crabber_agui::{
    Completion, ProjectionConfig, ProjectionError, Projector, encode_sse, sse_frame_len,
};
use futures::Stream;
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc, Mutex,
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
    ingress_gate: Arc<Mutex<()>>,
    pub ingress: Arc<AtomicUsize>,
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
            ingress_gate: Arc::new(Mutex::new(())),
            ingress: Arc::new(AtomicUsize::new(0)),
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
        {
            let _gate = self
                .ingress_gate
                .lock()
                .unwrap_or_else(std::sync::PoisonError::into_inner);
            self.capacity.close();
            self.shutdown.cancel();
            self.tasks.close();
        }
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

// Registration and shutdown share a synchronous gate. A handler cannot register
// after shutdown's join observes an empty tracker. The owner survives HTTP drop.
async fn run(State(host): State<Host>, request: Request) -> Response {
    let expires = tokio::time::Instant::now() + host.deadline;
    let (reply, response) = oneshot::channel();
    {
        let _gate = host
            .ingress_gate
            .lock()
            .unwrap_or_else(std::sync::PoisonError::into_inner);
        if host.shutdown.is_cancelled() {
            return error(StatusCode::SERVICE_UNAVAILABLE, "host_shutdown");
        }
        let Ok(permit) = host.capacity.clone().try_acquire_owned() else {
            return error(StatusCode::SERVICE_UNAVAILABLE, "host_capacity");
        };
        host.ingress.fetch_add(1, Ordering::SeqCst);
        let owner = host.clone();
        host.tasks.spawn(async move {
            let _ingress = CounterGuard(owner.ingress.clone());
            run_owned(owner, request, expires, reply, permit).await;
        });
    }
    response
        .await
        .unwrap_or_else(|_| error(StatusCode::INTERNAL_SERVER_ERROR, "startup_failed"))
}

struct CounterGuard(Arc<AtomicUsize>);
impl Drop for CounterGuard {
    fn drop(&mut self) {
        self.0.fetch_sub(1, Ordering::SeqCst);
    }
}

fn respond(reply: &mut Option<oneshot::Sender<Response>>, response: Response) {
    if let Some(reply) = reply.take() {
        let _ = reply.send(response);
    }
}

async fn run_owned(
    host: Host,
    request: Request,
    expires: tokio::time::Instant,
    reply: oneshot::Sender<Response>,
    permit: OwnedSemaphorePermit,
) {
    let mut reply = Some(reply);
    let response = serve_owned(&host, request, expires, &mut reply, permit).await;
    respond(&mut reply, response);
}

async fn serve_owned(
    host: &Host,
    request: Request,
    expires: tokio::time::Instant,
    reply: &mut Option<oneshot::Sender<Response>>,
    permit: OwnedSemaphorePermit,
) -> Response {
    let bytes = tokio::select! {
        biased;
        () = host.shutdown.cancelled() => return error(StatusCode::SERVICE_UNAVAILABLE, "host_shutdown"),
        () = tokio::time::sleep_until(expires) => return error(StatusCode::REQUEST_TIMEOUT, "request_timeout"),
        () = reply.as_mut().expect("response pending").closed() => return error(StatusCode::SERVICE_UNAVAILABLE, "client_closed"),
        bytes = to_bytes(request.into_body(), 131_072) => match bytes {
            Ok(bytes) => bytes,
            Err(_) => return error(StatusCode::PAYLOAD_TOO_LARGE, "body_limit"),
        },
    };
    let Ok(value) = serde_json::from_slice(&bytes) else {
        return error(StatusCode::BAD_REQUEST, "invalid_json");
    };
    let Ok((input, text)) = validate(value) else {
        return error(StatusCode::UNPROCESSABLE_ENTITY, "unsupported_input");
    };
    if host.shutdown.is_cancelled() {
        return error(StatusCode::SERVICE_UNAVAILABLE, "host_shutdown");
    }
    if tokio::time::Instant::now() >= expires {
        return error(StatusCode::REQUEST_TIMEOUT, "request_timeout");
    }
    let Ok(agent) = (host.factory)() else {
        return error(StatusCode::INTERNAL_SERVER_ERROR, "startup_failed");
    };
    let result = {
        let admission = agent.prompt(Some(SessionId::from(input.thread_id.to_string())), text);
        tokio::pin!(admission);
        tokio::select! {
            result = &mut admission => result,
            () = host.shutdown.cancelled() => {
                respond(reply, error(StatusCode::SERVICE_UNAVAILABLE, "host_shutdown"));
                late_admission(host, admission.as_mut()).await
            },
            () = tokio::time::sleep_until(expires) => {
                respond(reply, error(StatusCode::REQUEST_TIMEOUT, "request_timeout"));
                late_admission(host, admission.as_mut()).await
            },
            () = reply.as_mut().expect("response pending").closed() => {
                reply.take();
                late_admission(host, admission.as_mut()).await
            },
        }
    };
    let handle = match result {
        Ok(handle) => handle,
        Err(RuntimeError::SessionBusy) => return error(StatusCode::CONFLICT, "thread_busy"),
        Err(_) => return error(StatusCode::INTERNAL_SERVER_ERROR, "startup_failed"),
    };
    admitted_response(host, input, agent, handle, expires, reply, permit).await
}

#[allow(clippy::too_many_arguments)] // Admission transfers agent and run ownership together.
async fn admitted_response(
    host: &Host,
    input: RunAgentInput,
    agent: Agent,
    handle: RunHandle,
    expires: tokio::time::Instant,
    reply: &mut Option<oneshot::Sender<Response>>,
    permit: OwnedSemaphorePermit,
) -> Response {
    if reply.is_none() || host.shutdown.is_cancelled() || tokio::time::Instant::now() >= expires {
        handle.interrupt();
        // Never drop a late handle, even when its HTTP response already timed out.
        let _ = late_admission(host, Box::pin(handle.done()).as_mut()).await;
        return error(StatusCode::REQUEST_TIMEOUT, "request_timeout");
    }
    let Ok(projector) = Projector::new(
        handle.session_id().clone(),
        handle.run_id().clone(),
        input.thread_id.to_string(),
        input.run_id.to_string(),
        host.config.clone(),
    ) else {
        handle.interrupt();
        // Admission has happened: retain tracked ownership and capacity while
        // reporting the configuration error immediately to HTTP.
        respond(
            reply,
            error(
                StatusCode::INTERNAL_SERVER_ERROR,
                "projection_configuration",
            ),
        );
        let _ = late_admission(host, Box::pin(handle.done()).as_mut()).await;
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
    worker_host.deadline = expires.saturating_duration_since(tokio::time::Instant::now());
    host.tasks.spawn(worker(
        worker_host,
        agent,
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

async fn late_admission<F: Future>(host: &Host, mut pending: Pin<&mut F>) -> F::Output {
    if let Ok(result) = tokio::time::timeout(host.cleanup, pending.as_mut()).await {
        return result;
    }
    host.unresolved.fetch_add(1, Ordering::SeqCst);
    let _unresolved = CounterGuard(host.unresolved.clone());
    pending.await
}

const OUTPUT_BYTES: usize = 2 * 1024 * 1024;
const CONTROL_BYTES: usize = 8 * 1024;

struct Frame {
    bytes: Vec<u8>,
    permit: OwnedSemaphorePermit,
}
impl Frame {
    fn into_bytes(self) -> Vec<u8> {
        drop(self.permit);
        self.bytes
    }
}

struct Output {
    receiver: mpsc::Receiver<Frame>,
    terminal: oneshot::Receiver<Result<Vec<Frame>, ProjectionError>>,
    terminal_frames: std::vec::IntoIter<Frame>,
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
                Poll::Ready(Some(frame)) => {
                    return Poll::Ready(Some(Ok(frame.into_bytes().into())));
                }
                Poll::Ready(None) => self.data_closed = true,
                Poll::Pending => return Poll::Pending,
            }
        }
        if let Some(frame) = self.terminal_frames.next() {
            return Poll::Ready(Some(Ok(frame.into_bytes().into())));
        }
        if self.terminal_closed {
            return Poll::Ready(None);
        }
        match Pin::new(&mut self.terminal).poll(cx) {
            Poll::Ready(Ok(Ok(frames))) => {
                self.terminal_closed = true;
                self.terminal_frames = frames.into_iter();
                Poll::Ready(
                    self.terminal_frames
                        .next()
                        .map(|frame| Ok(frame.into_bytes().into())),
                )
            }
            Poll::Ready(Ok(Err(_)) | Err(_)) => {
                self.terminal_closed = true;
                Poll::Ready(Some(Err(std::io::Error::other(
                    "terminal projection failed",
                ))))
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
    projector.push_with_delivery(record, |events| {
        let slots = sender
            .try_reserve_many(events.len())
            .map_err(|_| ProjectionError::Transport)?;
        let mut reserved = Vec::with_capacity(events.len());
        for event in events {
            let count =
                u32::try_from(sse_frame_len(event, limit)?).map_err(|_| ProjectionError::Limit)?;
            let permit = budget
                .clone()
                .try_acquire_many_owned(count)
                .map_err(|_| ProjectionError::Transport)?;
            let bytes = encode_sse(event, limit)?;
            reserved.push(Frame { bytes, permit });
        }
        for (slot, frame) in slots.zip(reserved) {
            slot.send(frame);
        }
        Ok(())
    })?;
    Ok(())
}

#[allow(clippy::too_many_arguments)] // The worker exclusively owns runtime and delivery lifecycle.
async fn worker(
    host: Host,
    agent: Agent,
    mut handle: RunHandle,
    mut projector: Projector,
    sender: mpsc::Sender<Frame>,
    terminal: oneshot::Sender<Result<Vec<Frame>, ProjectionError>>,
    disconnect: CancellationToken,
    _capacity: OwnedSemaphorePermit,
) {
    let mut receiver = handle.events();
    let budget = Arc::new(Semaphore::new(OUTPUT_BYTES - CONTROL_BYTES));
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
        .and_then(|batch| control_frames(&batch, host.config.max_event_bytes));
    // Keep the event publisher alive until the run receiver and completion are drained.
    drop(agent);
    let _ = terminal.send(final_frames);
    drop(sender);
    if unresolved {
        host.unresolved.fetch_sub(1, Ordering::SeqCst);
    }
    host.active.fetch_sub(1, Ordering::SeqCst);
}

fn control_frames(events: &[Event], limit: usize) -> Result<Vec<Frame>, ProjectionError> {
    let fits = events.iter().try_fold(0usize, |sum, event| {
        sum.checked_add(sse_frame_len(event, limit)?)
            .filter(|total| *total <= CONTROL_BYTES)
            .ok_or(ProjectionError::Limit)
    });
    let fallback = [Event::RunError(RunErrorEvent {
        base: BaseEvent {
            timestamp: None,
            raw_event: None,
            metadata: None,
            subagent_run_id: None,
        },
        message: "The live run could not be projected.".into(),
        code: Some(ProjectionError::Limit.code().into()),
        usage: None,
    })];
    let events = if fits.is_ok() { events } else { &fallback };
    let budget = Arc::new(Semaphore::new(CONTROL_BYTES));
    frames(events, limit)?
        .into_iter()
        .map(|bytes| {
            let count = u32::try_from(bytes.len()).map_err(|_| ProjectionError::Limit)?;
            let permit = budget
                .clone()
                .try_acquire_many_owned(count)
                .map_err(|_| ProjectionError::Limit)?;
            Ok(Frame { bytes, permit })
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    fn output() -> (
        Output,
        mpsc::Sender<Frame>,
        oneshot::Sender<Result<Vec<Frame>, ProjectionError>>,
    ) {
        let (sender, receiver) = mpsc::channel(32);
        let (terminal_sender, terminal) = oneshot::channel();
        (
            Output {
                receiver,
                terminal,
                terminal_frames: Vec::new().into_iter(),
                disconnect: CancellationToken::new(),
                data_closed: false,
                terminal_closed: false,
            },
            sender,
            terminal_sender,
        )
    }

    #[tokio::test]
    async fn buffered_sse_is_drained_before_terminal_in_both_channel_orderings() {
        use futures::StreamExt as _;
        for terminal_first in [false, true] {
            let (mut output, sender, terminal) = output();
            let events: Vec<Event> = [
                json!({"type":"RUN_STARTED", "threadId":"t", "runId":"r"}),
                json!({"type":"TEXT_MESSAGE_START", "messageId":"m", "role":"assistant"}),
                json!({"type":"TEXT_MESSAGE_CONTENT", "messageId":"m", "delta":"buffered"}),
                json!({"type":"TEXT_MESSAGE_END", "messageId":"m"}),
            ]
            .into_iter()
            .map(|value| serde_json::from_value(value).unwrap())
            .collect();
            for frame in control_frames(&events, 1_048_576).unwrap() {
                sender
                    .try_send(frame)
                    .unwrap_or_else(|_| panic!("data queue full"));
            }
            let finished = serde_json::from_value(json!({"type":"RUN_FINISHED", "threadId":"t", "runId":"r", "outcome":{"type":"success"}})).unwrap();
            let final_frames = control_frames(&[finished], 1_048_576).unwrap();
            let mut raw = Vec::new();
            if terminal_first {
                assert!(terminal.send(Ok(final_frames)).is_ok());
                for _ in &events {
                    raw.extend(output.next().await.unwrap().unwrap());
                }
                // A ready terminal cannot overtake an open data channel.
                assert!(futures::poll!(output.next()).is_pending());
                drop(sender);
            } else {
                drop(sender);
                for _ in &events {
                    raw.extend(output.next().await.unwrap().unwrap());
                }
                // Data EOF cannot terminate the stream before the terminal arrives.
                assert!(futures::poll!(output.next()).is_pending());
                assert!(terminal.send(Ok(final_frames)).is_ok());
            }
            raw.extend(output.next().await.unwrap().unwrap());
            assert!(output.next().await.is_none());
            let mut decoder = crate::check::Decoder::default();
            decoder.push(&raw).unwrap();
            let decoded = decoder.finish().unwrap();
            assert_eq!(&decoded[..events.len()], events);
            assert!(matches!(decoded.last(), Some(Event::RunFinished(_))));
        }
    }

    #[tokio::test]
    async fn terminal_failure_or_sender_loss_is_a_body_error_after_buffered_data() {
        use futures::StreamExt as _;
        for sender_lost in [false, true] {
            let (mut output, sender, terminal) = output();
            let event =
                serde_json::from_value(json!({"type":"RUN_STARTED", "threadId":"t", "runId":"r"}))
                    .unwrap();
            sender
                .try_send(control_frames(&[event], 1_048_576).unwrap().remove(0))
                .unwrap_or_else(|_| panic!("data queue full"));
            if sender_lost {
                drop(terminal);
            } else {
                // Even the bounded fallback cannot fit this invalid event limit.
                let event = serde_json::from_value(
                    json!({"type":"RUN_FINISHED", "threadId":"t", "runId":"r"}),
                )
                .unwrap();
                let error = control_frames(&[event], 0);
                assert!(error.is_err());
                assert!(terminal.send(error).is_ok());
            }
            drop(sender);
            assert!(output.next().await.unwrap().is_ok());
            assert!(output.next().await.unwrap().is_err());
            assert!(output.next().await.is_none());
        }
    }

    #[test]
    fn large_terminal_closures_use_bounded_control_error() {
        let events: Vec<Event> = (0..3).map(|n| {
            serde_json::from_value(json!({"type":"TOOL_CALL_END", "toolCallId":format!("{n}{}", "x".repeat(900_000))})).unwrap()
        }).collect();
        assert!(
            events
                .iter()
                .map(|e| sse_frame_len(e, 1_048_576).unwrap())
                .sum::<usize>()
                > OUTPUT_BYTES
        );
        let control = control_frames(&events, 1_048_576).unwrap();
        assert_eq!(control.len(), 1);
        assert!(control[0].bytes.len() <= CONTROL_BYTES);
        let event: Event =
            serde_json::from_slice(&control[0].bytes[6..control[0].bytes.len() - 2]).unwrap();
        assert!(matches!(event, Event::RunError(_)));
        let data = Arc::new(Semaphore::new(OUTPUT_BYTES - CONTROL_BYTES));
        let held = data
            .clone()
            .try_acquire_many_owned(u32::try_from(OUTPUT_BYTES - CONTROL_BYTES).unwrap())
            .unwrap();
        assert_eq!(data.available_permits(), 0);
        assert_eq!(
            held.num_permits()
                + control
                    .iter()
                    .map(|frame| frame.permit.num_permits())
                    .sum::<usize>(),
            OUTPUT_BYTES - CONTROL_BYTES + control[0].bytes.len()
        );
    }
}
