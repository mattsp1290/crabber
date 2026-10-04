use agui_sse::{
    check::{self, CheckResult, Server, builder, factory, input, journey},
    host::Host,
};
use async_trait::async_trait;
use crabber::{
    FakeProvider, PermissionDecision, StaticPolicy, StreamDelta,
    core::SessionId,
    providers::{
        DeltaStream, ModelRequest, ProviderError, ProviderErrorKind, Resolver, Selection, Streamer,
    },
    session::{MemoryStore, Store},
};
use crabber_agui::ag_ui_core::event::Event;
use futures::StreamExt;
use serde_json::{Value, json};
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};
use tokio::sync::Notify;

fn text(s: &str) -> Vec<StreamDelta> {
    vec![StreamDelta::TextDelta(s.into()), StreamDelta::Completed]
}
fn failure(kind: ProviderErrorKind, retryable: bool) -> StreamDelta {
    StreamDelta::Error(ProviderError {
        kind,
        retryable,
        message: "SECRET_BACKEND".into(),
    })
}

#[tokio::test]
async fn pinned_client_and_fragmented_unicode() -> CheckResult {
    check::run().await
}

#[tokio::test]
async fn real_http_failures_retries_and_implicit_argument_end() -> CheckResult {
    let mut omitted = journey(false);
    omitted[0].retain(|d| !matches!(d, StreamDelta::ToolCallDone { .. }));
    for (scripts, expected) in [
        (vec![text("text only")], "RUN_FINISHED"),
        (
            vec![vec![failure(ProviderErrorKind::Server, false)]],
            "RUN_ERROR",
        ),
        (
            vec![vec![
                StreamDelta::TextDelta("partial".into()),
                failure(ProviderErrorKind::Server, false),
            ]],
            "RUN_ERROR",
        ),
        (
            vec![
                vec![
                    StreamDelta::TextDelta("partial".into()),
                    failure(ProviderErrorKind::Server, true),
                ],
                text("retry"),
            ],
            "RUN_ERROR",
        ),
        (
            vec![
                vec![
                    StreamDelta::TextDelta("partial".into()),
                    failure(ProviderErrorKind::ContextOverflow, false),
                ],
                text("summary"),
                text("retry"),
            ],
            "RUN_ERROR",
        ),
        (omitted, "RUN_FINISHED"),
    ] {
        let server =
            Server::start(Host::new(factory(Arc::new(MemoryStore::new()), scripts))).await?;
        let events = check::request(&server, "scenario").await?;
        assert_eq!(
            serde_json::to_value(events.last().unwrap())?["type"],
            expected
        );
        assert!(!serde_json::to_string(&events)?.contains("SECRET_BACKEND"));
        server.stop().await?;
    }
    Ok(())
}

struct Pause;
impl crabber::runtime::PermissionPolicy for Pause {
    fn decide(&self, _: &crabber::core::ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(
        &self,
        _: &crabber::core::ToolInfo,
        _: &Value,
    ) -> crabber::runtime::InterruptPolicy {
        crabber::runtime::InterruptPolicy::Pause
    }
}

#[tokio::test]
async fn permission_failure_is_successful_run_and_pause_is_interrupt() -> CheckResult {
    for pause in [false, true] {
        let store = Arc::new(MemoryStore::new());
        let host = Host::new(Arc::new(move || {
            builder(
                store.clone(),
                Arc::new(FakeProvider::scripted(journey(false))),
            )
            .policy(if pause {
                Arc::new(Pause)
            } else {
                Arc::new(StaticPolicy::new(PermissionDecision::Deny))
            })
            .build()
        }));
        let server = Server::start(host).await?;
        let events = check::request(&server, "policy").await?;
        let wire = serde_json::to_value(events.last().unwrap())?;
        assert_eq!(
            wire["outcome"]["type"],
            if pause { "interrupt" } else { "success" }
        );
        if !pause {
            for event in events {
                if let Event::ToolCallResult(result) = event {
                    let content: Value = serde_json::from_str(result.content.as_text().unwrap())?;
                    assert_eq!(content["is_error"], true);
                }
            }
        }
        server.stop().await?;
    }
    Ok(())
}

#[tokio::test]
async fn input_body_and_output_limits() -> CheckResult {
    let mut host = Host::new(factory(
        Arc::new(MemoryStore::new()),
        vec![text("large text")],
    ));
    host.config.max_text_bytes = 3;
    let server = Server::start(host).await?;
    let client = reqwest::Client::new();
    for patch in [
        json!({"unknown":true}),
        json!({"tools":[{"name":"client","description":"bad","parameters":{}}]}),
        json!({"state":{"private":true}}),
        json!({"protocolVersion":"2.0"}),
        json!({"parentRunId":"parent"}),
        json!({"resume":[]}),
        json!({"runId":"\n"}),
        json!({"threadId":""}),
        json!({"messages":[{"id":"a","role":"assistant","content":"bad"}]}),
        json!({"messages":[{"id":"a","role":"user","content":[{"type":"text","text":"bad"}]}]}),
        json!({"messages":[{"id":"a","role":"user","content":"a"},{"id":"b","role":"user","content":"b"}]}),
    ] {
        let mut request = input("invalid");
        request
            .as_object_mut()
            .unwrap()
            .extend(patch.as_object().unwrap().clone());
        assert_eq!(
            client
                .post(&server.url)
                .json(&request)
                .send()
                .await?
                .status(),
            422
        );
    }
    assert_eq!(
        client
            .post(&server.url)
            .body("{".repeat(131_073))
            .send()
            .await?
            .status(),
        413
    );
    assert_eq!(
        client.post(&server.url).body("{").send().await?.status(),
        400
    );
    let events = check::request(&server, "limit").await?;
    assert!(matches!(events.last(), Some(Event::RunError(_))));
    server.stop().await?;
    Ok(())
}

struct Controlled {
    entered: Notify,
    release: Arc<Notify>,
    requests: AtomicUsize,
    burst: bool,
    args: bool,
}
struct ControlledResolver(Arc<Controlled>);
#[async_trait]
impl Resolver for ControlledResolver {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(self.0.clone())
    }
}
#[async_trait]
impl Streamer for Controlled {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        self.requests.fetch_add(1, Ordering::SeqCst);
        self.entered.notify_one();
        let release = self.release.clone();
        let burst = self.burst;
        let args = self.args;
        let initial = if args {
            vec![
                StreamDelta::TextDelta("partial".into()),
                StreamDelta::ToolCallStart {
                    call_id: "call".into(),
                    name: "echo".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: "call".into(),
                    text: "{".into(),
                },
            ]
        } else {
            vec![StreamDelta::TextDelta("partial".into())]
        };
        Ok(Box::pin(
            futures::stream::iter(initial)
                .chain(futures::stream::once(async move {
                    release.notified().await;
                    StreamDelta::TextDelta("released".into())
                }))
                .chain(futures::stream::iter(if burst {
                    (0..600)
                        .map(|_| StreamDelta::TextDelta("burst".into()))
                        .chain(std::iter::once(StreamDelta::Completed))
                        .collect()
                } else {
                    vec![StreamDelta::Completed]
                })),
        ))
    }
}
fn controlled(args: bool, burst: bool) -> Arc<Controlled> {
    Arc::new(Controlled {
        entered: Notify::new(),
        release: Arc::new(Notify::new()),
        requests: AtomicUsize::new(0),
        args,
        burst,
    })
}
fn controlled_host(store: Arc<MemoryStore>, provider: Arc<Controlled>) -> Host {
    Host::new(Arc::new(move || {
        builder(
            store.clone(),
            Arc::new(ControlledResolver(provider.clone())),
        )
        .build()
    }))
}
async fn idle(host: &Host) {
    tokio::time::timeout(Duration::from_secs(5), async {
        while host.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
}

#[tokio::test]
async fn busy_disconnect_and_shutdown_cancel_owned_run_without_restarting() -> CheckResult {
    let store = Arc::new(MemoryStore::new());
    let provider = controlled(true, false);
    let server = Server::start(controlled_host(store.clone(), provider.clone())).await?;
    let client = reqwest::Client::new();
    let response = client.post(&server.url).json(&input("busy")).send().await?;
    provider.entered.notified().await;
    assert_eq!(
        client
            .post(&server.url)
            .json(&input("busy"))
            .send()
            .await?
            .status(),
        409
    );
    drop(response);
    idle(&server.host).await;
    assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
    assert_eq!(
        store.list_unfinished_runs().await?,
        [] as [crabber::core::Run; 0]
    );
    let events = store
        .list_events(&SessionId::from("busy"), None, 100)
        .await?;
    assert!(
        events
            .iter()
            .any(|e| e.kind == crabber::core::EventKind::RunSettled
                && e.payload["status"] == "interrupted")
    );
    let response = client
        .post(&server.url)
        .json(&input("shutdown"))
        .send()
        .await?;
    provider.entered.notified().await;
    server.host.shutdown().await?;
    drop(response);
    assert_eq!(provider.requests.load(Ordering::SeqCst), 2);
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn deadline_closes_argument_boundaries_before_error() -> CheckResult {
    let provider = controlled(true, false);
    let mut host = controlled_host(Arc::new(MemoryStore::new()), provider.clone());
    host.deadline = Duration::from_millis(50);
    let server = Server::start(host).await?;
    let events = check::request(&server, "deadline").await?;
    assert!(events.iter().any(|e| matches!(e, Event::ToolCallEnd(_))));
    assert!(events.iter().any(|e| matches!(e, Event::TextMessageEnd(_))));
    assert!(matches!(events.last(), Some(Event::RunError(_))));
    assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn isolated_concurrent_threads() -> CheckResult {
    let server = Server::start(Host::new(factory(
        Arc::new(MemoryStore::new()),
        journey(true),
    )))
    .await?;
    let (a, b) = tokio::join!(check::request(&server, "a"), check::request(&server, "b"));
    for events in [a?, b?] {
        assert!(matches!(events.last(), Some(Event::RunFinished(_))));
    }
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn active_capacity_is_bounded_before_admission() -> CheckResult {
    let provider = controlled(false, false);
    let server = Server::start(controlled_host(
        Arc::new(MemoryStore::new()),
        provider.clone(),
    ))
    .await?;
    let client = reqwest::Client::new();
    let mut responses = Vec::new();
    for n in 0..8 {
        responses.push(
            client
                .post(&server.url)
                .json(&input(&format!("thread-{n}")))
                .send()
                .await?,
        );
    }
    assert_eq!(
        client
            .post(&server.url)
            .json(&input("ninth"))
            .send()
            .await?
            .status(),
        503
    );
    assert_eq!(provider.requests.load(Ordering::SeqCst), 8);
    drop(responses);
    idle(&server.host).await;
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn burst_lag_faults_and_slow_reader_does_not_block_runtime() -> CheckResult {
    let store = Arc::new(MemoryStore::new());
    let provider = controlled(false, true);
    let server = Server::start(controlled_host(store.clone(), provider.clone())).await?;
    let response = reqwest::Client::new()
        .post(&server.url)
        .json(&input("slow"))
        .send()
        .await?;
    provider.entered.notified().await;
    // No body reads until the runtime/worker completes. Release a synchronous burst
    // larger than the facade broadcast capacity; this deterministically loses events.
    provider.release.notify_one();
    idle(&server.host).await;
    assert_eq!(
        store.list_unfinished_runs().await?,
        [] as [crabber::core::Run; 0]
    );
    assert_eq!(provider.requests.load(Ordering::SeqCst), 1);
    assert!(server.host.faults.load(Ordering::SeqCst) > 0);
    let mut decoder = check::Decoder::default();
    for byte in response.bytes().await? {
        decoder.push(&[byte])?;
    }
    assert!(matches!(decoder.finish()?.last(), Some(Event::RunError(_))));
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn opt_in_reasoning_is_accepted_by_pinned_handler() -> CheckResult {
    use ag_ui_client::{Agent as _, HttpAgent, RunAgentParams};
    let mut host = Host::new(factory(
        Arc::new(MemoryStore::new()),
        vec![vec![
            StreamDelta::ReasoningDelta("displayed thought".into()),
            StreamDelta::Completed,
        ]],
    ));
    host.config.reasoning = true;
    let server = Server::start(host).await?;
    let client = HttpAgent::builder().with_url_str(&server.url)?.build()?;
    let result = client
        .run_agent(&RunAgentParams::new().user("hello"), ())
        .await?;
    assert!(result.new_messages.iter().any(|m| matches!(m, crabber_agui::ag_ui_core::types::Message::Reasoning { content, .. } if content == "displayed thought")));
    server.stop().await?;
    Ok(())
}

struct HeldResult {
    count: Arc<AtomicUsize>,
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl crabber::extension::Extension for HeldResult {
    fn id(&self) -> &'static str {
        "held-result"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "held".into()
    }
    async fn install(
        &self,
        r: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        let entered = self.entered.clone();
        let count = self.count.clone();
        let release = self.release.clone();
        r.on_result_transform(
            10,
            "hold",
            Arc::new(move |_, value| {
                let entered = entered.clone();
                let count = count.clone();
                let release = release.clone();
                Box::pin(async move {
                    count.fetch_add(1, Ordering::SeqCst);
                    entered.notify_one();
                    release.notified().await;
                    Ok(crabber::extension::TransformOutput::new(value))
                })
            }),
        );
        Ok(())
    }
}

#[tokio::test]
async fn noncooperative_transform_is_dropped_on_interrupt() -> CheckResult {
    // The old unresolved-work characterization required result callbacks to ignore
    // cancellation. The result-chain driver now drops that future on disconnect.
    let entered = Arc::new(Notify::new());
    let release = Arc::new(Notify::new());
    let store = Arc::new(MemoryStore::new());
    let gate = Arc::new(HeldResult {
        count: Arc::new(AtomicUsize::new(0)),
        entered: entered.clone(),
        release,
    });
    let retained = store.clone();
    let server = Server::start(Host::new(Arc::new(move || {
        builder(
            store.clone(),
            Arc::new(FakeProvider::scripted(journey(false))),
        )
        .extension(gate.clone(), crabber::extension::Scope::Global)
        .build()
    })))
    .await?;
    let response = reqwest::Client::new()
        .post(&server.url)
        .json(&input("held"))
        .send()
        .await?;
    entered.notified().await;
    drop(response);
    tokio::time::timeout(crabber::runtime::INTERRUPT_SETTLEMENT_BOUND, async {
        while server.host.active.load(Ordering::SeqCst) != 0 {
            assert_eq!(server.host.unresolved.load(Ordering::SeqCst), 0);
            tokio::task::yield_now().await;
        }
    })
    .await?;
    assert_eq!(server.host.unresolved.load(Ordering::SeqCst), 0);
    server.host.shutdown().await?;
    let crabber::session::SnapshotOutcome::Page(page) = retained
        .snapshot(crabber::session::SnapshotRequest {
            session_id: SessionId::from("held"),
            continuation: None,
            limits: crabber::session::SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 100_000,
                encoded_bytes: 100_000,
            },
        })
        .await?
    else {
        panic!("expected complete durable snapshot");
    };
    assert!(page.continuation.is_none());
    assert_ne!(page.tool_calls, []);
    for call in page.tool_calls {
        assert_eq!(call.status, crabber::core::ToolCallStatus::Interrupted);
        assert_eq!(
            call.result.unwrap().content,
            vec![crabber::core::ContentBlock::Text {
                text: "interrupted".into(),
            }]
        );
    }
    server.stop().await?;
    Ok(())
}

struct StartupFailure;
#[async_trait]
impl Resolver for StartupFailure {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        Ok(Arc::new(Self))
    }
}
#[async_trait]
impl Streamer for StartupFailure {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        Err(ProviderError {
            kind: ProviderErrorKind::Server,
            message: "SECRET_STARTUP".into(),
            retryable: false,
        })
    }
}
#[tokio::test]
async fn startup_failure_has_no_invented_message_and_one_safe_terminal() -> CheckResult {
    let store = Arc::new(MemoryStore::new());
    let retained = store.clone();
    let server = Server::start(Host::new(Arc::new(move || {
        builder(store.clone(), Arc::new(StartupFailure)).build()
    })))
    .await?;
    let events = check::request(&server, "startup").await?;
    assert!(matches!(
        events.as_slice(),
        [Event::RunStarted(_), Event::RunError(_)]
    ));
    assert!(!serde_json::to_string(&events)?.contains("SECRET_STARTUP"));
    assert_eq!(
        retained
            .list_messages(&SessionId::from("startup"), None)
            .await?
            .len(),
        1
    );
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn interleaved_tool_then_text_stays_in_one_client_assistant_message() -> CheckResult {
    use ag_ui_client::{Agent as _, HttpAgent, RunAgentParams};
    for text_first in [true, false] {
        let mut scripts = journey(false);
        let delta = StreamDelta::TextDelta("same response".into());
        if text_first {
            scripts[0].insert(0, delta);
        } else {
            scripts[0].insert(2, delta);
        }
        let server =
            Server::start(Host::new(factory(Arc::new(MemoryStore::new()), scripts))).await?;
        let result = HttpAgent::builder()
            .with_url_str(&server.url)?
            .build()?
            .run_agent(&RunAgentParams::new().user("hello"), ())
            .await?;
        assert_eq!(result.new_messages.len(), 4);
        assert!(result.new_messages.iter().any(|m| matches!(m, crabber_agui::ag_ui_core::types::Message::Assistant { content: Some(content), tool_calls: Some(calls), .. } if content == "same response" && calls.len() == 2)));
        server.stop().await?;
    }
    Ok(())
}

struct HeldInstall {
    entered: Arc<Notify>,
    release: Arc<Notify>,
}
#[async_trait]
impl crabber::extension::Extension for HeldInstall {
    fn id(&self) -> &'static str {
        "held-install"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "held".into()
    }
    async fn install(
        &self,
        _: &mut crabber::extension::Registrar,
    ) -> Result<(), crabber::ExtensionError> {
        self.entered.notify_one();
        self.release.notified().await;
        Ok(())
    }
}

async fn owned_idle(host: &Host) -> CheckResult {
    tokio::time::timeout(Duration::from_secs(5), async {
        while host.ingress.load(Ordering::SeqCst) != 0 || host.active.load(Ordering::SeqCst) != 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    Ok(())
}

#[tokio::test]
async fn partial_body_cannot_admit_after_successful_shutdown() -> CheckResult {
    use tokio::io::{AsyncReadExt, AsyncWriteExt};
    let store = Arc::new(MemoryStore::new());
    let server = Server::start(Host::new(factory(store.clone(), vec![text("late")]))).await?;
    let address = server
        .url
        .strip_prefix("http://")
        .unwrap()
        .strip_suffix("/run")
        .unwrap();
    let mut socket = tokio::net::TcpStream::connect(address).await?;
    let body = serde_json::to_vec(&input("slow-body"))?;
    socket.write_all(format!("POST /run HTTP/1.1\r\nHost: localhost\r\nContent-Length: {}\r\nConnection: close\r\n\r\n", body.len()).as_bytes()).await?;
    socket.write_all(&body[..1]).await?;
    tokio::time::timeout(Duration::from_secs(2), async {
        while server.host.ingress.load(Ordering::SeqCst) == 0 {
            tokio::task::yield_now().await;
        }
    })
    .await?;
    server.host.shutdown().await?;
    let _ = socket.write_all(&body[1..]).await;
    let mut response = Vec::new();
    tokio::time::timeout(Duration::from_secs(2), socket.read_to_end(&mut response)).await??;
    assert!(String::from_utf8(response)?.starts_with("HTTP/1.1 503"));
    assert!(
        store
            .get_session(&SessionId::from("slow-body"))
            .await?
            .is_none()
    );
    assert_eq!(
        store.list_unfinished_runs().await?,
        [] as [crabber::core::Run; 0]
    );
    server.stop().await?;
    Ok(())
}

#[tokio::test]
async fn held_admission_times_out_or_shuts_down_without_losing_late_handle() -> CheckResult {
    for shutdown in [false, true] {
        let entered = Arc::new(Notify::new());
        let release = Arc::new(Notify::new());
        let store = Arc::new(MemoryStore::new());
        let admission_store = store.clone();
        let gate = Arc::new(HeldInstall {
            entered: entered.clone(),
            release: release.clone(),
        });
        let mut host = Host::new(Arc::new(move || {
            builder(
                admission_store.clone(),
                Arc::new(FakeProvider::scripted(vec![text("late")])),
            )
            .extension(gate.clone(), crabber::extension::Scope::Global)
            .build()
        }));
        host.deadline = if shutdown {
            Duration::from_secs(5)
        } else {
            Duration::from_millis(80)
        };
        host.cleanup = Duration::from_millis(20);
        let server = Server::start(host).await?;
        let url = server.url.clone();
        let request = tokio::spawn(async move {
            reqwest::Client::new()
                .post(url)
                .json(&input("held-admission"))
                .send()
                .await
        });
        tokio::time::timeout(Duration::from_secs(2), entered.notified()).await?;
        if shutdown {
            assert!(server.host.shutdown().await.is_err());
        }
        let response = tokio::time::timeout(Duration::from_secs(2), request).await???;
        assert_eq!(response.status().as_u16(), if shutdown { 503 } else { 408 });
        tokio::time::timeout(Duration::from_secs(2), async {
            while server.host.unresolved.load(Ordering::SeqCst) == 0 {
                tokio::task::yield_now().await;
            }
        })
        .await?;
        assert_eq!(server.host.ingress.load(Ordering::SeqCst), 1);
        assert!(server.host.shutdown().await.is_err());
        release.notify_one();
        owned_idle(&server.host).await?;
        assert_eq!(
            store.list_unfinished_runs().await?,
            [] as [crabber::core::Run; 0]
        );
        assert_eq!(server.host.unresolved.load(Ordering::SeqCst), 0);
        server.stop().await?;
    }
    Ok(())
}
