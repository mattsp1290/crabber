use async_trait::async_trait;
use crabber::core::{
    Clock, EventKind, EventRecord, Message, MessageId, Role, Run, RunStatus, SessionId,
    SystemClock, ToolCallId, ToolCallRecord, ToolCallStatus, ToolInfo,
};
use crabber::extension::{Extension, ExtensionError, Registrar, Scope};
use crabber::providers::{ProviderError, Resolver, Selection, Streamer};
use crabber::session::{AdmitRequest, MemoryStore, Store};
use crabber::{AbandonAuthority, AbandonOutcome, AbandonRequest, Agent, AgentConfig};
use std::{
    error::Error,
    process::Command,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

struct Unavailable(Arc<AtomicUsize>);
#[async_trait]
impl Resolver for Unavailable {
    async fn resolve(&self, _: &Selection) -> Result<Arc<dyn Streamer>, ProviderError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandonment resolved unavailable provider")
    }
}
struct NeverTool(Arc<AtomicUsize>);
#[async_trait]
impl crabber::ToolExecutor for NeverTool {
    async fn execute(&self, _: serde_json::Value) -> Result<serde_json::Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandonment executed unavailable tool")
    }
}
struct Unmountable(Arc<AtomicUsize>);
#[async_trait]
impl Extension for Unmountable {
    fn id(&self) -> &'static str {
        "unavailable"
    }
    fn version(&self) -> &'static str {
        "1"
    }
    fn config_hash(&self) -> String {
        "unavailable".into()
    }
    async fn install(&self, _: &mut Registrar) -> Result<(), ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        panic!("abandonment mounted unavailable extension/hooks")
    }
}
fn event(run: &Run, kind: EventKind) -> EventRecord {
    EventRecord {
        cursor: None,
        session_id: run.session_id.clone(),
        run_id: run.id.clone(),
        turn_id: None,
        kind,
        payload: serde_json::Value::Null,
        correlation: None,
        live_only: false,
        created_at: run.created_at,
    }
}
#[tokio::main(flavor = "current_thread")]
#[allow(clippy::too_many_lines)]
async fn main() -> Result<(), Box<dyn Error>> {
    let postgres = std::env::args().any(|arg| arg == "--postgres");
    let store = connect(postgres).await?;
    let counters = Arc::new(AtomicUsize::new(0));
    let tool = Arc::new(AtomicUsize::new(0));
    let mount = Arc::new(AtomicUsize::new(0));
    let agent = Agent::builder()
        .store(store.clone())
        .provider(Arc::new(Unavailable(counters.clone())))
        .tool(Arc::new(crabber::ToolDefinition {
            info: ToolInfo {
                name: "unavailable-unsafe-tool".into(),
                description: "never execute".into(),
                parameters: serde_json::json!({}),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(NeverTool(tool.clone())),
        }))
        .extension(Arc::new(Unmountable(mount.clone())), Scope::Global)
        .config(AgentConfig::new(Selection {
            provider_id: "unavailable".into(),
            model_id: "unavailable".into(),
        }))
        .build()?;
    if let Ok(encoded) = std::env::var("CRABBER_ABANDON_DEMO_REQUEST") {
        let request: AbandonRequest = serde_json::from_str(&encoded)?;
        let expected: AbandonOutcome =
            serde_json::from_str(&std::env::var("CRABBER_ABANDON_DEMO_OUTCOME")?)?;
        assert_eq!(
            store.get_run(&expected.run.id).await?,
            Some(expected.run.clone())
        );
        assert!(
            store
                .list_unfinished_tool_calls(&expected.run.id)
                .await?
                .is_empty()
        );
        let messages = store.list_all_messages(&expected.run.session_id).await?;
        let events = store
            .list_events(&expected.run.session_id, None, 100)
            .await?;
        assert_eq!(agent.abandon(request).await?, expected);
        assert_eq!(
            store.list_all_messages(&expected.run.session_id).await?,
            messages
        );
        assert_eq!(
            store
                .list_events(&expected.run.session_id, None, 100)
                .await?,
            events
        );
        print_identity(
            postgres,
            &expected,
            counters.load(Ordering::SeqCst),
            tool.load(Ordering::SeqCst),
            mount.load(Ordering::SeqCst),
        )?;
        println!("fresh_process_readback=true exact_retry=true");
        return Ok(());
    }
    let session = SessionId::new();
    // The disposable authoritative owner has no execution work. A production
    // host must stop its real worker and its renewal/replacement coordinator.
    let mut worker = OwnerProcess(Command::new("sleep").arg("60").spawn()?);
    let admitted = store
        .admit_run(AdmitRequest {
            session_id: None,
            workspace_id: "demo".into(),
            directory: ".".into(),
            title: "abandon demo".into(),
            config_hash: "unavailable".into(),
            plan_fingerprint: "unavailable".into(),
            owner: format!("process:{}", worker.0.id()),
            lease: Duration::from_secs(60),
            user_message: Message {
                id: MessageId::new(),
                session_id: session,
                run_id: None,
                role: Role::User,
                parent_id: None,
                parts: vec![],
                created_at: SystemClock.now(),
            },
        })
        .await?;
    let execution = store.execution(admitted.fence.clone()).await?;
    for running in [false, true] {
        let call = ToolCallRecord {
            id: ToolCallId::new(),
            run_id: admitted.run.id.clone(),
            name: "unavailable-unsafe-tool".into(),
            arguments: serde_json::json!({}),
            status: ToolCallStatus::Pending,
            retry_safe: false,
            result: None,
        };
        execution
            .create_tool_call(
                call.clone(),
                event(&admitted.run, EventKind::ToolCallPending),
            )
            .await?;
        if running {
            execution
                .claim_tool_call(&call.id, event(&admitted.run, EventKind::ToolCallRunning))
                .await?;
        }
    }
    let mut request = AbandonRequest {
        expected: admitted.fence.clone(),
        expected_owner: admitted.run.owner.clone(),
        authority: AbandonAuthority::ExpiredLease,
    };
    assert_eq!(
        agent.abandon(request.clone()).await.unwrap_err(),
        crabber::AbandonError::LiveLease
    );
    execution
        .pause_run(
            serde_json::json!({"unavailable_continuation":true}),
            event(&admitted.run, EventKind::RunPaused),
        )
        .await?;
    assert!(worker.0.try_wait()?.is_none());
    worker.0.kill()?;
    let exited = worker.0.wait()?;
    assert!(!exited.success());
    assert!(worker.0.try_wait()?.is_some());
    println!(
        "authoritative_owner_exit_waited=true process={}",
        worker.0.id()
    );
    request.authority = AbandonAuthority::HostStoppedOwner;
    // Persist this exact request before submission; an unknown response must
    // retry the original authority/owner/fence, never synthesize a new request.
    let persisted_request = serde_json::to_string(&request)?;
    let outcome = agent.abandon(request.clone()).await?;
    assert_eq!(outcome.run.status, RunStatus::Interrupted);
    assert_eq!(outcome.interrupted_tools.len(), 2);
    assert_eq!(agent.abandon(request).await?, outcome);
    print_identity(
        postgres,
        &outcome,
        counters.load(Ordering::SeqCst),
        tool.load(Ordering::SeqCst),
        mount.load(Ordering::SeqCst),
    )?;
    if postgres {
        let child = Command::new(std::env::current_exe()?)
            .arg("--postgres")
            .env("CRABBER_ABANDON_DEMO_REQUEST", persisted_request)
            .env(
                "CRABBER_ABANDON_DEMO_OUTCOME",
                serde_json::to_string(&outcome)?,
            )
            .output()?;
        assert!(
            child.status.success(),
            "{}",
            String::from_utf8_lossy(&child.stderr)
        );
        print!("{}", String::from_utf8_lossy(&child.stdout));
    }
    Ok(())
}
#[cfg_attr(not(feature = "postgres"), allow(clippy::unused_async))]
async fn connect(postgres: bool) -> Result<Arc<dyn Store>, Box<dyn Error>> {
    if !postgres {
        return Ok(Arc::new(MemoryStore::new()));
    }
    #[cfg(feature = "postgres")]
    {
        let url = std::env::var("CRABBER_POSTGRES_URL")
            .map_err(|_| "CRABBER_POSTGRES_URL is required")?;
        crabber::session::PostgresStore::migrate(&url).await?;
        Ok(Arc::new(
            crabber::session::PostgresStore::connect(&url).await?,
        ))
    }
    #[cfg(not(feature = "postgres"))]
    Err("--postgres requires postgres feature".into())
}
fn print_identity(
    postgres: bool,
    outcome: &AbandonOutcome,
    provider: usize,
    tool: usize,
    mount: usize,
) -> Result<(), Box<dyn Error>> {
    let output = Command::new("git")
        .args(["rev-parse", "HEAD"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .output()?;
    let runtime = String::from_utf8(output.stdout)?;
    assert!(output.status.success());
    assert_eq!(
        env!("CRABBER_COMPILED_SHA"),
        runtime.trim(),
        "rebuild after source commit changes"
    );
    assert_eq!((provider, tool, mount), (0, 0, 0));
    println!(
        "compiled_sha={} runtime_sha={} backend={} run={} terminal_event={:?} status=Interrupted provider_counter={} tool_counter={} mount_hook_counter={}",
        env!("CRABBER_COMPILED_SHA"),
        runtime.trim(),
        if postgres { "postgres" } else { "memory" },
        outcome.run.id,
        outcome.terminal_event.cursor,
        provider,
        tool,
        mount
    );
    Ok(())
}

struct OwnerProcess(std::process::Child);
impl Drop for OwnerProcess {
    fn drop(&mut self) {
        if matches!(self.0.try_wait(), Ok(None)) {
            let _ = self.0.kill();
            let _ = self.0.wait();
        }
    }
}
