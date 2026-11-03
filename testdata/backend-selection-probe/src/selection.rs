use crate::Result;
use crabber::{Agent, AgentBuilder, AgentConfig, FakeProvider, Selection, StreamDelta};
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
use crabber_core::{ContentBlock, RunStatus};
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
use crabber_session::Store;
#[cfg(any(feature = "postgres", feature = "sqlite"))]
use futures::FutureExt;
use std::sync::Arc;

const RESPONSE: &str = "backend selection response Ω";

fn provider() -> Arc<FakeProvider> {
    Arc::new(FakeProvider::scripted(vec![vec![
        StreamDelta::TextDelta(RESPONSE.into()),
        StreamDelta::Completed,
    ]]))
}
fn builder() -> AgentBuilder {
    Agent::builder()
        .behavior_version("backend-selection.v1")
        .provider(provider())
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
}
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
fn limits() -> crabber::PublicSessionLimits {
    crabber::PublicSessionLimits {
        header_bytes: 1024 * 1024,
        proof_bytes: 16 * 1024 * 1024,
        interactions: 64,
        history: crabber::SnapshotLimits {
            messages: 100,
            tool_calls: 100,
            parts: 1000,
            text_bytes: 1024 * 1024,
            encoded_bytes: 4 * 1024 * 1024,
        },
    }
}
#[cfg(any(
    feature = "default",
    feature = "memory",
    feature = "postgres",
    feature = "sqlite"
))]
async fn agent_turn(agent: Agent, store: Option<Arc<dyn Store>>) -> Result<()> {
    let result = async {
        let mut handle = agent.prompt(None, "select backend").await?;
        let session = handle.session_id().clone();
        let run = handle.run_id().clone();
        let mut events = handle.events();
        let mut streamed = String::new();
        while let Some(event) = events.recv().await? {
            if event.kind == crabber::EventKind::TextDelta {
                streamed.push_str(event.payload["text"].as_str().ok_or("text delta missing")?);
            }
        }
        check_eq!(streamed, RESPONSE);
        check_eq!(handle.done().await?.status, RunStatus::Completed);
        let crabber::PublicSessionOutcome::Page(page) = agent
            .public_session_snapshot(crabber::PublicSessionRequest {
                session_id: session.clone(),
                limits: limits(),
                continuation: None,
            })
            .await?
        else {
            return Err("complete public readback required".into());
        };
        check!(page.is_terminal());
        check_eq!(page.run.as_ref().ok_or("public run missing")?.run_id, run);
        check!(page.history.continuation.is_none());
        let text: String = page
            .history
            .messages
            .iter()
            .filter(|message| message.role == crabber_core::Role::Assistant)
            .flat_map(|message| &message.parts)
            .filter_map(|part| match &part.content {
                ContentBlock::Text { text } => Some(text.as_str()),
                _ => None,
            })
            .collect();
        check_eq!(text, RESPONSE);
        let mut subscription = crabber::PublicSessionSubscription::new(session.clone(), limits());
        check!(matches!(
            agent.poll_public_session(&mut subscription).await?,
            crabber::PublicSessionPoll::Page { reset: true, .. }
        ));
        check!(matches!(
            agent.poll_public_session(&mut subscription).await?,
            crabber::PublicSessionPoll::Unchanged { .. }
        ));
        if let Some(store) = store {
            check_eq!(
                store.get_run(&run).await?.ok_or("run missing")?.status,
                RunStatus::Completed
            );
            let messages = store.list_all_messages(&session).await?;
            check!(messages.iter().any(|message| message.parts.iter().any(
                |part| matches!(&part.content, ContentBlock::Text { text } if text == RESPONSE)
            )));
        }
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    }
    .await;
    let close = agent.close(crate::BOUND).await;
    if !close.local_drained() {
        if result.is_err() {
            eprintln!("agent primary failure and local cleanup incomplete");
            return Err("agent primary failure and local cleanup incomplete".into());
        }
        eprintln!("agent local cleanup incomplete");
        return Err("agent local close did not drain".into());
    }
    result
}

pub async fn run(backend: &str, case: &str) -> Result<()> {
    match case {
        #[cfg(any(feature = "default", feature = "memory"))]
        "fallback" => agent_turn(builder().build()?, None).await,
        #[cfg(any(feature = "default", feature = "memory"))]
        "memory-method" => agent_turn(builder().memory().build()?, None).await,
        #[cfg(any(feature = "default", feature = "memory"))]
        "memory-store" => {
            let store: Arc<dyn Store> = Arc::new(crabber_session::MemoryStore::default());
            agent_turn(builder().store(store.clone()).build()?, Some(store)).await
        }
        "missing-store" => {
            check!(matches!(
                builder().build(),
                Err(crabber::BuildError::Runtime(
                    crabber_runtime::RuntimeError::Missing("store")
                ))
            ));
            Ok(())
        }
        "runtime-missing-store" => {
            check!(matches!(
                crabber_runtime::Orchestrator::builder().build(),
                Err(crabber_runtime::RuntimeError::Missing("store"))
            ));
            Ok(())
        }
        #[cfg(feature = "sqlite")]
        _ if backend == "sqlite" => sqlite(case).await,
        #[cfg(feature = "postgres")]
        _ if backend == "postgres" => postgres(case).await,
        _ => Err(format!("unsupported case {backend}/{case}").into()),
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn runtime_turn(store: Arc<dyn Store>) -> Result<()> {
    let runtime = crabber_runtime::Orchestrator::builder()
        .behavior_version("backend-selection.runtime.v1")
        .store(store.clone())
        .resolver(provider())
        .plan_provider(Arc::new(crabber_extension::StaticPlanProvider::new(
            vec![],
            vec![],
        )))
        .build()?;
    let result =
        async {
            let handle = runtime
                .start(crabber_runtime::Request {
                    selection_policy: crabber_session::AdmissionSelectionPolicy::Explicit,
                    captured_revision: None,
                    input_envelope: None,
                    session_id: None,
                    workspace_id: "backend-selection".into(),
                    directory: "/backend-selection/host-admitted".into(),
                    title: "runtime selection".into(),
                    text: "select backend".into(),
                    selection: Selection {
                        provider_id: "fake".into(),
                        model_id: "scripted".into(),
                    },
                    system_prompt: None,
                    max_output_tokens: None,
                    model_controls: crabber_core::ModelControls::default(),
                })
                .await?;
            let run = handle.run_id().clone();
            let session = handle.session_id().clone();
            check_eq!(handle.done().await?.status, RunStatus::Completed);
            check_eq!(
                store
                    .get_run(&run)
                    .await?
                    .ok_or("runtime run missing")?
                    .status,
                RunStatus::Completed
            );
            let messages = store.list_all_messages(&session).await?;
            check!(messages.iter().any(|message| message.parts.iter().any(
                |part| matches!(&part.content, ContentBlock::Text { text } if text == RESPONSE)
            )));
            Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
        }
        .await;
    let close = runtime.close(crate::BOUND).await;
    if !close.local_drained() {
        if result.is_err() {
            eprintln!("runtime primary failure and local cleanup incomplete");
            return Err("runtime primary failure and local cleanup incomplete".into());
        }
        eprintln!("runtime local cleanup incomplete");
        return Err("runtime local close did not drain".into());
    }
    result
}

#[cfg(feature = "sqlite")]
async fn sqlite(case: &str) -> Result<()> {
    let directory = tempfile::tempdir()?;
    let path = directory.path().join("selection.sqlite");
    let result = Box::pin(async {
        crabber_session::SqliteStore::migrate(&path).await?;
        let store = crabber_session::SqliteStore::connect(&path).await?;
        sql_case(
            case,
            store.clone(),
            crate::native::Observer::sqlite(&path).await?,
        )
        .await?;
        drop(store);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    let cleanup = directory.close();
    match (result, cleanup) {
        (Err(_), Err(_)) => {
            eprintln!("SQLite primary failure and backing cleanup failed");
            Err("SQLite primary failure and backing cleanup failed".into())
        }
        (Err(error), _) => Err(error),
        (_, Err(_)) => {
            eprintln!("SQLite backing cleanup failed");
            Err("SQLite backing cleanup failed".into())
        }
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[cfg(feature = "postgres")]
async fn postgres(case: &str) -> Result<()> {
    use crabber_test_support::postgres::{DatabaseMode, TestDatabase};
    if std::env::var("CRABBER_REQUIRE_POSTGRES").as_deref() != Ok("1")
        || !TestDatabase::available()?
    {
        return Err("owned PostgreSQL is required".into());
    }
    let database = TestDatabase::create(DatabaseMode::Current).await?;
    let result = Box::pin(async {
        let url = database.url();
        crabber_session::PostgresStore::migrate(&url).await?;
        let store = crabber_session::PostgresStore::connect(&url).await?;
        sql_case(
            case,
            store.clone(),
            crate::native::Observer::postgres(&url).await?,
        )
        .await?;
        drop(store);
        Ok::<_, Box<dyn std::error::Error + Send + Sync>>(())
    })
    .await;
    let cleanup = database.finish().await;
    match (result, cleanup) {
        (Err(_), Err(_)) => {
            eprintln!("PostgreSQL primary failure and owned cleanup failed");
            Err("PostgreSQL case and owned cleanup failed".into())
        }
        (Err(error), _) => Err(error),
        (_, Err(_)) => {
            eprintln!("owned PostgreSQL cleanup failed");
            Err("owned PostgreSQL cleanup failed".into())
        }
        (Ok(()), Ok(())) => Ok(()),
    }
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
trait ClockStore: Store + Clone + 'static {
    #[cfg(feature = "conformance")]
    fn clocked(&self, clock: Arc<crabber_core::ManualClock>) -> Self;
}
#[cfg(feature = "sqlite")]
impl ClockStore for crabber_session::SqliteStore {
    #[cfg(feature = "conformance")]
    fn clocked(&self, clock: Arc<crabber_core::ManualClock>) -> Self {
        self.clone().with_clock(clock)
    }
}
#[cfg(feature = "postgres")]
impl ClockStore for crabber_session::PostgresStore {
    #[cfg(feature = "conformance")]
    fn clocked(&self, clock: Arc<crabber_core::ManualClock>) -> Self {
        self.clone().with_clock(clock)
    }
}
#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn sql_case<S: ClockStore>(
    case: &str,
    store: S,
    observer: crate::native::Observer,
) -> Result<()> {
    let actual: Arc<dyn Store> = Arc::new(store.clone());
    let result = std::panic::AssertUnwindSafe(async {
        match case {
            "agent" => agent_turn(builder().store(actual.clone()).build()?, Some(actual)).await,
            "runtime" => runtime_turn(actual).await,
            "queue" => queue(actual.as_ref()).await,
            "custom-store" => {
                let audited = Arc::new(crate::audited::AuditedStore::new(actual));
                agent_turn(
                    builder().store(audited.clone()).build()?,
                    Some(audited.clone()),
                )
                .await?;
                check!(audited.calls() > 0);
                Ok(())
            }
            #[cfg(feature = "conformance")]
            _ if case.starts_with("contract-") => contracts(case, &store).await,
            _ => crate::native::run(case, actual, &observer).await,
        }
    })
    .catch_unwind()
    .await;
    observer.close().await;
    result.map_err(|_| "SQL contract panicked after owned case cleanup")?
}

#[cfg(all(feature = "conformance", any(feature = "postgres", feature = "sqlite")))]
async fn contracts<S: ClockStore>(case: &str, store: &S) -> Result<()> {
    match case {
        "contract-management" => {
            crabber_session::session_management_contract::run_contract(store).await;
        }
        "contract-store" => {
            crabber_session::storetest::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-workspace-identity" => {
            crabber_session::storetest::run_workspace_identity_contract(|clock| {
                store.clocked(clock)
            })
            .await;
        }
        "contract-storable-records" => {
            crabber_session::storetest::run_storable_record_contract(|clock| store.clocked(clock))
                .await;
        }
        "contract-admission" => {
            crabber_session::admission_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-execution-binding" => {
            crabber_session::execution_binding_contract::run_contract(|clock| store.clocked(clock))
                .await;
        }
        "contract-typed-admission" => {
            crabber_session::typed_admission_contract::run_contract(|clock| store.clocked(clock))
                .await;
        }
        "contract-revision" => {
            crabber_session::revision_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-public-state" => {
            crabber_session::public_state_contract::run_contract(|clock| store.clocked(clock))
                .await;
        }
        "contract-attempt" => {
            crabber_session::attempt_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-phase" => {
            crabber_session::phase_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-attempt-control" => {
            crabber_session::attempt_control_contract::run_contract(|clock| store.clocked(clock))
                .await;
        }
        "contract-approval" => {
            crabber_session::approval_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-interaction" => {
            crabber_session::interaction_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-control" => {
            crabber_session::control_contract::run_contract(|clock| store.clocked(clock)).await;
        }
        "contract-session-owner" => {
            crabber_session::session_owner_contract::run_contract(|clock| store.clocked(clock))
                .await;
        }
        _ => return Err("unsupported contract case".into()),
    }
    Ok(())
}

#[cfg(any(feature = "postgres", feature = "sqlite"))]
async fn queue(store: &dyn Store) -> Result<()> {
    use crabber_core::{ControlId, QueueId, SessionId, SessionRevision};
    use crabber_session::{
        ManageSession, QueueAction, QueueInput, QueueMutation, QueueOutcome, QueueState,
        SessionChange, SessionDefaults,
    };
    let session = SessionId::new();
    let revision = store
        .manage_session(ManageSession {
            id: ControlId::new(),
            session_id: session.clone(),
            expected_revision: SessionRevision::INITIAL,
            change: SessionChange::Create {
                workspace_id: crate::profile::WORKSPACE.into(),
                directory: crate::profile::DIRECTORY.into(),
                title: "queue selection".into(),
                defaults: SessionDefaults::default(),
            },
        })
        .await?
        .revision;
    let bound = crate::profile::bound(&session, revision)?;
    let queue = QueueId::new();
    let request = QueueMutation {
        session_id: session.clone(),
        id: ControlId::new(),
        expected_revision: revision,
        action: QueueAction::Enqueue {
            queue_id: queue.clone(),
            input: Box::new(QueueInput {
                request: bound.binding.request,
                input: bound.binding.input_envelope.ok_or("queue input missing")?,
                admission: bound.keyed.options,
            }),
        },
    };
    let first = store.mutate_queue(request.clone()).await?;
    check_eq!(first.receipt().summary.state, QueueState::Queued);
    let replay = store.mutate_queue(request).await?;
    check!(matches!(replay, QueueOutcome::Replayed(_)));
    check_eq!(first.receipt(), replay.receipt());
    check_eq!(
        store.get_queue(&session, &queue).await?.state,
        QueueState::Queued
    );
    let cancelled = store
        .mutate_queue(QueueMutation {
            session_id: session.clone(),
            id: ControlId::new(),
            expected_revision: store.session_revision(&session).await?,
            action: QueueAction::Cancel {
                queue_id: queue.clone(),
            },
        })
        .await?;
    check_eq!(cancelled.receipt().summary.state, QueueState::Cancelled);
    check_eq!(
        store.get_queue(&session, &queue).await?.state,
        QueueState::Cancelled
    );
    Ok(())
}
