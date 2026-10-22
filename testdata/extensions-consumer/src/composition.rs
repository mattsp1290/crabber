use crate::{
    agent_support, common::*, custom, factory_options, owned_process, policies, scripted::Scripts,
};
use crabber::{
    Agent, AgentConfig, ExecutionContext,
    core::{
        EventKind, InputFingerprint, PublicSourceChange, PublicSourceEvent, RunStatus,
        SessionContext, SessionId,
    },
    extension::{FactoryScope, Scope, SessionInvocationRequest},
    session::{MemoryStore, Store},
};
use crabber_extensions::{
    background_jobs as jobs, delegate_task as delegate, python_repl as python,
    workspace_instructions::host::{AllowListResolver, BoundaryReader},
};
use serde_json::json;
use std::{
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    time::Duration,
};

type Result<T = ()> = std::result::Result<T, Box<dyn std::error::Error>>;
struct Host {
    cleanup: owned_process::ProcessCleanup,
    root: tempfile::TempDir,
    cwd: std::path::PathBuf,
    agent: Agent,
    store: Arc<MemoryStore>,
    scripts: Arc<Scripts>,
    effects: Arc<AtomicUsize>,
    jobs: Arc<jobs::Factory>,
    delegate: Arc<delegate::Factory>,
    contexts: [ExecutionContext; 3],
    entered: Arc<tokio::sync::Semaphore>,
}
fn initial() -> Vec<crabber::StreamDelta> {
    let items = vec![
        ("consumer_marker", json!({})),
        (
            python::EXECUTE_TOOL,
            json!({"code":"import os,pathlib; pathlib.Path(os.environ['FIXTURE_ROOT'],'pgid.'+str(os.getpgrp())).write_text(str(os.getpgrp())); x=1; (x,os.getcwd())"}),
        ),
        (
            jobs::START_TOOL,
            json!({"command":"printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/pgid.$PPID\"; printf '%s' \"$PWD\"; sleep 60"}),
        ),
        (
            delegate::TOOL_NAME,
            json!({"task":"inline", "profile":"synthetic"}),
        ),
        (
            delegate::START_TOOL,
            json!({"task":"background", "profile":"synthetic"}),
        ),
    ];
    batch(&items)
}
impl Host {
    fn new() -> Result<Self> {
        let cwd = std::env::current_dir()?;
        let root = tempfile::tempdir()?;
        for name in ["a", "b", "temporary"] {
            std::fs::create_dir(root.path().join(name))?;
        }
        let a = root.path().join("a").canonicalize()?;
        let b = root.path().join("b").canonicalize()?;
        let boundary = root.path().canonicalize()?;
        std::fs::write(boundary.join("AGENTS.md"), "boundary-instructions")?;
        for (path, label) in [(&a, "a"), (&b, "b")] {
            std::fs::write(path.join("AGENTS.md"), format!("instructions-{label}"))?;
            std::fs::write(path.join("marker.txt"), format!("marker-{label}"))?;
        }
        let cleanup = owned_process::ProcessCleanup::new(root.path());
        let inline = Arc::new(tokio::sync::Barrier::new(3));
        let entered = Arc::new(tokio::sync::Semaphore::new(0));
        let runner: delegate::Runner = {
            let inline = inline.clone();
            let entered = entered.clone();
            Arc::new(move |request| {
                let inline = inline.clone();
                let entered = entered.clone();
                Box::pin(async move {
                    if request.background() {
                        let child = SessionInvocationRequest::new(
                            "consumer/nested".into(),
                            InputFingerprint::new("c".repeat(64)).unwrap(),
                        );
                        request
                            .session_invocations()
                            .spawn(child, |inv| async move {
                                inv.cancel.cancelled().await;
                                Ok(())
                            })
                            .await
                            .map_err(|_| {
                                crabber::ExtensionError::Tool("nested registration".into())
                            })?;
                        entered.add_permits(1);
                        request.cancellation().cancelled().await;
                    } else {
                        inline.wait().await;
                    }
                    Ok(delegate::Response::Completed("synthetic completion".into()))
                })
            })
        };
        let jobs = Arc::new(
            jobs::Factory::new(factory_options::jobs(root.path()), FactoryScope::Session)?
                .with_shared_capacity(jobs::SharedCapacity::new(4)?),
        );
        let python = Arc::new(
            python::Factory::new(
                factory_options::python(root.path()).expect("required fixture Python"),
                FactoryScope::Session,
            )?
            .with_shared_capacity(python::SharedCapacity::new(4)?),
        );
        let delegate = Arc::new(
            delegate::Factory::new(factory_options::delegate(runner), FactoryScope::Session)?
                .with_shared_capacity(delegate::SharedCapacity::new(4)?),
        );
        let resolver = AllowListResolver::new(vec![
            (
                "ws-a".into(),
                a.to_str().unwrap().into(),
                Arc::new(BoundaryReader::for_workspace(&boundary, &a)?),
            ),
            (
                "ws-b".into(),
                b.to_str().unwrap().into(),
                Arc::new(BoundaryReader::for_workspace(&boundary, &b)?),
            ),
        ])?;
        let scripts = Arc::new(Scripts::default());
        let store = Arc::new(MemoryStore::new());
        let effects = Arc::new(AtomicUsize::new(0));
        let builder = agent_support::builder()
            .behavior_version("durable-host-consumer.v1")
            .store(store.clone())
            .provider(scripts.clone())
            .config(AgentConfig::new(selection()))
            .policy(Arc::new(custom::Policy))
            .workspace_reader_resolver(Arc::new(resolver))
            .shared_extension(Arc::new(policies::instructions(&boundary)?), Scope::Global)
            .shared_extension(Arc::new(policies::redactor()?), Scope::Global)
            .shared_extension(Arc::new(policies::guard()?), Scope::Global)
            .shared_extension(Arc::new(ask()), Scope::Global)
            .shared_extension(Arc::new(custom::Custom(effects.clone())), Scope::Global)
            .extension_factory(jobs.clone())
            .extension_factory(python.clone())
            .extension_factory(delegate.clone());
        let agent = builder.build()?;
        let execution = |path: &std::path::Path, workspace: &str| {
            ExecutionContext::new(
                SessionContext::new(SessionId::new(), workspace, path.to_str().unwrap()),
                selection(),
            )
        };
        let contexts = [
            execution(&a, "ws-a"),
            execution(&a, "ws-a"),
            execution(&b, "ws-b"),
        ];
        Ok(Self {
            cleanup,
            root,
            cwd,
            agent,
            store,
            scripts,
            effects,
            jobs,
            delegate,
            contexts,
            entered,
        })
    }
    async fn start(&self) -> Result<Arc<crabber::FakeProvider>> {
        let Self {
            agent,
            scripts,
            contexts,
            entered,
            ..
        } = self;
        let provider = scripts.set(vec![
            initial(),
            initial(),
            initial(),
            done(),
            done(),
            done(),
        ]);
        let runs = tokio::join!(
            agent_support::first_prompt(agent, &contexts[0], "compose"),
            agent_support::first_prompt(agent, &contexts[1], "compose"),
            agent_support::first_prompt(agent, &contexts[2], "compose")
        );
        for run in [runs.0?, runs.1?, runs.2?] {
            assert_eq!(
                tokio::time::timeout(Duration::from_secs(20), run.done())
                    .await??
                    .status,
                RunStatus::Completed
            );
        }
        tokio::time::timeout(Duration::from_secs(5), entered.acquire_many(3))
            .await?
            .unwrap()
            .forget();
        Ok(provider)
    }
    async fn verify_initial(&self, provider: &crabber::FakeProvider) -> Result {
        let Self {
            store,
            effects,
            delegate,
            contexts,
            agent,
            ..
        } = self;
        for ctx in contexts {
            let calls = records(&**store, ctx.session().session_id()).await;
            let marker = value(calls.iter().find(|c| c.name == "consumer_marker").unwrap());
            assert_eq!(marker["directory"], ctx.session().directory());
            assert_eq!(marker["workspace"], ctx.session().workspace_id());
            assert_eq!(marker["session"], ctx.session().session_id().0);
            assert_ne!(marker["secret"], custom::SECRET);
            let py = value(
                calls
                    .iter()
                    .find(|c| c.name == python::EXECUTE_TOOL)
                    .unwrap(),
            );
            assert_eq!(
                py["result"]["text"],
                format!("(1, '{}')", ctx.session().directory())
            );
            assert_eq!(
                delegate
                    .owner_report(
                        ctx.session().workspace_id(),
                        Some(ctx.session().session_id())
                    )
                    .unwrap()
                    .live,
                1
            );
            let observed = agent
                .drain_session_invocations(
                    ctx.session().session_id(),
                    false,
                    Duration::from_millis(1),
                )
                .await;
            assert_eq!(observed.len(), 2);
            assert!(observed.iter().all(|r| !r.local_joined));
        }
        let requests = format!("{:?}", provider.requests());
        assert!(!requests.contains(custom::SECRET));
        assert!(
            requests.contains("instructions-a")
                && requests.contains("instructions-b")
                && requests.contains("boundary-instructions")
        );
        for request in provider.requests() {
            assert!(request.tools.iter().any(|t| t.name == "consumer_marker"));
            assert!(!request.tools.iter().any(|t| t.name == "shell"));
        }
        assert_eq!(effects.load(Ordering::SeqCst), 3);
        Ok(())
    }
    async fn close_one(&self) -> Result {
        let Self {
            agent,
            scripts,
            store,
            jobs,
            contexts,
            ..
        } = self;
        // One owner closes while another session in the SAME workspace remains live.
        let reports = agent
            .drain_session_invocations(
                contexts[0].session().session_id(),
                true,
                Duration::from_secs(5),
            )
            .await;
        assert!(
            reports
                .iter()
                .all(|r| r.local_joined && r.status == crabber::SessionInvocationStatus::Joined)
        );
        agent
            .close_session_resources(contexts[0].session(), Duration::from_secs(5))
            .await?;
        scripts.set(vec![
            batch(&[(python::EXECUTE_TOOL, json!({"code":"x+=1; x"}))]),
            done(),
        ]);
        let continued =
            agent_support::next_prompt(agent, &contexts[1], "other owner alive").await?;
        let run = continued.run_id().clone();
        continued.done().await?;
        let calls = records(&**store, contexts[1].session().session_id()).await;
        assert_eq!(
            value(calls.iter().find(|c| c.run_id == run).unwrap())["result"]["text"],
            "2"
        );
        assert_eq!(
            jobs.owner_report("ws-a", Some(contexts[1].session().session_id()))
                .unwrap()
                .live,
            1
        );
        Ok(())
    }
    async fn durable_inputs(&self) -> Result {
        let Self {
            agent,
            scripts,
            effects,
            contexts,
            ..
        } = self;
        scripts.set(vec![
            batch(&[("guarded_marker", json!({"command":"rm -rf synthetic"}))]),
            done(),
        ]);
        agent_support::next_prompt(agent, &contexts[2], "guard denial")
            .await?
            .done()
            .await?;
        assert_eq!(effects.load(Ordering::SeqCst), 3);
        scripts.set(vec![batch(&[("confirmed_marker", json!({}))]), done()]);
        let paused = agent_support::next_prompt(agent, &contexts[2], "permission Ask").await?;
        let run = paused.run_id().clone();
        assert_eq!(paused.done().await?.status, RunStatus::Paused);
        deny_approval(agent, contexts[2].session().session_id(), &run).await;
        assert_eq!(effects.load(Ordering::SeqCst), 3);
        scripts.set(vec![
            batch(&[(crabber_extensions::ask_user::TOOL_NAME, question())]),
            done(),
        ]);
        let paused = agent_support::next_prompt(agent, &contexts[2], "durable input").await?;
        let run = paused.run_id().clone();
        assert_eq!(paused.done().await?.status, RunStatus::Paused);
        answer_question(agent, contexts[2].session().session_id(), &run).await;
        Ok(())
    }
    async fn drain_remaining(&self) -> Result {
        let Self {
            agent,
            store,
            contexts,
            ..
        } = self;
        for ctx in &contexts[1..] {
            let reports = agent
                .drain_session_invocations(ctx.session().session_id(), true, Duration::from_secs(5))
                .await;
            assert_eq!(reports.len(), 2);
            assert!(
            reports
                .iter()
                .all(|r| r.local_joined && r.status == crabber::SessionInvocationStatus::Joined)
        );
            let events = store
                .list_events(ctx.session().session_id(), None, 1000)
                .await?;
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == EventKind::InvocationStarted)
                    .count(),
                2
            );
            assert_eq!(
                events
                    .iter()
                    .filter(|e| e.kind == EventKind::InvocationFinished)
                    .count(),
                2
            );
            let statuses = events
                .iter()
                .filter(|e| matches!(e.kind, EventKind::ActivityDelta))
                .map(|e| {
                    let event: PublicSourceEvent =
                        serde_json::from_value(e.payload.clone()).unwrap();
                    let PublicSourceChange::ActivityDelta { delta, .. } = event.change else {
                        unreachable!()
                    };
                    delta[0].members()["value"].as_str().unwrap().to_owned()
                })
                .collect::<Vec<_>>();
            assert!(
                statuses.contains(&"cancelling".into()) && statuses.contains(&"cancelled".into())
            );
            agent
                .close_session_resources(ctx.session(), Duration::from_secs(5))
                .await?;
        }
        Ok(())
    }
    async fn close(&mut self) -> Result {
        let Self {
            agent,
            root,
            cleanup,
            cwd,
            ..
        } = self;
        let closed = agent.close(Duration::from_secs(5)).await;
        assert_eq!(closed.observation, crabber::CloseObservation::Complete);
        assert!(closed.local_drained());
        let groups = std::fs::read_dir(root.path())?
            .map(|entry| entry.unwrap())
            .filter_map(|entry| {
                entry
                    .file_name()
                    .to_str()?
                    .strip_prefix("pgid.")?
                    .parse::<i32>()
                    .ok()
            })
            .filter_map(rustix::process::Pid::from_raw)
            .collect::<Vec<_>>();
        assert_eq!(groups.len(), 6);
        tokio::time::timeout(Duration::from_secs(5), async {
            while groups
                .iter()
                .any(|pid| rustix::process::test_kill_process_group(*pid).is_ok())
            {
                tokio::time::sleep(Duration::from_millis(10)).await;
            }
        })
        .await?;
        cleanup.disarm_groups();
        assert_eq!(std::env::current_dir()?, *cwd);
        Ok(())
    }
}
pub async fn run() -> Result {
    let mut host = Host::new()?;
    let provider = host.start().await?;
    host.verify_initial(&provider).await?;
    host.close_one().await?;
    host.durable_inputs().await?;
    host.drain_remaining().await?;
    host.close().await?;
    println!(
        "{}",
        json!({"workspaces":2,"sessions":3,"inline":3,"detached":3,"nested":3,"custom_effects":3,"permission_ask_denied":true,"durable_answer_replayed":true,"local_drained":true,"cwd_unchanged":true})
    );
    Ok(())
}
