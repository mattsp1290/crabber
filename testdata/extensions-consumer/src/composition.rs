use crate::{
    agent_support, common::*, custom, factory_options, owned_process, policies, scripted::Scripts,
};
use crabber::{
    Agent, AgentConfig, ExecutionContext,
    core::{
        EventKind, EventRecord, InputFingerprint, PublicInvocationEvent, PublicInvocationStatus,
        PublicSourceChange, PublicSourceEvent, PublicSourceIdentity, RunStatus, SessionContext,
        SessionId, ToolCallRecord,
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
            delegate,
            ..
        } = self;
        for ctx in &contexts[1..] {
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
            let calls = records(&**store, ctx.session().session_id()).await;
            let owner = delegate
                .owner_report(
                    ctx.session().workspace_id(),
                    Some(ctx.session().session_id()),
                )
                .unwrap();
            assert_eq!(owner.live, 0);
            verify_activity_events(
                &events,
                ctx.session(),
                &calls,
                &reports,
                owner.publication_failures,
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
struct ActivityTrace<'a> {
    source: &'a PublicSourceIdentity,
    mode: &'static str,
    statuses: &'static [&'static str],
    activity_id: Option<String>,
    last: Option<usize>,
    count: u64,
}
impl<'a> ActivityTrace<'a> {
    fn new(
        source: &'a PublicSourceIdentity,
        mode: &'static str,
        statuses: &'static [&'static str],
    ) -> Self {
        Self {
            source,
            mode,
            statuses,
            activity_id: None,
            last: None,
            count: 0,
        }
    }
    fn observe(&mut self, event: &PublicSourceEvent, kind: &EventKind) {
        assert_eq!(event.version, 2);
        assert_eq!(&event.source, self.source);
        let (id, activity_type, status) = match &event.change {
            PublicSourceChange::ActivitySnapshot { activity } => {
                assert_eq!(*kind, EventKind::ActivitySnapshot);
                assert_eq!(
                    activity.content(),
                    &json!({"status":"running", "mode":self.mode, "profile":"synthetic"})
                );
                (activity.id(), activity.activity_type(), "running")
            }
            PublicSourceChange::ActivityDelta {
                activity_id,
                activity_type,
                delta,
            } => {
                assert_eq!(*kind, EventKind::ActivityDelta);
                let [operation] = &delta[..] else {
                    panic!("one status replacement")
                };
                let members = operation.members();
                assert_eq!(members.len(), 3);
                assert_eq!(members["op"], "replace");
                assert_eq!(members["path"], "/status");
                let status = members["value"].as_str().unwrap();
                assert_ne!(status, "running");
                (activity_id.as_str(), activity_type.as_str(), status)
            }
            _ => panic!("activity publication"),
        };
        assert_eq!(activity_type, "crabber-extensions.delegate-task");
        assert!(!id.is_empty());
        if let Some(expected) = &self.activity_id {
            assert_eq!(id, expected);
        } else {
            self.activity_id = Some(id.to_owned());
        }
        let slot = self
            .statuses
            .iter()
            .position(|expected| *expected == status)
            .expect("expected status");
        assert!(self.last.is_none_or(|previous| previous < slot));
        self.last = Some(slot);
        self.count += 1;
    }
}
fn delegate_source(
    events: &[EventRecord],
    session: &SessionContext,
    call: &ToolCallRecord,
    reports: &[crabber::SessionInvocationReport],
) -> PublicSourceIdentity {
    let starts = events
        .iter()
        .filter(|event| event.kind == EventKind::InvocationStarted)
        .map(|event| {
            (
                event,
                serde_json::from_value::<PublicInvocationEvent>(event.payload.clone()).unwrap(),
            )
        })
        .filter(|(_, event)| {
            event
                .source
                .invocation
                .as_ref()
                .unwrap()
                .parent_invocation
                .is_none()
        })
        .collect::<Vec<_>>();
    let [(start_record, start)] = &starts[..] else {
        panic!("one root delegate invocation")
    };
    assert_eq!(&start_record.session_id, session.session_id());
    assert_eq!(start_record.run_id, call.run_id);
    assert_eq!(start.version, 1);
    assert_eq!(start.status, PublicInvocationStatus::Started);
    assert_eq!(start.source.workspace_id, session.workspace_id());
    assert_eq!(&start.source.session_id, session.session_id());
    assert_eq!(start.source.run_id, call.run_id);
    assert_eq!(start.source.call_id.as_ref(), Some(&call.id));
    let invocation = start.source.invocation.as_ref().unwrap();
    assert!(invocation.parent_generation.is_none());
    assert_eq!(invocation.parent_call_id.as_ref(), Some(&call.id));
    assert!(reports.iter().any(|report| report.id.0 == invocation.id.0
        && &report.session_id == session.session_id()
        && report.local_joined));
    let finishes = events
        .iter()
        .filter(|event| event.kind == EventKind::InvocationFinished)
        .map(|event| {
            (
                event,
                serde_json::from_value::<PublicInvocationEvent>(event.payload.clone()).unwrap(),
            )
        })
        .filter(|(_, event)| event.source.invocation.as_ref().unwrap().id == invocation.id)
        .collect::<Vec<_>>();
    let [(finish_record, finish)] = &finishes[..] else {
        panic!("one delegate invocation finish")
    };
    assert_eq!(&finish_record.session_id, session.session_id());
    assert_eq!(finish_record.run_id, call.run_id);
    assert_eq!(finish.version, 1);
    assert_eq!(finish.status, PublicInvocationStatus::Finished);
    assert_eq!(finish.source, start.source);
    start.source.clone()
}
fn verify_activity_events(
    events: &[EventRecord],
    session: &SessionContext,
    calls: &[ToolCallRecord],
    reports: &[crabber::SessionInvocationReport],
    failures: u64,
) {
    let call = |name| {
        let matching = calls
            .iter()
            .filter(|call| call.name == name)
            .collect::<Vec<_>>();
        let [call] = &matching[..] else {
            panic!("one delegate call")
        };
        *call
    };
    let inline = call(delegate::TOOL_NAME);
    let background = call(delegate::START_TOOL);
    assert_eq!(inline.run_id, background.run_id);
    let background_source = delegate_source(events, session, background, reports);
    let mut inline_source = background_source.clone();
    inline_source.call_id = Some(inline.id.clone());
    inline_source.invocation = None;
    let mut traces = [
        ActivityTrace::new(&inline_source, "inline", &["running", "completed"]),
        ActivityTrace::new(
            &background_source,
            "background",
            &["running", "cancelling", "cancelled"],
        ),
    ];
    for event in events.iter().filter(|event| {
        matches!(
            event.kind,
            EventKind::ActivitySnapshot | EventKind::ActivityDelta
        )
    }) {
        assert_eq!(&event.session_id, session.session_id());
        assert_eq!(event.run_id, inline.run_id);
        let publication: PublicSourceEvent = serde_json::from_value(event.payload.clone()).unwrap();
        let trace = traces
            .iter_mut()
            .find(|trace| trace.source == &publication.source)
            .expect("owned delegate source");
        trace.observe(&publication, &event.kind);
    }
    verify_activity_accounting(&traces, failures);
}
fn verify_activity_accounting(traces: &[ActivityTrace<'_>; 2], failures: u64) {
    if let [Some(inline), Some(background)] = [&traces[0].activity_id, &traces[1].activity_id] {
        assert_ne!(inline, background);
    }
    assert_eq!(
        traces
            .iter()
            .map(|trace| trace.count)
            .sum::<u64>()
            .checked_add(failures),
        Some(5),
        "every missing publication must have an actual same-owner failure"
    );
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

#[cfg(test)]
mod activity_tests {
    use super::*;
    use crabber::core::{
        AttemptGeneration, PublicActivity, PublicAttemptIdentity, PublicInvocationIdentity,
        PublicPatchOperation, PublicStateContents, SessionOwnerGeneration, SessionRevision,
    };

    fn sources() -> [PublicSourceIdentity; 2] {
        let inline = PublicSourceIdentity {
            workspace_id: "workspace".into(),
            session_id: "session".into(),
            run_id: "run".into(),
            attempt: PublicAttemptIdentity {
                id: "attempt".into(),
                generation: AttemptGeneration::INITIAL,
            },
            call_id: Some("inline".into()),
            invocation: None,
        };
        let mut background = inline.clone();
        background.call_id = Some("background".into());
        background.invocation = Some(PublicInvocationIdentity {
            id: "delegate".into(),
            generation: SessionOwnerGeneration::INITIAL,
            parent_invocation: None,
            parent_generation: None,
            parent_call_id: background.call_id.clone(),
        });
        [inline, background]
    }
    fn publications() -> Vec<(EventKind, PublicSourceEvent)> {
        let sources = sources();
        [(0, "running"), (0, "completed"), (1, "running"), (1, "cancelling"), (1, "cancelled")].into_iter().map(|(index, status)| {
            let id = if index == 0 { "inline-activity" } else { "background-activity" };
            let (kind, change) = if status == "running" {
                (EventKind::ActivitySnapshot, PublicSourceChange::ActivitySnapshot { activity: PublicActivity::new(id, "crabber-extensions.delegate-task", json!({"status":status,"mode":if index == 0 { "inline" } else { "background" },"profile":"synthetic"})).unwrap() })
            } else {
                (EventKind::ActivityDelta, PublicSourceChange::ActivityDelta { activity_id: id.into(), activity_type: "crabber-extensions.delegate-task".into(), delta: vec![PublicPatchOperation::new(json!({"op":"replace","path":"/status","value":status})).unwrap()] })
            };
            (kind, PublicSourceEvent { version: 2, source: sources[index].clone(), revision: SessionRevision::new(1).unwrap(), change, contents: PublicStateContents::default(), before: None, before_event_cursor: None })
        }).collect()
    }
    fn verify(events: &[(EventKind, PublicSourceEvent)], failures: u64) {
        let sources = sources();
        let mut traces = [
            ActivityTrace::new(&sources[0], "inline", &["running", "completed"]),
            ActivityTrace::new(
                &sources[1],
                "background",
                &["running", "cancelling", "cancelled"],
            ),
        ];
        for (kind, event) in events {
            traces
                .iter_mut()
                .find(|trace| trace.source == &event.source)
                .expect("owned source")
                .observe(event, kind);
        }
        verify_activity_accounting(&traces, failures);
    }
    fn rejects(events: &[(EventKind, PublicSourceEvent)], failures: u64) {
        assert!(std::panic::catch_unwind(|| verify(events, failures)).is_err());
    }
    #[test]
    fn actual_failure_accounting_requires_each_missing_transition() {
        let events = publications();
        verify(&events, 0);
        verify(&events[..2], 3);
        verify(&[], 5);
        rejects(&events[..2], 0);
        rejects(&events, 1);
        rejects(&events[..4], 0);
        rejects(&events[..4], 2);
    }
    #[test]
    fn foreign_sources_and_nested_invocations_are_rejected() {
        for field in [
            "workspace",
            "session",
            "run",
            "call",
            "invocation",
            "generation",
            "parent",
        ] {
            let mut events = publications();
            let source = &mut events[2].1.source;
            match field {
                "workspace" => source.workspace_id = "foreign".into(),
                "session" => source.session_id = "foreign".into(),
                "run" => source.run_id = "foreign".into(),
                "call" => source.call_id = Some("foreign".into()),
                "invocation" => source.invocation.as_mut().unwrap().id = "nested".into(),
                "generation" => {
                    source.invocation.as_mut().unwrap().generation =
                        SessionOwnerGeneration::INITIAL.next().unwrap()
                }
                "parent" => {
                    source.invocation.as_mut().unwrap().parent_invocation = Some("delegate".into())
                }
                _ => unreachable!(),
            }
            rejects(&events, 0);
        }
    }
    #[test]
    fn duplicate_reversed_or_mismatched_activities_are_rejected() {
        let mut events = publications();
        events.push(events[2].clone());
        rejects(&events, 0);
        let mut events = publications();
        events.swap(3, 4);
        rejects(&events, 0);
        let mut events = publications();
        let PublicSourceChange::ActivityDelta { activity_id, .. } = &mut events[3].1.change else {
            unreachable!()
        };
        *activity_id = "other".into();
        rejects(&events, 0);
    }
    #[test]
    fn snapshot_shape_type_and_shared_activity_identity_are_rejected() {
        for (activity_type, content) in [
            (
                "other",
                json!({"status":"running","mode":"background","profile":"synthetic"}),
            ),
            (
                "crabber-extensions.delegate-task",
                json!({"status":"running","mode":"inline","profile":"synthetic"}),
            ),
            (
                "crabber-extensions.delegate-task",
                json!({"status":"running","mode":"background","profile":"other"}),
            ),
        ] {
            let mut events = publications();
            events[2].1.change = PublicSourceChange::ActivitySnapshot {
                activity: PublicActivity::new("background-activity", activity_type, content)
                    .unwrap(),
            };
            rejects(&events, 0);
        }
        let mut events = publications();
        events[0].0 = EventKind::ActivityDelta;
        rejects(&events, 0);
        let mut events = publications();
        for (_, event) in &mut events[2..] {
            match &mut event.change {
                PublicSourceChange::ActivitySnapshot { activity } => {
                    *activity = PublicActivity::new(
                        "inline-activity",
                        activity.activity_type(),
                        activity.content().clone(),
                    )
                    .unwrap();
                }
                PublicSourceChange::ActivityDelta { activity_id, .. } => {
                    *activity_id = "inline-activity".into()
                }
                _ => unreachable!(),
            }
        }
        rejects(&events, 0);
    }
    #[test]
    fn wrong_status_or_delta_shape_is_rejected() {
        for patch in [
            json!({"op":"replace","path":"/status","value":"timed_out"}),
            json!({"op":"replace","path":"/status","value":"completed"}),
            json!({"op":"add","path":"/status","value":"cancelling"}),
            json!({"op":"replace","path":"/other","value":"cancelling"}),
        ] {
            let mut events = publications();
            let PublicSourceChange::ActivityDelta { delta, .. } = &mut events[3].1.change else {
                unreachable!()
            };
            *delta = vec![PublicPatchOperation::new(patch).unwrap()];
            rejects(&events, 0);
        }
        let mut events = publications();
        let PublicSourceChange::ActivityDelta { delta, .. } = &mut events[3].1.change else {
            unreachable!()
        };
        delta.push(delta[0].clone());
        rejects(&events, 0);
    }
}
