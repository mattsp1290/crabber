//! Credential-free proofs against the in-memory store, public API only.

use async_trait::async_trait;
use crabber::{
    AdmissionKey, AdmissionOptions, InputFingerprint, RuntimeError,
    core::{CoreError, RunStatus},
    session::MemoryStore,
};
use crabber::{
    FakeProvider, PermissionDecision, StaticPolicy,
    core::{EventKind, ManualClock},
    extension::{Registry, Scope},
    runtime::{ApprovalRequester, INTERRUPT_SETTLEMENT_BOUND, Orchestrator, Request},
    session::Store,
};
use serde_json::Value;
use std::sync::Arc;
use std::{sync::Mutex, time::Duration};
use tokio::{
    sync::{Notify, oneshot},
    time::timeout,
};
use workspace_context_pinned_probe::{
    CANCEL_TOOL, Identity, Observations, ProbeExtension, ROOT_TOOL, TOOL, config, host,
    host_with_policy, identity, named_tool_call, text, tool_call,
};

const NONE: [Identity; 0] = [];

fn options() -> AdmissionOptions {
    AdmissionOptions {
        key: AdmissionKey::new("probe-key").unwrap(),
        fingerprint: InputFingerprint::new("a".repeat(64)).unwrap(),
        behavior_fingerprint: InputFingerprint::new("b".repeat(64)).unwrap(),
    }
}

fn mismatch<T>(result: &Result<T, RuntimeError>) {
    assert!(matches!(
        result,
        Err(RuntimeError::Store(CoreError::SessionIdentityMismatch))
    ));
}

#[tokio::test]
async fn two_sessions_observe_their_own_workspace_across_runs() {
    let store = Arc::new(MemoryStore::new());
    let mut sessions = Vec::new();
    for (workspace_id, directory) in [("ws-a", "/srv/a"), ("ws-b", "/srv/b")] {
        let seen = Arc::new(Observations::default());
        let agent = host(
            &seen,
            vec![
                tool_call(),
                named_tool_call(ROOT_TOOL),
                text("one"),
                tool_call(),
                named_tool_call(ROOT_TOOL),
                text("two"),
            ],
            config(workspace_id, directory),
            false,
        )
        .store(store.clone())
        .build()
        .unwrap();
        let run = agent.prompt(None, "first").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        // A later run in the same session observes the same values.
        agent
            .prompt(Some(session.clone()), "second")
            .await
            .unwrap()
            .done()
            .await
            .unwrap();
        let expected = identity(Some(workspace_id), Some(directory));
        assert_eq!(seen.tool(), vec![expected.clone(); 4]);
        // The prompt contributor sees what the tool sees, on every turn.
        assert_eq!(seen.assemble(), vec![expected.clone(); 6]);
        for name in [TOOL, ROOT_TOOL] {
            let entries: Vec<_> = seen
                .executions()
                .into_iter()
                .filter(|entry| entry.name == name)
                .collect();
            assert_eq!(entries.len(), 2);
            assert!(
                entries
                    .iter()
                    .all(|entry| entry.identity == expected && !entry.cancelled_at_entry)
            );
        }
        sessions.push(session);
    }
    assert_ne!(sessions[0], sessions[1]);
}

#[tokio::test]
async fn drifted_identity_is_rejected_and_the_tool_is_not_invoked() {
    let store = Arc::new(MemoryStore::new());
    let seen = Arc::new(Observations::default());
    let agent = host(
        &seen,
        vec![tool_call(), text("done")],
        config("ws-a", "/srv/a"),
        false,
    )
    .store(store.clone())
    .build()
    .unwrap();
    let run = agent.prompt(None, "first").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();

    for (workspace_id, directory) in [("ws-a", "/srv/replacement"), ("ws-b", "/srv/a")] {
        let drifted = Arc::new(Observations::default());
        let other = host(
            &drifted,
            vec![tool_call(), text("never")],
            config(workspace_id, directory),
            false,
        )
        .store(store.clone())
        .build()
        .unwrap();
        mismatch(&other.prompt(Some(session.clone()), "again").await);
        mismatch(
            &other
                .prompt_keyed(session.clone(), "again", options())
                .await,
        );
        mismatch(
            &other
                .recover_admission(session.clone(), "again", options())
                .await,
        );
        assert_eq!(drifted.tool(), NONE);
        assert_eq!(drifted.assemble(), NONE);
    }
    assert_eq!(seen.tool().len(), 1);
}

#[tokio::test]
async fn empty_identity_is_unavailable_not_a_host_default_or_model_argument() {
    let store = Arc::new(MemoryStore::new());
    let seen = Arc::new(Observations::default());
    // The session persists an empty workspace ID and directory.
    let first = host(&seen, vec![tool_call()], config("", ""), true)
        .store(store.clone())
        .build()
        .unwrap();
    let run = first.prompt(None, "first").await.unwrap();
    let (session, run_id) = (run.session_id().clone(), run.run_id().clone());
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    assert_eq!(seen.tool(), NONE);

    // A second host with different defaults ("default", ".") resumes the run;
    // the model's tool arguments name yet another workspace. Neither leaks.
    let resumed = Arc::new(Observations::default());
    let second = host(&resumed, vec![text("done")], config("default", "."), false)
        .store(store.clone())
        .build()
        .unwrap();
    assert_eq!(
        second.resume(&run_id).await.unwrap().status,
        RunStatus::Completed
    );
    assert_eq!(resumed.tool(), [identity(None, None)]);
    assert_eq!(resumed.assemble(), [identity(None, None)]);
    // That host cannot admit into the session under its own defaults.
    mismatch(&second.prompt(Some(session), "again").await);
}

#[tokio::test]
async fn one_empty_field_is_unavailable_while_the_other_is_exposed() {
    for (workspace_id, directory, expected) in [
        ("ws-only", "", identity(Some("ws-only"), None)),
        ("", "/srv/root-only", identity(None, Some("/srv/root-only"))),
    ] {
        let seen = Arc::new(Observations::default());
        let agent = host(
            &seen,
            vec![tool_call(), text("done")],
            config(workspace_id, directory),
            false,
        )
        .memory()
        .build()
        .unwrap();
        let run = agent.prompt(None, "first").await.unwrap();
        run.done().await.unwrap();
        assert_eq!(seen.tool(), std::slice::from_ref(&expected));
        assert_eq!(seen.assemble(), vec![expected; 2]);
    }
}

#[tokio::test]
async fn permission_denial_never_invokes_the_executor() {
    let seen = Arc::new(Observations::default());
    let agent = host_with_policy(
        &seen,
        vec![tool_call(), text("done")],
        config("ws-deny", "/srv/deny"),
        Arc::new(StaticPolicy::new(PermissionDecision::Deny)),
    )
    .memory()
    .build()
    .unwrap();
    let run = agent.prompt(None, "denied").await.unwrap();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    assert_eq!(seen.executions(), []);
    assert_eq!(
        seen.assemble(),
        vec![identity(Some("ws-deny"), Some("/srv/deny")); 2]
    );
}

#[tokio::test]
async fn cancellation_is_available_to_a_running_tool() {
    let seen = Arc::new(Observations::default());
    let agent = host(
        &seen,
        vec![named_tool_call(CANCEL_TOOL)],
        config("ws-cancel", "/srv/cancel"),
        false,
    )
    .memory()
    .build()
    .unwrap();
    let run = agent.prompt(None, "wait").await.unwrap();
    timeout(Duration::from_secs(2), seen.started.notified())
        .await
        .expect("tool started");
    run.interrupt();
    assert_eq!(
        timeout(INTERRUPT_SETTLEMENT_BOUND, run.done())
            .await
            .expect("interrupt settled")
            .unwrap()
            .status,
        RunStatus::Interrupted
    );
    timeout(
        Duration::from_secs(2),
        seen.cancellation_observed.notified(),
    )
    .await
    .expect("tool observed cancellation");
    let entries = seen.executions();
    assert_eq!(entries.len(), 1);
    assert_eq!(entries[0].name, CANCEL_TOOL);
    assert_eq!(
        entries[0].identity,
        identity(Some("ws-cancel"), Some("/srv/cancel"))
    );
    assert!(!entries[0].cancelled_at_entry);
}

struct GatedApprover {
    entered: Mutex<Option<oneshot::Sender<()>>>,
    release: Arc<Notify>,
}
#[async_trait]
impl ApprovalRequester for GatedApprover {
    async fn approve(&self, _tool: &crabber::core::ToolInfo, _arguments: &Value) -> bool {
        if let Some(sender) = self.entered.lock().unwrap().take() {
            let _ = sender.send(());
        }
        self.release.notified().await;
        true
    }
}

#[tokio::test]
async fn stale_fence_never_invokes_the_executor() {
    let now = time::OffsetDateTime::UNIX_EPOCH;
    let clock = Arc::new(ManualClock::new(now));
    let store = Arc::new(MemoryStore::with_clock(clock.clone()));
    let seen = Arc::new(Observations::default());
    let registry = Registry::new();
    let _mount = registry
        .mount(Arc::new(ProbeExtension(seen.clone())), Scope::Global)
        .await
        .unwrap();
    let (entered_tx, entered_rx) = oneshot::channel();
    let release = Arc::new(Notify::new());
    let runtime = Orchestrator::builder()
        .store(store.clone())
        .resolver(Arc::new(FakeProvider::scripted(vec![
            tool_call(),
            text("never"),
        ])))
        .plan_provider(Arc::new(registry))
        .clock(clock.clone())
        .heartbeat_interval(Duration::from_millis(10))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Ask)))
        .approver(Arc::new(GatedApprover {
            entered: Mutex::new(Some(entered_tx)),
            release: release.clone(),
        }))
        .build()
        .unwrap();
    let run = runtime
        .start(Request {
            session_id: None,
            workspace_id: "ws-fence".into(),
            directory: "/srv/fence".into(),
            title: "fence".into(),
            text: "wait".into(),
            selection: config("ws-fence", "/srv/fence").selection,
            system_prompt: None,
        })
        .await
        .unwrap();
    let session = run.session_id().clone();
    let run_id = run.run_id().clone();
    timeout(Duration::from_secs(2), entered_rx)
        .await
        .expect("approval entered")
        .unwrap();
    clock.set(now + time::Duration::seconds(31));
    let replacement = store
        .claim_expired_run(&run_id, "replacement owner")
        .await
        .unwrap();
    let result = timeout(Duration::from_secs(2), run.done())
        .await
        .expect("lease loss settled");
    release.notify_one();
    assert!(matches!(result, Err(RuntimeError::LeaseLost)));
    assert_eq!(seen.executions(), []);
    assert_eq!(
        seen.assemble(),
        [identity(Some("ws-fence"), Some("/srv/fence"))]
    );
    let events = store.list_events(&session, None, 100).await.unwrap();
    assert!(
        events
            .iter()
            .any(|event| event.kind == EventKind::PermissionRequested)
    );
    assert!(
        !events
            .iter()
            .any(|event| event.kind == EventKind::ToolCallSettled)
    );
    let stored = store.get_run(&run_id).await.unwrap().unwrap();
    assert_eq!(stored.owner, "replacement owner");
    assert_eq!(stored.claim_token, replacement.claim_token);
}
