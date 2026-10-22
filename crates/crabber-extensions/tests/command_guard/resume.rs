use super::runtime::PrepareProbe;
use super::*;
use crabber::{RuntimeError, core::RunId};

struct Paused {
    store: Arc<MemoryStore>,
    run: RunId,
    session: SessionId,
    guard: Arc<CommandGuard>,
    agent: Agent,
    count: Arc<AtomicUsize>,
}
async fn pause(mounted: bool, probe: Option<Arc<PrepareProbe>>) -> Paused {
    let store = Arc::new(MemoryStore::new());
    let g = guard();
    let count = Arc::new(AtomicUsize::new(0));
    let mut extensions: Vec<(Arc<dyn Extension>, Scope)> = vec![];
    if mounted {
        extensions.push((g.clone(), Scope::Global));
    }
    if let Some(probe) = probe {
        extensions.push((probe, Scope::Global));
    }
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![calls(&[
            ("shell", "cmd", "blocked SAVED_MARKER"),
            ("shell", "cmd", "echo ok"),
        ])])),
        CountingPolicy::new(PermissionDecision::Allow, true),
        extensions,
        vec![tool("shell", "cmd", count.clone())],
        None,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    let id = run.run_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 2);
    assert!(records.iter().all(|r| r.status == ToolCallStatus::Pending));
    assert_eq!(
        g.stats(),
        if mounted {
            Stats {
                abstain: 1,
                rule_match: 1,
                ..Stats::default()
            }
        } else {
            Stats::default()
        }
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    Paused {
        store,
        run: id,
        session,
        guard: g,
        agent: a,
        count,
    }
}
fn resumer(
    paused: &Paused,
    g: Arc<CommandGuard>,
    mounted: bool,
    scope: Scope,
    probe: Option<Arc<PrepareProbe>>,
) -> (Agent, Arc<CountingPolicy>) {
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let mut extensions: Vec<(Arc<dyn Extension>, Scope)> = vec![];
    if mounted {
        extensions.push((g, scope));
    }
    if let Some(probe) = probe {
        extensions.push((probe, Scope::Global));
    }
    (
        build(
            paused.store.clone(),
            Arc::new(FakeProvider::scripted(vec![done()])),
            p.clone(),
            extensions,
            vec![tool("shell", "cmd", paused.count.clone())],
            None,
        ),
        p,
    )
}
async fn assert_resumed(
    paused: &Paused,
    a: &Agent,
    g: &CommandGuard,
    p: &CountingPolicy,
    guarded: bool,
) {
    assert_eq!(
        a.resume(&paused.run).await.unwrap().status,
        RunStatus::Completed
    );
    let records = snapshot_tools(&paused.store, &paused.session).await;
    assert_eq!(records.len(), 2);
    let blocked = records
        .iter()
        .find(|r| r.arguments["cmd"] == "blocked SAVED_MARKER")
        .unwrap();
    assert_eq!(
        blocked.status,
        if guarded {
            ToolCallStatus::Failed
        } else {
            ToolCallStatus::Completed
        }
    );
    if guarded {
        assert_eq!(
            serde_json::from_str::<Value>(&result_text(blocked)).unwrap(),
            json!("permission denied")
        );
    }
    assert_eq!(
        records
            .iter()
            .find(|r| r.arguments["cmd"] == "echo ok")
            .unwrap()
            .status,
        ToolCallStatus::Completed
    );
    assert_eq!(p.count(), if guarded { 1 } else { 2 });
    assert_eq!(
        paused.count.load(Ordering::SeqCst),
        if guarded { 1 } else { 2 }
    );
    assert_eq!(
        g.stats(),
        if guarded {
            Stats {
                abstain: 1,
                rule_match: 1,
                ..Stats::default()
            }
        } else {
            Stats::default()
        }
    );
    assert_eq!(
        paused.guard.stats(),
        if guarded {
            Stats {
                abstain: 1,
                rule_match: 1,
                ..Stats::default()
            }
        } else {
            Stats::default()
        }
    );
}
#[tokio::test]
async fn paused_pending_calls_are_guarded_again_on_resume() {
    let first = Arc::new(AtomicUsize::new(0));
    let second = Arc::new(AtomicUsize::new(0));
    let paused = pause(
        true,
        Some(Arc::new(PrepareProbe {
            count: first.clone(),
            rewrite: false,
            second_guard: None,
        })),
    )
    .await;
    // ToolPrepare runs once per staged call, hence two for this two-call turn.
    assert_eq!(first.load(Ordering::SeqCst), 2);
    let g = guard();
    let (a, p) = resumer(
        &paused,
        g.clone(),
        true,
        Scope::Global,
        Some(Arc::new(PrepareProbe {
            count: second.clone(),
            rewrite: false,
            second_guard: None,
        })),
    );
    assert_resumed(&paused, &a, &g, &p, true).await;
    assert_eq!(second.load(Ordering::SeqCst), 0);
    assert_eq!(first.load(Ordering::SeqCst), 2);
    a.close_extensions().await.unwrap();
    paused.agent.close_extensions().await.unwrap();
}
// Each labelled row changes exactly one frozen policy field (the command-byte
// increment remains below max_analysis_bytes). Keep the fourteen limit rows explicit.
fn mutations() -> Vec<(&'static str, Options)> {
    let mut rows = Vec::new();
    macro_rules! changed {
        ($name:literal,$o:ident,$body:expr) => {{
            let mut $o = options();
            $body;
            rows.push(($name, $o));
        }};
    }
    changed!("rule id", o, o.rules[0].id = "other".into());
    changed!("executable", o, o.rules[0].executable = "other".into());
    changed!("prefix token", o, o.rules[1].arg_prefix[0] = "fetch".into());
    changed!("binding tool", o, o.bindings[0].tool_name = "other".into());
    changed!(
        "binding field",
        o,
        o.bindings[0].command_field = "script".into()
    );
    changed!("binding dialect", o, o.bindings[0].dialect = Dialect::Bash);
    changed!("max_bindings", o, o.limits.max_bindings += 1);
    changed!("max_rules", o, o.limits.max_rules += 1);
    changed!("max_rule_bytes", o, o.limits.max_rule_bytes += 1);
    changed!("max_prefix_args", o, o.limits.max_prefix_args += 1);
    changed!("max_json_depth", o, o.limits.max_json_depth += 1);
    changed!("max_json_nodes", o, o.limits.max_json_nodes += 1);
    changed!("max_command_bytes", o, o.limits.max_command_bytes += 1);
    changed!("max_analysis_bytes", o, o.limits.max_analysis_bytes += 1);
    changed!("max_ast_nodes", o, o.limits.max_ast_nodes += 1);
    changed!("max_ast_depth", o, o.limits.max_ast_depth += 1);
    changed!("max_words", o, o.limits.max_words += 1);
    changed!("max_word_bytes", o, o.limits.max_word_bytes += 1);
    changed!("max_wrapper_depth", o, o.limits.max_wrapper_depth += 1);
    changed!("max_in_flight", o, o.limits.max_in_flight += 1);
    rows
}
async fn snapshot_debug(paused: &Paused) -> String {
    format!(
        "{:?}",
        paused
            .store
            .snapshot(SnapshotRequest {
                session_id: paused.session.clone(),
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 100_000,
                    encoded_bytes: 1_000_000
                },
                continuation: None
            })
            .await
            .unwrap()
    )
}
#[tokio::test]
async fn resume_refuses_every_policy_drift_before_mutation() {
    let mut rows = mutations()
        .into_iter()
        .map(|(label, o)| (label, o, true, true))
        .collect::<Vec<_>>();
    rows.push(("guard removed", options(), true, false));
    rows.push(("guard added", options(), false, true));
    assert_eq!(rows.len(), 22);
    for (label, o, original, mounted) in rows {
        eprintln!("resume drift: {label}");
        let paused = pause(original, None).await;
        let g = guard_with(o);
        let before_run = format!("{:?}", paused.store.get_run(&paused.run).await.unwrap());
        let before_snapshot = snapshot_debug(&paused).await;
        let (a, p) = resumer(&paused, g.clone(), mounted, Scope::Global, None);
        assert!(
            matches!(a.resume(&paused.run).await, Err(RuntimeError::PlanChanged)),
            "{label}"
        );
        assert_eq!(
            format!("{:?}", paused.store.get_run(&paused.run).await.unwrap()),
            before_run,
            "{label}"
        );
        assert_eq!(snapshot_debug(&paused).await, before_snapshot, "{label}");
        assert_eq!(paused.count.load(Ordering::SeqCst), 0);
        assert_eq!(p.count(), 0);
        assert_eq!(g.stats(), Stats::default());
        let equal = guard();
        let (third, p) = resumer(&paused, equal.clone(), original, Scope::Global, None);
        assert_resumed(&paused, &third, &equal, &p, original).await;
        a.close_extensions().await.unwrap();
        third.close_extensions().await.unwrap();
        paused.agent.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn scope_drift_is_detected_before_mutation() {
    let paused = pause(true, None).await;
    let g = guard();
    let (a, p) = resumer(
        &paused,
        g.clone(),
        true,
        Scope::Session(paused.session.clone()),
        None,
    );
    let before = snapshot_debug(&paused).await;
    assert!(matches!(
        a.resume(&paused.run).await,
        Err(RuntimeError::PlanChanged)
    ));
    assert_eq!(snapshot_debug(&paused).await, before);
    assert_eq!(p.count(), 0);
    assert_eq!(paused.count.load(Ordering::SeqCst), 0);
    assert_eq!(g.stats(), Stats::default());
    a.close_extensions().await.unwrap();
    paused.agent.close_extensions().await.unwrap();
}
#[tokio::test]
async fn unchanged_options_in_a_new_process_shape_resume() {
    let paused = pause(true, None).await;
    let g = guard_with(Options {
        bindings: default_bindings(),
        rules: rules(),
        limits: limits(),
    });
    let (a, p) = resumer(&paused, g.clone(), true, Scope::Global, None);
    assert_resumed(&paused, &a, &g, &p, true).await;
    a.close_extensions().await.unwrap();
    paused.agent.close_extensions().await.unwrap();
}
