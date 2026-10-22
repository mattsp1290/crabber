use super::*;

fn assert_denied(record: &ToolCallRecord) {
    assert_eq!(record.status, ToolCallStatus::Failed);
    assert_eq!(
        serde_json::from_str::<Value>(&result_text(record)).unwrap(),
        json!("permission denied")
    );
}
#[tokio::test]
async fn denied_call_never_executes_and_persists_fixed_text() {
    let mut o = options();
    o.rules = vec![Rule {
        id: "RULE_ID_SENTINEL".into(),
        executable: "denyme".into(),
        arg_prefix: vec![],
    }];
    let g = guard_with(o);
    let store = Arc::new(MemoryStore::new());
    let counter = Arc::new(AtomicUsize::new(0));
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            ("shell", "cmd", "echo ok"),
            ("shell", "cmd", "denyme SECRET_MARKER"),
        ]),
        done(),
    ]));
    let a = build(
        store.clone(),
        provider.clone(),
        p.clone(),
        vec![(g.clone(), Scope::Global)],
        vec![tool("shell", "cmd", counter.clone())],
        None,
    );
    let mut run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    let mut events = run.events();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 2);
    let denied = records
        .iter()
        .find(|r| r.arguments["cmd"] == "denyme SECRET_MARKER")
        .unwrap();
    assert_denied(denied);
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Completed)
            .count(),
        1
    );
    assert_eq!(counter.load(Ordering::SeqCst), 1);
    assert_eq!(p.count(), 1);
    let next = format!("{:?}", provider.requests()[1]);
    assert!(next.contains("permission denied"));
    assert_eq!(next.matches("SECRET_MARKER").count(), 1);
    for marker in ["SECRET_MARKER", "rule-match", "RULE_ID_SENTINEL", GUARD_ID] {
        assert!(!result_text(denied).contains(marker));
        assert!(!format!("{g:?}").contains(marker) || marker == GUARD_ID);
    }
    for marker in ["rule-match", "RULE_ID_SENTINEL"] {
        assert!(!next.contains(marker));
    }
    let mut lifecycle = Vec::new();
    while let Ok(Some(event)) = events.recv().await {
        if event.payload.get("call_id") == Some(&json!(denied.id))
            && matches!(
                event.kind,
                crabber::core::EventKind::ToolCallPending
                    | crabber::core::EventKind::ToolCallRunning
                    | crabber::core::EventKind::ToolCallSettled
            )
        {
            lifecycle.push(event.kind.clone());
        }
    }
    assert_eq!(
        lifecycle,
        vec![
            crabber::core::EventKind::ToolCallPending,
            crabber::core::EventKind::ToolCallRunning,
            crabber::core::EventKind::ToolCallSettled
        ]
    );
    assert_eq!(
        g.stats(),
        Stats {
            abstain: 1,
            rule_match: 1,
            ..Stats::default()
        }
    );
    a.close_extensions().await.unwrap();
}
#[tokio::test]
async fn abstention_preserves_host_permission_authority() {
    for decision in [PermissionDecision::Deny, PermissionDecision::Ask] {
        let g = guard();
        let store = Arc::new(MemoryStore::new());
        let counter = Arc::new(AtomicUsize::new(0));
        let p = CountingPolicy::new(decision, false);
        let a = build(
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                call("shell", "cmd", "echo ok"),
                done(),
            ])),
            p.clone(),
            vec![(g.clone(), Scope::Global)],
            vec![tool("shell", "cmd", counter.clone())],
            None,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        assert_eq!(
            snapshot_tools(&store, &session).await[0].status,
            if matches!(decision, PermissionDecision::Ask) {
                ToolCallStatus::Pending
            } else {
                ToolCallStatus::Failed
            }
        );
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(p.count(), 1);
        assert_eq!(
            g.stats(),
            Stats {
                abstain: 1,
                ..Stats::default()
            }
        );
        a.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn structural_denials_through_runtime() {
    for (script, class) in [
        ("echo \"".into(), Outcome::InvalidCommand),
        ("$TOOL SECRET_MARKER".into(), Outcome::Unanalysable),
        ("trap 'echo harmless' EXIT".into(), Outcome::Unanalysable),
        (
            "printf -v 'a[$(echo harmless)0]' x".into(),
            Outcome::Unanalysable,
        ),
        ("x".repeat(4097), Outcome::AnalysisLimit),
    ] {
        let g = guard();
        assert_eq!(g.policy().analyze("shell", &json!({"cmd":script})), class);
        let store = Arc::new(MemoryStore::new());
        let count = Arc::new(AtomicUsize::new(0));
        let p = CountingPolicy::new(PermissionDecision::Allow, false);
        let a = build(
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                call("shell", "cmd", &script),
                done(),
            ])),
            p.clone(),
            vec![(g.clone(), Scope::Global)],
            vec![tool("shell", "cmd", count.clone())],
            None,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        assert_denied(&snapshot_tools(&store, &session).await[0]);
        let want = match class {
            Outcome::InvalidCommand => Stats {
                invalid_command: 1,
                ..Stats::default()
            },
            Outcome::Unanalysable => Stats {
                unanalysable: 1,
                ..Stats::default()
            },
            Outcome::AnalysisLimit => Stats {
                analysis_limit: 1,
                ..Stats::default()
            },
            _ => panic!("class"),
        };
        assert_eq!(g.stats(), want);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(p.count(), 0);
        a.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn background_job_start_binding_through_runtime() {
    let g = guard();
    let store = Arc::new(MemoryStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            calls(&[
                ("background_job_start", "command", "blocked"),
                ("background_job_start", "command", "echo ok"),
            ]),
            done(),
        ])),
        p.clone(),
        vec![(g.clone(), Scope::Global)],
        vec![tool("background_job_start", "command", count.clone())],
        None,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 2);
    assert_denied(
        records
            .iter()
            .find(|r| r.arguments["command"] == "blocked")
            .unwrap(),
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| r.status == ToolCallStatus::Completed)
            .count(),
        1
    );
    assert_eq!(count.load(Ordering::SeqCst), 1);
    assert_eq!(p.count(), 1);
    assert_eq!(
        g.stats(),
        Stats {
            abstain: 1,
            rule_match: 1,
            ..Stats::default()
        }
    );
    a.close_extensions().await.unwrap();
}
#[tokio::test]
async fn missing_or_non_string_field_denies_before_execution() {
    for args in [json!({}), json!({"cmd":3})] {
        let g = guard();
        let store = Arc::new(MemoryStore::new());
        let count = Arc::new(AtomicUsize::new(0));
        let p = CountingPolicy::new(PermissionDecision::Allow, false);
        let mut t = tool("shell", "cmd", count.clone());
        Arc::get_mut(&mut t).unwrap().info.parameters =
            json!({"type":"object","additionalProperties":true});
        let a = build(
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                raw_calls(&[("shell", args)]),
                done(),
            ])),
            p.clone(),
            vec![(g.clone(), Scope::Global)],
            vec![t],
            None,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        run.done().await.unwrap();
        assert_denied(&snapshot_tools(&store, &session).await[0]);
        assert_eq!(g.stats().invalid_command, 1);
        assert_eq!(count.load(Ordering::SeqCst), 0);
        assert_eq!(p.count(), 0);
        a.close_extensions().await.unwrap();
    }
}
#[tokio::test]
async fn parallel_mode_never_contends_within_one_run() {
    let g = guard_with(Options {
        limits: Limits {
            max_in_flight: 1,
            ..limits()
        },
        ..options()
    });
    let store = Arc::new(MemoryStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            calls(&[("shell", "cmd", "blocked"), ("shell", "cmd", "blocked")]),
            done(),
        ])),
        p.clone(),
        vec![(g.clone(), Scope::Global)],
        vec![tool("shell", "cmd", count.clone())],
        Some(ExecutionMode::Parallel { max: 2 }),
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 2);
    for r in records {
        assert_denied(&r);
    }
    assert_eq!(
        g.stats(),
        Stats {
            rule_match: 2,
            ..Stats::default()
        }
    );
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(p.count(), 0);
    a.close_extensions().await.unwrap();
}
#[tokio::test]
async fn redaction_cannot_change_denial_class() {
    use crabber_extensions::tool_result_redactor as redactor;
    let r = Arc::new(
        redactor::ToolResultRedactor::new(redactor::Options {
            order: -100,
            excluded_tools: vec![],
            additional_patterns: vec![redactor::Pattern {
                id: "denial".into(),
                expression: "permission".into(),
            }],
            limits: redactor::Limits {
                max_field_bytes: 4096,
                max_total_bytes: 16384,
                max_depth: 16,
                max_nodes: 100,
                max_matches_per_field: 16,
                max_patterns: 4,
                max_pattern_bytes: 256,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    );
    let g = guard();
    let store = Arc::new(MemoryStore::new());
    let count = Arc::new(AtomicUsize::new(0));
    let p = CountingPolicy::new(PermissionDecision::Allow, false);
    let a = build(
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call("shell", "cmd", "blocked"),
            done(),
        ])),
        p.clone(),
        vec![(g, Scope::Global), (r, Scope::Global)],
        vec![tool("shell", "cmd", count.clone())],
        None,
    );
    let run = a.prompt(None, "fixture").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records[0].status, ToolCallStatus::Failed);
    assert!(result_text(&records[0]).contains(redactor::PLACEHOLDER));
    assert!(!result_text(&records[0]).contains("permission"));
    assert_eq!(count.load(Ordering::SeqCst), 0);
    assert_eq!(p.count(), 0);
    a.close_extensions().await.unwrap();
}

#[cfg(unix)]
#[path = "runtime/canary.rs"]
mod canary;
#[path = "runtime/prepared.rs"]
mod prepared;
#[path = "runtime/stacks.rs"]
mod stacks;
pub(super) use prepared::PrepareProbe;
