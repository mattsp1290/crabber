use super::*;

async fn hard_cap_journeys() {
    let opts = Options {
        bindings: vec![
            default_bindings()[0].clone(),
            Binding {
                tool_name: "bash-tool".into(),
                command_field: "script".into(),
                dialect: Dialect::Bash,
            },
        ],
        limits: budgets::hard_caps(),
        ..options()
    };
    let g = guard_with(opts);
    let mut rows = budgets::stack_rows();
    rows.extend(budgets::nested_rows());
    // Depth 31 is the largest admitted substitution nesting at the hard cap 32.
    let n = 31;
    assert!(n >= MAX_AST_DEPTH - 2);
    let admitted = format!("{}blocked{}", "$(".repeat(n), ")".repeat(n));
    assert_eq!(
        g.policy().analyze_script(&admitted, Dialect::Posix),
        Outcome::RuleMatch
    );
    assert_eq!(
        g.policy()
            .analyze_script(&format!("$({admitted})"), Dialect::Posix),
        Outcome::AnalysisLimit
    );
    rows.push((admitted, Dialect::Posix));
    for (i, (script, dialect)) in rows.into_iter().enumerate() {
        eprintln!(
            "runtime hard-cap row {i} ({dialect:?}, {} bytes)",
            script.len()
        );
        let (name, field) = if dialect == Dialect::Posix {
            ("shell", "cmd")
        } else {
            ("bash-tool", "script")
        };
        let args = json!({field:script});
        let expected = g.policy().analyze(name, &args);
        let before = g.stats();
        let store = Arc::new(MemoryStore::new());
        let count = Arc::new(AtomicUsize::new(0));
        let policy = CountingPolicy::new(PermissionDecision::Allow, false);
        let a = build(
            store.clone(),
            Arc::new(FakeProvider::scripted(vec![
                raw_calls(&[(name, args.clone())]),
                done(),
            ])),
            policy.clone(),
            vec![(g.clone(), Scope::Global)],
            vec![tool(name, field, count.clone())],
            None,
        );
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let records = snapshot_tools(&store, &session).await;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].arguments, args);
        assert_eq!(
            records[0].status,
            if expected.denies() {
                ToolCallStatus::Failed
            } else {
                ToolCallStatus::Completed
            }
        );
        assert_eq!(
            count.load(Ordering::SeqCst),
            usize::from(!expected.denies())
        );
        assert_eq!(policy.count(), usize::from(!expected.denies()));
        let mut want = before;
        match expected {
            Outcome::RuleMatch => want.rule_match += 1,
            Outcome::InvalidCommand => want.invalid_command += 1,
            Outcome::Unanalysable => want.unanalysable += 1,
            Outcome::AnalysisLimit => want.analysis_limit += 1,
            Outcome::Abstain => want.abstain += 1,
        }
        assert_eq!(g.stats(), want);
        a.close_extensions().await.unwrap();
    }
}
#[tokio::test(flavor = "multi_thread", worker_threads = 2)]
async fn hard_cap_analysis_runs_through_the_agent() {
    hard_cap_journeys().await;
}
#[test]
fn hard_cap_analysis_runs_on_a_2_mib_worker() {
    tokio::runtime::Builder::new_multi_thread()
        .worker_threads(2)
        // The full Agent runtime and synchronous guard share this stack.
        // Keep the integrated proof at the supported Tokio worker size; direct
        // parser proofs in budgets.rs still exercise 512 KiB stacks.
        .thread_stack_size(2 << 20)
        .enable_all()
        .build()
        .unwrap()
        .block_on(hard_cap_journeys());
}
