use super::*;
use crabber::{
    core::RunId,
    extension::{GuardContext, GuardDecision, Registry, ToolGuard},
};

async fn registered(g: Arc<CommandGuard>) -> Arc<dyn ToolGuard> {
    let registry = Registry::new();
    let handle = registry.mount(g, Scope::Global).await.unwrap();
    let plan = registry.acquire(&SessionId::from("session"));
    assert_eq!(plan.guards.len(), 1);
    assert!(plan.tools.is_empty());
    let guard = plan.guards[0].clone();
    drop(plan);
    handle.close().await.unwrap();
    guard
}
#[tokio::test]
async fn decisions_and_counters_per_class() {
    let g = guard();
    let check = registered(g.clone()).await;
    for (script, outcome) in [
        ("echo ok".to_owned(), Outcome::Abstain),
        ("blocked SECRET_MARKER".into(), Outcome::RuleMatch),
        ("' SECRET_MARKER".into(), Outcome::InvalidCommand),
        ("$SECRET_MARKER".into(), Outcome::Unanalysable),
        ("x".repeat(4097), Outcome::AnalysisLimit),
    ] {
        for b in default_bindings() {
            let args = json!({b.command_field:script});
            assert_eq!(g.policy().analyze(&b.tool_name, &args), outcome);
            assert_eq!(
                check.check(&b.tool_name, &args),
                if outcome.denies() {
                    GuardDecision::Deny
                } else {
                    GuardDecision::Abstain
                }
            );
        }
    }
    assert_eq!(
        g.stats(),
        Stats {
            abstain: 2,
            rule_match: 2,
            invalid_command: 2,
            unanalysable: 2,
            analysis_limit: 2,
            ..Stats::default()
        }
    );
    for n in ["Shell", "shell-other", "standard.shell", "custom"] {
        assert_eq!(
            check.check(n, &Value::String("not JSON".into())),
            GuardDecision::Abstain
        );
    }
    assert_eq!(g.stats().denials(), 8);
    assert!(!format!("{g:?}").contains("SECRET_MARKER"));
    let custom = guard_with(Options {
        bindings: vec![Binding {
            tool_name: "custom".into(),
            command_field: "literal.key".into(),
            dialect: Dialect::Bash,
        }],
        ..options()
    });
    let check = registered(custom.clone()).await;
    assert_eq!(
        check.check("custom", &json!({"literal.key":"echo <(blocked)"})),
        GuardDecision::Deny
    );
    assert_eq!(
        check.check("custom", &json!({"wrong":"echo ok"})),
        GuardDecision::Deny
    );
    assert_eq!(
        check.check("shell", &json!({"cmd":"blocked"})),
        GuardDecision::Abstain
    );
    assert_eq!(
        custom.stats(),
        Stats {
            rule_match: 1,
            invalid_command: 1,
            ..Stats::default()
        }
    );
}
#[tokio::test]
async fn never_allows_or_asks_and_agrees_with_policy() {
    let g = guard();
    let check = registered(g.clone()).await;
    for script in generated_corpus(0x0bad_5eed, 500) {
        for b in default_bindings() {
            let args = json!({b.command_field:script});
            let want = if g.policy().analyze(&b.tool_name, &args).denies() {
                GuardDecision::Deny
            } else {
                GuardDecision::Abstain
            };
            for _ in 0..2 {
                assert_eq!(check.check(&b.tool_name, &args), want);
            }
        }
    }
}
#[tokio::test]
async fn check_with_context_uses_authoritative_tool_name() {
    let check = registered(guard()).await;
    let mut t = tool("shell", "cmd", Arc::new(AtomicUsize::new(0)))
        .info
        .clone();
    let args = json!({"cmd":"blocked"});
    let call = ToolCallId::from("call");
    let session = SessionId::from("session");
    let run = RunId::from("run");
    for (name, want) in [
        ("shell", GuardDecision::Deny),
        ("unbound", GuardDecision::Abstain),
    ] {
        t.name = name.into();
        assert_eq!(
            check.check_with_context(GuardContext {
                tool: &t,
                arguments: &args,
                call_id: &call,
                session_id: &session,
                run_id: &run
            }),
            want
        );
    }
}
#[tokio::test]
async fn identity_hash_and_frozen_plans() {
    let g = guard();
    assert_eq!(g.id(), GUARD_ID);
    assert_eq!(g.version(), env!("CARGO_PKG_VERSION"));
    assert_ne!(g.config_hash(), g.policy().config_hash());
    assert_eq!(g.config_hash(), guard().config_hash());
    for i in 0..3 {
        let mut o = options();
        match i {
            0 => o.rules[1].arg_prefix[0] = "fetch".into(),
            1 => o.bindings[0].command_field = "script".into(),
            _ => o.limits.max_in_flight += 1,
        }
        assert_ne!(g.config_hash(), guard_with(o).config_hash());
    }
    let r = Registry::new();
    let s = SessionId::from("session");
    let empty = r.acquire(&s);
    let h = r.mount(g, Scope::Global).await.unwrap();
    let frozen = r.acquire(&s);
    assert_ne!(empty.fingerprint(), frozen.fingerprint());
    assert_ne!(empty.components, frozen.components);
    assert!(
        frozen
            .components
            .iter()
            .any(|c| c.id == format!("extension:{GUARD_ID}"))
    );
    h.deactivate();
    assert!(r.acquire(&s).guards.is_empty());
    assert_eq!(
        frozen.guards[0].check("shell", &json!({"cmd":"git push"})),
        GuardDecision::Deny
    );
    drop(frozen);
    drop(empty);
    h.close().await.unwrap();
}
#[tokio::test]
async fn global_and_session_scopes_compose() {
    let r = Registry::new();
    let a = guard();
    let b = guard();
    let ha = r.mount(a.clone(), Scope::Global).await.unwrap();
    let hb = r
        .mount(b.clone(), Scope::Session(SessionId::from("exact")))
        .await
        .unwrap();
    let exact = r.acquire(&SessionId::from("exact"));
    let other = r.acquire(&SessionId::from("other"));
    assert_eq!(exact.guards.len(), 2);
    assert_eq!(other.guards.len(), 1);
    for c in &exact.guards {
        assert_eq!(
            c.check("shell", &json!({"cmd":"blocked"})),
            GuardDecision::Deny
        );
    }
    assert_eq!(a.stats().rule_match, 1);
    assert_eq!(b.stats().rule_match, 1);
    drop(exact);
    drop(other);
    ha.close().await.unwrap();
    hb.close().await.unwrap();
    // The same instance mounted twice shares counters across registries.
    let shared = guard();
    let first = registered(shared.clone()).await;
    let second = registered(shared.clone()).await;
    first.check("shell", &json!({"cmd":"blocked"}));
    second.check("shell", &json!({"cmd":"blocked"}));
    assert_eq!(shared.stats().rule_match, 2);
}
#[tokio::test]
async fn concurrent_checks_are_isolated() {
    let mut o = options();
    o.limits.max_in_flight = 64;
    o.bindings.push(Binding {
        tool_name: "bash-tool".into(),
        command_field: "script".into(),
        dialect: Dialect::Bash,
    });
    let g = guard_with(o);
    let check = registered(g.clone()).await;
    let threads = (0..32)
        .map(|_| {
            let check = check.clone();
            std::thread::spawn(move || {
                for i in 0..20 {
                    let (name, field, script, want) = match i % 4 {
                        0 => ("shell", "cmd", "echo \"$X\"", GuardDecision::Abstain),
                        1 => (
                            "bash-tool",
                            "script",
                            "echo <(blocked)",
                            GuardDecision::Deny,
                        ),
                        2 => ("shell", "cmd", "git push", GuardDecision::Deny),
                        _ => (
                            "bash-tool",
                            "script",
                            "echo $'data'",
                            GuardDecision::Abstain,
                        ),
                    };
                    assert_eq!(check.check(name, &json!({field:script})), want);
                }
            })
        })
        .collect::<Vec<_>>();
    for t in threads {
        t.join().unwrap();
    }
    assert_eq!(
        g.stats(),
        Stats {
            abstain: 320,
            rule_match: 320,
            ..Stats::default()
        }
    );
    assert_eq!(g.policy().bindings().len(), 3);
}
#[tokio::test]
async fn contention_conserves_counts() {
    let g = guard_with(Options {
        limits: Limits {
            max_in_flight: 2,
            ..limits()
        },
        ..options()
    });
    let check = registered(g.clone()).await;
    let barrier = Arc::new(std::sync::Barrier::new(8));
    let threads = (0..8)
        .map(|_| {
            let check = check.clone();
            let barrier = barrier.clone();
            std::thread::spawn(move || {
                barrier.wait();
                (0..50)
                    .filter(|_| match check.check("shell", &json!({"cmd":"echo ok"})) {
                        GuardDecision::Deny => true,
                        GuardDecision::Abstain => false,
                        _ => panic!("widened"),
                    })
                    .count()
            })
        })
        .collect::<Vec<_>>();
    let denied: usize = threads.into_iter().map(|t| t.join().unwrap()).sum();
    let s = g.stats();
    assert_eq!(s.abstain + s.capacity, 400);
    assert_eq!(s.capacity, denied as u64);
    assert_eq!(s.denials(), s.capacity);
}
