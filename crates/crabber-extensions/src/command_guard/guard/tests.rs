#![allow(
    clippy::panic,
    clippy::unwrap_used,
    clippy::expect_used,
    clippy::indexing_slicing,
    clippy::arithmetic_side_effects
)]
use super::*;
use crate::command_guard::{Limits, Rule, default_bindings};
use crabber::JsonToolExecutor;
use serde_json::json;
use std::sync::{Mutex, mpsc};
use std::time::Duration;

fn options() -> Options {
    Options {
        bindings: default_bindings(),
        rules: vec![Rule {
            id: "blocked".into(),
            executable: "blocked".into(),
            arg_prefix: vec![],
        }],
        limits: Limits {
            max_bindings: 8,
            max_rules: 32,
            max_rule_bytes: 2048,
            max_prefix_args: 16,
            max_json_depth: 16,
            max_json_nodes: 256,
            max_command_bytes: 4096,
            max_analysis_bytes: 8192,
            max_ast_nodes: 2048,
            max_ast_depth: 16,
            max_words: 512,
            max_word_bytes: 4096,
            max_wrapper_depth: 8,
            max_in_flight: 1,
        },
    }
}
#[test]
fn saturation_denies_immediately_and_recovers() {
    let (entered_tx, entered) = mpsc::channel();
    let (release, release_rx) = mpsc::channel();
    let release_rx = Mutex::new(release_rx);
    let first = AtomicUsize::new(0);
    let g = Arc::new(Guard::with_analyzer(
        Policy::new(options()).unwrap(),
        Arc::new(move |p, n, a| {
            if first.fetch_add(1, Ordering::SeqCst) == 0 {
                entered_tx.send(()).unwrap();
                release_rx
                    .lock()
                    .unwrap()
                    .recv_timeout(Duration::from_secs(5))
                    .unwrap();
            }
            p.analyze(n, a)
        }),
    ));
    let worker = {
        let g = g.clone();
        std::thread::spawn(move || g.check("shell", &json!({"cmd":"echo ok"})))
    };
    entered.recv_timeout(Duration::from_secs(5)).unwrap();
    // A regression that queues would time out the assertion thread rather than hang the suite.
    let (tx, rx) = mpsc::channel();
    let contender = {
        let g = g.clone();
        std::thread::spawn(move || {
            tx.send(g.check("shell", &json!({"cmd":"echo ok"})))
                .unwrap()
        })
    };
    let decision = rx.recv_timeout(Duration::from_secs(1));
    assert_eq!(g.check("unbound", &Value::Null), GuardDecision::Abstain);
    release.send(()).unwrap();
    worker.join().unwrap();
    contender.join().unwrap();
    assert_eq!(decision.unwrap(), GuardDecision::Deny);
    assert_eq!(g.stats().capacity, 1);
    assert_eq!(g.stats().abstain, 1);
    assert_eq!(g.in_flight(), 0);
    for guard in [&*g, &Guard::new(Policy::new(options()).unwrap())] {
        assert_eq!(
            guard.check("shell", &json!({"cmd":"echo ok"})),
            GuardDecision::Abstain
        );
        assert_eq!(
            guard.check("shell", &json!({"cmd":"blocked"})),
            GuardDecision::Deny
        );
        assert_eq!(guard.in_flight(), 0);
    }
}
#[test]
fn unbound_tool_takes_no_permit() {
    let g = Guard::with_analyzer(
        Policy::new(options()).unwrap(),
        Arc::new(|_, _, _| panic!("must not inspect")),
    );
    g.preload_permits(1);
    assert_eq!(
        g.check("not-bound", &Value::String("blocked".into())),
        GuardDecision::Abstain
    );
    assert_eq!(g.stats(), Stats::default());
    assert_eq!(g.in_flight(), 1);
}
#[test]
fn panic_in_analysis_denies_and_releases_permit() {
    let first = AtomicUsize::new(0);
    let g = CommandGuard::with_analyzer(
        options(),
        Arc::new(move |p, n, a| {
            if first.fetch_add(1, Ordering::SeqCst) == 0 {
                panic!("SECRET_MARKER");
            }
            p.analyze(n, a)
        }),
    )
    .unwrap();
    assert_eq!(
        g.guard.check("shell", &json!({"cmd":"echo ok"})),
        GuardDecision::Deny
    );
    assert_eq!(g.stats().internal, 1);
    assert_eq!(g.guard.in_flight(), 0);
    assert_eq!(
        g.guard.check("shell", &json!({"cmd":"echo ok"})),
        GuardDecision::Abstain
    );
    assert_eq!(g.guard.in_flight(), 0);
    assert!(!format!("{g:?} {:?}", g.stats()).contains("SECRET_MARKER"));
}
#[test]
fn analyzer_error_classes_map_to_counters() {
    for outcome in [
        Outcome::Abstain,
        Outcome::RuleMatch,
        Outcome::InvalidCommand,
        Outcome::Unanalysable,
        Outcome::AnalysisLimit,
    ] {
        let g = Guard::with_analyzer(
            Policy::new(options()).unwrap(),
            Arc::new(move |_, _, _| outcome),
        );
        assert_eq!(
            g.check("shell", &Value::Null),
            if outcome.denies() {
                GuardDecision::Deny
            } else {
                GuardDecision::Abstain
            }
        );
        let mut want = Stats::default();
        match outcome {
            Outcome::Abstain => want.abstain = 1,
            Outcome::RuleMatch => want.rule_match = 1,
            Outcome::InvalidCommand => want.invalid_command = 1,
            Outcome::Unanalysable => want.unanalysable = 1,
            Outcome::AnalysisLimit => want.analysis_limit = 1,
        }
        assert_eq!(g.stats(), want);
        assert_eq!(g.in_flight(), 0);
    }
    assert_eq!(
        Stats {
            rule_match: u64::MAX,
            capacity: 1,
            ..Stats::default()
        }
        .denials(),
        u64::MAX
    );
}

use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StreamDelta,
    core::{RunStatus, ToolCallId, ToolCallStatus, ToolInfo},
    extension::{Scope, ToolDefinition},
    runtime::PermissionPolicy,
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
struct Counter(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for Counter {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"ok":true}))
    }
}
impl PermissionPolicy for Counter {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        self.0.fetch_add(1, Ordering::SeqCst);
        PermissionDecision::Allow
    }
}
#[tokio::test]
async fn internal_and_capacity_denials_never_execute_through_the_agent() {
    for internal in [true, false] {
        let g = Arc::new(if internal {
            CommandGuard::with_analyzer(options(), Arc::new(|_, _, _| panic!("SECRET_MARKER")))
                .unwrap()
        } else {
            CommandGuard::new(options()).unwrap()
        });
        if !internal {
            g.guard().preload_permits(1);
        }
        let counter = Arc::new(AtomicUsize::new(0));
        let policy_counter = Arc::new(AtomicUsize::new(0));
        let store = Arc::new(MemoryStore::new());
        let id = ToolCallId::new();
        let provider = Arc::new(FakeProvider::scripted(vec![
            vec![
                StreamDelta::ToolCallStart {
                    call_id: id.clone(),
                    name: "shell".into(),
                },
                StreamDelta::ToolCallArgsDelta {
                    call_id: id.clone(),
                    text: json!({"cmd":"echo ok"}).to_string(),
                },
                StreamDelta::ToolCallDone { call_id: id },
                StreamDelta::Completed,
            ],
            vec![
                StreamDelta::TextDelta("done".into()),
                StreamDelta::Completed,
            ],
        ]));
        let mut config = AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        });
        config.workspace_id = "workspace".into();
        config.directory = "/workspace".into();
        let shell = Arc::new(ToolDefinition {
            info: ToolInfo {
                name: "shell".into(),
                description: "synthetic".into(),
                parameters: json!({
                    "type": "object",
                    "required": ["cmd"],
                    "properties": {"cmd": {"type": "string"}}
                }),
                retry_safe: false,
                required_permissions: vec![],
            },
            executor: Arc::new(Counter(counter.clone())),
        });
        let a = Agent::builder()
            .behavior_version("crabber-extensions-tests.v1")
            .store(store.clone())
            .provider(provider.clone())
            .config(config)
            .policy(Arc::new(Counter(policy_counter.clone())))
            .shared_extension(g.clone(), Scope::Global)
            .shared_tool(shell)
            .build()
            .unwrap();
        let run = a.prompt(None, "fixture").await.unwrap();
        let session = run.session_id().clone();
        assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
        let SnapshotOutcome::Page(page) = store
            .snapshot(SnapshotRequest {
                session_id: session,
                limits: SnapshotLimits {
                    messages: 100,
                    tool_calls: 100,
                    parts: 100,
                    text_bytes: 100_000,
                    encoded_bytes: 1_000_000,
                },
                continuation: None,
            })
            .await
            .unwrap()
        else {
            panic!("snapshot")
        };
        assert_eq!(page.tool_calls.len(), 1);
        assert_eq!(page.tool_calls[0].status, ToolCallStatus::Failed);
        assert!(format!("{:?}", page.tool_calls[0].result).contains("permission denied"));
        assert!(!format!("{page:?}").contains("SECRET_MARKER"));
        assert!(!format!("{:?}", provider.requests()[1]).contains("SECRET_MARKER"));
        assert_eq!(counter.load(Ordering::SeqCst), 0);
        assert_eq!(policy_counter.load(Ordering::SeqCst), 0);
        assert_eq!(
            g.stats(),
            if internal {
                Stats {
                    internal: 1,
                    ..Stats::default()
                }
            } else {
                Stats {
                    capacity: 1,
                    ..Stats::default()
                }
            }
        );
        if !internal {
            g.guard().preload_permits(0);
        }
        assert_eq!(g.guard().in_flight(), 0);
        a.close_extensions().await.unwrap();
    }
}
