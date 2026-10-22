use crabber::JsonToolExecutor;
use crabber_extensions::command_guard::*;

#[path = "command_guard/budgets.rs"]
mod budgets;
#[path = "command_guard/contract.rs"]
mod contract;
#[path = "command_guard/corpus.rs"]
mod corpus;
#[path = "command_guard/differential.rs"]
mod differential;
#[path = "command_guard/input.rs"]
mod input;

fn limits() -> Limits {
    Limits {
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
        max_in_flight: 4,
    }
}
fn rules() -> Vec<Rule> {
    vec![
        Rule {
            id: "blocked".into(),
            executable: "blocked".into(),
            arg_prefix: vec![],
        },
        Rule {
            id: "git-push".into(),
            executable: "git".into(),
            arg_prefix: vec!["push".into()],
        },
    ]
}
fn options() -> Options {
    Options {
        bindings: default_bindings(),
        rules: rules(),
        limits: limits(),
    }
}
fn policy() -> Policy {
    Policy::new(options()).unwrap()
}

fn generated_corpus(mut state: u64, count: usize) -> Vec<String> {
    const TOKENS: &[&str] = &[
        "blocked", "git", "push", "status", "echo", "tool", "x", "A=b", "env", "sudo", "-n",
        "timeout", "1", "command", "--", "-p", "exec", "sh", "bash", "-c", "-e", "printf", "test",
        "[", "]", "-v", "eval", "time", "$(", ")", "(", "((", "{", "}", "`", "\"", "'", "$'",
        "$\"", "\\", "$x", "${x}", "${x:-y}", "$1", "$[", "<<EOF", "<<'EOF'", "EOF", "<<<", "<",
        ">", ">>", "2>&1", "&>", "|", "|&", "||", "&&", ";", ";;", ";&", "&", "!", "#", "*", "?",
        "~", "@(", "if", "then", "fi", "for", "in", "do", "done", "case", "esac", "while", "until",
        " ", "\t", "\n", "\r", "\\\n",
    ];
    let mut next = || {
        state ^= state << 13;
        state ^= state >> 7;
        state ^= state << 17;
        state
    };
    (0..count)
        .map(|_| {
            let n = 1 + next() % 24;
            (0..n)
                .map(|_| TOKENS[(next() % TOKENS.len() as u64) as usize])
                .collect()
        })
        .collect()
}
#[path = "command_guard/robustness.rs"]
mod robustness;
#[cfg(unix)]
#[path = "command_guard/shell.rs"]
mod shell;

use async_trait::async_trait;
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StreamDelta,
    core::{RunStatus, SessionId, ToolCallId, ToolCallRecord, ToolCallStatus, ToolInfo},
    extension::{Extension, ExtensionError, Scope, ToolContext, ToolDefinition},
    runtime::{ExecutionMode, InterruptPolicy, PermissionPolicy},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use serde_json::{Value, json};
use std::sync::{
    Arc,
    atomic::{AtomicUsize, Ordering},
};

#[path = "command_guard/runtime.rs"]
mod runtime;

fn guard() -> Arc<CommandGuard> {
    guard_with(options())
}
fn guard_with(options: Options) -> Arc<CommandGuard> {
    Arc::new(CommandGuard::new(options).unwrap())
}
struct CountingExecutor(Arc<AtomicUsize>);
#[async_trait]
impl JsonToolExecutor for CountingExecutor {
    async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
        Err(ExtensionError::Tool("context required".into()))
    }
    async fn execute_with_context(
        &self,
        _: ToolContext,
        _: Value,
    ) -> Result<Value, ExtensionError> {
        self.0.fetch_add(1, Ordering::SeqCst);
        Ok(json!({"ok":true}))
    }
}
fn tool(name: &str, field: &str, counter: Arc<AtomicUsize>) -> Arc<ToolDefinition> {
    Arc::new(ToolDefinition {
        info: ToolInfo {
            name: name.into(),
            description: "synthetic".into(),
            parameters: json!({"type":"object","additionalProperties":false,"required":[field],"properties":{field:{"type":"string"}}}),
            retry_safe: false,
            required_permissions: vec![],
        },
        executor: Arc::new(CountingExecutor(counter)),
    })
}
struct CountingPolicy {
    decision: PermissionDecision,
    pause: bool,
    decisions: AtomicUsize,
}
impl CountingPolicy {
    fn new(decision: PermissionDecision, pause: bool) -> Arc<Self> {
        Arc::new(Self {
            decision,
            pause,
            decisions: AtomicUsize::new(0),
        })
    }
    fn count(&self) -> usize {
        self.decisions.load(Ordering::SeqCst)
    }
}
impl PermissionPolicy for CountingPolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        self.decisions.fetch_add(1, Ordering::SeqCst);
        self.decision
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        if self.pause {
            InterruptPolicy::Pause
        } else {
            InterruptPolicy::Continue
        }
    }
}
fn call(name: &str, field: &str, command: &str) -> Vec<StreamDelta> {
    calls(&[(name, field, command)])
}
fn calls(items: &[(&str, &str, &str)]) -> Vec<StreamDelta> {
    raw_calls(
        &items
            .iter()
            .map(|(n, f, c)| (*n, json!({*f:*c})))
            .collect::<Vec<_>>(),
    )
}
fn raw_calls(items: &[(&str, Value)]) -> Vec<StreamDelta> {
    let mut deltas = Vec::new();
    for (name, args) in items {
        let call_id = ToolCallId::new();
        deltas.extend([
            StreamDelta::ToolCallStart {
                call_id: call_id.clone(),
                name: (*name).into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: call_id.clone(),
                text: args.to_string(),
            },
            StreamDelta::ToolCallDone { call_id },
        ]);
    }
    deltas.push(StreamDelta::Completed);
    deltas
}
fn done() -> Vec<StreamDelta> {
    vec![
        StreamDelta::TextDelta("done".into()),
        StreamDelta::Completed,
    ]
}
fn agent_config() -> AgentConfig {
    let mut c = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    c.workspace_id = "workspace".into();
    c.directory = "/workspace".into();
    c
}
async fn snapshot_tools(store: &MemoryStore, session: &SessionId) -> Vec<ToolCallRecord> {
    let SnapshotOutcome::Page(page) = store
        .snapshot(SnapshotRequest {
            session_id: session.clone(),
            limits: SnapshotLimits {
                messages: 100,
                tool_calls: 100,
                parts: 100,
                text_bytes: 2_000_000,
                encoded_bytes: 4_000_000,
            },
            continuation: None,
        })
        .await
        .unwrap()
    else {
        panic!("snapshot")
    };
    page.tool_calls
}
fn result_text(record: &ToolCallRecord) -> String {
    let [crabber::core::ContentBlock::Text { text }] =
        record.result.as_ref().unwrap().content.as_slice()
    else {
        panic!("result")
    };
    text.clone()
}
fn build(
    store: Arc<MemoryStore>,
    provider: Arc<FakeProvider>,
    policy: Arc<CountingPolicy>,
    guards: Vec<(Arc<dyn Extension>, Scope)>,
    tools: Vec<Arc<ToolDefinition>>,
    mode: Option<ExecutionMode>,
) -> Agent {
    let mut b = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(provider)
        .config(agent_config())
        .policy(policy);
    for (e, s) in guards {
        b = b.shared_extension(e, s);
    }
    for t in tools {
        b = b.shared_tool(t);
    }
    if let Some(mode) = mode {
        b = b.execution_mode(mode);
    }
    b.build().unwrap()
}
#[path = "command_guard/guard.rs"]
mod guard;
#[path = "command_guard/resume.rs"]
mod resume;
