//! One revision supplies the runtime, tools, command guard, and final redactor.
use crate::matrix::{done, results, turn, value};
use crabber::{
    Agent, AgentConfig, ExtensionError, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    core::ToolCallStatus, extension::Scope, session::SqliteStore,
};
use crabber_extensions::{command_guard as guard, tool_result_redactor as redactor};
use crabber_tools_catalog::prelude::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};

fn guard() -> Result<guard::CommandGuard, ExtensionError> {
    guard::CommandGuard::new(guard::Options {
        bindings: vec![guard::Binding {
            tool_name: "shell".into(),
            command_field: "cmd".into(),
            dialect: guard::Dialect::Posix,
        }],
        rules: vec![guard::Rule {
            id: "protect-marker".into(),
            executable: "rm".into(),
            arg_prefix: vec![],
        }],
        limits: guard::Limits {
            max_bindings: 1,
            max_rules: 1,
            max_rule_bytes: 256,
            max_prefix_args: 16,
            max_json_depth: 16,
            max_json_nodes: 256,
            max_command_bytes: 4096,
            max_analysis_bytes: 8192,
            max_ast_nodes: 256,
            max_ast_depth: 16,
            max_words: 256,
            max_word_bytes: 4096,
            max_wrapper_depth: 8,
            max_in_flight: 2,
        },
    })
}

fn redactor() -> Result<redactor::ToolResultRedactor, ExtensionError> {
    redactor::ToolResultRedactor::new(redactor::Options {
        order: redactor::DEFAULT_ORDER,
        excluded_tools: vec![],
        additional_patterns: vec![],
        limits: redactor::Limits {
            max_field_bytes: 4096,
            max_total_bytes: 16384,
            max_depth: 16,
            max_nodes: 256,
            max_matches_per_field: 16,
            max_patterns: 4,
            max_pattern_bytes: 256,
            max_in_flight: 2,
        },
    })
}

fn policies() -> Result<Policies, ExtensionError> {
    Ok(Policies {
        enabled: EnabledSet::only([ToolId::Shell, ToolId::Search]),
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 4096,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
        url_fetch: None,
        tracker: None,
        user_interact: None,
        limits: Limits {
            max_in_flight: 2,
            max_blocking_wait: Duration::from_secs(2),
        },
    })
}

pub(super) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let workspace = tempfile::tempdir()?;
    let database = tempfile::tempdir()?;
    let path = database.path().join("tuple.sqlite");
    std::fs::write(workspace.path().join("keep.txt"), "tuple-needle\n")?;
    let root = WorkspaceRoot::open(workspace.path())?;
    SqliteStore::migrate(&path).await?;
    let store = Arc::new(SqliteStore::connect(&path).await?);
    let provider = Arc::new(FakeProvider::scripted(vec![
        turn(
            "shell",
            json!({"cmd":"printf 'Authorization: Bearer synthetic-tuple-secret'"}),
        ),
        turn("shell", json!({"cmd":"rm keep.txt"})),
        turn(
            "search",
            json!({"pattern":"tuple-needle","path":".","literal":true}),
        ),
        done(),
    ]));
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "tuple".into();
    config.directory = root.path().to_string_lossy().into_owned();
    let agent = Agent::builder()
        .behavior_version("consumer-single-revision.v1")
        .store(store.clone())
        .provider(provider)
        .config(config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(
            Arc::new(StandardTools::new(root, policies()?)?),
            Scope::Global,
        )
        .shared_extension(Arc::new(guard()?), Scope::Global)
        .shared_extension(Arc::new(redactor()?), Scope::Global)
        .build()?;
    let run = agent.prompt(None, "Exercise the complete tuple").await?;
    let session = run.session_id().clone();
    run.done().await?;
    agent.close_extensions().await?;
    drop(agent);
    drop(store);
    let reopened = SqliteStore::connect(&path).await?;
    let calls = results(&reopened, &session).await?;
    assert_eq!(calls.len(), 3);
    let shell = calls
        .iter()
        .find(|call| call.name == "shell" && call.status == ToolCallStatus::Completed)
        .ok_or("shell did not complete")?;
    let stdout = value(shell)["stdout"]
        .as_str()
        .ok_or("missing shell stdout")?
        .to_owned();
    assert!(stdout.contains(redactor::PLACEHOLDER));
    assert!(!stdout.contains("synthetic-tuple-secret"));
    assert_eq!(
        calls
            .iter()
            .filter(|call| call.name == "shell" && call.status == ToolCallStatus::Failed)
            .count(),
        1
    );
    assert_eq!(
        std::fs::read_to_string(workspace.path().join("keep.txt"))?,
        "tuple-needle\n"
    );
    let search = calls
        .iter()
        .find(|call| call.name == "search")
        .ok_or("missing search")?;
    assert_eq!(search.status, ToolCallStatus::Completed);
    assert_eq!(value(search)["match_count"], 1);
    println!(
        "{}",
        json!({"check":"runtime-tools-extensions","guard_denied":true,"persisted_redaction":true,"search":true})
    );
    Ok(())
}
