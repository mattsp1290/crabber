//! Observe preserved policies in factory-created workspaces without network I/O.
use super::durable::{done, results, turn, value};
use crabber::{
    Admission, Agent, AgentConfig, ExecutionContext, FakeProvider, PermissionDecision, Selection,
    SessionContext, StaticPolicy,
    core::{
        AdmissionKey, AdmissionOptions, ControlId, InputFingerprint, SessionId, ToolCallStatus,
    },
    extension::Scope,
    session::MemoryStore,
};
use crabber_tools_catalog::prelude::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
pub(super) fn policies(enabled: EnabledSet) -> Result<Policies, Box<dyn std::error::Error>> {
    Ok(Policies {
        deny: crabber_tools_catalog::prelude::PathDenyPolicy::none(),
        enabled,
        restrict_to_enabled: false,
        shell: ShellPolicy {
            shell_binary: "/bin/sh".into(),
            startup_mode: StartupMode::NonLogin,
            env: EnvPolicy::minimal_allowlist(),
            run_as: None,
            output_cap_bytes: 16,
        },
        search: SearchPolicy::resolve_rg_from_path(EnvPolicy::minimal_allowlist())?,
        url_fetch: Some(UrlFetchPolicy {
            allow: vec![HostPattern::exact("allowed.invalid")?],
            ..UrlFetchPolicy::default()
        }),
        tracker: None,
        user_interact: None,
        limits: Limits {
            max_in_flight: 1,
            max_blocking_wait: Duration::from_millis(100),
        },
    })
}

#[derive(Clone)]
struct Scenario {
    name: &'static str,
    enabled: &'static [ToolId],
    restrict: bool,
    permission: PermissionDecision,
    command: &'static str,
    shell_status: ToolCallStatus,
    marker_status: ToolCallStatus,
    stdout: Option<&'static str>,
    absent: Option<&'static str>,
}
fn scenarios() -> [Scenario; 4] {
    let subset = Scenario {
        name: "enabled-subset",
        enabled: &[ToolId::FileRead],
        restrict: false,
        permission: PermissionDecision::Allow,
        command: "printf 12345678901234567890",
        shell_status: ToolCallStatus::Failed,
        marker_status: ToolCallStatus::Completed,
        stdout: None,
        absent: None,
    };
    let restricted = Scenario {
        name: "run-wide-restriction",
        restrict: true,
        marker_status: ToolCallStatus::Failed,
        ..subset.clone()
    };
    let capped = Scenario {
        name: "capped-output",
        enabled: &[ToolId::FileRead, ToolId::Shell, ToolId::UrlFetch],
        shell_status: ToolCallStatus::Completed,
        stdout: Some("1234567890123456"),
        ..subset.clone()
    };
    let denied = Scenario {
        name: "host-denial",
        permission: PermissionDecision::Deny,
        command: "touch forbidden",
        shell_status: ToolCallStatus::Failed,
        stdout: None,
        absent: Some("forbidden"),
        ..capped.clone()
    };
    [subset, restricted, capped, denied]
}
pub(super) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let a = tempfile::tempdir()?;
    let b = tempfile::tempdir()?;
    for root in [a.path(), b.path()] {
        std::fs::write(root.join("x"), "owned")?;
    }
    path_deny_reopen().await?;
    for case in scenarios() {
        let store = Arc::new(MemoryStore::new());
        let mut p = policies(EnabledSet::only(case.enabled.iter().copied()))?;
        p.restrict_to_enabled = case.restrict;
        let include_url = p.enabled.contains(ToolId::UrlFetch);
        let script = (0..2)
            .flat_map(|_| {
                let mut turns = vec![
                    turn("shell", json!({"cmd":case.command})),
                    turn("file_read", json!({"path":"x"})),
                    turn("consumer_marker", json!({})),
                ];
                if include_url {
                    turns.push(turn("url_fetch", json!({"url":"https://example.com/"})));
                }
                turns.push(done());
                turns
            })
            .collect();
        let roots = ExactRoots::new([
            ("a".into(), a.path().canonicalize()?),
            ("b".into(), b.path().canonicalize()?),
        ])?;
        let permission = StaticPolicy::new(PermissionDecision::Allow)
            .with_rule("shell", case.permission.clone());
        let agent = Agent::builder()
            .behavior_version("consumer-policies/0.4.0")
            .store(store.clone())
            .provider(Arc::new(FakeProvider::scripted(script)))
            .config(AgentConfig::new(Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            }))
            .policy(Arc::new(permission))
            .extension_factory(Arc::new(StandardToolsFactory::new(p, Arc::new(roots))?))
            .shared_extension(Arc::new(crate::Marker), Scope::Global)
            .build()?;
        for (workspace, root) in [("a", a.path()), ("b", b.path())] {
            let session = SessionId::new();
            let directory = root.canonicalize()?;
            let ctx = ExecutionContext::new(
                SessionContext::new(session.clone(), workspace, directory.to_str().unwrap()),
                Selection {
                    provider_id: "fake".into(),
                    model_id: "scripted".into(),
                },
            );
            let options = AdmissionOptions {
                key: AdmissionKey::new(ControlId::new().0)?,
                fingerprint: InputFingerprint::new("a".repeat(64))?,
                behavior_fingerprint: agent.behavior_fingerprint().clone(),
            };
            let Admission::Started { handle, .. } = agent
                .prompt_keyed_in(&ctx, "policies", options, None)
                .await?
            else {
                return Err("fresh admission replayed".into());
            };
            handle.done().await?;
            let records = results(&*store, &session).await?;
            assert_eq!(
                records[0].status, case.shell_status,
                "{} in {}",
                case.name, workspace
            );
            if let Some(stdout) = case.stdout {
                assert_eq!(value(&records[0])["stdout"], stdout);
                assert_eq!(value(&records[0])["stdout_truncated"], true);
            }
            assert_eq!(value(&records[1])["content"], "owned");
            assert_eq!(
                records[2].status, case.marker_status,
                "{} in {}",
                case.name, workspace
            );
            if let Some(absent) = case.absent {
                assert!(!root.join(absent).exists());
            }
            if include_url {
                assert_eq!(records[3].status, ToolCallStatus::Failed);
                assert_eq!(value(&records[3])["error"]["category"], "validation");
            }
        }
        agent.close_extensions().await?;
    }
    println!(
        "{}",
        json!({"check":"factory_allowlist_restriction_host_deny_shell_cap_and_url_policy","ok":true})
    );
    Ok(())
}

async fn path_deny_reopen() -> Result<(), Box<dyn std::error::Error>> {
    use crabber::session::SqliteStore;
    let temp = tempfile::tempdir()?;
    let a = temp.path().join("a");
    let b = temp.path().join("b");
    for root in [&a, &b] {
        std::fs::create_dir(root)?;
        std::fs::write(root.join(".env"), "private fixture")?;
    }
    let path = temp.path().join("deny.sqlite");
    SqliteStore::migrate(&path).await?;
    let store = Arc::new(SqliteStore::connect(&path).await?);
    let mut policy = policies(EnabledSet::only([ToolId::FileRead]))?;
    policy.deny = PathDenyPolicy::new([".env".to_owned()])?;
    let roots = ExactRoots::new([
        ("a".into(), a.canonicalize()?),
        ("b".into(), b.canonicalize()?),
    ])?;
    let agent = Agent::builder()
        .behavior_version("consumer-path-deny/1")
        .store(store.clone())
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn("file_read", json!({"path":".env"})),
            done(),
            turn("file_read", json!({"path":".env"})),
            done(),
        ])))
        .config(AgentConfig::new(Selection {
            provider_id: "fake".into(),
            model_id: "scripted".into(),
        }))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension_factory(Arc::new(StandardToolsFactory::new(
            policy,
            Arc::new(roots),
        )?))
        .build()?;
    let mut sessions = Vec::new();
    for (workspace, root) in [("a", &a), ("b", &b)] {
        let session = SessionId::new();
        let directory = root.canonicalize()?;
        let context = ExecutionContext::new(
            SessionContext::new(session.clone(), workspace, directory.to_str().unwrap()),
            Selection {
                provider_id: "fake".into(),
                model_id: "scripted".into(),
            },
        );
        let options = AdmissionOptions {
            key: AdmissionKey::new(ControlId::new().0)?,
            fingerprint: InputFingerprint::new("a".repeat(64))?,
            behavior_fingerprint: agent.behavior_fingerprint().clone(),
        };
        let Admission::Started { handle, .. } = agent
            .prompt_keyed_in(&context, "denied read", options, None)
            .await?
        else {
            return Err("fresh deny admission replayed".into());
        };
        handle.done().await?;
        agent
            .close_workspace_resources(context.session(), Duration::from_secs(2))
            .await?;
        sessions.push(session);
    }
    agent.close_extensions().await?;
    drop(agent);
    drop(store);
    let reopened = SqliteStore::connect(&path).await?;
    for session in sessions {
        let records = results(&reopened, &session).await?;
        assert_eq!(records.len(), 1);
        assert_eq!(records[0].status, ToolCallStatus::Failed);
        assert_eq!(value(&records[0])["error"]["category"], "path_denied");
        assert_eq!(
            value(&records[0])["error"]["message"],
            "path denied by host policy"
        );
    }
    println!(
        "{}",
        json!({"check":"path_deny_sqlite_reopen_two_workspaces","ok":true})
    );
    Ok(())
}
