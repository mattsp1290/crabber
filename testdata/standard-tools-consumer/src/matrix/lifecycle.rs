//! Interruption leaves no live process and reports actual registered joins.
use super::durable::{done, turn};
use crabber::{
    Agent, AgentConfig, FakeProvider, PermissionDecision, Selection, StaticPolicy,
    extension::Scope, session::MemoryStore,
};
use crabber_tools_catalog::prelude::*;
use serde_json::json;
use std::{sync::Arc, time::Duration};
fn running(pid: u32) -> bool {
    #[cfg(target_os = "linux")]
    if let Ok(stat) = std::fs::read_to_string(format!("/proc/{pid}/stat"))
        && stat
            .rsplit_once(") ")
            .is_some_and(|(_, s)| s.starts_with('Z'))
    {
        return false;
    }
    std::process::Command::new("kill")
        .args(["-0", &pid.to_string()])
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .status()
        .is_ok_and(|s| s.success())
}
pub(super) async fn run() -> Result<(), Box<dyn std::error::Error>> {
    let root = tempfile::tempdir()?;
    let admitted = WorkspaceRoot::open(root.path())?;
    let store = Arc::new(MemoryStore::new());
    let p = super::policies::policies(EnabledSet::only([ToolId::Shell]))?;
    let mut config = AgentConfig::new(Selection {
        provider_id: "fake".into(),
        model_id: "scripted".into(),
    });
    config.workspace_id = "cleanup".into();
    config.directory = admitted.path().to_string_lossy().into_owned();
    let agent = Agent::builder()
        .behavior_version("consumer-lifecycle/0.4.0")
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(vec![
            turn(
                "shell",
                json!({"cmd":"echo $$ > leader; sleep 60 & echo $! > child; wait"}),
            ),
            done(),
        ])))
        .config(config)
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(Arc::new(StandardTools::new(admitted, p)?), Scope::Global)
        .build()?;
    let handle = agent.prompt(None, "interrupt").await?;
    let run = handle.run_id().clone();
    tokio::time::timeout(Duration::from_secs(3), async {
        while !root.path().join("child").exists() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    let leader = std::fs::read_to_string(root.path().join("leader"))?
        .trim()
        .parse::<u32>()?;
    let child = std::fs::read_to_string(root.path().join("child"))?
        .trim()
        .parse::<u32>()?;
    handle.interrupt();
    assert!(matches!(
        handle.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    let drain = agent.drain_execution_tasks(Duration::from_secs(2)).await;
    let resources = &drain
        .resources
        .iter()
        .find(|entry| entry.run_id == run)
        .ok_or("missing resource drain")?
        .resources;
    assert!(resources.sealed);
    assert!(resources.joined >= 1);
    assert_eq!(resources.pending, 0);
    assert!(resources.failures.is_empty());
    tokio::time::timeout(Duration::from_secs(2), async {
        while running(leader) || running(child) {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
    })
    .await?;
    agent.close_extensions().await?;
    println!(
        "{}",
        json!({"check":"interrupted_process_group_joined_outcome_uncertain","ok":true})
    );
    Ok(())
}
