use crate::{agent_support, fixture::*, options, owned_process, scripted::Scripts};
use crabber::{AgentConfig, PermissionDecision, StaticPolicy, extension::FactoryScope};
use crabber_extensions::background_jobs as jobs;
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn bounded_owner_close_retains_slow_term_cleanup_and_repeated_close_converges() {
    let root = tempfile::tempdir().unwrap();
    let mut cleanup = owned_process::ProcessCleanup::new(root.path());
    let mut config = options::jobs(root.path());
    config.limits.shutdown_grace = Duration::from_secs(10);
    config.limits.terminate_grace = Duration::from_secs(5);
    let factory = Arc::new(jobs::Factory::new(config, FactoryScope::Session).unwrap());
    let scripts = Arc::new(Scripts::default());
    let agent = agent_support::builder()
        .memory()
        .provider(scripts.clone())
        .config(AgentConfig::new(selection()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .extension_factory(factory.clone())
        .build()
        .unwrap();
    let execution = context(root.path(), "slow-ws", "slow-session");
    scripts.set(vec![calls(&[(jobs::START_TOOL, json!({"command":"trap 'printf term > \"$FIXTURE_ROOT/term\"; sleep 3; exit 0' TERM; printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/pgid.$PPID\"; printf '%s' \"$PPID\" > \"$FIXTURE_ROOT/ready\"; while :; do sleep .02; done"}))]), done()]);
    agent_support::first_prompt(&agent, &execution, "start slow cleanup")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    let group = super::processes::pid_file(root.path(), "ready").await;
    cleanup.set_group(group.as_raw_nonzero().get());
    let first = agent
        .close_session_resources(execution.session(), Duration::from_millis(200))
        .await;
    assert!(
        matches!(
            first,
            Err(crabber::RuntimeError::ExtensionPlan(
                crabber::ExtensionError::ContextCleanupIncomplete
            ))
        ),
        "{first:?}"
    );
    let report = factory
        .owner_report("slow-ws", Some(execution.session().session_id()))
        .unwrap();
    assert_eq!(report.live, 1);
    assert!(report.closing && !report.closed);
    assert!(
        root.path().join("term").exists(),
        "TERM handler must have started"
    );
    super::processes::gone(&[group]).await;
    agent
        .close_session_resources(execution.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        factory
            .owner_report("slow-ws", Some(execution.session().session_id()))
            .is_none_or(|r| r.live == 0 && r.closed)
    );
    let closed = agent.close(Duration::from_secs(5)).await;
    assert_eq!(closed.observation, crabber::CloseObservation::Complete);
    assert!(closed.local_drained());
    cleanup.disarm_groups();
}
