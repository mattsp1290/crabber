use super::observe::{ProbeMount, ResultProbe, assert_provider_record, assert_settled};
use super::*;
use crabber::extension::ToolOutcomeClass;
use crabber_extensions::{background_jobs as jobs, tool_result_redactor as redact};
const MARKER: &str = "ghp_0123456789abcdef";
fn redactor() -> Arc<redact::ToolResultRedactor> {
    Arc::new(
        redact::ToolResultRedactor::new(redact::Options {
            order: -100,
            excluded_tools: vec![],
            additional_patterns: vec![],
            limits: redact::Limits {
                max_field_bytes: 8192,
                max_total_bytes: 32768,
                max_depth: 16,
                max_nodes: 128,
                max_matches_per_field: 16,
                max_patterns: 4,
                max_pattern_bytes: 256,
                max_in_flight: 4,
            },
        })
        .unwrap(),
    )
}
fn jobs_options() -> jobs::Options {
    jobs::Options {
        shell_path: "/bin/sh".into(),
        shell_identity: "test-sh-v1".into(),
        environment: jobs::Environment {
            mode: jobs::EnvironmentMode::ExplicitOnly,
            overrides: BTreeMap::from([("PATH".into(), "/usr/bin:/bin".into())]),
            identity: "test-env-v1".into(),
        },
        limits: jobs::Limits {
            max_running: 2,
            max_tracked: 4,
            max_command_bytes: 4096,
            max_working_directory_bytes: 1024,
            max_output_bytes_per_stream: 4096,
            max_environment_entries: 16,
            max_environment_bytes: 4096,
            default_timeout: Duration::ZERO,
            max_timeout: Duration::from_secs(30),
            terminate_grace: Duration::from_millis(300),
            kill_wait: Duration::from_secs(3),
            shutdown_grace: Duration::from_secs(5),
        },
    }
}
#[tokio::test(flavor = "multi_thread")]
async fn redactor_protects_python_output_before_persistence() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.environment
            .overrides
            .insert("FIXTURE_SECRET".into(), MARKER.into());
    })
    .await;
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![
        call(
            EXECUTE_TOOL,
            json!({"code":record_code("import os; print(os.environ['FIXTURE_SECRET'],end='')")}),
        ),
        done(),
    ]));
    let probe = Arc::new(ResultProbe::default());
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider.clone())
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(h.ext.clone(), Scope::Global)
        .shared_extension(redactor(), Scope::Global)
        .shared_extension(Arc::new(ProbeMount(probe.clone())), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(None, "redact").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(result_value(&records[0])["stdout"]["text"], "[REDACTED]");
    assert_eq!(
        *probe.classes.lock().unwrap(),
        vec![(records[0].id.clone(), ToolOutcomeClass::Succeeded)]
    );
    assert_provider_record(&provider, 1, &records[0]);
    assert_settled(&store, &session, &records).await;
    for text in [
        format!("{:?}", store.list_all_messages(&session).await.unwrap()),
        format!(
            "{:?}",
            store.list_events(&session, None, 100).await.unwrap()
        ),
        format!("{:?}", provider.requests()),
    ] {
        assert!(!text.contains(MARKER));
        assert!(text.contains("[REDACTED]"));
    }
    a.close_extensions().await.unwrap();
    h.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn redactor_python_and_background_jobs_compose_in_one_turn() {
    if !python_available() {
        return;
    }
    let h = Harness::new(|o| {
        o.environment
            .overrides
            .insert("FIXTURE_SECRET".into(), MARKER.into());
    })
    .await;
    let jobs = Arc::new(jobs::BackgroundJobs::new(jobs_options()).unwrap());
    let store = Arc::new(MemoryStore::new());
    let command = format!(
        "printf '%s' \"$PPID\" > '{}/pgid.'\"$PPID\"; printf allowed",
        h.dir.path().display()
    );
    let provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[
            (jobs::START_TOOL, json!({"command":command})),
            (
                EXECUTE_TOOL,
                json!({"code":record_code("import os; print(os.environ['FIXTURE_SECRET'],end='')")}),
            ),
            (CLEAR_TOOL, json!({})),
        ]),
        done(),
    ]));
    let probe = Arc::new(ResultProbe::default());
    let a = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store.clone())
        .provider(provider.clone())
        .config(agent_config(h.dir.path()))
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(h.ext.clone(), Scope::Global)
        .shared_extension(jobs.clone(), Scope::Global)
        .shared_extension(redactor(), Scope::Global)
        .shared_extension(Arc::new(ProbeMount(probe.clone())), Scope::Global)
        .build()
        .unwrap();
    let run = a.prompt(None, "compose").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    let records = snapshot_tools(&store, &session).await;
    assert_eq!(records.len(), 3);
    for record in &records {
        assert_eq!(record.status, ToolCallStatus::Completed);
        assert_provider_record(&provider, 1, record);
        assert!(!result_text(record).contains(MARKER));
    }
    assert_eq!(
        result_value(records.iter().find(|r| r.name == EXECUTE_TOOL).unwrap())["stdout"]["text"],
        "[REDACTED]"
    );
    assert_eq!(
        result_value(records.iter().find(|r| r.name == CLEAR_TOOL).unwrap())["had_state"],
        true
    );
    assert!(
        probe
            .classes
            .lock()
            .unwrap()
            .iter()
            .all(|(_, class)| *class == ToolOutcomeClass::Succeeded)
    );
    assert_settled(&store, &session, &records).await;
    a.close_extensions().await.unwrap();
    assert_eq!(jobs.live_jobs(), 0);
    h.close().await;
}
