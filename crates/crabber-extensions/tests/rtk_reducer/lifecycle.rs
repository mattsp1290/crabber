use super::observe::assert_three_surfaces;
use super::*;
#[tokio::test(flavor = "multi_thread")]
async fn interrupt_while_child_runs_settles_fixed_text_and_joins_cleanup() {
    if !shell_available() {
        return;
    }
    let original = json!("sensitive".repeat(100));
    let j = Journey::new(
        "printf '%s' \"$$\" > \"$XDG_DATA_HOME/pid\"; trap '' TERM; exec /bin/sleep 60",
        original.clone(),
        "fixture_text",
        json!({}),
        |_| {},
    );
    let run = j.agent.prompt(None, "hold").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&j.dir.path().join("pids")).await;
    let start = std::time::Instant::now();
    run.interrupt();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Interrupted);
    assert!(start.elapsed() < crabber::runtime::INTERRUPT_SETTLEMENT_BOUND);
    let records = snapshot_tools(&j.store, &session).await;
    assert_eq!(
        records[0].status,
        crabber::core::ToolCallStatus::Interrupted
    );
    assert_eq!(
        result_text(&records[0]),
        crabber::runtime::INTERRUPTED_RESULT_TEXT
    );
    j.agent
        .prompt(Some(session.clone()), "observe interruption")
        .await
        .unwrap()
        .done()
        .await
        .unwrap();
    assert_three_surfaces(
        &j.store,
        &session,
        &j.provider,
        &records[0].id,
        &json!(crabber::runtime::INTERRUPTED_RESULT_TEXT),
    )
    .await;
    assert!(j.probe.final_results.lock().unwrap().iter().all(|(_,v)| *v!=original && !v.as_str().unwrap_or_default().starts_with("[Reduced")));
    j.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn close_joins_active_child_and_removes_root() {
    if !shell_available() {
        return;
    }
    let original = json!("x".repeat(300));
    let j = Journey::new(
        "trap '' TERM; exec /bin/sleep 60",
        original,
        "fixture_text",
        json!({}),
        |_| {},
    );
    let run = j.agent.prompt(None, "hold").await.unwrap();
    wait_file(&j.dir.path().join("pids")).await;
    j.agent.close_extensions().await.unwrap();
    let _ = run.done().await;
    assert!(j.ext.stats().closed);
    j.close().await;
}
#[tokio::test(flavor = "multi_thread")]
async fn parallel_capacity_denies_second_call_without_spawning() {
    if !shell_available() {
        return;
    }
    let original = json!("x".repeat(300));
    let mut j = Journey::new(
        "exit 0",
        original.clone(),
        "fixture_text",
        json!({}),
        |_| {},
    );
    let body = format!(
        "while [ ! -f '{}/released' ]; do /bin/sleep 0.01; done; /bin/cat >/dev/null; printf R:x",
        j.dir.path().display()
    );
    let mut o = options(j.dir.path(), &body);
    o.limits.max_concurrent = 1;
    j.ext = Arc::new(RtkReducer::new(o).unwrap());
    j.provider = Arc::new(FakeProvider::scripted(vec![
        calls(&[("fixture_text", json!({})), ("fixture_text", json!({}))]),
        done(),
    ]));
    j.agent = Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(j.store.clone())
        .provider(j.provider.clone())
        .config(agent_config(j.dir.path()))
        .execution_mode(crabber::runtime::ExecutionMode::Parallel { max: 2 })
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_tool(tool("fixture_text", j.executor.clone()))
        .shared_extension(j.ext.clone(), Scope::Global)
        .shared_extension(
            Arc::new(observe::ProbeMount(j.probe.clone())),
            Scope::Global,
        )
        .build()
        .unwrap();
    let run = j.agent.prompt(None, "parallel").await.unwrap();
    let session = run.session_id().clone();
    wait_file(&j.dir.path().join("pids")).await;
    tokio::time::timeout(Duration::from_secs(3), async {
        loop {
            if j.probe.final_results.lock().unwrap().len() == 1 {
                break;
            }
            tokio::time::sleep(Duration::from_millis(5)).await;
        }
    })
    .await
    .unwrap();
    assert_eq!(j.argv().len(), 1);
    assert_eq!(j.ext.stats().capacity_denied, 1);
    fs::write(j.dir.path().join("released"), "release").unwrap();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Completed);
    let records = snapshot_tools(&j.store, &session).await;
    let expected = json!(marker(Filter::Log, 300) + "R:x");
    assert_eq!(
        records
            .iter()
            .filter(|r| result_value(r) == expected)
            .count(),
        1
    );
    assert_eq!(
        records
            .iter()
            .filter(|r| result_value(r) == original)
            .count(),
        1
    );
    for record in &records {
        assert_three_surfaces(
            &j.store,
            &session,
            &j.provider,
            &record.id,
            &result_value(record),
        )
        .await;
    }
    j.close().await;
}
