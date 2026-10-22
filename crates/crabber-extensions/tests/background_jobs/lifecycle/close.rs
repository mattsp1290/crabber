use super::*;

#[tokio::test(flavor = "multi_thread")]
async fn mount_close_kills_running_jobs_and_joins() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    h.start("sleep 60").await;
    h.start("sleep 60").await;
    let a = agent(
        h.ext.clone(),
        Arc::new(MemoryStore::new()),
        Arc::new(FakeProvider::scripted(vec![done()])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    // Mount eagerly by completing a harmless model turn.
    a.prompt(None, "mount").await.unwrap().done().await.unwrap();
    let start = Instant::now();
    a.close_extensions().await.unwrap();
    assert!(start.elapsed() < Duration::from_secs(6));
    groups_gone(h.dir.path()).await;
    assert_eq!(h.ext.live_jobs(), 0);
    let store = Arc::new(MemoryStore::new());
    let later = agent(
        h.ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![
            call(START_TOOL, json!({"command":"sleep 60"})),
            done(),
        ])),
        PermissionDecision::Allow,
        h.dir.path(),
    );
    let run = later.prompt(None, "closed").await.unwrap();
    let session = run.session_id().clone();
    run.done().await.unwrap();
    assert!(
        result_value(&snapshot_tools(&store, &session).await[0])
            .as_str()
            .unwrap()
            .ends_with("manager-closing")
    );
    later.close_extensions().await.unwrap();
    h.close().await;
}

#[tokio::test(flavor = "multi_thread")]
async fn close_with_tracked_finished_jobs_returns_promptly() {
    if !shell_available() {
        return;
    }
    let h = Harness::new(|_| {}).await;
    let finished = h.start("exit 0").await;
    h.terminal(id(&finished)).await;
    h.start("sleep 60").await;
    let start = Instant::now();
    h.close().await;
    assert!(start.elapsed() < Duration::from_secs(5));
}
