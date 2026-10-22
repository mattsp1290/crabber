use super::*;
use crate::background::Journey;

#[tokio::test]
async fn inline_publications_are_phase_bound_and_never_include_task_or_output() {
    let ext = Arc::new(extension(Response::Completed(
        "private synthetic output".into(),
    )));
    let store = Arc::new(MemoryStore::new());
    let provider = Arc::new(FakeProvider::scripted(vec![call(), done()]));
    let host = agent(ext, store.clone(), provider, PermissionDecision::Allow);
    let handle = host.prompt(None, "inline").await.unwrap();
    let session = handle.session_id().clone();
    let run = handle.run_id().clone();
    assert_eq!(handle.done().await.unwrap().status, RunStatus::Completed);
    let call = snapshot_tool(&store, &session).await;
    let events = store.list_events(&session, None, 1000).await.unwrap();
    let publications = publications(&events);
    assert_eq!(
        publications.iter().map(status).collect::<Vec<_>>(),
        ["running", "completed"]
    );
    for publication in &publications {
        assert_eq!(publication.source.session_id, session);
        assert_eq!(publication.source.run_id, run);
        assert_eq!(publication.source.call_id.as_ref(), Some(&call.id));
        assert!(publication.source.invocation.is_none());
    }
    let encoded = serde_json::to_string(&publications).unwrap();
    assert!(
        !encoded.contains("Inspect the synthetic project")
            && !encoded.contains("private synthetic output")
    );
    assert!(host.close(Duration::from_secs(2)).await.local_drained());
}
#[tokio::test]
async fn failed_background_work_is_a_join_with_invocation_attribution_and_failed_activity() {
    let mut j = Journey::new(runner(Response::Failed("private output".into())), limits());
    let id = j.start().await;
    let value = j.wait(&[id]).await;
    assert_eq!(value["tasks"][0]["status"], "failed");
    assert_eq!(value["tasks"][0]["detached"], true);
    let session = j.session.as_ref().unwrap();
    let reports = j
        .agent
        .drain_session_invocations(session, false, Duration::from_secs(2))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(reports[0].local_joined);
    assert_eq!(reports[0].status, crabber::SessionInvocationStatus::Joined);
    let calls = snapshot_tools(&j.store, session).await;
    let start = calls
        .iter()
        .find(|c| c.name == crabber_extensions::delegate_task::START_TOOL)
        .unwrap();
    let events = j.store.list_events(session, None, 1000).await.unwrap();
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InvocationStarted)
            .count(),
        1
    );
    assert_eq!(
        events
            .iter()
            .filter(|e| e.kind == EventKind::InvocationFinished)
            .count(),
        1
    );
    assert!(!events.iter().any(|e| e.kind == EventKind::InvocationError));
    let publications = publications(&events);
    assert_eq!(
        publications.iter().map(status).collect::<Vec<_>>(),
        ["running", "failed"]
    );
    for publication in &publications {
        assert_eq!(&publication.source.session_id, session);
        assert_eq!(publication.source.run_id, start.run_id);
        let invocation = publication.source.invocation.as_ref().unwrap();
        assert_eq!(invocation.id.0, reports[0].id.0);
        assert_eq!(invocation.parent_call_id.as_ref(), Some(&start.id));
        assert!(invocation.parent_invocation.is_none());
    }
    assert!(
        !serde_json::to_string(&publications)
            .unwrap()
            .contains("private output")
    );
    j.close().await;
}
#[tokio::test]
async fn hostile_provider_call_ids_still_publish_with_hash_derived_activity_ids() {
    let id = ToolCallId::from("provider / call with spaces é");
    let provider = Arc::new(FakeProvider::scripted(vec![
        vec![
            StreamDelta::ToolCallStart {
                call_id: id.clone(),
                name: TOOL_NAME.into(),
            },
            StreamDelta::ToolCallArgsDelta {
                call_id: id.clone(),
                text: arguments().to_string(),
            },
            StreamDelta::ToolCallDone {
                call_id: id.clone(),
            },
            StreamDelta::Completed,
        ],
        done(),
    ]));
    let store = Arc::new(MemoryStore::new());
    let host = agent(
        Arc::new(extension(Response::Completed("ok".into()))),
        store.clone(),
        provider,
        PermissionDecision::Allow,
    );
    let handle = host.prompt(None, "hostile identifier").await.unwrap();
    let session = handle.session_id().clone();
    handle.done().await.unwrap();
    let publications = publications(&store.list_events(&session, None, 1000).await.unwrap());
    assert_eq!(publications.len(), 2);
    assert!(
        publications
            .iter()
            .all(|p| p.source.call_id.as_ref() == Some(&id))
    );
    let PublicSourceChange::ActivitySnapshot { activity } = &publications[0].change else {
        panic!("snapshot")
    };
    assert!(
        activity
            .id()
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() || b"-_.".contains(&b))
    );
    assert!(host.close(Duration::from_secs(2)).await.local_drained());
}

#[tokio::test]
async fn inline_interrupt_attempts_settlement_inside_the_actual_phase_drain() {
    let mut pending = PendingHost::new(true);
    let ext = Arc::new(configured(pending.runner.clone(), limits()));
    let store = Arc::new(MemoryStore::new());
    let host = agent(
        ext.clone(),
        store.clone(),
        Arc::new(FakeProvider::scripted(vec![call(), done()])),
        PermissionDecision::Allow,
    );
    let handle = host.prompt(None, "cancel inline").await.unwrap();
    let session = handle.session_id().clone();
    pending.entered(1).await;
    handle.interrupt();
    assert!(matches!(
        handle.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    pending.exited(1).await;
    let closed = host.close(Duration::from_secs(2)).await;
    assert!(!closed.local_drained());
    assert!(
        closed
            .runtime
            .run_issues
            .iter()
            .any(|issue| issue.kind == crabber::runtime::RunCloseIssueKind::NativeOutcomeUnknown)
    );
    assert!(closed.runtime.executions.pending.is_empty());
    assert_eq!(ext.live_runners(), 0);
    let publications = publications(&store.list_events(&session, None, 1000).await.unwrap());
    let statuses = publications.iter().map(status).collect::<Vec<_>>();
    assert_eq!(
        statuses,
        ["running", "cancelled"],
        "failures {}",
        ext.owner_report().publication_failures
    );
}
