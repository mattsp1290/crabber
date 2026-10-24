use super::*;
use crabber::{ExecutionContext, core::SessionContext};
use std::sync::Mutex;

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
#[allow(
    clippy::too_many_lines,
    reason = "Keep the public API scenario and its ordered assertions together."
)]
async fn interleaved_sessions_keep_each_inline_and_detached_source_on_its_own_parent() {
    let barrier = Arc::new(tokio::sync::Barrier::new(2));
    let seen = Arc::new(Mutex::new(Vec::new()));
    let host: Runner = {
        let seen = seen.clone();
        Arc::new(move |request| {
            let barrier = barrier.clone();
            let seen = seen.clone();
            Box::pin(async move {
                seen.lock().unwrap().push(request.clone());
                if request.background() {
                    request.cancellation().cancelled().await;
                } else {
                    barrier.wait().await;
                }
                Ok(Response::Completed("owned".into()))
            })
        })
    };
    let batch = || {
        let mut calls = tool_call(TOOL_NAME, arguments());
        calls.pop();
        calls.extend(tool_call(
            crabber_extensions::delegate_task::START_TOOL,
            arguments(),
        ));
        calls
    };
    let provider = Arc::new(FakeProvider::scripted(vec![
        batch(),
        batch(),
        done(),
        done(),
    ]));
    let store = Arc::new(MemoryStore::new());
    let ext = Arc::new(configured(
        host,
        Limits {
            max_in_flight: 4,
            ..limits()
        },
    ));
    let agent = agent_support::builder()
        .store(store.clone())
        .provider(provider)
        .config(agent_config())
        .policy(Arc::new(StaticPolicy::new(PermissionDecision::Allow)))
        .shared_extension(ext.clone(), Scope::Global)
        .build()
        .unwrap();
    let a = ExecutionContext::new(
        SessionContext::new("one".into(), "ws-one", "/one"),
        agent_config().selection,
    );
    let b = ExecutionContext::new(
        SessionContext::new("two".into(), "ws-two", "/two"),
        agent_config().selection,
    );
    let (ar, br) = tokio::join!(
        agent_support::first_prompt(&agent, &a, "one"),
        agent_support::first_prompt(&agent, &b, "two")
    );
    let (ar, br) = (ar.unwrap(), br.unwrap());
    let runs = [ar.run_id().clone(), br.run_id().clone()];
    let (ad, bd) = tokio::join!(ar.done(), br.done());
    assert_eq!(ad.unwrap().status, RunStatus::Completed);
    assert_eq!(bd.unwrap().status, RunStatus::Completed);
    tokio::time::timeout(Duration::from_secs(3), async {
        while seen.lock().unwrap().len() < 4 {
            tokio::task::yield_now().await;
        }
    })
    .await
    .unwrap();
    // Both inline phases and background start attempts have settled. Before
    // cancelling either job, account for the six best-effort publications:
    // two inline transitions and one detached start for each parent.
    let publication_failures = ext.owner_report().publication_failures;
    let mut publication_count = 0;
    for (execution, run) in [(&a, &runs[0]), (&b, &runs[1])] {
        let session = execution.session().session_id();
        let calls = snapshot_tools(&store, session).await;
        let inline = calls.iter().find(|r| r.name == TOOL_NAME).unwrap();
        let detached = calls
            .iter()
            .find(|r| r.name == crabber_extensions::delegate_task::START_TOOL)
            .unwrap();
        let events = store.list_events(session, None, 1000).await.unwrap();
        let publications = publications(&events);
        publication_count += publications.len();
        assert!(publications.len() <= 3);
        if publication_failures == 0 {
            assert!(publications.len() >= 3);
        }
        for p in &publications {
            assert_eq!(&p.source.session_id, session);
            assert_eq!(&p.source.run_id, run);
            assert_eq!(p.source.workspace_id, execution.session().workspace_id());
            if let Some(inv) = &p.source.invocation {
                assert_eq!(inv.parent_call_id.as_ref(), Some(&detached.id));
            } else {
                assert_eq!(p.source.call_id.as_ref(), Some(&inline.id));
            }
        }
        {
            let captured = seen.lock().unwrap();
            let requests = captured
                .iter()
                .filter(|request| request.session_id() == session)
                .collect::<Vec<_>>();
            assert_eq!(requests.len(), 2);
            for request in requests {
                assert_eq!(request.run_id(), run);
                assert_eq!(
                    request.workspace().workspace_id(),
                    Some(execution.session().workspace_id())
                );
                assert_eq!(
                    request.call_id(),
                    if request.background() {
                        &detached.id
                    } else {
                        &inline.id
                    }
                );
            }
        }
    }
    assert_eq!(
        u64::try_from(publication_count).unwrap() + publication_failures,
        6,
        "every missing publication must be reported as a best-effort failure"
    );
    for execution in [&a, &b] {
        let session = execution.session().session_id();
        let reports = agent
            .drain_session_invocations(session, true, Duration::from_secs(2))
            .await;
        assert_eq!(reports.len(), 1);
        assert!(reports[0].local_joined);
    }
    {
        let seen = seen.lock().unwrap();
        assert_eq!(seen.len(), 4);
        for request in &*seen {
            assert!(request.workspace().workspace_id().is_some());
        }
    }
    assert!(agent.close(Duration::from_secs(2)).await.local_drained());
}
