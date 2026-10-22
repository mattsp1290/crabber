use crate::fixture::*;
use crabber::extension::FactoryScope;
use crabber_extensions::delegate_task as delegate;
use serde_json::json;
use std::{sync::Arc, time::Duration};

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn owner_close_retains_noncooperative_capacity_and_host_close_waits_for_actual_join() {
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let release = Arc::new(tokio::sync::Notify::new());
    let runner: delegate::Runner = {
        let entered = entered.clone();
        let release = release.clone();
        Arc::new(move |_| {
            let entered = entered.clone();
            let release = release.clone();
            Box::pin(async move {
                entered.add_permits(1);
                release.notified().await;
                Ok(delegate::Response::Completed("released".into()))
            })
        })
    };
    let Some(h) = Harness::new(FactoryScope::Session, runner) else {
        return;
    };
    let a = context(&h.root.path().join("a"), "ws", "a");
    let records = h
        .first(
            &a,
            &[(
                delegate::START_TOOL,
                json!({"task":"hold", "profile":"test"}),
            )],
        )
        .await;
    assert_eq!(value(&records[0])["status"], "started");
    entered.acquire().await.unwrap().forget();
    let close = h
        .agent
        .close_session_resources(a.session(), Duration::from_millis(20))
        .await;
    assert!(
        matches!(
            close,
            Err(crabber::RuntimeError::ExtensionPlan(
                crabber::ExtensionError::ContextCleanupIncomplete
            ))
        ),
        "{close:?}"
    );
    let report = h
        .delegate
        .owner_report("ws", Some(a.session().session_id()))
        .unwrap();
    assert_eq!(report.live, 1);
    assert!(report.closing);
    assert!(!report.closed);
    let reports = h
        .agent
        .drain_session_invocations(a.session().session_id(), true, Duration::from_millis(1))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(!reports[0].local_joined);
    let first = h.agent.close(Duration::from_millis(1)).await;
    assert_ne!(first.observation, crabber::CloseObservation::Complete);
    assert!(!first.local_drained());
    assert_eq!(
        h.delegate
            .owner_report("ws", Some(a.session().session_id()))
            .unwrap()
            .live,
        1
    );
    release.notify_one();
    let reports = h
        .agent
        .drain_session_invocations(a.session().session_id(), false, Duration::from_secs(5))
        .await;
    assert_eq!(reports.len(), 1);
    assert!(reports[0].local_joined);
    assert_eq!(reports[0].status, crabber::SessionInvocationStatus::Joined);
    h.agent
        .close_session_resources(a.session(), Duration::from_secs(5))
        .await
        .unwrap();
    assert!(
        h.delegate
            .owner_report("ws", Some(a.session().session_id()))
            .is_none_or(|r| r.live == 0 && r.closed)
    );
    h.close().await;
}

#[tokio::test(flavor = "multi_thread", worker_threads = 4)]
async fn parent_interruption_does_not_cancel_its_detached_child() {
    let entered = Arc::new(tokio::sync::Semaphore::new(0));
    let runner: delegate::Runner = {
        let entered = entered.clone();
        Arc::new(move |request| {
            let entered = entered.clone();
            Box::pin(async move {
                entered.add_permits(1);
                request.cancellation().cancelled().await;
                Ok(delegate::Response::Completed("cancel observed".into()))
            })
        })
    };
    let root = tempfile::tempdir().unwrap();
    let mut options = crate::options::delegate(runner);
    options.limits.max_in_flight = 2;
    let factory = Arc::new(delegate::Factory::new(options, FactoryScope::Session).unwrap());
    let scripts = Arc::new(crate::scripted::Scripts::default());
    let agent = crate::agent_support::builder()
        .memory()
        .provider(scripts.clone())
        .config(crabber::AgentConfig::new(selection()))
        .policy(Arc::new(crabber::StaticPolicy::new(
            crabber::PermissionDecision::Allow,
        )))
        .extension_factory(factory.clone())
        .build()
        .unwrap();
    let a = context(root.path(), "ws", "interrupted-parent");
    scripts.set(vec![
        calls(&[(
            delegate::START_TOOL,
            json!({"task":"detached", "profile":"test"}),
        )]),
        calls(&[(
            delegate::TOOL_NAME,
            json!({"task":"joined", "profile":"test"}),
        )]),
        done(),
    ]);
    let run = crate::agent_support::first_prompt(&agent, &a, "detach then await")
        .await
        .unwrap();
    tokio::time::timeout(Duration::from_secs(5), entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    run.interrupt();
    assert!(matches!(
        run.done().await,
        Err(crabber::RuntimeError::PhaseOutcomeUnknown { .. })
    ));
    let observed = agent
        .drain_session_invocations(a.session().session_id(), false, Duration::from_millis(1))
        .await;
    assert_eq!(observed.len(), 1);
    assert_eq!(observed[0].status, crabber::SessionInvocationStatus::Active);
    assert!(!observed[0].local_joined);
    assert!(
        factory
            .owner_report("ws", Some(a.session().session_id()))
            .unwrap()
            .live
            >= 1
    );
    let joined = agent
        .drain_session_invocations(a.session().session_id(), true, Duration::from_secs(5))
        .await;
    assert_eq!(joined.len(), 1);
    assert_eq!(joined[0].status, crabber::SessionInvocationStatus::Joined);
    assert!(joined[0].local_joined);
    let closed = agent.close(Duration::from_secs(5)).await;
    assert!(
        !closed.local_drained(),
        "native uncertainty remains after actual workers join"
    );
    assert!(closed.runtime.executions.pending.is_empty());
    assert!(
        closed
            .runtime
            .run_issues
            .iter()
            .any(|i| i.kind == crabber::runtime::RunCloseIssueKind::NativeOutcomeUnknown)
    );
    assert!(
        factory
            .owner_report("ws", Some(a.session().session_id()))
            .is_none_or(|r| r.live == 0)
    );
}
