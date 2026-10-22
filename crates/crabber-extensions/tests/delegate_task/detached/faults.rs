use super::*;
use crabber::extension::{
    SessionInvocationAdmission, SessionInvocationError as Error, SessionInvocationRequest,
    SessionInvocationSpawner, SessionInvocationTask, SessionInvocations,
};
use std::sync::Mutex;
struct Refusal {
    outcome: Result<SessionInvocationAdmission, Error>,
    requests: Mutex<Vec<SessionInvocationRequest>>,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for Refusal {
    async fn spawn(
        &self,
        request: SessionInvocationRequest,
        _: SessionInvocationTask,
    ) -> Result<SessionInvocationAdmission, Error> {
        self.requests.lock().unwrap().push(request);
        self.outcome
    }
}
#[tokio::test]
async fn nondispatched_registration_releases_record_and_runner_permits() {
    for outcome in [
        Err(Error::UnknownOutcome),
        Err(Error::Capacity),
        Err(Error::Rejected),
        Err(Error::Unsupported),
        Ok(SessionInvocationAdmission::Replayed),
    ] {
        let calls = Arc::new(AtomicUsize::new(0));
        let host: Runner = {
            let calls = calls.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Response::Completed("must not run".into())) })
            })
        };
        let ext = Arc::new(configured(host, limits()));
        let (_, handle, tools) = mounted(ext.clone()).await;
        let spawner = Arc::new(Refusal {
            outcome,
            requests: Mutex::new(vec![]),
        });
        let context = context(CancellationToken::new())
            .with_session_invocations(SessionInvocations::from_host(spawner.clone()));
        let result = tools[crabber_extensions::delegate_task::START_TOOL]
            .execute_json_with_context(context, arguments())
            .await;
        if matches!(outcome, Err(Error::Capacity | Error::Rejected)) {
            assert_eq!(result.unwrap()["status"], "unavailable");
        } else {
            assert!(
                result
                    .unwrap_err()
                    .to_string()
                    .ends_with("failed: session-jobs")
            );
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        let report = ext.owner_report();
        assert_eq!(report.live, 0);
        assert_eq!(report.tracked, 0);
        assert_eq!(
            report.unresolved_invocations,
            usize::from(outcome == Err(Error::UnknownOutcome))
        );
        {
            let requests = spawner.requests.lock().unwrap();
            assert_eq!(requests.len(), 1);
            assert_eq!(
                requests[0].component_id,
                "crabber-extensions/delegate-task.background"
            );
            let ids = [
                &requests[0].reserve_id,
                &requests[0].begin_id,
                &requests[0].join_id,
            ];
            assert_ne!(ids[0], ids[1]);
            assert_ne!(ids[0], ids[2]);
            assert_ne!(ids[1], ids[2]);
        }
        handle.close().await.unwrap();
    }
}
#[tokio::test]
async fn unresolved_reconciliation_is_bounded_without_evicting_unknown_requests() {
    let ext = Arc::new(extension(Response::Completed("must not run".into())));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let spawner = Arc::new(Refusal {
        outcome: Err(Error::UnknownOutcome),
        requests: Mutex::new(vec![]),
    });
    for i in 0..64 {
        let mut context = context(CancellationToken::new())
            .with_session_invocations(SessionInvocations::from_host(spawner.clone()));
        context.call_id = ToolCallId::from(format!("unknown-{i}"));
        assert!(
            tools[crabber_extensions::delegate_task::START_TOOL]
                .execute_json_with_context(context, arguments())
                .await
                .is_err()
        );
    }
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(spawner.clone()));
    assert_eq!(
        tools[crabber_extensions::delegate_task::START_TOOL]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap()["status"],
        "unavailable"
    );
    assert_eq!(spawner.requests.lock().unwrap().len(), 64);
    let report = ext.owner_report();
    assert_eq!(report.live, 0);
    assert_eq!(report.tracked, 0);
    assert_eq!(report.unresolved_invocations, 64);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn identical_registration_replay_returns_the_retained_id_without_running_twice() {
    let calls = Arc::new(AtomicUsize::new(0));
    let host: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed("once".into())) })
        })
    };
    let ext = Arc::new(configured(host, limits()));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let spawner = Arc::new(crate::invocation_support::JoinedSpawner::default());
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(spawner));
    let first = tools[crabber_extensions::delegate_task::START_TOOL]
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    let second = tools[crabber_extensions::delegate_task::START_TOOL]
        .execute_json_with_context(context, arguments())
        .await
        .unwrap();
    assert_eq!(first, second);
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    assert_eq!(ext.owner_report().tracked, 1);
    assert_eq!(ext.live_runners(), 0);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn caller_drop_retains_the_original_registration_for_reconciliation() {
    struct Pending {
        entered: tokio::sync::Notify,
        request: Mutex<Option<SessionInvocationRequest>>,
    }
    #[async_trait::async_trait]
    impl SessionInvocationSpawner for Pending {
        async fn spawn(
            &self,
            request: SessionInvocationRequest,
            task: SessionInvocationTask,
        ) -> Result<SessionInvocationAdmission, Error> {
            *self.request.lock().unwrap() = Some(request);
            let _task = task;
            self.entered.notify_one();
            std::future::pending().await
        }
    }
    let ext = Arc::new(extension(Response::Completed("must not run".into())));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let spawner = Arc::new(Pending {
        entered: tokio::sync::Notify::new(),
        request: Mutex::new(None),
    });
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(spawner.clone()));
    let tool = tools[crabber_extensions::delegate_task::START_TOOL].clone();
    let call =
        tokio::spawn(async move { tool.execute_json_with_context(context, arguments()).await });
    tokio::time::timeout(Duration::from_secs(2), spawner.entered.notified())
        .await
        .unwrap();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert!(spawner.request.lock().unwrap().is_some());
    assert_eq!(ext.live_runners(), 0);
    assert_eq!(ext.owner_report().unresolved_invocations, 1);
    handle.close().await.unwrap();
}
