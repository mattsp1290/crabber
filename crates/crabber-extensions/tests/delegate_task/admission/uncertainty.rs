use super::*;
struct Held {
    execute: bool,
    entered: tokio::sync::Semaphore,
    reply: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for Held {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        let mut task = Some(task);
        if self.execute {
            task.take().unwrap()(invocation()).await.unwrap();
        }
        self.entered.add_permits(1);
        self.reply.acquire().await.unwrap().forget();
        drop(task);
        if self.execute {
            Ok(Admission::Started)
        } else {
            Err(Error::Rejected)
        }
    }
}
#[tokio::test]
async fn fast_terminal_but_unacknowledged_registration_cannot_be_pruned_or_replayed_as_started() {
    let ext = Arc::new(configured(
        runner(Response::Completed("output".into())),
        Limits {
            max_tracked: 1,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let held = Arc::new(Held {
        execute: true,
        entered: tokio::sync::Semaphore::new(0),
        reply: tokio::sync::Semaphore::new(0),
    });
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(held.clone()));
    let tool = tools[START_TOOL].clone();
    let original = context.clone();
    let call =
        tokio::spawn(async move { tool.execute_json_with_context(original, arguments()).await });
    held.entered.acquire().await.unwrap().forget();
    assert_eq!(ext.live_runners(), 0);
    for candidate in [context.clone(), next(context.clone())] {
        assert_eq!(
            tools[START_TOOL]
                .execute_json_with_context(candidate, arguments())
                .await
                .unwrap()["status"],
            "unavailable"
        );
    }
    assert_eq!(ext.owner_report().tracked, 1);
    held.reply.add_permits(1);
    let first = call.await.unwrap().unwrap();
    assert_eq!(
        tools[START_TOOL]
            .execute_json_with_context(context, arguments())
            .await
            .unwrap(),
        first
    );
    handle.close().await.unwrap();
}
#[tokio::test]
async fn dropped_nondispatched_registration_rolls_back_even_during_close() {
    let ext = Arc::new(configured(
        runner(Response::Completed("original".into())),
        Limits {
            max_tracked: 1,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let context =
        context(CancellationToken::new()).with_session_invocations(SessionInvocations::from_host(
            Arc::new(crate::invocation_support::JoinedSpawner::default()),
        ));
    let first = tools[START_TOOL]
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    let before = tools[LIST_TOOL]
        .execute_json_with_context(context.clone(), json!({}))
        .await
        .unwrap();
    let held = Arc::new(Held {
        execute: false,
        entered: tokio::sync::Semaphore::new(0),
        reply: tokio::sync::Semaphore::new(0),
    });
    let candidate =
        next(context.clone()).with_session_invocations(SessionInvocations::from_host(held.clone()));
    let tool = tools[START_TOOL].clone();
    let original = candidate.clone();
    let call =
        tokio::spawn(async move { tool.execute_json_with_context(original, arguments()).await });
    held.entered.acquire().await.unwrap().forget();
    for candidate in [candidate.clone(), next(candidate)] {
        assert_eq!(
            tools[START_TOOL]
                .execute_json_with_context(candidate, arguments())
                .await
                .unwrap()["status"],
            "unavailable"
        );
    }
    ext.shutdown().await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(
        tools[LIST_TOOL]
            .execute_json_with_context(context.clone(), json!({}))
            .await
            .unwrap(),
        before
    );
    assert_eq!(
        tools[STATUS_TOOL]
            .execute_json_with_context(context, json!({"task_id":first["task_id"]}))
            .await
            .unwrap()["output"],
        "original"
    );
    let report = ext.owner_report();
    assert_eq!(
        (report.live, report.tracked, report.unresolved_invocations),
        (0, 1, 1)
    );
    assert!(report.closed);
    handle.close().await.unwrap();
}

#[tokio::test]
async fn concurrent_refusal_and_drop_restore_distinct_reserved_victims_during_close() {
    let ext = Arc::new(configured(
        runner(Response::Completed("retained".into())),
        Limits {
            max_tracked: 2,
            max_in_flight: 2,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let context =
        context(CancellationToken::new()).with_session_invocations(SessionInvocations::from_host(
            Arc::new(crate::invocation_support::JoinedSpawner::default()),
        ));
    for candidate in [context.clone(), next(context.clone())] {
        tools[START_TOOL]
            .execute_json_with_context(candidate, arguments())
            .await
            .unwrap();
    }
    let before = tools[LIST_TOOL]
        .execute_json_with_context(context.clone(), json!({}))
        .await
        .unwrap();
    let held = Arc::new(Held {
        execute: false,
        entered: tokio::sync::Semaphore::new(0),
        reply: tokio::sync::Semaphore::new(0),
    });
    let mut calls = Vec::new();
    for _ in 0..2 {
        let candidate = next(context.clone())
            .with_session_invocations(SessionInvocations::from_host(held.clone()));
        let tool = tools[START_TOOL].clone();
        calls.push(tokio::spawn(async move {
            tool.execute_json_with_context(candidate, arguments()).await
        }));
    }
    tokio::time::timeout(Duration::from_secs(2), held.entered.acquire_many(2))
        .await
        .unwrap()
        .unwrap()
        .forget();
    assert_eq!(ext.owner_report().tracked, 2);
    ext.shutdown().await;
    let dropped = calls.pop().unwrap();
    dropped.abort();
    assert!(dropped.await.unwrap_err().is_cancelled());
    held.reply.add_permits(1);
    assert_eq!(
        calls.pop().unwrap().await.unwrap().unwrap()["status"],
        "unavailable"
    );
    assert_eq!(
        tools[LIST_TOOL]
            .execute_json_with_context(context, json!({}))
            .await
            .unwrap(),
        before
    );
    let report = ext.owner_report();
    assert_eq!(
        (report.live, report.tracked, report.unresolved_invocations),
        (0, 2, 1)
    );
    handle.close().await.unwrap();
}

struct LateFactory {
    task: Mutex<Option<SessionInvocationTask>>,
    entered: tokio::sync::Semaphore,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for LateFactory {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        *self.task.lock().unwrap() = Some(task);
        self.entered.add_permits(1);
        std::future::pending().await
    }
}
#[tokio::test]
async fn late_factory_after_nondispatched_rollback_never_starts_business_work() {
    let callbacks = Arc::new(AtomicUsize::new(0));
    let runner: Runner = {
        let callbacks = callbacks.clone();
        Arc::new(move |_| {
            callbacks.fetch_add(1, Ordering::SeqCst);
            Box::pin(async { Ok(Response::Completed("original".into())) })
        })
    };
    let ext = Arc::new(configured(
        runner,
        Limits {
            max_tracked: 1,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let context =
        context(CancellationToken::new()).with_session_invocations(SessionInvocations::from_host(
            Arc::new(crate::invocation_support::JoinedSpawner::default()),
        ));
    let first = tools[START_TOOL]
        .execute_json_with_context(context.clone(), arguments())
        .await
        .unwrap();
    let late = Arc::new(LateFactory {
        task: Mutex::new(None),
        entered: tokio::sync::Semaphore::new(0),
    });
    let candidate =
        next(context.clone()).with_session_invocations(SessionInvocations::from_host(late.clone()));
    let tool = tools[START_TOOL].clone();
    let call =
        tokio::spawn(async move { tool.execute_json_with_context(candidate, arguments()).await });
    late.entered.acquire().await.unwrap().forget();
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(
        ext.owner_report().live,
        1,
        "retained unpolled factory still holds capacity"
    );
    assert_eq!(
        tools[STATUS_TOOL]
            .execute_json_with_context(context, json!({"task_id":first["task_id"]}))
            .await
            .unwrap()["output"],
        "original"
    );
    let task = late.task.lock().unwrap().take().unwrap();
    task(invocation()).await.unwrap();
    assert_eq!(callbacks.load(Ordering::SeqCst), 1);
    assert_eq!(
        (
            ext.owner_report().live,
            ext.owner_report().tracked,
            ext.owner_report().unresolved_invocations
        ),
        (0, 1, 1)
    );
    handle.close().await.unwrap();
}
struct Dispatched {
    workers: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
#[async_trait::async_trait]
impl SessionInvocationSpawner for Dispatched {
    async fn spawn(
        &self,
        _: SessionInvocationRequest,
        task: SessionInvocationTask,
    ) -> Result<Admission, Error> {
        self.workers.lock().unwrap().push(tokio::spawn(async move {
            task(invocation()).await.unwrap();
        }));
        std::future::pending().await
    }
}
#[tokio::test]
async fn lost_acknowledgement_after_dispatch_retains_actual_runner_for_close() {
    let mut pending = PendingHost::new(true);
    let ext = Arc::new(configured(
        pending.runner.clone(),
        Limits {
            max_tracked: 1,
            ..limits()
        },
    ));
    let (_, handle, tools) = mounted(ext.clone()).await;
    let host = Arc::new(Dispatched {
        workers: Mutex::new(vec![]),
    });
    let context = context(CancellationToken::new())
        .with_session_invocations(SessionInvocations::from_host(host.clone()));
    let tool = tools[START_TOOL].clone();
    let original = context.clone();
    let call =
        tokio::spawn(async move { tool.execute_json_with_context(original, arguments()).await });
    pending.entered(1).await;
    call.abort();
    assert!(call.await.unwrap_err().is_cancelled());
    assert_eq!(
        (
            ext.owner_report().live,
            ext.owner_report().tracked,
            ext.owner_report().unresolved_invocations
        ),
        (1, 1, 1)
    );
    assert_eq!(
        tools[START_TOOL]
            .execute_json_with_context(context.clone(), arguments())
            .await
            .unwrap()["status"],
        "unavailable"
    );
    ext.shutdown().await;
    pending.exited(1).await;
    let workers = std::mem::take(&mut *host.workers.lock().unwrap());
    for worker in workers {
        worker.await.unwrap();
    }
    assert_eq!(
        tools[LIST_TOOL]
            .execute_json_with_context(context, json!({}))
            .await
            .unwrap()["tasks"][0]["status"],
        "cancelled"
    );
    assert_eq!(ext.owner_report().live, 0);
    handle.close().await.unwrap();
}
