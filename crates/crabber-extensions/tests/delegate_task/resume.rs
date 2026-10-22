use super::*;
use crabber::{
    RuntimeError,
    core::ToolInfo,
    runtime::{InterruptPolicy, PermissionPolicy},
};
use crabber_extensions::delegate_task::{LIST_TOOL, START_TOOL, STATUS_TOOL, WAIT_TOOL};

struct PausePolicy(bool);
impl PermissionPolicy for PausePolicy {
    fn decide(&self, _: &ToolInfo, _: &Value) -> PermissionDecision {
        PermissionDecision::Allow
    }
    fn interrupt_policy(&self, _: &ToolInfo, _: &Value) -> InterruptPolicy {
        if self.0 {
            InterruptPolicy::Pause
        } else {
            InterruptPolicy::Continue
        }
    }
}
fn build(
    ext: Arc<DelegateTask>,
    store: Arc<MemoryStore>,
    pause: bool,
    scripts: Vec<Vec<StreamDelta>>,
) -> Agent {
    Agent::builder()
        .behavior_version("crabber-extensions-tests.v1")
        .store(store)
        .provider(Arc::new(FakeProvider::scripted(scripts)))
        .config(agent_config())
        .policy(Arc::new(PausePolicy(pause)))
        .shared_extension(ext, Scope::Global)
        .build()
        .unwrap()
}
async fn paused(
    ext: Arc<DelegateTask>,
    store: Arc<MemoryStore>,
    session: Option<SessionId>,
    name: &str,
    args: Value,
) -> (Agent, RunId, SessionId) {
    let a = build(ext, store, true, vec![tool_call(name, args)]);
    let run = a.prompt(session, "pause").await.unwrap();
    let id = run.run_id().clone();
    let session = run.session_id().clone();
    assert_eq!(run.done().await.unwrap().status, RunStatus::Paused);
    (a, id, session)
}
#[tokio::test]
async fn paused_pending_start_runs_once_on_resume() {
    let calls = Arc::new(AtomicUsize::new(0));
    let (entered, mut admission) = tokio::sync::watch::channel(false);
    let runner: Runner = {
        let calls = calls.clone();
        Arc::new(move |_| {
            calls.fetch_add(1, Ordering::SeqCst);
            entered.send_replace(true);
            Box::pin(async { Ok(Response::Completed("ok".into())) })
        })
    };
    let ext = Arc::new(configured(runner, limits()));
    let store = Arc::new(MemoryStore::new());
    let (a, id, session) = paused(ext.clone(), store.clone(), None, START_TOOL, arguments()).await;
    assert_eq!(calls.load(Ordering::SeqCst), 0);
    assert_eq!(
        snapshot_tool(&store, &session).await.status,
        crabber::core::ToolCallStatus::Pending
    );
    let b = build(ext, store.clone(), false, vec![done()]);
    assert_eq!(b.resume(&id).await.unwrap().status, RunStatus::Completed);
    let record = snapshot_tool(&store, &session).await;
    assert_eq!(result_value(&record)["status"], "started");
    admission.wait_for(|entered| *entered).await.unwrap();
    assert_eq!(calls.load(Ordering::SeqCst), 1);
    a.close_extensions().await.unwrap();
    b.close_extensions().await.unwrap();
}

#[tokio::test]
async fn resume_refuses_every_drift_before_mutation() {
    for field in 0..=DRIFTS.len() {
        let calls = Arc::new(AtomicUsize::new(0));
        let host: Runner = {
            let calls = calls.clone();
            Arc::new(move |_| {
                calls.fetch_add(1, Ordering::SeqCst);
                Box::pin(async { Ok(Response::Completed(String::new())) })
            })
        };
        let store = Arc::new(MemoryStore::new());
        let ext = Arc::new(configured(host.clone(), limits()));
        let (a, id, session) = paused(ext, store.clone(), None, START_TOOL, arguments()).await;
        let mut l = limits();
        if let Some(drift) = DRIFTS.get(field) {
            drift(&mut l);
        }
        let changed = Arc::new(
            DelegateTask::new(Options {
                runner_identity: if field == DRIFTS.len() {
                    "host-v2"
                } else {
                    "host-v1"
                }
                .into(),
                runner: host,
                limits: l,
            })
            .unwrap(),
        );
        let b = build(changed, store.clone(), false, vec![done()]);
        let before = snapshot_page(&store, &session).await;
        assert!(
            matches!(b.resume(&id).await, Err(RuntimeError::PlanChanged)),
            "field {field}"
        );
        let after = snapshot_page(&store, &session).await;
        assert_eq!(format!("{before:?}"), format!("{after:?}"), "field {field}");
        assert_eq!(calls.load(Ordering::SeqCst), 0);
        a.close_extensions().await.unwrap();
        b.close_extensions().await.unwrap();
    }
}

#[tokio::test(start_paused = true)]
async fn paused_pending_status_and_wait_reexecute_on_resume() {
    for name in [STATUS_TOOL, WAIT_TOOL] {
        for settle in [true, false] {
            let mut host = PendingHost::new(false);
            let mut j = super::background::Journey::new(
                host.runner.clone(),
                Limits {
                    max_background_duration: Duration::from_secs(60),
                    ..limits()
                },
            );
            let task = j.start().await;
            host.entered(1).await;
            let args = if name == STATUS_TOOL {
                json!({"task_id":task})
            } else {
                json!({"task_ids":[task],"timeout_seconds":1})
            };
            let (a, id, session) = paused(
                j.ext.clone(),
                j.store.clone(),
                j.session.clone(),
                name,
                args,
            )
            .await;
            if settle {
                host.release.notify_one();
                host.exited(1).await;
                tokio::task::yield_now().await;
            }
            // Time spent paused cannot consume the resumed wait's timeout.
            tokio::time::advance(Duration::from_secs(2)).await;
            let before = tokio::time::Instant::now();
            let b = build(j.ext.clone(), j.store.clone(), false, vec![done()]);
            assert_eq!(b.resume(&id).await.unwrap().status, RunStatus::Completed);
            let records = snapshot_tools(&j.store, &session).await;
            let reply = result_value(records.last().unwrap());
            assert_eq!(
                reply["status"],
                if name == STATUS_TOOL {
                    if settle { "completed" } else { "running" }
                } else if settle {
                    "done"
                } else {
                    "timed_out"
                }
            );
            assert_eq!(
                before.elapsed(),
                if name == WAIT_TOOL && !settle {
                    Duration::from_secs(1)
                } else {
                    Duration::ZERO
                }
            );
            if !settle {
                host.release.notify_one();
                host.exited(1).await;
            }
            a.close_extensions().await.unwrap();
            b.close_extensions().await.unwrap();
        }
    }
}

#[tokio::test]
async fn status_wait_and_list_after_restart_see_no_tasks() {
    for name in [STATUS_TOOL, WAIT_TOOL, LIST_TOOL] {
        let mut j =
            super::background::Journey::new(runner(Response::Completed("ok".into())), limits());
        let task = j.start().await;
        j.wait(std::slice::from_ref(&task)).await;
        let args = match name {
            STATUS_TOOL => json!({"task_id":task}),
            WAIT_TOOL => json!({"task_ids":[task]}),
            _ => json!({}),
        };
        let (a, id, session) = paused(
            j.ext.clone(),
            j.store.clone(),
            j.session.clone(),
            name,
            args,
        )
        .await;
        let fresh = Arc::new(extension(Response::Completed("ok".into())));
        assert_eq!(fresh.config_hash(), j.ext.config_hash());
        let b = build(fresh, j.store.clone(), false, vec![done()]);
        assert_eq!(b.resume(&id).await.unwrap().status, RunStatus::Completed);
        let records = snapshot_tools(&j.store, &session).await;
        let record = records.last().unwrap();
        if name == LIST_TOOL {
            assert_eq!(result_value(record), json!({"tasks":[]}));
        } else {
            assert_eq!(record.status, crabber::core::ToolCallStatus::Failed);
            assert!(format!("{:?}", record.result).contains("input invalid: task_id"));
        }
        a.close_extensions().await.unwrap();
        b.close_extensions().await.unwrap();
    }
}
