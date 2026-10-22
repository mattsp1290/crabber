use crate::json_output::JsonOutputExecutor as _;
use crabber::{
    Agent, FakeProvider, PermissionDecision, StaticPolicy, StreamDelta,
    core::{RunId, RunStatus, SessionId, ToolCallId},
    extension::{Extension, ExtensionError, Registry, Scope, ToolExecutor},
    session::{MemoryStore, SnapshotLimits, SnapshotOutcome, SnapshotRequest, Store},
};
use crabber_extensions::delegate_task::{
    DelegateTask, Limits, Options, PERMISSION_DELEGATE, Request, Response, Runner, TOOL_NAME,
};
use serde_json::{Value, json};
use std::{
    future::Future,
    pin::Pin,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

fn limits() -> Limits {
    Limits {
        max_tracked: 4,
        max_background_duration: Duration::from_secs(5),
        max_wait_ids: 4,
        max_wait_reply_bytes: 300,
        max_task_bytes: 100,
        max_profile_bytes: 50,
        max_result_bytes: 100,
        max_in_flight: 1,
        max_wait: Duration::from_secs(5),
        shutdown_grace: Duration::from_secs(2),
    }
}
fn runner(reply: Response) -> Runner {
    Arc::new(move |_| {
        let reply = match &reply {
            Response::Completed(v) => Response::Completed(v.clone()),
            Response::Failed(v) => Response::Failed(v.clone()),
            Response::Rejected(v) => Response::Rejected(v.clone()),
            Response::Unavailable(v) => Response::Unavailable(v.clone()),
        };
        Box::pin(async move { Ok(reply) })
    })
}
fn configured(runner: Runner, limits: Limits) -> DelegateTask {
    DelegateTask::new(Options {
        runner_identity: "host-v1".into(),
        runner,
        limits,
    })
    .unwrap()
}
fn extension(reply: Response) -> DelegateTask {
    configured(runner(reply), limits())
}
fn arguments() -> Value {
    json!({"task":" Inspect the synthetic project. ","profile":"read-only"})
}

fn call() -> Vec<StreamDelta> {
    call_with(arguments())
}

struct PendingHost {
    runner: Runner,
    counts: tokio::sync::watch::Receiver<(usize, usize)>,
    seen: Arc<std::sync::Mutex<Vec<Request>>>,
    dropped: Arc<AtomicUsize>,
    release: Arc<tokio::sync::Notify>,
}
impl PendingHost {
    fn new(cooperative: bool) -> Self {
        let (counts, rx) = tokio::sync::watch::channel((0, 0));
        let seen = Arc::new(std::sync::Mutex::new(Vec::new()));
        let dropped = Arc::new(AtomicUsize::new(0));
        let release = Arc::new(tokio::sync::Notify::new());
        let runner: Runner = {
            let seen = seen.clone();
            let dropped = dropped.clone();
            let release = release.clone();
            Arc::new(move |request| {
                counts.send_modify(|c| c.0 += 1);
                let token = request.cancellation().clone();
                seen.lock().unwrap().push(request);
                let counts = counts.clone();
                let release = release.clone();
                let flag = DropFlag(dropped.clone());
                Box::pin(async move {
                    let _flag = flag;
                    if cooperative {
                        token.cancelled().await;
                    } else {
                        release.notified().await;
                    }
                    counts.send_modify(|c| c.1 += 1);
                    Ok(Response::Completed("released".into()))
                })
            })
        };
        Self {
            runner,
            counts: rx,
            seen,
            dropped,
            release,
        }
    }
    fn pending_after(&self, count: usize) -> Runner {
        Self::first_then(
            runner(Response::Completed("ok".into())),
            count,
            self.runner.clone(),
        )
    }
    fn first_then(first: Runner, count: usize, after: Runner) -> Runner {
        let calls = AtomicUsize::new(0);
        Arc::new(move |request| {
            if calls.fetch_add(1, Ordering::SeqCst) < count {
                first(request)
            } else {
                after(request)
            }
        })
    }
    fn pending_on(&self, index: usize, output: &str) -> Runner {
        let calls = AtomicUsize::new(0);
        let pending = self.runner.clone();
        let otherwise = runner(Response::Completed(output.into()));
        Arc::new(move |request| {
            if calls.fetch_add(1, Ordering::SeqCst) == index {
                pending(request)
            } else {
                otherwise(request)
            }
        })
    }
    async fn entered(&mut self, count: usize) {
        self.wait(count, false).await;
    }
    async fn exited(&mut self, count: usize) {
        self.wait(count, true).await;
    }
    async fn wait(&mut self, count: usize, exit: bool) {
        loop {
            let value = *self.counts.borrow_and_update();
            if (if exit { value.1 } else { value.0 }) >= count {
                return;
            }
            self.counts.changed().await.unwrap();
        }
    }
}
fn spawn_call(
    tool: Arc<dyn ToolExecutor>,
    cancel: CancellationToken,
) -> tokio::task::JoinHandle<Result<Value, ExtensionError>> {
    tokio::spawn(async move {
        tool.execute_json_with_context(context(cancel), arguments())
            .await
    })
}

#[path = "delegate_task/contract.rs"]
mod contract;

#[path = "delegate_task/runtime.rs"]
mod runtime;

#[path = "delegate_task/lifecycle.rs"]
mod lifecycle;

#[path = "delegate_task/child.rs"]
mod child;

#[path = "delegate_task/routing.rs"]
mod routing;

#[path = "support/delegate_task.rs"]
mod delegate_support;
use delegate_support::*;

#[path = "delegate_task/background.rs"]
mod background;

#[path = "delegate_task/background_lifecycle.rs"]
mod background_lifecycle;

#[path = "delegate_task/resume.rs"]
mod resume;

#[path = "support/json_output.rs"]
mod json_output;

#[path = "support/scripted.rs"]
mod scripted;

#[path = "support/agent.rs"]
mod agent_support;

#[path = "delegate_task/attribution.rs"]
mod attribution;

#[path = "delegate_task/detached.rs"]
mod detached;

#[path = "support/invocations.rs"]
mod invocation_support;

#[path = "delegate_task/admission.rs"]
mod admission;
