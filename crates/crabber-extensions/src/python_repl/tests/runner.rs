use super::support::{group_gone, group_members, python};
use crate::{process::RunnerLaunch, python_repl::runner::*};
use serde_json::json;
use std::time::Duration;
use tokio::time::{Instant, sleep};
use tokio_util::sync::CancellationToken;

const BOUNDS: Bounds = Bounds {
    request: 100_000,
    response: 100_000,
    output: 32,
    result: 1024,
    exception: 2048,
};
const GRACE: Duration = Duration::from_millis(100);
const WAIT: Duration = Duration::from_secs(3);

async fn start(
    source: &str,
    cancel: &CancellationToken,
    close: &CancellationToken,
    timeout: Duration,
) -> Option<Result<Runner, StartFailure>> {
    let path = python()?;
    let args = arguments(source, BOUNDS);
    Some(
        Runner::start(
            RunnerLaunch {
                program: &path,
                args: &args,
                directory: std::path::Path::new("/tmp"),
                environment: &[],
            },
            BOUNDS,
            cancel,
            close,
            Instant::now() + timeout,
            GRACE,
            WAIT,
        )
        .await,
    )
}

async fn real() -> Option<Runner> {
    start(
        RUNNER_SOURCE,
        &CancellationToken::new(),
        &CancellationToken::new(),
        WAIT,
    )
    .await
    .map(|r| r.unwrap_or_else(|e| panic!("runner startup: {:?}", e.fault)))
}
async fn response(runner: &mut Runner, code: &str) -> Response {
    match runner
        .execute(
            code,
            &CancellationToken::new(),
            &CancellationToken::new(),
            Instant::now() + WAIT,
        )
        .await
    {
        ExecuteOutcome::Completed(response) => response,
        other => panic!("unexpected outcome: {other:?}"),
    }
}
async fn stop(runner: &mut Runner) {
    let id = runner.child.pgid().as_raw_nonzero().get();
    assert!(runner.terminate(GRACE, WAIT).await.reaped);
    group_gone(id).await;
}

const READY_SLEEP: &str = "import os,struct,json,time; b=json.dumps({'version':'python-repl-runner-v1','phase':'ready','python':[3,14]}).encode(); os.write(1,struct.pack('>I',len(b))+b); time.sleep(60)";
#[path = "runner/execution.rs"]
mod execution;
#[path = "runner/lifecycle.rs"]
mod lifecycle;
#[path = "runner/protocol.rs"]
mod protocol;
