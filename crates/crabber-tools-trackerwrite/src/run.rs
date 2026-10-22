use crate::{Options, args::Args, result};
use crabber_tools_core::*;
use serde_json::{Value, json};
use std::{ffi::OsString, process::Stdio, time::Duration};
use tokio::{process::Command, time::Instant};
/// Production command deadline; cleanup follows the shared bounded reap grace.
pub const TIMEOUT: Duration = Duration::from_secs(60);
/// Retained diagnostic bytes per stream; excess bytes are drained.
pub const DIAGNOSTIC_CAP: usize = 4096;
fn diagnostic(code: i32, stderr: &CappedOutput) -> (&'static str, &'static str) {
    // Classify capped diagnostics into fixed messages. Raw process output can
    // contain credentials or host paths and never enters a model-facing error.
    let text = stderr.text().to_ascii_lowercase();
    match code {
        1 if text.contains("status") => (category::VALIDATION, "tracker rejected the target state"),
        1 => (category::VALIDATION, "tracker rejected the operation"),
        2 => (category::NOT_FOUND, "tracker issue was not found"),
        3 => ("api_request", "tracker synchronization failed"),
        4 => ("rate_limited", "tracker hub is busy"),
        _ => (category::UNKNOWN, "tracker command failed"),
    }
}
pub(crate) async fn run(
    options: &Options,
    args: &Args,
    argv: Vec<OsString>,
    ctx: &crabber::extension::ToolContext,
    permit: impl Send + 'static,
) -> Value {
    run_until(options, args, argv, Instant::now() + TIMEOUT, ctx, permit).await
}
async fn run_until(
    options: &Options,
    args: &Args,
    argv: Vec<OsString>,
    deadline: Instant,
    ctx: &crabber::extension::ToolContext,
    permit: impl Send + 'static,
) -> Value {
    let fail = |e| result::failure(&args.op, &args.id, e);
    let mut command = Command::new(&options.policy.bn_binary);
    command
        .args(argv)
        .current_dir(options.root.path())
        .stdout(Stdio::piped())
        .stderr(Stdio::piped());
    apply_policy(&mut command, &options.policy.env(), None);
    let captured =
        match run_process_owned_guarded(ctx, command, DIAGNOSTIC_CAP, deadline, permit).await {
            Ok(c) => c,
            Err(ProcessRunError::Unavailable) => {
                return fail(ToolError::new(
                    category::UNAVAILABLE,
                    "call resources unavailable",
                ));
            }
            Err(ProcessRunError::Spawn) => {
                return fail(ToolError::new(
                    "api_request",
                    "could not start tracker command",
                ));
            }
            Err(_) => {
                return fail(ToolError::new(
                    category::IO,
                    "could not capture tracker diagnostics",
                ));
            }
        };
    if captured.timed_out {
        return fail(ToolError::new(
            category::TIMEOUT,
            "tracker operation exceeded 60s timeout",
        ));
    }
    let code = captured.status.and_then(|s| s.code()).unwrap_or(-1);
    if code == 0 {
        json!({"outcome":"succeeded","op":args.op,"id":args.id})
    } else {
        let (category, message) = diagnostic(code, &captured.stderr);
        fail(ToolError::new(category, message))
    }
}

#[cfg(test)]
use crabber_tools_testkit as support;
#[cfg(test)]
mod tests {
    use super::*;
    use crate::TrackerPolicy;
    use std::{collections::BTreeSet, sync::Arc};
    #[tokio::test]
    async fn deadline_kills_group_and_reports_timeout() {
        let root = tempfile::tempdir().unwrap();
        let hub = tempfile::tempdir().unwrap();
        let binary = hub.path().join("fake-bn");
        support::script(
            &binary,
            "#!/bin/sh\nsleep 60 &\necho $! > \"$BEANS_HUB/child\"\necho $$ > \"$BEANS_HUB/leader\"\nwait\n",
        );
        let limits = support::limits();
        let options = Arc::new(Options {
            root: WorkspaceRoot::open(root.path()).unwrap(),
            limits: limits.clone(),
            capacity: Capacity::new(&limits).unwrap(),
            policy: TrackerPolicy {
                bn_binary: binary,
                project: "demo".into(),
                hub: hub.path().into(),
                actor: "tester".into(),
                statuses: BTreeSet::from(["open".into()]),
                environment: vec![("PATH".into(), "/usr/bin:/bin".into())],
            },
        });
        let child_options = options.clone();
        let task = tokio::spawn(async move {
            let args: Args =
                serde_json::from_value(json!({"op":"close","id":"demo-ab12"})).unwrap();
            let argv = args.command(&child_options.policy).unwrap();
            run_until(
                &child_options,
                &args,
                argv,
                Instant::now() + Duration::from_secs(1),
                &support::context(
                    &child_options.root,
                    tokio_util::sync::CancellationToken::new(),
                ),
                (),
            )
            .await
        });
        support::wait_file(&hub.path().join("child")).await;
        support::wait_file(&hub.path().join("leader")).await;
        let child = support::pid(&hub.path().join("child"));
        let leader = support::pid(&hub.path().join("leader"));
        let v = task.await.unwrap();
        assert_eq!(v["error"]["category"], "timeout");
        support::assert_gone(child).await;
        support::assert_gone(leader).await;
        assert_eq!(TIMEOUT, Duration::from_secs(60));
    }
}
