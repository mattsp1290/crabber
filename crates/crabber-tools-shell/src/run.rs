use crate::{Options, StartupMode, args::Args};
use crabber_tools_core::*;
use serde_json::{Value, json};
use std::{os::unix::process::ExitStatusExt, process::Stdio, time::Duration};
use tokio::{process::Command, time::Instant};
pub(crate) async fn run(
    options: &Options,
    args: Args,
    ctx: &crabber::extension::ToolContext,
    guards: impl Send + 'static,
) -> Value {
    let start = Instant::now();
    let mut cmd = Command::new(&options.policy.shell_binary);
    cmd.arg(match options.policy.startup_mode {
        StartupMode::Login => "-lc",
        StartupMode::NonLogin => "-c",
    })
    .arg(&args.cmd)
    .current_dir(options.root.path())
    .stdout(Stdio::piped())
    .stderr(Stdio::piped());
    apply_policy(&mut cmd, &options.policy.env, options.policy.run_as);
    let capture = match run_process_owned_guarded(
        ctx,
        cmd,
        options.policy.output_cap_bytes,
        start + Duration::from_secs(args.timeout_seconds),
        guards,
    )
    .await
    {
        Ok(c) => c,
        Err(ProcessRunError::Unavailable) => {
            return failed(category::UNAVAILABLE, "call resources unavailable");
        }
        Err(ProcessRunError::Spawn) => {
            return failed(category::EXEC_FAILED, "could not start shell");
        }
        Err(_) => return failed(category::IO, "could not capture shell output"),
    };
    let code = capture
        .status
        .map(|s| s.code().unwrap_or_else(|| 128 + s.signal().unwrap_or(0)))
        .unwrap_or(-1);
    let mut value = if capture.timed_out {
        failed(
            category::TIMEOUT,
            format!("command exceeded {}s timeout", args.timeout_seconds),
        )
    } else {
        json!({"outcome":"succeeded"})
    };
    value["exit_code"] = json!(code);
    value["stdout"] = json!(capture.stdout.text());
    value["stderr"] = json!(capture.stderr.text());
    value["duration_ms"] = json!(start.elapsed().as_millis() as u64);
    if capture.stdout.truncated {
        value["stdout_truncated"] = json!(true);
    }
    if capture.stderr.truncated {
        value["stderr_truncated"] = json!(true);
    }
    if capture.timed_out {
        value["timed_out"] = json!(true);
    }
    value
}
