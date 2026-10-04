use super::{Probes, child};
use crabber::extension::{ExtensionError, ToolResultContext};
use std::process::Stdio;
use tokio::{
    io::{AsyncBufReadExt, AsyncReadExt, BufReader},
    process::Command,
    sync::{OwnedSemaphorePermit, oneshot},
};

type Ready = oneshot::Receiver<Result<(), ExtensionError>>;

pub(super) fn spawn(
    context: &ToolResultContext,
    permit: OwnedSemaphorePermit,
    probes: Probes,
) -> Result<Ready, ExtensionError> {
    let executable = std::env::current_exe().map_err(|failure| error(&failure))?;
    let mut child = Command::new(executable)
        .args(["--exact", child::TEST_NAME, "--nocapture"])
        .env(child::MODE_ENV, "reduce")
        .stdin(Stdio::piped())
        .stdout(Stdio::piped())
        .stderr(Stdio::null())
        .kill_on_drop(true)
        .spawn()
        .map_err(|failure| error(&failure))?;
    let pid = child.id().expect("spawned child PID");
    let stdout = child.stdout.take().expect("piped child stdout");
    let cancellation = context.cancellation().clone();
    let closing = context.cleanup().closing();
    let (ready_tx, ready_rx) = oneshot::channel();
    // No await between spawn and transfer: even readiness belongs to the tracker.
    context.cleanup().spawn(async move {
        let mut stdout = BufReader::new(stdout);
        tokio::pin!(closing);
        let readiness = async {
            let mut line = String::new();
            loop {
                line.clear();
                if stdout.read_line(&mut line).await.map_err(|failure| error(&failure))? == 0 {
                    return Err(ExtensionError::Tool("fixture exited before ready".into()));
                }
                if line.trim_end() == child::READY {
                    probes.ready_pid.send_replace(Some(pid));
                    return Ok(());
                }
            }
        };
        let ready = tokio::select! {
            biased;
            () = cancellation.cancelled() => Err(ExtensionError::Tool("fixture cancelled before ready".into())),
            () = probes.callback_dropped.wait() => Err(ExtensionError::Tool("fixture callback dropped before ready".into())),
            () = &mut closing => Err(ExtensionError::Tool("fixture closed before ready".into())),
            ready = readiness => ready,
        };
        let started = ready.is_ok();
        let _ = ready_tx.send(ready);
        if started {
            tokio::select! {
                biased;
                () = cancellation.cancelled() => {},
                () = &mut closing => {},
                () = probes.callback_dropped.wait() => {},
            }
        }
        child.start_kill().expect("start fixture kill");
        probes.kill_started.set();
        // Held after start_kill, before wait, for deterministic permit/reap sampling.
        probes.reap_gate.clone().acquire_owned().await.unwrap().forget();
        child.wait().await.expect("reap fixture child");
        probes.reaped.set();
        drop(child); // closes parent stdin after the child has been reaped
        let mut remaining = Vec::new();
        stdout.read_to_end(&mut remaining).await.expect("fixture stdout EOF");
        drop(stdout);
        probes.pipe_closed.set();
        drop(permit);
    });
    Ok(ready_rx)
}

fn error(error: &std::io::Error) -> ExtensionError {
    ExtensionError::Tool(format!("fixture process: {error}"))
}
