//! Concurrent bounded pipe I/O followed by a natural-exit probe and group sweep.
use super::{
    Filter,
    engine::{Guard, Reason, Reduced},
};
use crate::process::{RunnerLaunch, spawn_runner};
use crabber::extension::ToolResultContext;
use std::time::Duration;
use tokio::{
    io::{AsyncReadExt, AsyncWriteExt},
    sync::oneshot,
    time::Instant,
};

pub(super) async fn run(
    guard: &mut Guard,
    context: ToolResultContext,
    filter: Filter,
    input: String,
    deadline: Instant,
    mut sender: oneshot::Sender<Reduced>,
) {
    let (output, reaped) = invoke(guard, &context, filter, input, deadline, &mut sender).await;
    // invoke always reaps a spawned child; the panic guard handles unwind.
    guard.finish(reaped).await;
    if let Reduced::Unchanged(reason) = &output {
        guard.engine.count(*reason);
    }
    let _ = sender.send(output);
}
async fn invoke(
    guard: &mut Guard,
    context: &ToolResultContext,
    filter: Filter,
    input: String,
    deadline: Instant,
    sender: &mut oneshot::Sender<Reduced>,
) -> (Reduced, bool) {
    if context.cancellation().is_cancelled() || sender.is_closed() {
        return (Reduced::Unchanged(Reason::Cancelled), true);
    }
    if guard.engine.closing.is_cancelled() || context.cleanup().is_closing() {
        return (Reduced::Unchanged(Reason::Closed), true);
    }
    if Instant::now() >= deadline {
        return (Reduced::Unchanged(Reason::Timeout), true);
    }
    let environment = match super::private_dirs::prepare(guard) {
        Ok(e) => e,
        Err(r) => return (Reduced::Unchanged(r), true),
    };
    let args = ["pipe".into(), "--filter".into(), filter.cli_name().into()];
    let directory = guard.dir.as_ref().unwrap().join("work");
    let mut child = match spawn_runner(RunnerLaunch {
        program: &guard.engine.configuration.executable,
        args: &args,
        directory: &directory,
        environment: &environment,
    }) {
        Ok(child) => child,
        Err(_) => return (Reduced::Unchanged(Reason::Spawn), true),
    };
    #[cfg(all(test, unix))]
    {
        let hooks = {
            let mut hooks = guard.engine.hooks.lock().unwrap();
            hooks.spawned_groups.push(child.pgid().as_raw_pid());
            hooks.clone()
        };
        child.hooks.reap_timeout_once.store(
            hooks.reap_timeout_once,
            std::sync::atomic::Ordering::Relaxed,
        );
        assert!(!hooks.panic_after_spawn, "synthetic owner panic");
    }
    // Keep the reader owned through the sweep. Dropping it on overflow could
    // SIGPIPE the writer just as Darwin's group signaling probes its exit.
    let (stdin, mut stdout) = child.take_pipes().unzip();
    let io = async {
        let mut stdin = stdin.ok_or(Reason::WriteFailed)?;
        let write = async {
            stdin
                .write_all(input.as_bytes())
                .await
                .map_err(|_| Reason::WriteFailed)?;
            drop(stdin);
            Ok::<_, Reason>(())
        };
        let read = async {
            let cap = guard.engine.configuration.limits.max_stdout_bytes;
            let mut bytes = Vec::new();
            stdout
                .as_mut()
                .ok_or(Reason::WriteFailed)?
                .take((cap + 1) as u64)
                .read_to_end(&mut bytes)
                .await
                .map_err(|_| Reason::ExitFailure)?;
            if bytes.len() > cap {
                return Err(Reason::Overflow);
            }
            Ok(bytes)
        };
        // Require a complete stdin write. Small inputs can fit in the pipe even
        // if the child exits without reading; large inputs then fail the write.
        // Retain a write failure while observing stdout EOF and natural exit;
        // the shared deadline bounds a child that keeps its output open.
        // A read error or overflow proceeds straight to the group sweep.
        tokio::pin!(write, read);
        let (written, bytes) = tokio::select! {
            biased;
            written = &mut write => (written, read.await?),
            bytes = &mut read => { let bytes = bytes?; (write.await, bytes) }
        };
        while !child.leader_exited() {
            tokio::time::sleep(Duration::from_millis(10)).await;
        }
        written?;
        Ok::<_, Reason>(bytes)
    };
    let outcome = tokio::select! {
        biased;
        _ = context.cancellation().cancelled() => Err(Reason::Cancelled),
        _ = sender.closed() => Err(Reason::Cancelled),
        _ = context.cleanup().closing() => Err(Reason::Closed),
        _ = guard.engine.closing.cancelled() => Err(Reason::Closed),
        _ = tokio::time::sleep_until(deadline) => Err(Reason::Timeout),
        result = io => result,
    };
    #[cfg(all(test, unix))]
    {
        let hooks = guard.engine.hooks.lock().unwrap().clone();
        if let Some(entered) = hooks.at_reap {
            entered.add_permits(1);
        }
        if let Some(gate) = hooks.before_reap {
            gate.acquire().await.unwrap().forget();
        }
    }
    let reap = child
        .terminate(Duration::ZERO, guard.engine.configuration.limits.kill_wait)
        .await;
    drop(stdout);
    let result = classify(
        outcome,
        reap.status.is_some_and(|s| s.success()),
        reap.reaped,
    );
    (result, reap.reaped)
}
fn classify(outcome: Result<Vec<u8>, Reason>, success: bool, reaped: bool) -> Reduced {
    let reason = match outcome {
        Err(r) => r,
        Ok(_) if !success || !reaped => Reason::ExitFailure,
        Ok(bytes) if bytes.is_empty() => Reason::EmptyOutput,
        Ok(bytes) => match String::from_utf8(bytes) {
            Ok(text) => return Reduced::Output(text),
            Err(_) => Reason::InvalidUtf8,
        },
    };
    Reduced::Unchanged(reason)
}
