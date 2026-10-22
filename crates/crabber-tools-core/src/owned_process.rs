//! Registered child ownership, including streaming capture and cancellation reap.
use crate::{Capture, ProcessGroupGuard, WAIT_AFTER_KILL, capture};
use crabber::extension::{ResourceFailure, ToolContext};
use std::{future::Future, io, pin::Pin};
use tokio::{process::Command, sync::oneshot, time::Instant};

/// Sanitized process lifecycle failure; never exposes command or output text.
#[derive(Debug)]
pub enum ProcessRunError {
    /// Process creation failed.
    Spawn,
    /// Reading output or waiting failed.
    Capture,
    /// Call resource ownership is absent, sealed or full.
    Unavailable,
    /// The executor was interrupted or its owner closed.
    Cancelled,
}

/// Run bounded capture inside a registered process resource.
///
/// # Errors
/// Returns a process error on cancellation, unavailable resources, spawn failure, or capture failure.
pub async fn run_process_owned(
    ctx: &ToolContext,
    cmd: Command,
    cap: usize,
    deadline: Instant,
) -> Result<Capture, ProcessRunError> {
    run_process_owned_with(ctx, cmd, (), move |guard| {
        Box::pin(capture(guard, cap, deadline))
    })
    .await
}

/// Retain capacity and writer guards until capture and process cleanup finish.
///
/// # Errors
/// Returns a process error on cancellation, unavailable resources, spawn failure, or capture failure.
pub async fn run_process_owned_guarded(
    ctx: &ToolContext,
    cmd: Command,
    cap: usize,
    deadline: Instant,
    held: impl Send + 'static,
) -> Result<Capture, ProcessRunError> {
    run_process_owned_with(ctx, cmd, held, move |guard| {
        Box::pin(capture(guard, cap, deadline))
    })
    .await
}

/// Run a streaming reader under the same registered process ownership as capture.
/// The callback must await its own output work; cancellation drops it before the
/// owner kills and reaps the process group. No callback may detach pipe readers.
///
/// # Errors
/// Returns a process error on cancellation, unavailable resources, spawn failure, or callback/cleanup failure.
pub async fn run_process_owned_with<T: Send + 'static>(
    ctx: &ToolContext,
    mut cmd: Command,
    held: impl Send + 'static,
    work: impl for<'a> FnOnce(
        &'a mut ProcessGroupGuard,
    ) -> Pin<Box<dyn Future<Output = io::Result<T>> + Send + 'a>>
    + Send
    + 'static,
) -> Result<T, ProcessRunError> {
    if ctx.cancel.is_cancelled() {
        return Err(ProcessRunError::Cancelled);
    }
    let (mut tx, rx) = oneshot::channel();
    let cancel = ctx.cancel.clone();
    let closing = ctx.resources().closing();
    ctx.resources()
        .spawn(async move {
            let _held = held;
            let closed = async {
                match closing {
                    Some(closing) => closing.await,
                    None => std::future::pending().await,
                }
            };
            tokio::pin!(closed);
            tokio::select! { biased;
                () = cancel.cancelled() => { let _=tx.send(Err(ProcessRunError::Cancelled));return Ok(()); },
                () = &mut closed => { let _=tx.send(Err(ProcessRunError::Cancelled));return Ok(()); },
                () = tx.closed() => return Ok(()),
                () = std::future::ready(()) => {},
            }
            let Ok(mut guard) = ProcessGroupGuard::spawn(&mut cmd) else {
                let _ = tx.send(Err(ProcessRunError::Spawn));
                return Ok(());
            };
            let result = tokio::select! { biased;
                () = cancel.cancelled() => Err(ProcessRunError::Cancelled),
                () = &mut closed => Err(ProcessRunError::Cancelled),
                () = tx.closed() => Err(ProcessRunError::Cancelled),
                result = work(&mut guard) => result.map_err(|_| ProcessRunError::Capture),
            };
            // A completed callback may have already reaped and disarmed the guard.
            // Otherwise its remaining group and leader are still owned here.
            if guard.needs_cleanup() {
                guard.kill_group();
                if !matches!(
                    tokio::time::timeout(WAIT_AFTER_KILL, guard.wait()).await,
                    Ok(Ok(_))
                ) {
                    let _ = tx.send(Err(ProcessRunError::Capture));
                    return Err(ResourceFailure::CleanupFailed);
                }
                guard.finish();
            }
            let _ = tx.send(result);
            Ok(())
        })
        .map_err(|_| ProcessRunError::Unavailable)?;
    tokio::select! { biased;
        () = ctx.cancel.cancelled() => Err(ProcessRunError::Cancelled),
        result = rx => result.unwrap_or(Err(ProcessRunError::Capture)),
    }
}
