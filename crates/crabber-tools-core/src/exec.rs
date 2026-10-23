//! Cancellation-safe offloading. Started blocking work must own its permits.
use crate::{category, failed};
use crabber::ExtensionError;
use serde_json::Value;
use std::fmt::Write as _;
use std::sync::atomic::{AtomicU64, Ordering};
use tokio_util::sync::CancellationToken;

/// Reject a pre-cancelled invocation before side effects.
///
/// # Errors
/// Returns a tool error when the invocation is already cancelled.
pub fn check_cancelled(cancel: &CancellationToken) -> Result<(), ExtensionError> {
    if cancel.is_cancelled() {
        Err(ExtensionError::Tool("cancelled".into()))
    } else {
        Ok(())
    }
}
/// Offload a bounded operation. A panic produces failed(unknown); cancellation
/// returns immediately while the closure finishes with its guards still owned.
#[doc(hidden)]
pub async fn run_blocking(
    cancel: &CancellationToken,
    f: impl FnOnce() -> Value + Send + 'static,
) -> Result<Value, ExtensionError> {
    check_cancelled(cancel)?;
    let work = tokio::task::spawn_blocking(f);
    tokio::select! { biased;
        () = cancel.cancelled() => Err(ExtensionError::Tool("cancelled".into())),
        result = work => Ok(result.unwrap_or_else(|_| failed(category::UNKNOWN,"internal"))),
    }
}
/// Unpredictable per-process sibling name; randomness failure returns an I/O error.
///
/// # Errors
/// Returns an I/O error if the operating system cannot supply randomness.
pub fn temp_name(prefix: &str) -> std::io::Result<String> {
    static COUNTER: AtomicU64 = AtomicU64::new(0);
    let mut random = [0; 8];
    getrandom::fill(&mut random).map_err(|_| std::io::Error::other("randomness unavailable"))?;
    let hex: String = random.iter().fold(String::new(), |mut hex, byte| {
        let _ = write!(&mut hex, "{byte:02x}");
        hex
    });
    Ok(format!(
        "{prefix}{}-{}-{hex}",
        std::process::id(),
        COUNTER.fetch_add(1, Ordering::Relaxed)
    ))
}

/// Register blocking work with the call owner. Cancellation returns promptly;
/// the resource scope retains the join and the closure's permits until it ends.
/// Ownerless or sealed contexts never start the closure.
///
/// # Errors
/// Returns a tool error if the invocation is cancelled before or during the wait.
pub async fn run_blocking_owned(
    ctx: &crabber::extension::ToolContext,
    f: impl FnOnce(&CancellationToken) -> Value + Send + 'static,
) -> Result<Value, ExtensionError> {
    check_cancelled(&ctx.cancel)?;
    let (tx, rx) = tokio::sync::oneshot::channel();
    let cancel = ctx.cancel.clone();
    if ctx
        .resources()
        .spawn(async move {
            let value = tokio::task::spawn_blocking(move || f(&cancel))
                .await
                .unwrap_or_else(|_| failed(category::UNKNOWN, "internal"));
            let _ = tx.send(value);
            Ok(())
        })
        .is_err()
    {
        return Ok(failed(category::UNAVAILABLE, "call resources unavailable"));
    }
    tokio::select! { biased;
        () = ctx.cancel.cancelled() => Err(ExtensionError::Tool("cancelled".into())),
        result = rx => Ok(result.unwrap_or_else(|_| failed(category::UNKNOWN, "internal"))),
    }
}
