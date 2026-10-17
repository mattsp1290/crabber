//! Poll/dispose the literal borrowed future, with returned values installed first.
use crate::dispatch::discard_panic_payload;
use futures::{FutureExt, future::BoxFuture};
use std::panic::AssertUnwindSafe;
use tokio_util::sync::CancellationToken;

// The borrowed future remains a named original throughout poll-unwind handling;
// no added async factory wrapper gets to dispose it during another unwind.
pub(crate) async fn original<'a, T, R>(
    construct: impl FnOnce() -> BoxFuture<'a, T>,
    install: impl FnOnce(T) -> R,
    cancel: &CancellationToken,
) -> Result<Option<R>, ()> {
    let mut future = match std::panic::catch_unwind(AssertUnwindSafe(construct)) {
        Ok(future) => future,
        Err(payload) => {
            discard_panic_payload(payload);
            return Err(());
        }
    };
    let output = tokio::select! {
        biased;
        () = cancel.cancelled() => Ok(None),
        output = AssertUnwindSafe(&mut future).catch_unwind() => output.map(Some),
    };
    let output = match output {
        Ok(Some(value)) => std::panic::catch_unwind(AssertUnwindSafe(|| install(value))).map(Some),
        Ok(None) => Ok(None),
        Err(payload) => Err(payload),
    };
    let disposed = std::panic::catch_unwind(AssertUnwindSafe(|| drop(future)));
    let output = match output {
        Ok(output) => Ok(output),
        Err(payload) => {
            discard_panic_payload(payload);
            Err(())
        }
    };
    if let Err(payload) = disposed {
        discard_panic_payload(payload);
        std::mem::forget(output);
        return Err(());
    }
    output
}
