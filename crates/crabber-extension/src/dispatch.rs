//! Ordered, bounded extension callback dispatch.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::{
    CleanupTracker, ExtensionError, FINAL_REDACTION_DEADLINE, ResultTransformCallback,
    ToolResultContext, ToolResultOutcome, TransformOutput, TransformPhase, json_result_transform,
};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
};
use tokio_util::sync::CancellationToken;

pub type Callback =
    Arc<dyn Fn(Value) -> BoxFuture<'static, Result<Value, ExtensionError>> + Send + Sync>;
pub type AroundCallback =
    Arc<dyn Fn(Value, Next) -> BoxFuture<'static, Result<Value, ExtensionError>> + Send + Sync>;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Mode {
    Notify,
    Hook,
    Transform,
    Gate,
    Around,
}

pub trait Point {
    const ID: &'static str;
    const MODE: Mode;
}
macro_rules! points { ($( $name:ident => ($id:literal, $mode:ident) ),* $(,)?) => { $(pub struct $name; impl Point for $name { const ID: &'static str = $id; const MODE: Mode = Mode::$mode; })* }; }
points! {
    RunAdmitted => ("crabber/run/admitted", Notify),
    RunBeforeExecute => ("crabber/run/before-execute", Gate),
    RunStarted => ("crabber/run/started", Notify),
    TurnStarted => ("crabber/turn/started", Notify),
    ContextAssemble => ("crabber/context/assemble", Transform),
    TurnPrepare => ("crabber/turn/prepare", Hook),
    ModelRequested => ("crabber/model/requested", Notify),
    ModelStream => ("crabber/model/stream", Around),
    ModelRequestError => ("crabber/model/request-error", Transform),
    ModelCompleted => ("crabber/model/completed", Notify),
    ToolPrepare => ("crabber/tool/prepare", Transform),
    ToolStarted => ("crabber/tool/started", Notify),
    ToolExecute => ("crabber/tool/execute", Around),
    ToolResultTransform => ("crabber/tool/result-transform", Transform),
    ToolSettled => ("crabber/tool/settled", Notify),
    TurnCompleted => ("crabber/turn/completed", Notify),
    RunSettled => ("crabber/run/settled", Notify),
    EventPublished => ("crabber/event/published", Notify),
}

#[derive(Clone)]
pub enum HandlerFn {
    Ordinary(Callback),
    Around(AroundCallback),
    /// Typed result-transform callback; run by the chain driver, not by the
    /// generic waterfall.
    ResultTransform(ResultTransformCallback),
}
#[derive(Clone)]
pub struct Handler {
    pub point: &'static str,
    pub mode: Mode,
    pub order: i32,
    pub id: String,
    pub(crate) scope_rank: u8,
    pub(crate) mount_seq: u64,
    pub(crate) registration_seq: usize,
    pub(crate) mount_id: u64,
    pub(crate) phase: TransformPhase,
    pub(crate) cleanup: CleanupTracker,
    pub(crate) callback: HandlerFn,
}
impl Handler {
    fn is_ordinary(&self) -> bool {
        self.phase == TransformPhase::Ordinary
    }
    /// Phase dominates `order`: every final redactor sorts after every ordinary
    /// handler. Only the result-transform point registers final redactors.
    pub(crate) fn sort_key(&self) -> (u8, i32, u8, u64, usize) {
        (
            u8::from(matches!(self.phase, TransformPhase::FinalRedaction)),
            self.order,
            self.scope_rank,
            self.mount_seq,
            self.registration_seq,
        )
    }
}

#[derive(Clone)]
pub struct Dispatcher {
    handlers: Arc<Vec<Handler>>,
}
impl Dispatcher {
    #[must_use]
    pub fn new(mut handlers: Vec<Handler>) -> Self {
        handlers.sort_by_key(Handler::sort_key);
        Self {
            handlers: Arc::new(handlers),
        }
    }
    fn matching(&self, point: &'static str, mode: Mode) -> impl Iterator<Item = &Handler> {
        self.handlers
            .iter()
            .filter(move |h| h.point == point && h.mode == mode)
    }
    /// Notify callbacks run independently. Panics and errors are contained.
    pub async fn notify<P: Point>(&self, value: Value) {
        use futures::FutureExt;
        for handler in self.matching(P::ID, Mode::Notify) {
            if let HandlerFn::Ordinary(callback) = &handler.callback {
                let callback = Arc::clone(callback);
                let mount_id = handler.mount_id;
                let input = value.clone();
                let _ =
                    AssertUnwindSafe(async move { with_mount(mount_id, callback(input)).await })
                        .catch_unwind()
                        .await;
            }
        }
    }
    /// Hook callbacks stop on the first error.
    pub async fn hook<P: Point>(&self, value: Value) -> Result<(), ExtensionError> {
        for handler in self.matching(P::ID, Mode::Hook) {
            if let HandlerFn::Ordinary(callback) = &handler.callback {
                with_mount(handler.mount_id, callback(value.clone())).await?;
            }
        }
        Ok(())
    }
    /// Transform callbacks form an ordered waterfall.
    pub async fn transform<P: Point>(&self, value: Value) -> Result<Value, ExtensionError> {
        self.waterfall(P::ID, value, |_| Ok(())).await
    }
    /// Runs the transform handlers of `point` in order, applying `after` to the
    /// initial value and to every handler's result.
    async fn waterfall(
        &self,
        point: &'static str,
        mut value: Value,
        after: impl Fn(&mut Value) -> Result<(), ExtensionError>,
    ) -> Result<Value, ExtensionError> {
        after(&mut value)?;
        for handler in self.matching(point, Mode::Transform) {
            if let HandlerFn::Ordinary(callback) = &handler.callback {
                value = with_mount(handler.mount_id, callback(value)).await?;
                after(&mut value)?;
            }
        }
        Ok(value)
    }
    /// A transform waterfall over a JSON object whose `pinned` keys are owned
    /// by the caller: they are re-asserted after every handler, so no handler
    /// can change, add or remove them for a later handler or for the caller.
    /// A value that is not an object, initially or from a handler, is rejected.
    pub async fn transform_pinned<P: Point>(
        &self,
        value: Value,
        pinned: &[(&str, Value)],
    ) -> Result<Value, ExtensionError> {
        let reassert_pinned = |value: &mut Value| {
            let Value::Object(object) = value else {
                return Err(ExtensionError::Rejected(P::ID));
            };
            for (key, pinned) in pinned {
                object.insert((*key).to_owned(), pinned.clone());
            }
            Ok(())
        };
        self.waterfall(P::ID, value, reassert_pinned).await
    }
    /// Runs the `crabber/tool/result-transform` chain: every ordinary handler,
    /// then every final redactor (the handler order is the sort order).
    ///
    /// The driver owns the accepted value, so no callback future holds it and
    /// dropping an in-flight future never loses or corrupts it.
    ///
    /// Commit point: a handler's valid output becomes the accepted value only
    /// when the driver, having received it, finds the token not cancelled. The
    /// select polls cancellation first and the token is read again right before
    /// the commit, so a completion that races a cancellation resolves as
    /// cancelled. After the last commit the driver returns `Completed` without
    /// looking at the token again: cancellation that arrives later was not
    /// observed (settlement precedence).
    ///
    /// A handler whose mount starts closing before or while it runs is dropped
    /// (D2/D7): the select order is cancellation, then the mount close signal,
    /// then the callback. The call fails with `Failed { handler }` unless
    /// cancellation was observed, in which case it is `Interrupted` and the
    /// in-flight handler is dropped, final redactors included.
    // One driver loop: its states share the accepted value and the deadline.
    #[allow(clippy::too_many_lines)]
    pub async fn transform_tool_result(
        &self,
        context: ToolResultContext,
        seed: TransformOutput,
    ) -> ToolResultOutcome {
        let token = context.cancellation().clone();
        if token.is_cancelled() {
            return ToolResultOutcome::Interrupted { redacted: None };
        }
        let interrupted = || ToolResultOutcome::Interrupted { redacted: None };
        let mut is_error = context.class().is_error() || seed.mark_error;
        // The seed is never an accepted value (D6); only a handler's valid
        // output is committed here.
        let mut accepted: Option<Value> = None;
        let handlers: Vec<&Handler> = self
            .matching(ToolResultTransform::ID, Mode::Transform)
            .filter(|h| {
                matches!(
                    h.callback,
                    HandlerFn::Ordinary(_) | HandlerFn::ResultTransform(_)
                )
            })
            .collect();
        let has_final = handlers.iter().any(|h| !h.is_ordinary());
        // `Some` once cancellation has been observed: the end of the single
        // final-redaction budget, measured from that instant.
        let mut deadline: Option<tokio::time::Instant> = None;
        // Observing cancellation with nothing accepted or no final redactor
        // ends the call at once; otherwise it starts the final-redaction budget.
        macro_rules! observe_cancellation {
            () => {
                if accepted.is_none() || !has_final {
                    return interrupted();
                }
                deadline = Some(tokio::time::Instant::now() + FINAL_REDACTION_DEADLINE);
            };
        }
        for handler in handlers {
            let ordinary = handler.is_ordinary();
            if deadline.is_some() && ordinary {
                continue;
            }
            if deadline.is_none() && token.is_cancelled() {
                // Observed between handlers: the next one never started.
                observe_cancellation!();
                if ordinary {
                    continue;
                }
            }
            if handler.cleanup.is_closing() {
                return if deadline.is_some() {
                    interrupted()
                } else {
                    ToolResultOutcome::Failed {
                        handler: handler.id.clone(),
                    }
                };
            }
            let mut handler_context = context.clone().with_cleanup(handler.cleanup.clone());
            handler_context.set_phase(handler.phase);
            handler_context.set_is_error(is_error);
            let current = accepted.as_ref().unwrap_or(&seed.result).clone();
            let mut future =
                InFlight::new(invoke_result_handler(handler, handler_context, current));
            let mut result = None;
            let closing = handler.cleanup.closing();
            tokio::pin!(closing);
            if deadline.is_none() {
                // `biased` with cancellation first keeps a handler from being
                // polled once the token is already cancelled; the re-check below
                // is what decides a race between completion and cancellation.
                // The mount close signal comes second, ahead of the callback.
                let mut mount_closed = false;
                tokio::select! {
                    biased;
                    () = token.cancelled() => {}
                    () = &mut closing => mount_closed = true,
                    output = &mut future => result = Some(output),
                }
                // Also true when the handler completed while cancellation
                // arrived: that resolves as cancelled and the output is not
                // committed unless it is a final redactor's, run on an
                // accepted value, which then counts as finished in time.
                if token.is_cancelled() {
                    observe_cancellation!();
                    if ordinary {
                        // Drops only this handler's future; the driver goes on.
                        continue;
                    }
                    // A final redactor in flight whose mount is closing is dropped
                    // below, where the close arm decides `Interrupted`.
                } else if mount_closed {
                    return ToolResultOutcome::Failed {
                        handler: handler.id.clone(),
                    };
                }
            }
            let output = match (result, deadline) {
                (Some(output), _) => output,
                // An in-flight final redactor on an accepted value is not
                // dropped by cancellation, only by the deadline.
                // A closing mount drops it too.
                (None, Some(deadline)) => {
                    tokio::select! {
                        biased;
                        () = &mut closing => return interrupted(),
                        output = finish_before(deadline, &mut future) => match output {
                            Some(output) => output,
                            None => return interrupted(),
                        },
                    }
                }
                // Unreachable: no output means cancellation was observed.
                (None, None) => return interrupted(),
            };
            match output {
                Some(output) => {
                    is_error |= output.mark_error;
                    accepted = Some(output.result);
                }
                None if deadline.is_some() => return interrupted(),
                None => {
                    return ToolResultOutcome::Failed {
                        handler: handler.id.clone(),
                    };
                }
            }
        }
        match (deadline, accepted) {
            (Some(_), Some(redacted)) => ToolResultOutcome::Interrupted {
                redacted: Some(redacted),
            },
            // Unreachable today; never fall through to `Completed`.
            (Some(_), None) => interrupted(),
            (None, accepted) => ToolResultOutcome::Completed {
                result: accepted.unwrap_or(seed.result),
                is_error,
            },
        }
    }
    /// Gate callbacks reject on the first false result.
    pub async fn gate<P: Point>(&self, value: Value) -> Result<(), ExtensionError> {
        for handler in self.matching(P::ID, Mode::Gate) {
            if let HandlerFn::Ordinary(callback) = &handler.callback
                && with_mount(handler.mount_id, callback(value.clone())).await?
                    == Value::Bool(false)
            {
                return Err(ExtensionError::Rejected(P::ID));
            }
        }
        Ok(())
    }
    pub async fn around<P: Point>(
        &self,
        value: Value,
        terminal: Callback,
    ) -> Result<Value, ExtensionError> {
        let handlers = self
            .matching(P::ID, Mode::Around)
            .cloned()
            .collect::<Vec<_>>();
        around_at(Arc::new(handlers), 0, value, terminal).await
    }
}

/// Owns the in-flight handler future so every drop site of the driver (cancel,
/// deadline, early return, later additions) is contained by construction: a
/// panicking destructor inside a handler future must not unwind out of the
/// driver. The panic is swallowed and never changes the outcome, which is
/// already decided at those sites (`Interrupted`), so it can neither upgrade a
/// result to success nor surface as `Failed` after observed cancellation.
struct InFlight<F: Future>(Option<Pin<Box<F>>>);
impl<F: Future> InFlight<F> {
    fn new(future: F) -> Self {
        Self(Some(Box::pin(future)))
    }
}
impl<F: Future> Future for InFlight<F> {
    type Output = F::Output;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<F::Output> {
        match self.0.as_mut() {
            Some(future) => future.as_mut().poll(cx),
            None => Poll::Pending,
        }
    }
}
impl<F: Future> Drop for InFlight<F> {
    fn drop(&mut self) {
        if let Some(future) = self.0.take()
            && let Err(payload) = std::panic::catch_unwind(AssertUnwindSafe(move || drop(future)))
        {
            // The payload's own destructor may panic too.
            let _ = std::panic::catch_unwind(AssertUnwindSafe(move || drop(payload)));
        }
    }
}

/// Drives `future` to completion unless `deadline` passes first; the future is
/// then left to the caller to drop. A deadline already past admits nothing.
async fn finish_before<T>(
    deadline: tokio::time::Instant,
    future: &mut (impl Future<Output = T> + Unpin),
) -> Option<T> {
    if tokio::time::Instant::now() >= deadline {
        return None;
    }
    tokio::time::timeout_at(deadline, future).await.ok()
}

/// Runs one result handler under its mount with panics contained. `None` is a
/// D2 failure (error, panic or envelope violation); handler-authored error
/// text is discarded here so it can never reach the outcome.
async fn invoke_result_handler(
    handler: &Handler,
    context: ToolResultContext,
    value: Value,
) -> Option<TransformOutput> {
    use futures::FutureExt;
    let callback = match &handler.callback {
        HandlerFn::ResultTransform(callback) => Arc::clone(callback),
        HandlerFn::Ordinary(callback) => json_result_transform(Arc::clone(callback)),
        HandlerFn::Around(_) => return None,
    };
    AssertUnwindSafe(with_mount(handler.mount_id, async move {
        callback(context, value).await
    }))
    .catch_unwind()
    .await
    .ok()?
    .ok()
}

#[derive(Default)]
struct NextState {
    active: bool,
    calls: usize,
    inflight: usize,
}
struct NextShared {
    state: Mutex<NextState>,
    cancel: CancellationToken,
    done: tokio::sync::Notify,
}
impl NextShared {
    fn revoke(&self) {
        self.state.lock().unwrap().active = false;
        self.cancel.cancel();
        self.done.notify_waiters();
    }
}
struct RevokeOnDrop(Arc<NextShared>);
impl Drop for RevokeOnDrop {
    fn drop(&mut self) {
        self.0.revoke();
    }
}
struct Inflight(Arc<NextShared>);
impl Drop for Inflight {
    fn drop(&mut self) {
        self.0.state.lock().unwrap().inflight -= 1;
        self.0.done.notify_waiters();
    }
}
#[derive(Clone)]
pub struct Next {
    shared: Arc<NextShared>,
    callback_token: u64,
    invoke: Callback,
}
impl Next {
    pub async fn call(&self, value: Value) -> Result<Value, ExtensionError> {
        {
            let mut state = self.shared.state.lock().unwrap();
            if !state.active {
                return Err(ExtensionError::NextExpired);
            }
            if !ACTIVE_NEXT
                .try_with(|token| *token == self.callback_token)
                .unwrap_or(false)
            {
                return Err(ExtensionError::NextOutsideCallback);
            }
            if state.calls > 0 {
                state.calls += 1;
                return Err(ExtensionError::NextCalledTwice);
            }
            state.calls = 1;
            state.inflight += 1;
        }
        let _inflight = Inflight(Arc::clone(&self.shared));
        tokio::select! {
            biased;
            () = self.shared.cancel.cancelled() => Err(ExtensionError::NextExpired),
            output = (self.invoke)(value) => output,
        }
    }
}
fn around_at(
    handlers: Arc<Vec<Handler>>,
    index: usize,
    value: Value,
    terminal: Callback,
) -> BoxFuture<'static, Result<Value, ExtensionError>> {
    Box::pin(async move {
        let Some(handler) = handlers.get(index) else {
            return terminal(value).await;
        };
        let HandlerFn::Around(callback) = &handler.callback else {
            unreachable!()
        };
        let callback = Arc::clone(callback);
        let mount_id = handler.mount_id;
        let shared = Arc::new(NextShared {
            state: Mutex::new(NextState {
                active: true,
                calls: 0,
                inflight: 0,
            }),
            cancel: CancellationToken::new(),
            done: tokio::sync::Notify::new(),
        });
        let _revoke = RevokeOnDrop(Arc::clone(&shared));
        let next_handlers = Arc::clone(&handlers);
        let next_terminal = Arc::clone(&terminal);
        let callback_token = NEXT_SEQUENCE.fetch_add(1, Ordering::Relaxed);
        let next = Next {
            shared: Arc::clone(&shared),
            callback_token,
            invoke: Arc::new(move |v| {
                around_at(
                    Arc::clone(&next_handlers),
                    index + 1,
                    v,
                    Arc::clone(&next_terminal),
                )
            }),
        };
        let result = ACTIVE_NEXT
            .scope(callback_token, with_mount(mount_id, callback(value, next)))
            .await;
        shared.revoke();
        loop {
            let notified = shared.done.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if shared.state.lock().unwrap().inflight == 0 {
                break;
            }
            notified.await;
        }
        match shared.state.lock().unwrap().calls {
            0 => Err(ExtensionError::NextNotCalled),
            1 => result,
            _ => Err(ExtensionError::NextCalledTwice),
        }
    })
}

static NEXT_SEQUENCE: AtomicU64 = AtomicU64::new(1);
tokio::task_local! { static ACTIVE_NEXT: u64; }
tokio::task_local! { static ACTIVE_MOUNTS: Vec<u64>; }
pub(crate) async fn with_mount<T>(id: u64, future: impl Future<Output = T>) -> T {
    let mut stack = ACTIVE_MOUNTS.try_with(Clone::clone).unwrap_or_default();
    stack.push(id);
    ACTIVE_MOUNTS.scope(stack, future).await
}
pub(crate) fn is_active_mount(id: u64) -> bool {
    ACTIVE_MOUNTS
        .try_with(|stack| stack.contains(&id))
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::sync::atomic::{AtomicUsize, Ordering};
    fn callback(
        f: impl Fn(Value) -> Result<Value, ExtensionError> + Send + Sync + 'static,
    ) -> Callback {
        Arc::new(move |v| Box::pin(std::future::ready(f(v))))
    }
    fn handler(
        point: &'static str,
        mode: Mode,
        order: i32,
        id: &str,
        callback: HandlerFn,
    ) -> Handler {
        Handler {
            point,
            mode,
            order,
            id: id.into(),
            scope_rank: 0,
            mount_seq: 0,
            registration_seq: 0,
            mount_id: 1,
            phase: TransformPhase::Ordinary,
            cleanup: CleanupTracker::detached(),
            callback,
        }
    }
    #[tokio::test]
    async fn dispatch_modes_and_ordering() {
        let d = Dispatcher::new(vec![
            handler(
                ToolPrepare::ID,
                Mode::Transform,
                1000,
                "late",
                HandlerFn::Ordinary(callback(|v| Ok(Value::String(format!("{v}b"))))),
            ),
            handler(
                ToolPrepare::ID,
                Mode::Transform,
                -1000,
                "early",
                HandlerFn::Ordinary(callback(|_| Ok(Value::String("a".into())))),
            ),
            handler(
                TurnPrepare::ID,
                Mode::Hook,
                0,
                "hook",
                HandlerFn::Ordinary(callback(|_| Ok(Value::Null))),
            ),
            handler(
                RunBeforeExecute::ID,
                Mode::Gate,
                0,
                "gate",
                HandlerFn::Ordinary(callback(|_| Ok(Value::Bool(false)))),
            ),
        ]);
        assert_eq!(
            d.transform::<ToolPrepare>(Value::Null).await.unwrap(),
            Value::String("\"a\"b".into())
        );
        d.hook::<TurnPrepare>(Value::Null).await.unwrap();
        assert_eq!(
            d.gate::<RunBeforeExecute>(Value::Null).await,
            Err(ExtensionError::Rejected(RunBeforeExecute::ID))
        );
    }
    // Expected to change with crabber-gl4i / crabber-f7iv (typed result-transform chain, D5/D6 phases).
    #[tokio::test]
    async fn characterize_result_transform_order_across_two_mounts() {
        // (tag, order, scope_rank, mount_seq, registration_seq, mount_id), deliberately shuffled.
        let specs: [(&str, i32, u8, u64, usize, u64); 6] = [
            ("last", 5, 0, 1, 2, 1),
            ("r1m1s0", 0, 1, 1, 0, 1),
            ("r0m2s0", 0, 0, 2, 0, 2),
            ("r0m1s1", 0, 0, 1, 1, 1),
            ("first", -1, 1, 2, 0, 2),
            ("r0m1s0", 0, 0, 1, 0, 1),
        ];
        let handlers = specs
            .into_iter()
            .map(
                |(tag, order, scope_rank, mount_seq, registration_seq, mount_id)| {
                    let mut h = handler(
                        ToolResultTransform::ID,
                        Mode::Transform,
                        order,
                        tag,
                        HandlerFn::Ordinary(callback(move |v| {
                            Ok(Value::String(format!("{}{tag};", v.as_str().unwrap_or(""))))
                        })),
                    );
                    h.scope_rank = scope_rank;
                    h.mount_seq = mount_seq;
                    h.registration_seq = registration_seq;
                    h.mount_id = mount_id;
                    h
                },
            )
            .collect();
        let out = Dispatcher::new(handlers)
            .transform::<ToolResultTransform>(Value::Null)
            .await
            .unwrap();
        assert_eq!(out, "first;r0m1s0;r0m1s1;r0m2s0;r1m1s0;last;");
    }
    fn typed_handler(order: i32, id: &str, phase: TransformPhase) -> Handler {
        let mut h = handler(
            ToolResultTransform::ID,
            Mode::Transform,
            order,
            id,
            HandlerFn::ResultTransform(Arc::new(|_, result| {
                Box::pin(std::future::ready(Ok(crate::TransformOutput::new(result))))
            })),
        );
        h.phase = phase;
        h
    }
    #[test]
    fn final_redaction_sorts_after_ordinary_across_mounts_and_orders() {
        use TransformPhase::{FinalRedaction as F, Ordinary as O};
        // (id, order, phase, scope_rank, mount_seq, registration_seq), shuffled.
        let specs: [(&str, i32, TransformPhase, u8, u64, usize); 8] = [
            ("f-m2", 0, F, 0, 2, 0),
            ("o-m2", 0, O, 0, 2, 0),
            ("f-low", -100, F, 0, 1, 1),
            ("o-high", 100, O, 0, 1, 2),
            ("f-m1", 0, F, 0, 1, 0),
            ("o-m1", 0, O, 0, 1, 0),
            ("f-session", 0, F, 1, 1, 0),
            ("o-low", -5, O, 1, 2, 0),
        ];
        let handlers = specs
            .into_iter()
            .map(
                |(id, order, phase, scope_rank, mount_seq, registration_seq)| {
                    let mut h = typed_handler(order, id, phase);
                    h.scope_rank = scope_rank;
                    h.mount_seq = mount_seq;
                    h.registration_seq = registration_seq;
                    h
                },
            )
            .collect();
        let ids = Dispatcher::new(handlers)
            .handlers
            .iter()
            .map(|h| h.id.clone())
            .collect::<Vec<_>>();
        assert_eq!(
            ids,
            [
                "o-low",
                "o-m1",
                "o-m2",
                "o-high",
                "f-low",
                "f-m1",
                "f-m2",
                "f-session"
            ]
        );
    }
    #[tokio::test]
    async fn generic_waterfall_skips_typed_result_transform_handlers() {
        let d = Dispatcher::new(vec![
            typed_handler(0, "typed", TransformPhase::Ordinary),
            handler(
                ToolResultTransform::ID,
                Mode::Transform,
                1,
                "json",
                HandlerFn::Ordinary(callback(|_| Ok(Value::String("json".into())))),
            ),
        ]);
        let out = d
            .transform::<ToolResultTransform>(Value::Null)
            .await
            .unwrap();
        assert_eq!(out, "json");
    }
    // Expected to change with crabber-gl4i (D2: handler error settles Failed naming the handler id).
    #[tokio::test]
    async fn characterize_result_transform_error_aborts_waterfall() {
        let later = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&later);
        let d = Dispatcher::new(vec![
            handler(
                ToolResultTransform::ID,
                Mode::Transform,
                0,
                "fails",
                HandlerFn::Ordinary(callback(|_| Err(ExtensionError::Plan("boom".into())))),
            ),
            handler(
                ToolResultTransform::ID,
                Mode::Transform,
                1,
                "skipped",
                HandlerFn::Ordinary(callback(move |v| {
                    seen.fetch_add(1, Ordering::SeqCst);
                    Ok(v)
                })),
            ),
        ]);
        assert_eq!(
            d.transform::<ToolResultTransform>(Value::Null).await,
            Err(ExtensionError::Plan("boom".into()))
        );
        assert_eq!(later.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn notify_contains_panics() {
        let d = Dispatcher::new(vec![handler(
            EventPublished::ID,
            Mode::Notify,
            0,
            "panic",
            HandlerFn::Ordinary(callback(|_| panic!("contained"))),
        )]);
        d.notify::<EventPublished>(Value::Null).await;
    }
    #[tokio::test]
    async fn around_checks_exactly_once() {
        let terminal = callback(Ok);
        let missing = Dispatcher::new(vec![handler(
            ToolExecute::ID,
            Mode::Around,
            0,
            "missing",
            HandlerFn::Around(Arc::new(|v, _| Box::pin(async move { Ok(v) }))),
        )]);
        assert_eq!(
            missing
                .around::<ToolExecute>(Value::Null, Arc::clone(&terminal))
                .await,
            Err(ExtensionError::NextNotCalled)
        );
        let twice = Dispatcher::new(vec![handler(
            ToolExecute::ID,
            Mode::Around,
            0,
            "twice",
            HandlerFn::Around(Arc::new(|v, next| {
                Box::pin(async move {
                    let _ = next.call(v.clone()).await;
                    next.call(v).await
                })
            })),
        )]);
        assert_eq!(
            twice.around::<ToolExecute>(Value::Null, terminal).await,
            Err(ExtensionError::NextCalledTwice)
        );
    }
    #[tokio::test]
    async fn detached_next_expires_before_terminal() {
        let gate = Arc::new(tokio::sync::Notify::new());
        let (sender, receiver) = tokio::sync::oneshot::channel();
        let sender = Arc::new(std::sync::Mutex::new(Some(sender)));
        let callback = {
            let gate = Arc::clone(&gate);
            let sender = Arc::clone(&sender);
            Arc::new(move |_value, next: Next| {
                let gate = Arc::clone(&gate);
                let sender = Arc::clone(&sender);
                Box::pin(async move {
                    tokio::spawn(async move {
                        gate.notified().await;
                        let result = next.call(Value::Null).await;
                        let _ = sender.lock().unwrap().take().unwrap().send(result);
                    });
                    Ok(Value::Null)
                }) as BoxFuture<'static, Result<Value, ExtensionError>>
            })
        };
        let calls = Arc::new(AtomicUsize::new(0));
        let terminal = {
            let calls = Arc::clone(&calls);
            Arc::new(move |v| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(v)
                }) as BoxFuture<'static, Result<Value, ExtensionError>>
            })
        };
        let dispatcher = Dispatcher::new(vec![handler(
            ToolExecute::ID,
            Mode::Around,
            0,
            "detach",
            HandlerFn::Around(callback),
        )]);
        assert_eq!(
            dispatcher
                .around::<ToolExecute>(Value::Null, terminal)
                .await,
            Err(ExtensionError::NextNotCalled)
        );
        gate.notify_one();
        assert_eq!(receiver.await.unwrap(), Err(ExtensionError::NextExpired));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn detached_next_before_callback_returns_cannot_enter_terminal() {
        let calls = Arc::new(AtomicUsize::new(0));
        let terminal: Callback = {
            let calls = Arc::clone(&calls);
            Arc::new(move |value| {
                let calls = Arc::clone(&calls);
                Box::pin(async move {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(value)
                })
            })
        };
        let callback: AroundCallback = Arc::new(|_, next| {
            Box::pin(async move {
                let detached = tokio::spawn(async move { next.call(Value::Null).await });
                assert_eq!(
                    detached.await.unwrap(),
                    Err(ExtensionError::NextOutsideCallback)
                );
                Ok(Value::Null)
            })
        });
        let dispatcher = Dispatcher::new(vec![handler(
            ToolExecute::ID,
            Mode::Around,
            0,
            "detach-before-return",
            HandlerFn::Around(callback),
        )]);
        assert_eq!(
            dispatcher
                .around::<ToolExecute>(Value::Null, terminal)
                .await,
            Err(ExtensionError::NextNotCalled)
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }
    #[tokio::test]
    async fn pinned_keys_survive_every_transform_handler() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let observe = |seen: &Arc<Mutex<Vec<Value>>>| {
            let seen = Arc::clone(seen);
            callback(move |mut v| {
                seen.lock().unwrap().push(v.clone());
                v["pinned"] = serde_json::json!("forged");
                v.as_object_mut().unwrap().remove("absent");
                v["free"] = serde_json::json!("changed");
                Ok(v)
            })
        };
        let dispatcher = Dispatcher::new(vec![
            handler(
                ContextAssemble::ID,
                Mode::Transform,
                0,
                "first",
                HandlerFn::Ordinary(observe(&seen)),
            ),
            handler(
                ContextAssemble::ID,
                Mode::Transform,
                1,
                "second",
                HandlerFn::Ordinary(observe(&seen)),
            ),
        ]);
        let pinned = [
            ("pinned", serde_json::json!("real")),
            ("absent", Value::Null),
        ];
        let out = dispatcher
            .transform_pinned::<ContextAssemble>(
                serde_json::json!({"free":"original","pinned":"caller"}),
                &pinned,
            )
            .await
            .unwrap();
        for value in seen.lock().unwrap().iter().chain([&out]) {
            assert_eq!(value["pinned"], "real");
            assert_eq!(value.get("absent"), Some(&Value::Null));
        }
        assert_eq!(out["free"], "changed");
    }
    #[tokio::test]
    async fn pinned_transform_rejects_a_handler_that_returns_a_non_object() {
        let later = Arc::new(AtomicUsize::new(0));
        let seen = Arc::clone(&later);
        for replacement in [Value::Null, serde_json::json!([]), serde_json::json!("x")] {
            let seen = Arc::clone(&seen);
            let dispatcher = Dispatcher::new(vec![
                handler(
                    ContextAssemble::ID,
                    Mode::Transform,
                    0,
                    "replace",
                    HandlerFn::Ordinary(callback(move |_| Ok(replacement.clone()))),
                ),
                handler(
                    ContextAssemble::ID,
                    Mode::Transform,
                    1,
                    "later",
                    HandlerFn::Ordinary(callback(move |v| {
                        seen.fetch_add(1, Ordering::SeqCst);
                        Ok(v)
                    })),
                ),
            ]);
            assert_eq!(
                dispatcher
                    .transform_pinned::<ContextAssemble>(
                        serde_json::json!({}),
                        &[("pinned", serde_json::json!("real"))],
                    )
                    .await,
                Err(ExtensionError::Rejected(ContextAssemble::ID))
            );
        }
        assert_eq!(later.load(Ordering::SeqCst), 0);
        assert_eq!(
            Dispatcher::new(Vec::new())
                .transform_pinned::<ContextAssemble>(Value::Null, &[])
                .await,
            Err(ExtensionError::Rejected(ContextAssemble::ID))
        );
    }
}

#[cfg(test)]
mod driver_tests {
    use super::*;
    use crate::{ToolInput, ToolOutcomeClass};
    use crabber_core::{RunId, SessionId, ToolCallId};
    use futures::FutureExt;
    use serde_json::json;
    use std::sync::atomic::{AtomicUsize, Ordering};

    const SEED_SECRET: &str = "SEED-SECRET-7f3a";
    const ERROR_SECRET: &str = "HANDLER-ERROR-SECRET-91bc";

    fn context(class: ToolOutcomeClass) -> ToolResultContext {
        ToolResultContext::new(
            "read_file".into(),
            true,
            ToolInput::Normalized(json!({"path": "a.txt"})),
            ToolCallId::from("call-1"),
            SessionId::from("session-1"),
            RunId::from("run-1"),
            class,
        )
    }
    fn seed(value: Value) -> TransformOutput {
        TransformOutput::new(value)
    }
    fn make(order: i32, id: &str, phase: TransformPhase, callback: HandlerFn) -> Handler {
        Handler {
            point: ToolResultTransform::ID,
            mode: Mode::Transform,
            order,
            id: id.into(),
            scope_rank: 0,
            mount_seq: 0,
            registration_seq: 0,
            mount_id: 1,
            phase,
            cleanup: CleanupTracker::detached(),
            callback,
        }
    }
    fn typed(
        f: impl Fn(ToolResultContext, Value) -> Result<TransformOutput, ExtensionError>
        + Send
        + Sync
        + 'static,
    ) -> HandlerFn {
        HandlerFn::ResultTransform(Arc::new(move |c, v| Box::pin(std::future::ready(f(c, v)))))
    }
    fn json_cb(
        f: impl Fn(Value) -> Result<Value, ExtensionError> + Send + Sync + 'static,
    ) -> HandlerFn {
        HandlerFn::Ordinary(Arc::new(move |v| Box::pin(std::future::ready(f(v)))))
    }
    /// Appends `tag` to a string result.
    fn append(tag: &'static str) -> HandlerFn {
        typed(move |_, v| {
            Ok(TransformOutput::new(json!(format!(
                "{}{tag}",
                v.as_str().unwrap_or("")
            ))))
        })
    }
    fn counting(counter: &Arc<AtomicUsize>) -> HandlerFn {
        let counter = Arc::clone(counter);
        typed(move |_, v| {
            counter.fetch_add(1, Ordering::SeqCst);
            Ok(TransformOutput::new(v))
        })
    }
    fn completed(result: Value, is_error: bool) -> ToolResultOutcome {
        ToolResultOutcome::Completed { result, is_error }
    }
    const O: TransformPhase = TransformPhase::Ordinary;
    const F: TransformPhase = TransformPhase::FinalRedaction;

    #[tokio::test]
    async fn no_handlers_completes_with_seed() {
        let d = Dispatcher::new(Vec::new());
        assert_eq!(
            d.transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
                .await,
            completed(json!("s"), false)
        );
        assert_eq!(
            d.transform_tool_result(
                context(ToolOutcomeClass::Succeeded),
                TransformOutput::marked_error(json!("s"))
            )
            .await,
            completed(json!("s"), true)
        );
        assert_eq!(
            d.transform_tool_result(context(ToolOutcomeClass::UnknownTool), seed(json!("s")))
                .await,
            completed(json!("s"), true)
        );
    }

    #[tokio::test]
    async fn reducer_then_redactor_composes_regardless_of_order_and_mount() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let redactor = typed(move |_, v| {
            recorder.lock().unwrap().push(v.clone());
            Ok(TransformOutput::new(json!(
                v.as_str().unwrap().replace("secret", "[redacted]")
            )))
        });
        let mut late_mount = make(-100, "reducer", O, append("+secret"));
        late_mount.mount_seq = 9;
        let mut redactor = make(-1000, "redactor", F, redactor);
        redactor.mount_seq = 0;
        let d = Dispatcher::new(vec![
            redactor,
            late_mount,
            make(100, "second", O, append("+more")),
        ]);
        let out = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("raw")))
            .await;
        assert_eq!(out, completed(json!("raw+[redacted]+more"), false));
        assert_eq!(*seen.lock().unwrap(), [json!("raw+secret+more")]);
    }

    /// A handler per D2 trigger, native and JSON.
    fn failing_handlers() -> Vec<(&'static str, HandlerFn)> {
        let envelope_with = |edit: fn(&mut Value)| {
            json_cb(move |mut envelope| {
                edit(&mut envelope);
                Ok(envelope)
            })
        };
        vec![
            (
                "native err",
                typed(|_, _| Err(ExtensionError::Tool(ERROR_SECRET.into()))),
            ),
            ("native panic", typed(|_, _| panic!("{ERROR_SECRET}"))),
            (
                "json err",
                json_cb(|_| Err(ExtensionError::Tool(ERROR_SECRET.into()))),
            ),
            ("json panic", json_cb(|_| panic!("{ERROR_SECRET}"))),
            ("json non-object", json_cb(|_| Ok(json!(ERROR_SECRET)))),
            (
                "json missing result",
                envelope_with(|e| {
                    e.as_object_mut().unwrap().remove("result");
                }),
            ),
            (
                "json leftover is_error",
                envelope_with(|e| e["is_error"] = json!(true)),
            ),
            (
                "json changed context",
                envelope_with(|e| e["context"]["tool_name"] = json!(ERROR_SECRET)),
            ),
            (
                "json non-bool mark_error",
                envelope_with(|e| e["mark_error"] = json!("true")),
            ),
            (
                "json missing context",
                envelope_with(|e| {
                    e.as_object_mut().unwrap().remove("context");
                }),
            ),
            (
                "json missing mark_error",
                envelope_with(|e| {
                    e.as_object_mut().unwrap().remove("mark_error");
                }),
            ),
            (
                "json extra top-level key",
                envelope_with(|e| e["extra"] = json!(ERROR_SECRET)),
            ),
            (
                "native panic at poll time",
                HandlerFn::ResultTransform(Arc::new(|_, _| {
                    Box::pin(async { panic!("{ERROR_SECRET}") })
                })),
            ),
            (
                "json panic at poll time",
                HandlerFn::Ordinary(Arc::new(|_| Box::pin(async { panic!("{ERROR_SECRET}") }))),
            ),
        ]
    }

    #[tokio::test]
    async fn every_d2_trigger_fails_with_only_the_handler_id() {
        for (name, failing) in failing_handlers() {
            let later = Arc::new(AtomicUsize::new(0));
            let final_calls = Arc::new(AtomicUsize::new(0));
            let d = Dispatcher::new(vec![
                make(0, "first", O, append("+INTERMEDIATE-SECRET")),
                make(1, "bad", O, failing),
                make(2, "later", O, counting(&later)),
                make(0, "final", F, counting(&final_calls)),
            ]);
            let outcome = d
                .transform_tool_result(
                    context(ToolOutcomeClass::Succeeded),
                    seed(json!(SEED_SECRET)),
                )
                .await;
            assert_eq!(
                outcome,
                ToolResultOutcome::Failed {
                    handler: "bad".into()
                },
                "{name}"
            );
            assert_eq!(later.load(Ordering::SeqCst), 0, "{name}");
            assert_eq!(final_calls.load(Ordering::SeqCst), 0, "{name}");
            let text = format!("{outcome:?}");
            for secret in [SEED_SECRET, ERROR_SECRET, "INTERMEDIATE-SECRET"] {
                assert!(!text.contains(secret), "{name}: {text}");
            }
        }
    }

    #[tokio::test]
    async fn failing_final_redactor_fails_the_call() {
        let d = Dispatcher::new(vec![
            make(0, "ok", O, append("+x")),
            make(
                0,
                "redactor",
                F,
                typed(|_, _| Err(ExtensionError::Tool(ERROR_SECRET.into()))),
            ),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "redactor".into()
            }
        );
    }

    #[tokio::test]
    async fn json_tampering_fails_that_handler_and_later_context_stays_authoritative() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let d = Dispatcher::new(vec![
            make(
                0,
                "tamper",
                O,
                json_cb(|mut e| {
                    e["context"]["class"] = json!("succeeded");
                    e["context"]["is_error"] = json!(false);
                    Ok(e)
                }),
            ),
            make(
                1,
                "later",
                O,
                typed(move |c, v| {
                    recorder.lock().unwrap().push((c.class(), c.is_error()));
                    Ok(TransformOutput::new(v))
                }),
            ),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::ExecutionFailed), seed(json!("s")))
            .await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "tamper".into()
            }
        );
        assert!(seen.lock().unwrap().is_empty());
    }

    #[tokio::test]
    async fn mark_error_is_sticky_and_visible_to_later_handlers() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        let d = Dispatcher::new(vec![
            make(
                0,
                "plain",
                O,
                typed({
                    let recorder = Arc::clone(&seen);
                    move |c, v| {
                        recorder.lock().unwrap().push(c.is_error());
                        Ok(TransformOutput::new(v))
                    }
                }),
            ),
            make(
                1,
                "marker",
                O,
                typed(|_, v| Ok(TransformOutput::marked_error(v))),
            ),
            make(
                2,
                "json-clear",
                O,
                json_cb({
                    let recorder = Arc::clone(&seen);
                    move |e| {
                        recorder
                            .lock()
                            .unwrap()
                            .push(e["context"]["is_error"] == json!(true));
                        Ok(e)
                    }
                }),
            ),
            make(
                0,
                "final",
                F,
                typed(move |c, v| {
                    recorder.lock().unwrap().push(c.is_error());
                    Ok(TransformOutput::new(v))
                }),
            ),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, completed(json!("s"), true));
        assert_eq!(*seen.lock().unwrap(), [false, true, true]);
    }

    #[tokio::test]
    async fn pre_stage_mark_error_reaches_handlers() {
        let seen = Arc::new(Mutex::new(None));
        let recorder = Arc::clone(&seen);
        let d = Dispatcher::new(vec![make(
            0,
            "h",
            O,
            typed(move |c, v| {
                *recorder.lock().unwrap() = Some(c.is_error());
                Ok(TransformOutput::new(v))
            }),
        )]);
        let outcome = d
            .transform_tool_result(
                context(ToolOutcomeClass::Succeeded),
                TransformOutput::marked_error(json!("s")),
            )
            .await;
        assert_eq!(outcome, completed(json!("s"), true));
        assert_eq!(*seen.lock().unwrap(), Some(true));
    }

    #[tokio::test]
    async fn error_class_cannot_be_cleared() {
        let d = Dispatcher::new(vec![
            make(0, "typed", O, typed(|_, v| Ok(TransformOutput::new(v)))),
            make(1, "json", O, json_cb(Ok)),
            make(0, "final", F, typed(|_, v| Ok(TransformOutput::new(v)))),
        ]);
        for class in [
            ToolOutcomeClass::ExecutionFailed,
            ToolOutcomeClass::PermissionDenied,
            ToolOutcomeClass::UnknownTool,
            ToolOutcomeClass::PrepareFailed,
        ] {
            assert_eq!(
                d.transform_tool_result(context(class), seed(json!("s")))
                    .await,
                completed(json!("s"), true),
                "{class:?}"
            );
        }
    }

    #[tokio::test]
    async fn each_handler_gets_its_own_phase_and_cleanup_tracker() {
        let tracker_a = CleanupTracker::detached();
        let tracker_b = CleanupTracker::detached();
        let (_hold_a, gate_a) = tokio::sync::oneshot::channel::<()>();
        let phases = Arc::new(Mutex::new(Vec::new()));
        let (rec_a, rec_b, rec_json) = (
            Arc::clone(&phases),
            Arc::clone(&phases),
            Arc::clone(&phases),
        );
        let gate_a = Mutex::new(Some(gate_a));
        let mut a = make(
            0,
            "a",
            O,
            typed(move |c, v| {
                rec_a
                    .lock()
                    .unwrap()
                    .push((c.phase(), c.cleanup().pending()));
                if let Some(gate) = gate_a.lock().unwrap().take() {
                    c.cleanup().spawn(async move {
                        let _ = gate.await;
                    });
                }
                Ok(TransformOutput::new(v))
            }),
        );
        a.cleanup = tracker_a.clone();
        let mut j = make(
            1,
            "j",
            O,
            json_cb(move |e| {
                let phase = if e["context"]["phase"] == "ordinary" {
                    O
                } else {
                    F
                };
                rec_json.lock().unwrap().push((phase, usize::MAX));
                Ok(e)
            }),
        );
        j.cleanup = tracker_b.clone();
        let mut b = make(
            0,
            "b",
            F,
            typed(move |c, v| {
                rec_b
                    .lock()
                    .unwrap()
                    .push((c.phase(), c.cleanup().pending()));
                Ok(TransformOutput::new(v))
            }),
        );
        b.cleanup = tracker_b.clone();
        let d = Dispatcher::new(vec![a, j, b]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, completed(json!("s"), false));
        assert_eq!(*phases.lock().unwrap(), [(O, 0), (O, usize::MAX), (F, 0)]);
        assert_eq!(tracker_a.pending(), 1);
        assert_eq!(tracker_b.pending(), 0);
    }

    #[tokio::test]
    async fn closing_mount_fails_the_handler_before_it_runs() {
        let closing = CancellationToken::new();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut h = make(0, "closing", O, counting(&calls));
        h.cleanup =
            CleanupTracker::from_parts(tokio_util::task::TaskTracker::new(), closing.clone());
        closing.cancel();
        let outcome = Dispatcher::new(vec![h])
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "closing".into()
            }
        );
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test]
    async fn panic_does_not_unwind_out_of_the_driver_and_mount_is_scoped() {
        let observed = Arc::new(AtomicUsize::new(0));
        let probe = Arc::clone(&observed);
        let d = Dispatcher::new(vec![
            make(
                0,
                "scoped",
                O,
                HandlerFn::ResultTransform(Arc::new(move |_, v| {
                    let probe = Arc::clone(&probe);
                    async move {
                        if is_active_mount(1) {
                            probe.fetch_add(1, Ordering::SeqCst);
                        }
                        Ok(TransformOutput::new(v))
                    }
                    .boxed()
                })),
            ),
            make(1, "boom", O, typed(|_, _| panic!("{ERROR_SECRET}"))),
        ]);
        let outcome = AssertUnwindSafe(
            d.transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s"))),
        )
        .catch_unwind()
        .await
        .expect("driver must not unwind");
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "boom".into()
            }
        );
        assert_eq!(observed.load(Ordering::SeqCst), 1);
    }

    // ---- cancellation, fallback acceptance and final redaction (crabber-ctj9) ----

    use std::{sync::atomic::AtomicBool, time::Duration};
    use tokio::sync::Notify;
    use tokio::time::Instant;

    const ACCEPTED: &str = "ACCEPTED-UNREDACTED-55d1";
    fn short() -> Duration {
        FINAL_REDACTION_DEADLINE / 5
    }
    fn long() -> Duration {
        FINAL_REDACTION_DEADLINE * 2
    }
    fn three_fifths() -> Duration {
        FINAL_REDACTION_DEADLINE * 3 / 5
    }
    fn interrupted(redacted: Option<Value>) -> ToolResultOutcome {
        ToolResultOutcome::Interrupted { redacted }
    }
    fn async_typed<Fut>(
        f: impl Fn(ToolResultContext, Value) -> Fut + Send + Sync + 'static,
    ) -> HandlerFn
    where
        Fut: Future<Output = Result<TransformOutput, ExtensionError>> + Send + 'static,
    {
        HandlerFn::ResultTransform(Arc::new(move |c, v| Box::pin(f(c, v))))
    }
    /// Replaces the value with the (unredacted) accepted marker.
    fn accept() -> HandlerFn {
        typed(|_, _| Ok(TransformOutput::new(json!(ACCEPTED))))
    }
    /// Redacts the accepted marker and appends `tag`.
    fn redact(tag: &'static str) -> HandlerFn {
        typed(move |_, v| {
            Ok(TransformOutput::new(json!(format!(
                "{}{tag}",
                v.as_str().unwrap().replace(ACCEPTED, "[red]")
            ))))
        })
    }
    struct DropFlag(Arc<AtomicBool>);
    impl Drop for DropFlag {
        fn drop(&mut self) {
            self.0.store(true, Ordering::SeqCst);
        }
    }
    /// Signals `started`, then never finishes. `dropped` is set when its future is dropped.
    fn hang(started: &Arc<Notify>, dropped: &Arc<AtomicBool>) -> HandlerFn {
        let (started, dropped) = (Arc::clone(started), Arc::clone(dropped));
        async_typed(move |_, _| {
            let (started, guard) = (Arc::clone(&started), DropFlag(Arc::clone(&dropped)));
            async move {
                let _guard = guard;
                started.notify_one();
                std::future::pending::<Result<TransformOutput, ExtensionError>>().await
            }
        })
    }
    /// Signals `started`, sleeps `delay`, then appends `tag`.
    fn slow_append(
        tag: &'static str,
        delay: Duration,
        started: &Arc<Notify>,
        dropped: &Arc<AtomicBool>,
    ) -> HandlerFn {
        let (started, dropped) = (Arc::clone(started), Arc::clone(dropped));
        async_typed(move |_, v| {
            let (started, guard) = (Arc::clone(&started), DropFlag(Arc::clone(&dropped)));
            async move {
                let _guard = guard;
                started.notify_one();
                tokio::time::sleep(delay).await;
                Ok(TransformOutput::new(json!(format!(
                    "{}{tag}",
                    v.as_str().unwrap().replace(ACCEPTED, "[red]")
                ))))
            }
        })
    }
    /// Records whether the handler saw a cancelled token.
    fn observing(seen: &Arc<Mutex<Vec<bool>>>) -> HandlerFn {
        let seen = Arc::clone(seen);
        typed(move |c, v| {
            seen.lock().unwrap().push(c.cancellation().is_cancelled());
            Ok(TransformOutput::new(v))
        })
    }
    /// Cancels the per-call token and returns immediately in the same poll.
    fn cancel_and_return(tag: &'static str) -> HandlerFn {
        typed(move |c, v| {
            c.cancellation().cancel();
            Ok(TransformOutput::new(json!(format!(
                "{}{tag}",
                v.as_str().unwrap_or("")
            ))))
        })
    }
    fn assert_no_leak(outcome: &ToolResultOutcome) {
        let text = format!("{outcome:?}");
        for secret in [SEED_SECRET, ACCEPTED, ERROR_SECRET, "INTERMEDIATE-SECRET"] {
            assert!(!text.contains(secret), "{text}");
        }
    }
    fn real_tracker() -> CleanupTracker {
        CleanupTracker::from_parts(
            tokio_util::task::TaskTracker::new(),
            CancellationToken::new(),
        )
    }
    /// Runs the driver and cancels the token once `started` fires.
    async fn run_cancelled_after(
        d: &Dispatcher,
        seed_value: Value,
        started: &Notify,
    ) -> ToolResultOutcome {
        let token = CancellationToken::new();
        let ctx = context(ToolOutcomeClass::Succeeded).with_cancellation(token.clone());
        let driver = d.transform_tool_result(ctx, seed(seed_value));
        tokio::pin!(driver);
        tokio::select! {
            biased;
            out = &mut driver => return out,
            () = started.notified() => {}
        }
        token.cancel();
        driver.await
    }
    fn flag() -> Arc<AtomicBool> {
        Arc::new(AtomicBool::new(false))
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_during_ordinary_handler_drops_its_future_but_cleanup_survives() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let tracker = real_tracker();
        let (release, gate) = tokio::sync::oneshot::channel::<()>();
        let gate = Arc::new(Mutex::new(Some(gate)));
        let cleanup_task = Arc::new(Mutex::new(None));
        let slow = {
            let (started, dropped) = (Arc::clone(&started), Arc::clone(&dropped));
            let (gate, cleanup_task) = (Arc::clone(&gate), Arc::clone(&cleanup_task));
            async_typed(move |c, _| {
                let gate = gate.lock().unwrap().take();
                let task = c.cleanup().spawn(async move {
                    if let Some(gate) = gate {
                        let _ = gate.await;
                    }
                });
                *cleanup_task.lock().unwrap() = Some(task);
                let (started, guard) = (Arc::clone(&started), DropFlag(Arc::clone(&dropped)));
                async move {
                    let _guard = guard;
                    started.notify_one();
                    std::future::pending::<Result<TransformOutput, ExtensionError>>().await
                }
            })
        };
        let mut slow = make(1, "slow", O, slow);
        slow.cleanup = tracker.clone();
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            slow,
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F"))));
        assert!(dropped.load(Ordering::SeqCst), "handler future not dropped");
        assert_eq!(tracker.pending(), 1, "cleanup must outlive the future");
        release.send(()).unwrap();
        let task = cleanup_task.lock().unwrap().take().unwrap();
        task.await.unwrap();
        assert_eq!(tracker.pending(), 0);
    }

    // ---- mount close signal in the driver (crabber-b4zy) ----

    /// A tracker whose close signal the test controls.
    fn closable() -> (CleanupTracker, CancellationToken) {
        let closing = CancellationToken::new();
        (
            CleanupTracker::from_parts(tokio_util::task::TaskTracker::new(), closing.clone()),
            closing,
        )
    }
    /// Polls the driver to a pending state, so every in-flight handler has started.
    async fn park<F: Future + Unpin>(driver: &mut F) {
        for _ in 0..8 {
            assert!(futures::poll!(&mut *driver).is_pending());
            tokio::task::yield_now().await;
        }
    }

    #[tokio::test(start_paused = true)]
    async fn close_signal_while_an_ordinary_handler_runs_fails_the_call_and_drops_it() {
        let (started, dropped, later) = (
            Arc::new(Notify::new()),
            flag(),
            Arc::new(AtomicUsize::new(0)),
        );
        let (tracker, closing) = closable();
        let mut hung = make(0, "hang", O, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![hung, make(1, "later", O, counting(&later))]);
        let driver = d.transform_tool_result(
            context(ToolOutcomeClass::Succeeded),
            seed(json!(SEED_SECRET)),
        );
        tokio::pin!(driver);
        park(&mut driver).await;
        assert!(!dropped.load(Ordering::SeqCst));
        closing.cancel();
        let outcome = driver.await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "hang".into()
            }
        );
        assert!(dropped.load(Ordering::SeqCst), "handler future not dropped");
        assert_eq!(later.load(Ordering::SeqCst), 0);
        assert_no_leak(&outcome);
    }

    #[tokio::test(start_paused = true)]
    async fn close_signal_while_a_final_redactor_runs_fails_the_call() {
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let (tracker, closing) = closable();
        let mut hung = make(0, "hang", F, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![make(0, "accept", O, accept()), hung]);
        let driver = d.transform_tool_result(
            context(ToolOutcomeClass::Succeeded),
            seed(json!(SEED_SECRET)),
        );
        tokio::pin!(driver);
        park(&mut driver).await;
        closing.cancel();
        let outcome = driver.await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "hang".into()
            }
        );
        assert!(dropped.load(Ordering::SeqCst));
        assert_no_leak(&outcome);
    }

    #[tokio::test(start_paused = true)]
    async fn close_signal_after_cancellation_was_observed_is_interrupted_without_payload() {
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let (tracker, closing) = closable();
        let mut hung = make(0, "hang", F, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![make(0, "accept", O, accept()), hung]);
        let token = CancellationToken::new();
        let ctx = context(ToolOutcomeClass::Succeeded).with_cancellation(token.clone());
        let driver = d.transform_tool_result(ctx, seed(json!(SEED_SECRET)));
        tokio::pin!(driver);
        park(&mut driver).await;
        // Cancellation is observed with the final redactor in flight on an
        // accepted value, so it keeps running; the mount close then drops it.
        token.cancel();
        assert!(futures::poll!(&mut driver).is_pending());
        assert!(!dropped.load(Ordering::SeqCst));
        closing.cancel();
        let outcome = driver.await;
        assert_eq!(outcome, interrupted(None));
        assert!(dropped.load(Ordering::SeqCst));
        assert_no_leak(&outcome);
    }

    #[tokio::test(start_paused = true)]
    async fn cancellation_wins_when_both_it_and_the_close_signal_are_ready() {
        // Nothing accepted: Interrupted, never Failed.
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let (tracker, closing) = closable();
        let mut hung = make(0, "hang", O, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![hung]);
        let token = CancellationToken::new();
        let ctx = context(ToolOutcomeClass::Succeeded).with_cancellation(token.clone());
        let driver = d.transform_tool_result(ctx, seed(json!(SEED_SECRET)));
        tokio::pin!(driver);
        park(&mut driver).await;
        token.cancel();
        closing.cancel();
        assert_eq!(driver.await, interrupted(None));
        assert!(dropped.load(Ordering::SeqCst));

        // A value was accepted: the cancellation arm is taken, so the in-flight
        // ordinary handler is dropped and the final redactor still runs. Had the
        // close arm won, this would be `Failed`.
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let (tracker, closing) = closable();
        let mut hung = make(1, "hang", O, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            hung,
            make(0, "final", F, redact("+F")),
        ]);
        let token = CancellationToken::new();
        let ctx = context(ToolOutcomeClass::Succeeded).with_cancellation(token.clone());
        let driver = d.transform_tool_result(ctx, seed(json!(SEED_SECRET)));
        tokio::pin!(driver);
        park(&mut driver).await;
        token.cancel();
        closing.cancel();
        assert_eq!(driver.await, interrupted(Some(json!("[red]+F"))));
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactor_with_cancellation_and_close_ready_together_is_interrupted() {
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let (tracker, closing) = closable();
        let mut hung = make(0, "hang", F, hang(&started, &dropped));
        hung.cleanup = tracker;
        let d = Dispatcher::new(vec![make(0, "accept", O, accept()), hung]);
        let token = CancellationToken::new();
        let ctx = context(ToolOutcomeClass::Succeeded).with_cancellation(token.clone());
        let driver = d.transform_tool_result(ctx, seed(json!(SEED_SECRET)));
        tokio::pin!(driver);
        park(&mut driver).await;
        // Both ready at once on a final redactor running on an accepted value:
        // cancellation is observed, so never `Failed`, and no payload.
        token.cancel();
        closing.cancel();
        let outcome = driver.await;
        assert_eq!(outcome, interrupted(None));
        assert!(dropped.load(Ordering::SeqCst));
        assert_no_leak(&outcome);
    }

    // Same-poll completion and cancellation: the handler cancels as it
    // completes, so its output is not committed (commit point) and the
    // remaining ordinary handlers are skipped. A cancel that lands strictly
    // between a commit and the next handler needs another thread; see
    // `cross_thread_cancellation_leaves_only_legal_outcomes`.
    #[tokio::test(start_paused = true)]
    async fn ordinary_handler_cancelling_as_it_completes_skips_the_remaining_ones() {
        let later = Arc::new(AtomicUsize::new(0));
        let d = Dispatcher::new(vec![
            make(0, "first", O, accept()),
            make(1, "second", O, cancel_and_return("+h2")),
            make(2, "third", O, counting(&later)),
            make(3, "fourth", O, counting(&later)),
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F"))));
        assert_eq!(later.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_while_a_final_redactor_completes_keeps_its_output_and_runs_the_rest() {
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(0, "f1", F, cancel_and_return("+F1")),
            make(1, "f2", F, redact("+F2")),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(
            outcome,
            interrupted(Some(json!(
                format!("{ACCEPTED}+F1").replace(ACCEPTED, "[red]") + "+F2"
            )))
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_driver_that_returned_is_not_retroactively_interrupted() {
        let d = Dispatcher::new(vec![make(0, "last", F, append("+last"))]);
        let ctx = context(ToolOutcomeClass::Succeeded);
        let token = ctx.cancellation().clone();
        let outcome = d.transform_tool_result(ctx, seed(json!("s"))).await;
        token.cancel();
        assert_eq!(outcome, completed(json!("s+last"), false));
    }

    #[tokio::test(start_paused = true)]
    async fn simultaneous_completion_and_cancellation_resolves_as_cancelled() {
        // The output of a handler that cancels while completing is not committed.
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(1, "racer", O, cancel_and_return("+LOST")),
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F"))));
        // Nothing accepted before the racer: nothing to fall back to.
        let d = Dispatcher::new(vec![
            make(0, "racer", O, cancel_and_return("+LOST")),
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = d
            .transform_tool_result(
                context(ToolOutcomeClass::Succeeded),
                seed(json!(SEED_SECRET)),
            )
            .await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactor_in_flight_on_an_accepted_value_may_finish_in_time() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(0, "f1", F, slow_append("+F1", short(), &started, &dropped)),
            make(1, "f2", F, redact("+F2")),
        ]);
        // Cancellation arrives while f1 is in flight; f1 finishes, f2 runs after.
        let start = Instant::now();
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F1+F2"))));
        assert!(start.elapsed() < FINAL_REDACTION_DEADLINE);
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactor_in_flight_past_the_deadline_is_dropped() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let later = Arc::new(AtomicUsize::new(0));
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(0, "f1", F, slow_append("+F1", long(), &started, &dropped)),
            make(1, "f2", F, counting(&later)),
        ]);
        let start = Instant::now();
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(later.load(Ordering::SeqCst), 0);
        let elapsed = start.elapsed();
        assert!(elapsed >= FINAL_REDACTION_DEADLINE, "{elapsed:?}");
        assert!(elapsed < FINAL_REDACTION_DEADLINE + short(), "{elapsed:?}");
    }

    /// Cancellation observed while an ordinary handler runs; `finals` then run.
    async fn cancel_in_ordinary_then(finals: Vec<Handler>) -> ToolResultOutcome {
        let started = Arc::new(Notify::new());
        let mut handlers = vec![
            make(0, "accept", O, accept()),
            make(1, "trigger", O, hang(&started, &flag())),
        ];
        handlers.extend(finals);
        run_cancelled_after(&Dispatcher::new(handlers), json!(SEED_SECRET), &started).await
    }

    #[tokio::test(start_paused = true)]
    async fn failing_final_redactor_after_cancellation_is_interrupted_not_failed() {
        for (name, failing) in failing_handlers() {
            let outcome = cancel_in_ordinary_then(vec![make(0, "bad", F, failing)]).await;
            assert_eq!(outcome, interrupted(None), "{name}");
            assert_no_leak(&outcome);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactor_failing_while_in_flight_at_cancellation_is_interrupted() {
        for panics in [false, true] {
            let started = Arc::new(Notify::new());
            let failing = {
                let started = Arc::clone(&started);
                async_typed(move |_, _| {
                    let started = Arc::clone(&started);
                    async move {
                        started.notify_one();
                        tokio::task::yield_now().await;
                        assert!(!panics, "{ERROR_SECRET}");
                        Err(ExtensionError::Tool(ERROR_SECRET.into()))
                    }
                })
            };
            let d = Dispatcher::new(vec![
                make(0, "accept", O, accept()),
                make(0, "bad", F, failing),
            ]);
            let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
            assert_eq!(outcome, interrupted(None), "panics: {panics}");
            assert_no_leak(&outcome);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactor_timing_out_after_cancellation_is_interrupted() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let outcome = cancel_in_ordinary_then(vec![make(
            0,
            "slow",
            F,
            slow_append("+F", long(), &started, &dropped),
        )])
        .await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert!(dropped.load(Ordering::SeqCst));
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_with_accepted_value_and_no_final_redactor_persists_nothing() {
        let later = Arc::new(AtomicUsize::new(0));
        let started = Arc::new(Notify::new());
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(1, "trigger", O, hang(&started, &flag())),
            make(2, "later", O, counting(&later)),
        ]);
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert_eq!(later.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn cancel_before_any_value_is_accepted_never_runs_final_redactors_on_the_seed() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let finals = Arc::new(AtomicUsize::new(0));
        let d = Dispatcher::new(vec![
            make(0, "trigger", O, hang(&started, &dropped)),
            make(0, "final", F, counting(&finals)),
        ]);
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(finals.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn sole_final_redactor_in_flight_on_the_seed_is_dropped_with_no_payload() {
        let started = Arc::new(Notify::new());
        let dropped = flag();
        let later = Arc::new(AtomicUsize::new(0));
        let d = Dispatcher::new(vec![
            make(0, "final", F, hang(&started, &dropped)),
            make(1, "final-2", F, counting(&later)),
        ]);
        let start = Instant::now();
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert!(dropped.load(Ordering::SeqCst));
        assert_eq!(later.load(Ordering::SeqCst), 0);
        // At once: no deadline wait.
        assert!(start.elapsed() < FINAL_REDACTION_DEADLINE);
    }

    #[tokio::test(start_paused = true)]
    async fn two_final_redactors_after_cancellation_compose_in_order() {
        let outcome = cancel_in_ordinary_then(vec![
            make(0, "f1", F, redact("+F1")),
            make(1, "f2", F, redact("+F2")),
        ])
        .await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F1+F2"))));
    }

    #[tokio::test(start_paused = true)]
    async fn the_final_redaction_deadline_is_one_budget_across_redactors() {
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let sleeping = |tag| slow_append(tag, three_fifths(), &started, &dropped);
        // Each fits the deadline alone; together they exceed it.
        let start = Instant::now();
        let outcome = cancel_in_ordinary_then(vec![
            make(0, "f1", F, sleeping("+F1")),
            make(1, "f2", F, sleeping("+F2")),
        ])
        .await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        let elapsed = start.elapsed();
        assert!(elapsed >= FINAL_REDACTION_DEADLINE, "{elapsed:?}");
        assert!(elapsed < FINAL_REDACTION_DEADLINE + short(), "{elapsed:?}");
        // Within the budget, both pass.
        let outcome = cancel_in_ordinary_then(vec![
            make(0, "f1", F, slow_append("+F1", short(), &started, &dropped)),
            make(1, "f2", F, slow_append("+F2", short(), &started, &dropped)),
        ])
        .await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F1+F2"))));
    }

    #[tokio::test(start_paused = true)]
    async fn final_redactors_see_the_cancelled_token() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let outcome = cancel_in_ordinary_then(vec![
            make(0, "f1", F, observing(&seen)),
            make(1, "f2", F, observing(&seen)),
        ])
        .await;
        assert_eq!(outcome, interrupted(Some(json!(ACCEPTED))));
        assert_eq!(*seen.lock().unwrap(), [true, true]);
        // Without cancellation the same handlers see a live token.
        seen.lock().unwrap().clear();
        let d = Dispatcher::new(vec![make(0, "f", F, observing(&seen))]);
        d.transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(*seen.lock().unwrap(), [false]);
    }

    #[tokio::test(start_paused = true)]
    async fn token_cancelled_on_entry_is_interrupted_and_runs_nothing() {
        let calls = Arc::new(AtomicUsize::new(0));
        for handlers in [
            Vec::new(),
            vec![
                make(0, "o", O, counting(&calls)),
                make(0, "f", F, counting(&calls)),
            ],
        ] {
            let token = CancellationToken::new();
            token.cancel();
            let outcome = Dispatcher::new(handlers)
                .transform_tool_result(
                    context(ToolOutcomeClass::Succeeded).with_cancellation(token),
                    seed(json!(SEED_SECRET)),
                )
                .await;
            assert_eq!(outcome, interrupted(None));
            assert_no_leak(&outcome);
        }
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn a_handler_cancelling_its_own_call_token_is_observed_cancellation() {
        let parent = CancellationToken::new();
        let hangs_after_cancel = async_typed(|c, _| async move {
            c.cancellation().cancel();
            std::future::pending::<Result<TransformOutput, ExtensionError>>().await
        });
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(1, "canceller", O, hangs_after_cancel),
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = d
            .transform_tool_result(
                context(ToolOutcomeClass::Succeeded).with_cancellation(parent.child_token()),
                seed(json!(SEED_SECRET)),
            )
            .await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F"))));
        assert!(
            !parent.is_cancelled(),
            "child cancel must not reach the parent"
        );
        // No accepted value: nothing tool-authored is persisted.
        let d = Dispatcher::new(vec![make(
            0,
            "canceller",
            O,
            async_typed(|c, _| async move {
                c.cancellation().cancel();
                std::future::pending::<Result<TransformOutput, ExtensionError>>().await
            }),
        )]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, interrupted(None));
    }

    #[tokio::test(start_paused = true)]
    async fn closing_final_redactor_after_cancellation_is_interrupted_not_failed() {
        let closing = CancellationToken::new();
        closing.cancel();
        let calls = Arc::new(AtomicUsize::new(0));
        let mut f = make(0, "closing", F, counting(&calls));
        f.cleanup = CleanupTracker::from_parts(tokio_util::task::TaskTracker::new(), closing);
        let outcome = cancel_in_ordinary_then(vec![f]).await;
        assert_eq!(outcome, interrupted(None));
        assert_eq!(calls.load(Ordering::SeqCst), 0);
    }

    #[tokio::test(start_paused = true)]
    async fn without_cancellation_final_redactors_are_the_unbounded_last_step() {
        let (started, dropped) = (Arc::new(Notify::new()), flag());
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(0, "slow", F, slow_append("+F", long(), &started, &dropped)),
        ]);
        let outcome = d
            .transform_tool_result(
                context(ToolOutcomeClass::Succeeded).with_cancellation(CancellationToken::new()),
                seed(json!("s")),
            )
            .await;
        assert_eq!(outcome, completed(json!("[red]+F"), false));
        // D2 still applies to a failing final redactor.
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(
                0,
                "bad",
                F,
                typed(|_, _| Err(ExtensionError::Tool(ERROR_SECRET.into()))),
            ),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "bad".into()
            }
        );
        assert_no_leak(&outcome);
    }

    #[tokio::test]
    async fn final_redaction_json_handler_sees_its_phase_in_the_envelope() {
        let seen = Arc::new(Mutex::new(Vec::new()));
        let recorder = Arc::clone(&seen);
        // What `Registrar::on_final_redaction_json` stores.
        let cb: Callback = Arc::new(move |e| {
            recorder.lock().unwrap().push(e["context"]["phase"].clone());
            Box::pin(std::future::ready(Ok(e)))
        });
        let d = Dispatcher::new(vec![
            make(0, "o", O, json_cb(Ok)),
            make(
                0,
                "f",
                F,
                HandlerFn::ResultTransform(json_result_transform(cb)),
            ),
        ]);
        let outcome = d
            .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s")))
            .await;
        assert_eq!(outcome, completed(json!("s"), false));
        assert_eq!(*seen.lock().unwrap(), [json!("final_redaction")]);
    }

    /// Panics when dropped (unless already unwinding).
    struct Bomb;
    impl Drop for Bomb {
        fn drop(&mut self) {
            assert!(std::thread::panicking(), "drop bomb");
        }
    }
    /// Like `hang`, but its future panics in its destructor.
    fn hang_bomb(started: &Arc<Notify>) -> HandlerFn {
        let started = Arc::clone(started);
        async_typed(move |_, _| {
            let started = Arc::clone(&started);
            async move {
                let _bomb = Bomb;
                started.notify_one();
                std::future::pending::<Result<TransformOutput, ExtensionError>>().await
            }
        })
    }
    async fn no_unwind(d: &Dispatcher, seed_value: Value, started: &Notify) -> ToolResultOutcome {
        AssertUnwindSafe(run_cancelled_after(d, seed_value, started))
            .catch_unwind()
            .await
            .expect("driver must not unwind out of a panicking destructor")
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_destructor_of_a_dropped_ordinary_handler_is_contained() {
        let started = Arc::new(Notify::new());
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(1, "bomb", O, hang_bomb(&started)),
            make(0, "final", F, redact("+F")),
        ]);
        let outcome = no_unwind(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(Some(json!("[red]+F"))));
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_destructor_of_a_final_redactor_dropped_at_the_deadline_is_contained() {
        let started = Arc::new(Notify::new());
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            make(0, "bomb", F, hang_bomb(&started)),
        ]);
        let outcome = no_unwind(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_destructor_of_a_handler_in_flight_with_nothing_accepted_is_contained() {
        for phase in [O, F] {
            let started = Arc::new(Notify::new());
            let d = Dispatcher::new(vec![
                make(0, "bomb", phase, hang_bomb(&started)),
                make(1, "final", F, redact("+F")),
            ]);
            let outcome = no_unwind(&d, json!(SEED_SECRET), &started).await;
            assert_eq!(outcome, interrupted(None), "{phase:?}");
            assert_no_leak(&outcome);
        }
    }

    #[tokio::test(start_paused = true)]
    async fn panicking_destructor_at_normal_completion_is_a_failure_without_cancellation() {
        let bomb = typed(|_, v| {
            // The bomb is dropped when this (ready) future completes.
            let _bomb = Bomb;
            Ok(TransformOutput::new(v))
        });
        let bomb = match bomb {
            HandlerFn::ResultTransform(f) => HandlerFn::ResultTransform(Arc::new(move |c, v| {
                let f = Arc::clone(&f);
                Box::pin(async move {
                    let _bomb = Bomb;
                    f(c, v).await
                })
            })),
            other => other,
        };
        let outcome = AssertUnwindSafe(
            Dispatcher::new(vec![make(0, "bomb", O, bomb)])
                .transform_tool_result(context(ToolOutcomeClass::Succeeded), seed(json!("s"))),
        )
        .catch_unwind()
        .await
        .expect("driver must not unwind");
        assert_eq!(
            outcome,
            ToolResultOutcome::Failed {
                handler: "bomb".into()
            }
        );
    }

    #[tokio::test(start_paused = true)]
    async fn a_final_redactor_starting_after_the_deadline_has_passed_is_not_run() {
        let started = Arc::new(Notify::new());
        let later = Arc::new(AtomicUsize::new(0));
        let d = Dispatcher::new(vec![
            make(0, "accept", O, accept()),
            // Takes exactly the whole budget, then f2 is instantly ready.
            make(
                0,
                "f1",
                F,
                slow_append("+F1", FINAL_REDACTION_DEADLINE, &started, &flag()),
            ),
            make(1, "f2", F, counting(&later)),
        ]);
        let outcome = run_cancelled_after(&d, json!(SEED_SECRET), &started).await;
        assert_eq!(outcome, interrupted(None));
        assert_no_leak(&outcome);
        assert_eq!(later.load(Ordering::SeqCst), 0);
    }

    /// Cancels from another OS thread at varying points of a chain of
    /// ordinary handlers that yield, then one final redactor. Determinism is
    /// not possible without a hook in the driver (nothing awaits between a
    /// commit and the next handler's start), so this asserts the invariants
    /// every interleaving must satisfy.
    #[tokio::test(flavor = "multi_thread", worker_threads = 2)]
    async fn cross_thread_cancellation_leaves_only_legal_outcomes() {
        const ORDINARY: usize = 8;
        for i in 0..2000usize {
            let ordinary_calls = Arc::new(AtomicUsize::new(0));
            let final_calls = Arc::new(AtomicUsize::new(0));
            let mut handlers = Vec::new();
            for n in 0..ORDINARY {
                let calls = Arc::clone(&ordinary_calls);
                handlers.push(make(
                    i32::try_from(n).unwrap(),
                    "ordinary",
                    O,
                    async_typed(move |_, v| {
                        calls.fetch_add(1, Ordering::SeqCst);
                        async move {
                            tokio::task::yield_now().await;
                            Ok(TransformOutput::new(json!(format!(
                                "{}x",
                                v.as_str().unwrap()
                            ))))
                        }
                    }),
                ));
            }
            let calls = Arc::clone(&final_calls);
            handlers.push(make(
                0,
                "final",
                F,
                typed(move |_, v| {
                    calls.fetch_add(1, Ordering::SeqCst);
                    Ok(TransformOutput::new(json!(format!(
                        "{}F",
                        v.as_str().unwrap()
                    ))))
                }),
            ));
            let d = Dispatcher::new(handlers);
            let ctx = context(ToolOutcomeClass::Succeeded);
            let token = ctx.cancellation().clone();
            let spins = (i % 97) * 40;
            let canceller = std::thread::spawn(move || {
                for _ in 0..spins {
                    std::hint::spin_loop();
                }
                token.cancel();
            });
            let outcome = d.transform_tool_result(ctx, seed(json!(""))).await;
            canceller.join().unwrap();
            let ordinary = ordinary_calls.load(Ordering::SeqCst);
            let finals = final_calls.load(Ordering::SeqCst);
            match outcome {
                ToolResultOutcome::Completed { result, is_error } => {
                    assert_eq!(result, json!(format!("{}F", "x".repeat(ORDINARY))));
                    assert!(!is_error);
                    assert_eq!((ordinary, finals), (ORDINARY, 1));
                }
                ToolResultOutcome::Interrupted {
                    redacted: Some(value),
                } => {
                    let value = value.as_str().unwrap().to_owned();
                    let k = value.len() - 1;
                    assert_eq!(value, format!("{}F", "x".repeat(k)));
                    assert!((1..=ORDINARY).contains(&k), "{value}");
                    // At most one handler beyond the committed ones was
                    // started (and dropped); none after observation.
                    assert!(ordinary <= k + 1, "{ordinary} ordinary calls, k = {k}");
                    assert_eq!(finals, 1);
                }
                ToolResultOutcome::Interrupted { redacted: None } => {
                    // Nothing accepted: at most the first handler started.
                    assert!(ordinary <= 1, "{ordinary} ordinary calls");
                    assert_eq!(finals, 0);
                }
                ToolResultOutcome::Failed { handler } => panic!("Failed({handler})"),
            }
        }
    }
}
