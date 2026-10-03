//! Ordered, bounded extension callback dispatch.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::{
    CleanupTracker, ExtensionError, ResultTransformCallback, ToolResultContext, ToolResultOutcome,
    TransformOutput, TransformPhase, json_result_transform,
};
use futures::future::BoxFuture;
use serde_json::Value;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
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
    /// The driver owns the accepted value, so no callback future holds it.
    /// Cancellation (entry check, selecting around a handler, the mount close
    /// signal, final-redaction fallback) is not handled yet: crabber-ctj9 wraps
    /// `invoke_result_handler` and uses `accepted` for fallback acceptance.
    pub async fn transform_tool_result(
        &self,
        context: ToolResultContext,
        seed: TransformOutput,
    ) -> ToolResultOutcome {
        let mut is_error = context.class().is_error() || seed.mark_error;
        // The seed is never an accepted value (D6); only a handler's valid
        // output is committed here.
        let mut accepted: Option<Value> = None;
        let handlers = self
            .matching(ToolResultTransform::ID, Mode::Transform)
            .filter(|h| {
                matches!(
                    h.callback,
                    HandlerFn::Ordinary(_) | HandlerFn::ResultTransform(_)
                )
            });
        for handler in handlers {
            let failed = || ToolResultOutcome::Failed {
                handler: handler.id.clone(),
            };
            if handler.cleanup.is_closing() {
                return failed();
            }
            let mut handler_context = context.clone().with_cleanup(handler.cleanup.clone());
            handler_context.set_phase(handler.phase);
            handler_context.set_is_error(is_error);
            let current = accepted.as_ref().unwrap_or(&seed.result).clone();
            match invoke_result_handler(handler, handler_context, current).await {
                Some(output) => {
                    is_error |= output.mark_error;
                    accepted = Some(output.result);
                }
                None => return failed(),
            }
        }
        ToolResultOutcome::Completed {
            result: accepted.unwrap_or(seed.result),
            is_error,
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
}
