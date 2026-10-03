//! Ordered, bounded extension callback dispatch.
#![allow(clippy::missing_errors_doc, clippy::missing_panics_doc)]
use crate::ExtensionError;
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
    pub(crate) callback: HandlerFn,
}
impl Handler {
    pub(crate) fn sort_key(&self) -> (i32, u8, u64, usize) {
        (
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
    pub async fn transform<P: Point>(&self, mut value: Value) -> Result<Value, ExtensionError> {
        for handler in self.matching(P::ID, Mode::Transform) {
            if let HandlerFn::Ordinary(callback) = &handler.callback {
                value = with_mount(handler.mount_id, callback(value)).await?;
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
        mut value: Value,
        pinned: &[(&str, Value)],
    ) -> Result<Value, ExtensionError> {
        let assert = |value: &mut Value| {
            let Value::Object(object) = value else {
                return Err(ExtensionError::Rejected(P::ID));
            };
            for (key, pinned) in pinned {
                object.insert((*key).to_owned(), pinned.clone());
            }
            Ok(())
        };
        assert(&mut value)?;
        for handler in self.matching(P::ID, Mode::Transform) {
            if let HandlerFn::Ordinary(callback) = &handler.callback {
                value = with_mount(handler.mount_id, callback(value)).await?;
                assert(&mut value)?;
            }
        }
        Ok(value)
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
