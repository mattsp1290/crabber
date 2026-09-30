//! Ordered, bounded extension callback dispatch.
#![allow(clippy::missing_errors_doc)]
use crate::ExtensionError;
use futures::future::BoxFuture;
use serde_json::Value;
use std::{
    future::Future,
    panic::AssertUnwindSafe,
    sync::{
        Arc,
        atomic::{AtomicUsize, Ordering},
    },
};

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

#[derive(Clone)]
pub struct Next {
    calls: Arc<AtomicUsize>,
    invoke: Callback,
}
impl Next {
    pub async fn call(&self, value: Value) -> Result<Value, ExtensionError> {
        if self.calls.fetch_add(1, Ordering::SeqCst) != 0 {
            return Err(ExtensionError::NextCalledTwice);
        }
        (self.invoke)(value).await
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
        let calls = Arc::new(AtomicUsize::new(0));
        let next_handlers = Arc::clone(&handlers);
        let next_terminal = Arc::clone(&terminal);
        let next = Next {
            calls: Arc::clone(&calls),
            invoke: Arc::new(move |v| {
                around_at(
                    Arc::clone(&next_handlers),
                    index + 1,
                    v,
                    Arc::clone(&next_terminal),
                )
            }),
        };
        let result = with_mount(mount_id, callback(value, next)).await;
        match calls.load(Ordering::SeqCst) {
            0 => Err(ExtensionError::NextNotCalled),
            1 => result,
            _ => Err(ExtensionError::NextCalledTwice),
        }
    })
}

tokio::task_local! { static ACTIVE_MOUNT: u64; }
pub(crate) async fn with_mount<T>(id: u64, future: impl Future<Output = T>) -> T {
    ACTIVE_MOUNT.scope(id, future).await
}
pub(crate) fn is_active_mount(id: u64) -> bool {
    ACTIVE_MOUNT
        .try_with(|current| *current == id)
        .unwrap_or(false)
}

#[cfg(test)]
mod tests {
    use super::*;
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
}
