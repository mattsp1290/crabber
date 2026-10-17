//! One retained actual stream actor, one bounded demand, no per-delta task.
use super::{
    facade::unavailable,
    value::{Owned, Reference},
};
use crate::{
    CallbackInvocationCapability,
    contextual::Owner,
    dispatch::CallbackTaskContext,
    registry::{Actor, ActorClose, InvocationSource},
};
use async_trait::async_trait;
use crabber_providers::{DeltaStream, ModelRequest, ProviderError, StreamDelta, Streamer};
use futures::{Stream, future::poll_fn};
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    task::{Context, Poll},
};
use tokio::sync::{mpsc, oneshot};

pub(super) struct Binding {
    pub(super) owner: Owner,
    pub(super) body: Arc<Owned<dyn Streamer>>,
    pub(super) source: InvocationSource,
    pub(super) _reference: Reference<dyn Streamer>,
    pub(super) streams: Mutex<Vec<Arc<Owned<OriginalStream>>>>,
}
impl Drop for Binding {
    fn drop(&mut self) {
        let streams = self.streams.lock().unwrap().clone();
        for stream in streams {
            stream.close();
        }
    }
}
pub(super) struct OriginalStream(Mutex<Option<DeltaStream>>);
impl OriginalStream {
    fn poll_next(&self, context: &mut Context<'_>) -> Poll<Option<StreamDelta>> {
        let mut original = self
            .0
            .lock()
            .unwrap()
            .take()
            .expect("one original stream poll");
        let polled = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            original.as_mut().poll_next(context)
        }));
        // Install the original back before unwinding so its disposal stays owned.
        *self.0.lock().unwrap() = Some(original);
        match polled {
            Ok(polled) => polled,
            Err(payload) => std::panic::resume_unwind(payload),
        }
    }
}
struct Demand {
    context: CallbackTaskContext,
    result: oneshot::Sender<Option<StreamDelta>>,
}
struct Endpoint {
    actor: Arc<Actor>,
    demand: mpsc::Sender<Demand>,
    pending: Option<oneshot::Receiver<Option<StreamDelta>>>,
    owner: CallbackInvocationCapability,
    _reference: Reference<OriginalStream>,
}
impl Drop for Endpoint {
    fn drop(&mut self) {
        self.actor.close();
    }
}
impl Stream for Endpoint {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if !self.owner.matches_current() {
            return Poll::Ready(Some(StreamDelta::Error(unavailable(
                "stream caller owner changed or is absent",
            ))));
        }
        if self.pending.is_none() {
            let (send, receive) = oneshot::channel();
            let demand = Demand {
                context: CallbackTaskContext::capture_existing(),
                result: send,
            };
            if self.demand.try_send(demand).is_err() {
                return Poll::Ready(Some(StreamDelta::Error(unavailable(
                    "original stream actor is closed",
                ))));
            }
            self.pending = Some(receive);
        }
        let pending = self.pending.as_mut().unwrap();
        match Pin::new(pending).poll(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(result) => {
                self.pending = None;
                Poll::Ready(result.unwrap_or_else(|_| {
                    Some(StreamDelta::Error(unavailable(
                        "original stream actor failed",
                    )))
                }))
            }
        }
    }
}
#[async_trait]
impl Streamer for Binding {
    async fn stream(&self, request: ModelRequest) -> Result<DeltaStream, ProviderError> {
        let owner = crate::registry::current_callback_invocations()
            .ok_or_else(|| unavailable("provider stream requires an actual caller owner"))?;
        let streams = self.streams.lock().unwrap().clone();
        for stream in streams {
            stream.reap_join().await;
        }
        let cell = {
            let mut streams = self.streams.lock().unwrap();
            streams.retain(|stream| stream.observed() != Some(true));
            if streams.len() >= 64 {
                return Err(unavailable("active original stream capacity"));
            }
            let cell = Owned::new(self.owner.clone(), true);
            streams.push(cell.clone());
            cell
        };
        let using = match self.body.admit_use() {
            Ok(using) => using,
            Err(error) => {
                cell.constructor_done(false);
                cell.close();
                return Err(unavailable(error));
            }
        };
        let completion = match cell.admit_completion() {
            Ok(completion) => completion,
            Err(error) => {
                self.body.refused_use(using);
                cell.constructor_done(false);
                cell.close();
                return Err(unavailable(error));
            }
        };
        let close = Arc::new(StreamClose {
            parent: self.body.clone(),
            using,
            cell: cell.clone(),
            completion,
            entered: AtomicBool::new(false),
        });
        let actual = close.clone();
        let (send, receive) = oneshot::channel();
        let (demand, requests) = mpsc::channel::<Demand>(1);
        let actor = match self
            .source
            .register_actor(close.clone(), move |cancel| {
                Box::pin(run_stream(actual, request, send, requests, cancel))
            })
            .await
        {
            Ok(actor) => Arc::new(actor),
            Err(error) => {
                close.parent.refused_use(close.using);
                close.cell.refused_completion(close.completion);
                close.cell.constructor_done(false);
                close.close();
                return Err(unavailable(error));
            }
        };
        let mut waiter = actor.waiter();
        let created = receive.await;
        if let Ok(Ok(())) = created {
            let reference = cell.reference().map_err(unavailable)?;
            waiter.disarm();
            Ok(Box::pin(Endpoint {
                actor,
                demand,
                pending: None,
                owner,
                _reference: reference,
            }))
        } else {
            actor.close();
            let joined = actor.join().await;
            waiter.disarm();
            joined.map_err(unavailable)?;
            Err(created.err().map_or_else(
                || unavailable("provider stream creation refused"),
                unavailable,
            ))
        }
    }
}
struct Constructed {
    cell: Arc<Owned<OriginalStream>>,
    done: bool,
}
impl Drop for Constructed {
    fn drop(&mut self) {
        if !self.done {
            self.cell.constructor_done(true);
            self.cell.close();
        }
    }
}
async fn create_original(
    parent: &Arc<Owned<dyn Streamer>>,
    cell: &Arc<Owned<OriginalStream>>,
    request: ModelRequest,
    cancel: &tokio_util::sync::CancellationToken,
) -> Result<Result<(), ProviderError>, ()> {
    let borrowed = match parent.borrow() {
        Ok(borrowed) => borrowed,
        Err(error) => return Ok(Err(unavailable(error))),
    };
    let result = super::original::original(
        || borrowed.get().stream(request),
        |result| {
            result.map(|stream| cell.install(Box::new(OriginalStream(Mutex::new(Some(stream))))))
        },
        cancel,
    )
    .await;
    drop(borrowed);
    result.map(|result| {
        result.unwrap_or_else(|| Err(unavailable("original stream creation cancelled")))
    })
}

struct StreamClose {
    parent: Arc<Owned<dyn Streamer>>,
    using: u64,
    cell: Arc<Owned<OriginalStream>>,
    completion: u64,
    entered: AtomicBool,
}
impl ActorClose for StreamClose {
    fn close(&self) {
        self.cell.close();
    }
    fn observed(&self, success: bool) {
        if !success {
            self.cell.close();
        }
        if !self.entered.load(Ordering::SeqCst) {
            self.cell.constructor_done(false);
            self.cell.close();
        }
        self.parent.observed_use(self.using, success);
    }
    fn released(&self, success: bool) -> futures::future::BoxFuture<'static, bool> {
        self.cell.observed_completion(self.completion, success);
        let cell = self.cell.clone();
        Box::pin(async move { success && cell.join().await.is_ok() })
    }
}

async fn run_stream(
    actual: Arc<StreamClose>,
    request: ModelRequest,
    send: oneshot::Sender<Result<(), ProviderError>>,
    mut requests: mpsc::Receiver<Demand>,
    cancel: tokio_util::sync::CancellationToken,
) -> bool {
    actual.entered.store(true, Ordering::SeqCst);
    let mut done = Constructed {
        cell: actual.cell.clone(),
        done: false,
    };
    let created = create_original(&actual.parent, &actual.cell, request, &cancel).await;
    actual.cell.constructor_done(created.is_err());
    done.done = true;
    let Ok(created) = created else {
        actual.cell.close();
        return false;
    };
    let success = created.is_ok();
    if send.send(created).is_err() || !success {
        actual.cell.close();
        return actual.cell.body_join().await.is_ok();
    }
    loop {
        let demand = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            demand = requests.recv() => match demand { Some(demand) => demand, None => break },
        };
        let cell = actual.cell.clone();
        let next = demand.context.scope(poll_fn(move |cx| match cell.borrow() {
            Ok(borrowed) => borrowed.get().poll_next(cx),
            Err(error) => Poll::Ready(Some(StreamDelta::Error(unavailable(error)))),
        }));
        tokio::pin!(next);
        let output = tokio::select! {
            biased;
            () = cancel.cancelled() => break,
            output = &mut next => output,
        };
        let _ = demand.result.send(output);
    }
    actual.cell.close();
    actual.cell.body_join().await.is_ok()
}
