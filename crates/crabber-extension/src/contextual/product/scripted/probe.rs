//! Test-only barriers on the actual private creation future and returned stream.
use super::*;
use futures::future::BoxFuture;
use std::{
    future::Future,
    sync::{Condvar, mpsc},
    thread::JoinHandle,
};

pub(super) struct Gate {
    released: Mutex<bool>,
    changed: Condvar,
    entered: mpsc::Sender<()>,
    timed_out: std::sync::atomic::AtomicBool,
}
impl Gate {
    pub(super) fn new(held: bool) -> (Arc<Self>, mpsc::Receiver<()>) {
        let (entered, receive) = mpsc::channel();
        (
            Arc::new(Self {
                released: Mutex::new(!held),
                changed: Condvar::new(),
                entered,
                timed_out: std::sync::atomic::AtomicBool::new(false),
            }),
            receive,
        )
    }
    fn arrive(&self) {
        let _ = self.entered.send(());
        let mut released = self.released.lock().unwrap();
        while !*released {
            released = self.changed.wait(released).unwrap();
        }
    }
    pub(super) fn release(&self) {
        *self.released.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub(super) fn watchdog(self: &Arc<Self>) -> Watchdog {
        let gate = self.clone();
        let handle = std::thread::spawn(move || {
            let released = gate.released.lock().unwrap();
            let (mut released, _) = gate
                .changed
                .wait_timeout_while(released, Duration::from_secs(5), |released| !*released)
                .unwrap();
            if !*released {
                gate.timed_out.store(true, Ordering::SeqCst);
                *released = true;
                gate.changed.notify_all();
            }
        });
        Watchdog {
            gate: self.clone(),
            handle: Some(handle),
        }
    }
}
pub(super) struct Watchdog {
    gate: Arc<Gate>,
    handle: Option<JoinHandle<()>>,
}
impl Watchdog {
    pub(super) fn finish(mut self) -> bool {
        self.gate.release();
        self.handle.take().unwrap().join().unwrap();
        !self.gate.timed_out.load(Ordering::SeqCst)
    }
}
impl Drop for Watchdog {
    fn drop(&mut self) {
        self.gate.release();
        if let Some(handle) = self.handle.take() {
            let _ = handle.join();
        }
    }
}
pub(super) struct Probe {
    pub(super) creation: Option<Arc<Gate>>,
    pub(super) stream: Arc<Gate>,
    pub(super) stream_panics: bool,
    pub(super) source: std::sync::Weak<State>,
    pub(super) frames: Mutex<Vec<(bool, bool)>>,
    pub(super) initializer: Mutex<Option<Owner>>,
}
pub(super) struct Creation {
    pub(super) original: Option<BoxFuture<'static, Result<DeltaStream, stream::CreationError>>>,
    pub(super) probe: Option<Arc<Probe>>,
}
impl Future for Creation {
    type Output = Result<DeltaStream, stream::CreationError>;
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Self::Output> {
        self.original.as_mut().unwrap().as_mut().poll(cx)
    }
}
impl Drop for Creation {
    fn drop(&mut self) {
        // This is the same literal returned future, including after Ready.
        drop(self.original.take());
        if let Some(gate) = self
            .probe
            .as_ref()
            .and_then(|probe| probe.creation.as_ref())
        {
            gate.arrive();
        }
    }
}
pub(super) struct Output {
    pub(super) original: Option<DeltaStream>,
    pub(super) probe: Option<Arc<Probe>>,
}
impl Stream for Output {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.original.as_mut().unwrap().as_mut().poll_next(cx)
    }
}
impl Drop for Output {
    fn drop(&mut self) {
        drop(self.original.take());
        if let Some(probe) = &self.probe {
            *probe.initializer.lock().unwrap() = INITIALIZING.try_with(Clone::clone).ok();
            if let Some(source) = probe.source.upgrade() {
                probe.frames.lock().unwrap().push((
                    source.creator.matches_current(),
                    source.check_close_allowed() == Err(ExtensionError::SelfClose),
                ));
            }
            probe.stream.arrive();
            assert!(
                !probe.stream_panics,
                "actual scripted stream disposal panic"
            );
        }
    }
}

pub(super) struct Installation {
    pub(super) entered: tokio::sync::Notify,
    pub(super) release: tokio::sync::Notify,
    pub(super) state: Mutex<Option<std::sync::Weak<State>>>,
}
impl Installation {
    pub(super) fn new() -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            release: tokio::sync::Notify::new(),
            state: Mutex::new(None),
        })
    }
}
