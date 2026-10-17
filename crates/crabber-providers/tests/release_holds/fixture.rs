use crabber_providers::{
    BoundProvider, DeltaStream, ModelRequest, ProviderError, ProviderReleaseHolds, Resolver,
    Selection, Streamer,
};
use std::{
    pin::Pin,
    sync::{
        Arc, Condvar, Mutex,
        atomic::{AtomicBool, AtomicUsize, Ordering},
    },
    task::{Context, Poll},
};
pub struct Seen {
    pub entered: tokio::sync::Notify,
    pub returned: tokio::sync::Notify,
    pub observe: tokio::sync::Semaphore,
    pub over_limit: AtomicBool,
    open: Mutex<bool>,
    changed: Condvar,
    panic: bool,
    tasks: Mutex<Vec<tokio::task::JoinHandle<()>>>,
}
impl Seen {
    pub fn new(panic: bool) -> Arc<Self> {
        Arc::new(Self {
            entered: tokio::sync::Notify::new(),
            returned: tokio::sync::Notify::new(),
            observe: tokio::sync::Semaphore::new(0),
            over_limit: AtomicBool::new(false),
            open: Mutex::new(false),
            changed: Condvar::new(),
            panic,
            tasks: Mutex::new(vec![]),
        })
    }
    pub fn release(&self) {
        *self.open.lock().unwrap() = true;
        self.changed.notify_all();
    }
    pub async fn finish(&self) {
        let tasks = std::mem::take(&mut *self.tasks.lock().unwrap());
        for task in tasks {
            task.await.unwrap();
        }
    }
}
pub struct Original(pub Arc<Seen>);
impl Drop for Original {
    fn drop(&mut self) {
        self.0.entered.notify_one();
        let mut open = self.0.open.lock().unwrap();
        while !*open {
            open = self.0.changed.wait(open).unwrap();
        }
        drop(open);
        self.0.returned.notify_one();
        assert!(!self.0.panic, "actual original disposal failed");
    }
}
impl futures::Stream for Original {
    type Item = crabber_providers::StreamDelta;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
pub fn retain_original<T: Send + 'static>(
    seen: &Arc<Seen>,
    original: T,
    creation: ProviderReleaseHolds,
) {
    let holds = ProviderReleaseHolds::capture_current();
    seen.over_limit
        .store(holds.check_admission_limit(64).is_err(), Ordering::SeqCst);
    let (release, ready) = std::sync::mpsc::channel();
    let actual = tokio::task::spawn_blocking(move || {
        ready.recv().unwrap();
        drop(original);
    });
    let observing = seen.clone();
    let observer = tokio::spawn(async move {
        // The original handle is retained BEFORE effect; no is_finished inference.
        let _permit = observing.observe.acquire().await.unwrap();
        if actual.await.is_ok() {
            drop(creation);
            drop(holds);
        } else {
            std::mem::forget(creation);
            std::mem::forget(holds);
        }
    });
    seen.tasks.lock().unwrap().push(observer);
    release.send(()).unwrap();
}
pub struct Backend {
    pub seen: Arc<Seen>,
    pub original: Option<Original>,
}
#[async_trait::async_trait]
impl Streamer for Backend {
    async fn stream(&self, _: ModelRequest) -> Result<DeltaStream, ProviderError> {
        Ok(Box::pin(Facade {
            original: Some(Original(self.seen.clone())),
            seen: self.seen.clone(),
            creation: ProviderReleaseHolds::capture_current(),
        }))
    }
}
impl Drop for Backend {
    fn drop(&mut self) {
        if let Some(original) = self.original.take() {
            retain_original(&self.seen, original, ProviderReleaseHolds::default());
        }
    }
}
struct Facade {
    original: Option<Original>,
    seen: Arc<Seen>,
    creation: ProviderReleaseHolds,
}
impl futures::Stream for Facade {
    type Item = crabber_providers::StreamDelta;
    fn poll_next(self: Pin<&mut Self>, _: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Pending
    }
}
impl Drop for Facade {
    fn drop(&mut self) {
        retain_original(
            &self.seen,
            self.original.take().unwrap(),
            std::mem::take(&mut self.creation),
        );
    }
}
pub struct Lease(pub Arc<AtomicUsize>);
impl Drop for Lease {
    fn drop(&mut self) {
        self.0.fetch_add(1, Ordering::SeqCst);
    }
}
pub fn binding(streamer: Arc<dyn Streamer>) -> BoundProvider {
    let selection = Selection {
        provider_id: "fake".into(),
        model_id: "model".into(),
    };
    let descriptor = crabber_providers::FakeProvider::scripted(vec![])
        .binding_descriptor(&selection)
        .unwrap();
    BoundProvider::new(&selection, descriptor, streamer).unwrap()
}
pub fn request() -> ModelRequest {
    ModelRequest {
        context: vec![],
        identity: crabber_providers::RequestIdentity {
            session_id: crabber_core::SessionId::new(),
            run_id: crabber_core::RunId::new(),
            turn_id: crabber_core::TurnId::new(),
        },
        selection: Selection {
            provider_id: "fake".into(),
            model_id: "model".into(),
        },
        system: None,
        messages: vec![],
        tools: vec![],
        temperature: None,
        max_tokens: None,
        tool_choice: None,
    }
}
