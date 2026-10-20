//! Concrete value-only source. Local use and disposal confer no native authority.
mod lifetime;
use super::super::{INITIALIZING, Owner};
use super::{
    provider::{stream, value::Owned},
    reference::Node,
};
use crate::{CallbackInvocationCapability, ExtensionError, OwnedMountHandle};
use crabber_core::SessionContext;
use crabber_providers::{DeltaStream, ScriptedError, ScriptedProvider, Selection, StreamDelta};
use futures::Stream;
use std::{
    pin::Pin,
    sync::{
        Arc, Mutex,
        atomic::{AtomicU64, Ordering},
    },
    task::{Context, Poll},
    time::Duration,
};
use tokio_util::sync::CancellationToken;

static NEXT: AtomicU64 = AtomicU64::new(0);

/// Finite leaf errors remain distinct from failure to observe actual cleanup.
#[derive(Debug, thiserror::Error)]
pub enum LibraryScriptedError {
    #[error(transparent)]
    Script(#[from] ScriptedError),
    #[error(transparent)]
    Lifecycle(#[from] ExtensionError),
}
/// Pristine owned recipe, with no effect methods or conversion from used objects.
/// Future Session intake must consume this type, never an active carrier.
pub struct LibraryScriptedSource {
    provider: ScriptedProvider,
    #[cfg(test)]
    installation: Option<Arc<probe::Installation>>,
}
impl LibraryScriptedSource {
    /// Internally construct a fresh provider from bounded owned values.
    /// # Errors
    /// Rejects the concrete provider's route and script limits.
    pub fn new(selection: Selection, responses: Vec<String>) -> Result<Self, ScriptedError> {
        Ok(Self {
            provider: ScriptedProvider::new(selection, responses)?,
            #[cfg(test)]
            installation: None,
        })
    }
    /// Borrow original values only; this provides no authentication or permission.
    #[must_use]
    pub fn recipe(&self) -> (&Selection, &[String]) {
        self.provider.recipe()
    }
    /// Consume the pristine carrier under an actual caller resource owner.
    /// # Errors
    /// Refuses missing/sealed ownership or failed local installation.
    pub async fn activate(
        self,
        session: SessionContext,
    ) -> Result<LibraryScriptedActive, ExtensionError> {
        lifetime::activate(
            self.provider,
            session,
            #[cfg(test)]
            self.installation,
        )
        .await
    }
}
/// One active concrete source. Drop seals it; references here pin no provider lease.
pub struct LibraryScriptedActive {
    state: Arc<State>,
}
impl LibraryScriptedActive {
    /// Create a concrete stream within the same real caller owner.
    /// # Errors
    /// Preserves routing/exhaustion errors and refuses closed/foreign callers.
    pub async fn stream(
        &self,
        selection: Selection,
    ) -> Result<LibraryScriptedStream, LibraryScriptedError> {
        self.state.check_current()?;
        let source = self.state.invocation_source()?;
        let created = stream::open(
            self.state.owner.clone(),
            &source,
            &self.state.streams,
            stream::Input::Scripted {
                source: self.state.clone(),
                selection,
            },
        )
        .await;
        let stream = created.map_err(|error| match error {
            stream::CreationError::Scripted(error) => error,
            stream::CreationError::Provider(_) => {
                LibraryScriptedError::Lifecycle(ExtensionError::ContextCleanupIncomplete)
            }
        })?;
        self.state.check_current()?;
        Ok(LibraryScriptedStream {
            stream,
            state: self.state.clone(),
            terminal: false,
        })
    }
    /// Seal and start the original retained close leader without waiting.
    /// # Errors
    /// Refuses closing from this source's own active original callback/destructor.
    pub fn request_close(&self) -> Result<(), ExtensionError> {
        self.state.check_close_allowed()?;
        self.state.signal_close();
        Ok(())
    }
    /// Observe the same original source after dropping its active carrier.
    #[must_use]
    pub fn observation(&self) -> LibraryScriptedSourceObservation {
        LibraryScriptedSourceObservation {
            state: self.state.clone(),
        }
    }
}
impl Drop for LibraryScriptedActive {
    fn drop(&mut self) {
        self.state.signal_close();
    }
}
/// Opaque concrete stream; exhaustion/Completed is not a source close observation.
pub struct LibraryScriptedStream {
    stream: DeltaStream,
    state: Arc<State>,
    terminal: bool,
}
impl Stream for LibraryScriptedStream {
    type Item = StreamDelta;
    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.terminal {
            return Poll::Ready(None);
        }
        if self.state.check_current().is_err() {
            self.terminal = true;
            return Poll::Ready(Some(closed_delta()));
        }
        let output = self.stream.as_mut().poll_next(cx);
        if output.is_ready() && self.state.check_current().is_err() {
            self.terminal = true;
            return Poll::Ready(Some(closed_delta()));
        }
        if matches!(output, Poll::Ready(None)) {
            self.terminal = true;
        }
        output
    }
}
/// Matching original source observation, without a public success constructor.
#[derive(Clone)]
pub struct LibraryScriptedSourceObservation {
    state: Arc<State>,
}
impl LibraryScriptedSourceObservation {
    /// Start close and await actual original root, mount and stream joins.
    /// A timeout retains the same handles and grants no native/policy evidence.
    /// # Errors
    /// Refuses self-close, timeout, or sticky original disposal failure.
    pub async fn close(&self, bound: Duration) -> Result<(), ExtensionError> {
        self.state.check_close_allowed()?;
        self.state.signal_close();
        tokio::time::timeout(bound, self.state.join())
            .await
            .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}

pub(crate) struct State {
    owner: Owner,
    root: Arc<Node<Mutex<ScriptedProvider>>>,
    mount: Mutex<Option<OwnedMountHandle>>,
    streams: Mutex<Vec<Arc<Owned<stream::OriginalStream>>>>,
    admission: Mutex<bool>,
    closing: CancellationToken,
    installed: CancellationToken,
    creator: CallbackInvocationCapability,
    runtime: tokio::runtime::Handle,
    failed: std::sync::atomic::AtomicBool,
    #[cfg(test)]
    probe: Mutex<Option<Arc<probe::Probe>>>,
}
impl State {
    fn invocation_source(&self) -> Result<crate::registry::InvocationSource, ExtensionError> {
        let mounted = self
            .mount
            .lock()
            .unwrap()
            .clone()
            .ok_or(ExtensionError::ContextOwnerClosed)?;
        Ok(mounted.scripted_source()?.0)
    }
    pub(crate) fn creation(
        self: Arc<Self>,
        selection: Selection,
    ) -> futures::future::BoxFuture<'static, Result<DeltaStream, stream::CreationError>> {
        #[cfg(test)]
        let probe = self.probe.lock().unwrap().clone();
        let original = Box::pin(async move { self.create_stream(&selection) });
        #[cfg(test)]
        return Box::pin(probe::Creation {
            original: Some(original),
            probe,
        });
        #[cfg(not(test))]
        original
    }
    fn check_current(&self) -> Result<(), ExtensionError> {
        if !self.creator.matches_current() || !*self.admission.lock().unwrap() {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        Ok(())
    }
    pub(crate) fn child_admission(
        &self,
    ) -> Result<std::sync::MutexGuard<'_, bool>, ExtensionError> {
        let gate = self.admission.lock().unwrap();
        if !*gate || !self.creator.matches_current() {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        Ok(gate)
    }
    pub(crate) fn create_stream(
        &self,
        selection: &Selection,
    ) -> Result<DeltaStream, stream::CreationError> {
        let _gate = self
            .child_admission()
            .map_err(|e| stream::CreationError::Scripted(e.into()))?;
        let original = self
            .root
            .borrow()
            .map_err(|e| stream::CreationError::Scripted(e.into()))?;
        let created = original
            .value()
            .lock()
            .unwrap()
            .stream(selection)
            .map_err(|e| stream::CreationError::Scripted(e.into()))?;
        let original: DeltaStream = Box::pin(created);
        #[cfg(test)]
        return Ok(Box::pin(probe::Output {
            original: Some(original),
            probe: self.probe.lock().unwrap().clone(),
        }));
        #[cfg(not(test))]
        Ok(original)
    }
    fn check_close_allowed(&self) -> Result<(), ExtensionError> {
        if INITIALIZING
            .try_with(|owner| owner == &self.owner)
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        if let Some(mount) = self.mount.lock().unwrap().as_ref() {
            mount.check_close_allowed()?;
        }
        Ok(())
    }
    pub(crate) async fn closing(&self) {
        self.closing.cancelled().await;
    }
    pub(crate) fn signal_close(&self) {
        let children = {
            let mut admitted = self.admission.lock().unwrap();
            *admitted = false;
            self.streams.lock().unwrap().clone()
        };
        for child in children {
            child.close();
        }
        self.root.close();
        let mount = self.mount.lock().unwrap().clone();
        if let Some(mount) = mount
            && mount.start_scripted_close(&self.runtime).is_err()
        {
            self.failed.store(true, Ordering::SeqCst);
        }
        self.closing.cancel();
    }
    pub(crate) async fn join(&self) -> Result<(), ExtensionError> {
        // Installation may still be disposing its original future. Only that
        // original guard can close the final mount/child registration set.
        self.installed.cancelled().await;
        let mount = self.mount.lock().unwrap().clone();
        let streams = self.streams.lock().unwrap().clone();
        let mounted = async {
            if let Some(mount) = mount {
                mount.close_registered().await?;
            }
            Ok::<_, ExtensionError>(())
        };
        let (root, mount, children) = tokio::join!(
            self.root.disposed(),
            mounted,
            futures::future::join_all(streams.iter().map(|stream| stream.join()))
        );
        root?;
        mount?;
        for child in children {
            child?;
        }
        if self.failed.load(Ordering::SeqCst) {
            return Err(ExtensionError::ContextCleanupIncomplete);
        }
        Ok(())
    }
}

fn closed_delta() -> StreamDelta {
    StreamDelta::Error(crabber_providers::ProviderError {
        kind: crabber_providers::ProviderErrorKind::Canceled,
        message: "scripted source caller or source closed".into(),
        retryable: false,
    })
}
#[cfg(test)]
mod tests;

#[cfg(test)]
mod probe;
