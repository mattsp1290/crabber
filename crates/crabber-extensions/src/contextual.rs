//! Shared owner binding and admission primitives for contextual factories.
use crabber::{
    core::SessionId,
    extension::{ExtensionError, FactoryContext, ToolContext},
};
use std::{
    collections::BTreeMap,
    sync::{Arc, Mutex, Weak},
};
use tokio::sync::{OwnedSemaphorePermit, Semaphore};

/// An optional nonqueueing live-resource budget shared by factory instances.
/// Clone a pool to share it; constructing another pool creates another budget.
#[derive(Clone)]
pub struct SharedCapacity {
    limit: usize,
    permits: Arc<Semaphore>,
}
impl SharedCapacity {
    /// Reject zero and limits beyond Tokio's representable semaphore capacity.
    pub fn new(limit: usize) -> Result<Self, ExtensionError> {
        if limit == 0 || limit > Semaphore::MAX_PERMITS {
            return Err(crate::config_error("shared-capacity"));
        }
        Ok(Self {
            limit,
            permits: Arc::new(Semaphore::new(limit)),
        })
    }
    /// Configured aggregate budget, included in the factory fingerprint.
    pub fn limit(&self) -> usize {
        self.limit
    }
    pub(crate) fn acquire(&self) -> Result<OwnedSemaphorePermit, ()> {
        self.permits.clone().try_acquire_owned().map_err(|_| ())
    }
}

/// Metadata-only snapshot. A terminal result or accepted cancellation does not
/// prove that live resources have exited. Fields can change immediately.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
#[non_exhaustive]
pub struct OwnerReport {
    pub live: usize,
    pub tracked: usize,
    pub closing: bool,
    pub closed: bool,
    pub unresolved_invocations: usize,
    pub publication_failures: u64,
}

#[derive(Clone, PartialEq, Eq, PartialOrd, Ord)]
pub(crate) struct Binding {
    workspace: String,
    session: Option<SessionId>,
}
impl Binding {
    pub(crate) fn from_factory(context: &FactoryContext) -> Result<Self, ExtensionError> {
        Ok(Self {
            workspace: context
                .workspace()
                .workspace_id()
                .ok_or_else(|| crate::config_error("owner"))?
                .into(),
            session: context.session_id().cloned(),
        })
    }
    pub(crate) fn matches(&self, context: &ToolContext) -> bool {
        context.workspace().workspace_id() == Some(self.workspace.as_str())
            && self
                .session
                .as_ref()
                .is_none_or(|session| *session == context.session_id)
    }
}

pub(crate) struct Instances<P>(Mutex<BTreeMap<Binding, Weak<P>>>);
impl<P> Default for Instances<P> {
    fn default() -> Self {
        Self(Mutex::new(BTreeMap::new()))
    }
}
impl<P> Instances<P> {
    pub(crate) fn insert(&self, binding: Binding, policy: &Arc<P>) {
        let mut instances = self.0.lock().unwrap();
        instances.retain(|_, policy| policy.strong_count() != 0);
        instances.insert(binding, Arc::downgrade(policy));
    }
    pub(crate) fn get(&self, workspace: &str, session: Option<&SessionId>) -> Option<Arc<P>> {
        self.0
            .lock()
            .unwrap()
            .get(&Binding {
                workspace: workspace.into(),
                session: session.cloned(),
            })
            .and_then(Weak::upgrade)
    }
}

/// Retain owner cleanup until its bounded closes actually converge. The host's
/// observation deadline can expire while this owner still holds its resources.
pub(crate) fn retain_close<E: crabber::extension::Extension + 'static>(
    context: &FactoryContext,
    instance: Arc<E>,
    report: fn(&E) -> OwnerReport,
) -> Result<(), ExtensionError> {
    let closing = context.cleanup().closing();
    context
        .cleanup()
        .spawn_checked(async move {
            closing.await;
            loop {
                instance.shutdown().await;
                let state = report(&instance);
                if state.closed && state.live == 0 {
                    return Ok(());
                }
                tokio::time::sleep(std::time::Duration::from_millis(10)).await;
            }
        })
        .map_err(|_| crate::config_error("owner-cleanup"))
}

pub(crate) fn scope_name(scope: crabber::extension::FactoryScope) -> &'static str {
    match scope {
        crabber::extension::FactoryScope::Session => "session",
        crabber::extension::FactoryScope::Workspace => "workspace",
    }
}
