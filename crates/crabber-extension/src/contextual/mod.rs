//! Owned initialization and contextual instantiation of a frozen registration profile.
mod instances;
mod lifecycle;
mod profile;
use profile::SharedRegistration;

use crate::{
    CleanupOwner, CleanupTracker, ComponentIdentity, Extension, ExtensionError, MountHandle,
    Registry, RunPlan, RunPlanProvider, Scope, WorkspaceContext,
};
use async_trait::async_trait;
use crabber_core::{SessionContext, SessionId};
use std::{
    collections::{BTreeMap, BTreeSet},
    sync::{
        Arc, Mutex,
        atomic::{AtomicBool, Ordering},
    },
    time::Duration,
};
use tokio::sync::{Mutex as AsyncMutex, Notify};

/// The owner of a newly instantiated component, rather than a filter on one Arc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryScope {
    Workspace,
    Session,
}

/// Context and owned cleanup for one factory invocation. No filesystem authority
/// is granted. Workspace factories never receive an arbitrary first session ID.
#[derive(Clone)]
pub struct FactoryContext {
    workspace: WorkspaceContext,
    session_id: Option<SessionId>,
    cleanup: CleanupTracker,
}
impl FactoryContext {
    #[must_use]
    pub fn workspace(&self) -> &WorkspaceContext {
        &self.workspace
    }
    #[must_use]
    pub fn session_id(&self) -> Option<&SessionId> {
        self.session_id.as_ref()
    }
    #[must_use]
    pub fn cleanup(&self) -> &CleanupTracker {
        &self.cleanup
    }
}

/// Creates a fresh extension/tool/service graph per owner. Creation and install
/// configure resources, never execute provider/tool business effects. Declare
/// stable behavior/configuration identities; credentials are not identity data.
/// Use the supplied cleanup tracker for work retained beyond the creation future.
#[async_trait]
pub trait ExtensionFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    /// Pure declaration of every input determining owner registrations, including
    /// schemas and prompts. Change this or the declared version when those change.
    /// Metadata replay does not create instances to discover hidden changes.
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError>;
}

struct FactoryRegistration {
    identity: ComponentIdentity,
    config_hash: String,
    scope: FactoryScope,
    factory: Arc<dyn ExtensionFactory>,
}

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Owner {
    Shared,
    Workspace(String),
    Session(SessionId),
}
tokio::task_local! { static INITIALIZING: Owner; }

struct Record {
    cleanup: Arc<CleanupOwner>,
    mount: Option<MountHandle>,
}
#[derive(Default)]
struct SlotState {
    ready: bool,
    closed: bool,
    records: Vec<Record>,
    initialization: Option<Arc<Initialization>>,
}
#[derive(Default)]
struct Initialization {
    result: Mutex<Option<Result<(), ExtensionError>>>,
    finished: Notify,
}
impl Initialization {
    async fn wait(&self) -> Result<(), ExtensionError> {
        loop {
            let notified = self.finished.notified();
            tokio::pin!(notified);
            notified.as_mut().enable();
            if let Some(result) = self.result.lock().unwrap().clone() {
                return result;
            }
            notified.await;
        }
    }
}
struct Slot {
    owner: Owner,
    context: Option<SessionContext>,
    gate: AsyncMutex<()>,
    state: Mutex<SlotState>,
}
#[derive(Default)]
struct Owners {
    slots: BTreeMap<Owner, Arc<Slot>>,
    roots: BTreeMap<String, String>,
    closed_sessions: BTreeSet<SessionId>,
    closed_workspaces: BTreeSet<String>,
}
struct Inner {
    registry: Registry,
    shared: Vec<SharedRegistration>,
    base_profile: Result<crate::registry::RegistrationSnapshot, ExtensionError>,
    factories: Vec<FactoryRegistration>,
    owners: Mutex<Owners>,
    closed: AtomicBool,
    initializers: CleanupOwner,
    reapers: CleanupOwner,
    initialization_timeout: Duration,
}

/// A frozen profile with real instances and retained initialization/cleanup owners.
/// Unstable until the durable-host consumer contract is qualified.
#[derive(Clone)]
pub struct ContextualPlanProvider {
    inner: Arc<Inner>,
}

impl ContextualPlanProvider {
    /// Freeze shared and factory metadata once. Shared registration failures are
    /// retained for readiness rollback; no component is installed here.
    /// # Errors
    /// Rejects duplicate or invalid factory identities before invoking factories.
    pub fn new(
        registry: Registry,
        shared: Vec<(Arc<dyn Extension>, Scope)>,
        factories: Vec<Arc<dyn ExtensionFactory>>,
        initialization_timeout: Duration,
    ) -> Result<Self, ExtensionError> {
        let mut names = BTreeSet::new();
        let mut registrations = Vec::new();
        for factory in factories {
            let (identity, config_hash, scope) =
                std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
                    (factory.identity(), factory.config_hash(), factory.scope())
                }))
                .map_err(|payload| {
                    crate::dispatch::discard_panic_payload(payload);
                    ExtensionError::ContextInitializationFailed {
                        component: "factory-metadata".into(),
                    }
                })?;
            if identity.id.is_empty()
                || identity.id.len() > 128
                || identity.id.chars().any(char::is_control)
                || identity.version.is_empty()
                || identity.version.len() > 128
                || identity.version.chars().any(char::is_control)
                || !names.insert(identity.id.clone())
            {
                return Err(ExtensionError::Plan(
                    "invalid or duplicate factory identity".into(),
                ));
            }
            if config_hash.len() > 16384 {
                return Err(ExtensionError::Plan(
                    "factory config identity too large".into(),
                ));
            }
            registrations.push(FactoryRegistration {
                identity,
                config_hash,
                scope,
                factory,
            });
        }
        Ok(Self {
            inner: Arc::new(Inner {
                base_profile: registry.registration_snapshot(),
                registry,
                shared: shared
                    .into_iter()
                    .map(|(extension, scope)| SharedRegistration::capture(extension, scope))
                    .collect(),
                factories: registrations,
                owners: Mutex::new(Owners::default()),
                closed: AtomicBool::new(false),
                initializers: CleanupOwner::new(),
                reapers: CleanupOwner::new(),
                initialization_timeout,
            }),
        })
    }

    #[must_use]
    pub fn is_closed(&self) -> bool {
        self.inner.closed.load(Ordering::SeqCst)
    }

    /// Initialize shared registrations. Dropping this waiter never drops setup.
    /// # Errors
    /// Incomplete initialization retains its task and never grants a plan.
    /// # Panics
    /// Panics if a previous internal panic poisoned an ownership lock.
    pub async fn ready(&self) -> Result<(), ExtensionError> {
        if INITIALIZING
            .try_with(|owner| owner == &Owner::Shared)
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        self.wait_initialization(self.ensure_owner(Owner::Shared, None))
            .await
    }

    fn check_open(&self) -> Result<(), ExtensionError> {
        if self.is_closed() || self.inner.registry.is_closed() {
            Err(ExtensionError::RegistryClosed)
        } else {
            Ok(())
        }
    }

    async fn wait_initialization<T>(
        &self,
        future: impl std::future::Future<Output = Result<T, ExtensionError>>,
    ) -> Result<T, ExtensionError> {
        tokio::time::timeout(self.inner.initialization_timeout, future)
            .await
            .unwrap_or(Err(ExtensionError::ContextInitializationIncomplete))
    }
}

#[async_trait]
impl RunPlanProvider for ContextualPlanProvider {
    fn check_preparation_context(&self, context: &SessionContext) -> Result<(), ExtensionError> {
        self.check_open()?;
        self.inner.owners.lock().unwrap().check_context(context)
    }
    fn registration_fingerprint(
        &self,
        context: &SessionContext,
    ) -> Result<crabber_core::InputFingerprint, ExtensionError> {
        let base = self
            .inner
            .base_profile
            .as_ref()
            .map_err(Clone::clone)?
            .for_context(context);
        let declarations =
            profile::declarations(&self.inner.shared, &self.inner.factories, context)?;
        crate::plan::registration_hash(serde_json::json!(["contextual", base, declarations]))
    }

    async fn acquire_plan(&self, _session: &SessionId) -> Result<RunPlan, ExtensionError> {
        Err(ExtensionError::MissingCapability(
            "session execution context",
        ))
    }
    async fn acquire_plan_in(&self, context: &SessionContext) -> Result<RunPlan, ExtensionError> {
        if INITIALIZING
            .try_with(|owner| match owner {
                Owner::Shared => true,
                Owner::Workspace(id) => id == context.workspace_id(),
                Owner::Session(id) => id == context.session_id(),
            })
            .unwrap_or(false)
        {
            return Err(ExtensionError::SelfClose);
        }
        {
            let mut owners = self.inner.owners.lock().unwrap();
            self.check_open()?;
            owners.validate(context)?;
        }
        self.wait_initialization(async {
            self.ensure_owner(Owner::Shared, None).await?;
            if self
                .inner
                .factories
                .iter()
                .any(|f| f.scope == FactoryScope::Workspace)
            {
                if context.workspace_id().is_empty() {
                    return Err(ExtensionError::WorkspaceUnavailable);
                }
                self.ensure_owner(
                    Owner::Workspace(context.workspace_id().into()),
                    Some(context.clone()),
                )
                .await?;
            }
            if self
                .inner
                .factories
                .iter()
                .any(|f| f.scope == FactoryScope::Session)
            {
                self.ensure_owner(
                    Owner::Session(context.session_id().clone()),
                    Some(context.clone()),
                )
                .await?;
            }
            let mut owners = self.inner.owners.lock().unwrap();
            self.check_open()?;
            owners.validate(context)?;
            self.inner.registry.try_acquire_in(context)
        })
        .await
    }
}

impl Owners {
    fn check_context(&self, context: &SessionContext) -> Result<(), ExtensionError> {
        if self.closed_sessions.contains(context.session_id())
            || self.closed_workspaces.contains(context.workspace_id())
        {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if !context.workspace_id().is_empty()
            && self
                .roots
                .get(context.workspace_id())
                .is_some_and(|root| root != context.directory())
        {
            return Err(ExtensionError::ContextIdentityMismatch);
        }
        Ok(())
    }
    fn validate(&mut self, context: &SessionContext) -> Result<(), ExtensionError> {
        if self.closed_sessions.contains(context.session_id())
            || self.closed_workspaces.contains(context.workspace_id())
        {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        if !context.workspace_id().is_empty() {
            if self
                .roots
                .get(context.workspace_id())
                .is_some_and(|root| root != context.directory())
            {
                return Err(ExtensionError::ContextIdentityMismatch);
            }
            self.roots
                .entry(context.workspace_id().into())
                .or_insert_with(|| context.directory().into());
        }
        Ok(())
    }
}
