//! Owned initialization and contextual instantiation of a frozen registration profile.
mod acquisition;
mod cleanup;
mod dependencies;
mod generation;
mod initialization;
mod instances;
mod lifecycle;
mod native_dependency;
mod producer;
pub(crate) mod product;
pub use product::{
    ContextualProductFactory, ContextualProductRoot, ContextualProductRootObservation,
    DisposedContextualProductRoot,
};
mod profile;
mod provider_service;
pub use producer::{
    ContextualGenerationAdmission, ContextualGenerationHost, ContextualGenerationMember,
    ContextualGenerationMemberDisposition, ContextualGenerationObservation,
    ContextualGenerationWork, JoinedContextualGenerationGraph,
};
pub use provider_service::{ContextualServiceBorrowLease, ContextualServiceBorrowSource};
mod selection;
pub use generation::{
    ServiceGenerationCleanupPolicy, ServiceGenerationDescriptor, ServiceGenerationId,
    ServiceGenerationScope,
};
use profile::SharedRegistration;

use crate::{
    CleanupOwner, CleanupTracker, ComponentIdentity, Extension, ExtensionError, Registry, RunPlan,
    RunPlanProvider, Scope, WorkspaceContext,
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
use tokio::sync::Mutex as AsyncMutex;

/// The owner of a newly instantiated component, rather than a filter on one Arc.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum FactoryScope {
    Workspace,
    Session,
}

/// Context and owned cleanup for one factory invocation. No filesystem authority
/// is granted. Workspace factories never receive an arbitrary first session ID.
/// Cleanup registration is bounded by [`crate::MAX_TOOL_RESOURCE_TASKS`] and
/// sealed when this owner closes. Retained trackers cannot start work afterward.
/// Use [`CleanupTracker::spawn_checked`] to report cleanup failures; ordinary
/// task return values remain data. Close retains actual joins after a timeout.
#[derive(Clone)]
pub struct FactoryContext {
    workspace: WorkspaceContext,
    session_id: Option<SessionId>,
    cleanup: CleanupTracker,
    generation: ServiceGenerationDescriptor,
    provider_source: ContextualServiceBorrowSource,
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
    /// Borrow-only capability for this actual factory member. It pins no lease
    /// and supplies no physical shutdown or global-construction certificate.
    #[must_use]
    pub fn provider_service_source(&self) -> Arc<dyn crabber_providers::ProviderServiceSource> {
        Arc::new(self.provider_source.clone())
    }
    /// Local generation metadata, without dispatch or physical cleanup authority.
    #[must_use]
    pub fn generation(&self) -> &ServiceGenerationDescriptor {
        &self.generation
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

enum FactoryBody {
    Extension(Arc<dyn ExtensionFactory>),
    ProductRoot(Arc<dyn ContextualProductFactory>),
    ToolProduct(Arc<dyn product::tool::ContextualToolProductFactory>),
    ToolGuardProduct(Arc<dyn product::tool_guard::ContextualToolGuardProductFactory>),
    PromptProduct(Arc<dyn product::prompt::ContextualPromptProductFactory>),
    WorkspaceFsProduct(Arc<dyn product::workspace_fs::ContextualWorkspaceFsProductFactory>),
    SubprocessProduct(Arc<dyn product::subprocess::ContextualSubprocessProductFactory>),
    WorkspaceReaderResolverProduct(
        Arc<dyn product::workspace_reader::ContextualWorkspaceReaderResolverProductFactory>,
    ),
    ProviderProduct(Arc<dyn product::provider::ContextualProviderProductFactory>),
}
struct FactoryRegistration {
    root_component: Option<ComponentIdentity>,
    identity: ComponentIdentity,
    config_hash: String,
    scope: FactoryScope,
    factory: FactoryBody,
}

const FACTORY_RESOURCE_CONTRACT: &str = "crabber.contextual-factory.v3";
const FACTORY_MOUNT_CONTRACT: &str = "crabber.registry.mount.sealed.v1";

#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord)]
enum Owner {
    Shared,
    Workspace(String),
    Session(SessionId),
    LibraryScripted(u64),
}
tokio::task_local! { static INITIALIZING: Owner; }
/// Propagate only an actual existing initializer identity into retained owned
/// callback tasks. This is a local reentry guard, never native authority.
pub(crate) struct InitializationContext(Option<Owner>);
impl InitializationContext {
    pub(crate) fn capture() -> Self {
        Self(INITIALIZING.try_with(Clone::clone).ok())
    }
    pub(crate) async fn scope<T>(self, future: impl std::future::Future<Output = T>) -> T {
        match self.0 {
            Some(owner) => INITIALIZING.scope(owner, future).await,
            None => future.await,
        }
    }
}

struct Record {
    cleanup: Arc<cleanup::RecordCleanup>,
    mount: Option<cleanup::RecordMount>,
    failed_cleanup: bool,
}
#[derive(Default)]
struct SlotState {
    ready: bool,
    closed: bool,
    records: Vec<Record>,
    initialization: Option<Arc<Initialization>>,
    generation: Option<ServiceGenerationDescriptor>,
    producer: Option<Arc<producer::Registration>>,
}
#[derive(Default)]
struct Initialization {
    #[cfg(test)]
    tail: Mutex<Option<tokio::sync::oneshot::Receiver<()>>>,
    result: Mutex<Option<Result<(), ExtensionError>>>,
    task: AsyncMutex<initialization::Task>,
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
    dependencies: dependencies::Dependencies,
    generation_host: Option<Arc<dyn ContextualGenerationHost>>,
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
                factory: FactoryBody::Extension(factory),
                root_component: None,
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
                dependencies: dependencies::Dependencies::default(),
                generation_host: None,
            }),
        })
    }

    /// Opt into local scheduling of actual Workspace/Session constructors.
    /// The host is fixed before readiness/acquisition; Shared inputs retain
    /// their legacy lifecycle. This grants no native permission or coverage.
    /// # Errors
    /// Returns the same metadata/profile validation errors as `new`.
    pub fn new_with_generation_host(
        registry: Registry,
        shared: Vec<(Arc<dyn Extension>, Scope)>,
        factories: Vec<Arc<dyn ExtensionFactory>>,
        initialization_timeout: Duration,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<Self, ExtensionError> {
        let mut provider = Self::new(registry, shared, factories, initialization_timeout)?;
        Arc::get_mut(&mut provider.inner)
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .generation_host = Some(host);
        Ok(provider)
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
        self.inner.owners.lock().unwrap().check_context(context)?;
        self.inner.dependencies.check_context(context)
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
        let value = serde_json::json!(["contextual", base, declarations]);
        crate::plan::registration_hash(if self.inner.generation_host.is_some() {
            serde_json::json!(["contextual.local-generation-host.v1", value])
        } else {
            value
        })
    }

    async fn acquire_plan(&self, _session: &SessionId) -> Result<RunPlan, ExtensionError> {
        Err(ExtensionError::MissingCapability(
            "session execution context",
        ))
    }
    async fn acquire_plan_in(&self, context: &SessionContext) -> Result<RunPlan, ExtensionError> {
        self.acquire_context(context, None).await
    }

    async fn acquire_plan_in_preparation(
        &self,
        context: &crate::PreparationResourceContext,
    ) -> Result<RunPlan, ExtensionError> {
        let binding = native_dependency::Binding {
            context: context.session().clone(),
            services: context.service_dependencies().clone(),
        };
        context
            .acquire_plan(|| self.acquire_context(context.session(), Some(binding)))
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
