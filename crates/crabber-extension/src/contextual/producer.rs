//! Opt-in local scheduling of the actual constructor, never native authority.
use super::{ContextualPlanProvider, FactoryScope, Owner, ServiceGenerationDescriptor, Slot};
use crate::{ComponentIdentity, ExtensionError};
use crabber_core::InputFingerprint;
use futures::future::BoxFuture;
use std::sync::{Arc, Mutex, Weak};
mod graph;
pub use graph::{
    ContextualGenerationMemberDisposition, ContextualGenerationObservation,
    JoinedContextualGenerationGraph,
};
use tokio::{sync::Mutex as AsyncMutex, sync::oneshot, task::JoinHandle};

const MAX_MEMBERS: usize = 64;

/// Frozen identity of one constructor in this exact selected generation.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ContextualGenerationMember {
    identity: ComponentIdentity,
    configuration: InputFingerprint,
}
impl ContextualGenerationMember {
    #[must_use]
    pub fn identity(&self) -> &ComponentIdentity {
        &self.identity
    }
    #[must_use]
    pub fn configuration(&self) -> &InputFingerprint {
        &self.configuration
    }
}
struct Source {
    generation: ServiceGenerationDescriptor,
    members: Vec<ContextualGenerationMember>,
    closing: tokio_util::sync::CancellationToken,
    registration: Mutex<Weak<Registration>>,
}

/// Move-only local scheduling carrier. Only the producer can construct it. Its
/// brand refers to the original slot-owned parked constructor; dropping this
/// carrier cannot drop that constructor or its actual join. No native grant,
/// dispatch method, physical-close proof, or reconstruction API is exposed.
pub struct ContextualGenerationWork {
    source: Arc<Source>,
}
impl ContextualGenerationWork {
    /// Retain the original registered graph independently of this scheduling carrier.
    /// This supplies no native authority or physical-object disposal proof.
    /// # Errors
    /// Returns `ContextInitializationIncomplete` if this original registration
    /// has expired. A replacement slot is never consulted.
    /// # Panics
    /// Panics if an internal registration lock is poisoned.
    pub fn observation(&self) -> Result<ContextualGenerationObservation, ExtensionError> {
        let registration = self
            .source
            .registration
            .lock()
            .unwrap()
            .upgrade()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?;
        Ok(ContextualGenerationObservation { registration })
    }

    #[must_use]
    pub fn generation(&self) -> &ServiceGenerationDescriptor {
        &self.source.generation
    }
    #[must_use]
    pub fn members(&self) -> &[ContextualGenerationMember] {
        &self.source.members
    }
    /// Observe local owner close; this is not a native cancellation receipt.
    pub fn closing(&self) -> impl std::future::Future<Output = ()> + Send + 'static {
        self.source.closing.clone().cancelled_owned()
    }
}
impl std::fmt::Debug for ContextualGenerationWork {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ContextualGenerationWork")
            .field("generation", &self.source.generation)
            .field("members", &self.source.members)
            .finish_non_exhaustive()
    }
}

/// A local scheduling decision only. Even Admitted carries no native permission
/// or physical cleanup certificate; return the exact input carrier unchanged.
#[derive(Debug)]
pub enum ContextualGenerationAdmission {
    Admitted(ContextualGenerationWork),
    Refused,
    Replayed,
    Uncertain,
}

/// Installed immutably before any generation initializes. Calls are retained
/// independently of acquisition waiters. The future's construction, polling,
/// disposal and returned carrier are observed before local constructor dispatch.
/// A future native adapter remains unsupported until its source protocol exists.
pub trait ContextualGenerationHost: Send + Sync {
    fn register(
        &self,
        work: ContextualGenerationWork,
    ) -> BoxFuture<'static, ContextualGenerationAdmission>;
}

#[derive(Clone, Copy)]
enum Decision {
    Admitted,
    Refused,
    Uncertain,
}
struct Main {
    handle: Option<JoinHandle<Option<Result<(), ExtensionError>>>>,
    observed: Option<Result<Option<Result<(), ExtensionError>>, ExtensionError>>,
}
/// Both actual handles remain in the slot, outside the untrusted host future.
pub(super) struct Registration {
    source: Arc<Source>,
    graph: graph::Graph,
    gate: Mutex<Option<oneshot::Sender<()>>>,
    host: AsyncMutex<Option<JoinHandle<ContextualGenerationAdmission>>>,
    decision: Mutex<Option<Decision>>,
    main: AsyncMutex<Main>,
}
impl Registration {
    pub(super) fn new(
        provider: ContextualPlanProvider,
        slot: Arc<Slot>,
        generation: ServiceGenerationDescriptor,
        host: Arc<dyn ContextualGenerationHost>,
    ) -> Result<(Arc<Self>, oneshot::Sender<()>), ExtensionError> {
        let scope = match slot.owner {
            Owner::Workspace(_) => FactoryScope::Workspace,
            Owner::Session(_) => FactoryScope::Session,
            Owner::Shared => return Err(ExtensionError::PreparationResourcesUnsupported),
        };
        let selected = || provider.inner.factories.iter().filter(|f| f.scope == scope);
        if selected().take(MAX_MEMBERS + 1).count() > MAX_MEMBERS {
            return Err(ExtensionError::Plan(
                "too many generation constructors".into(),
            ));
        }
        let members = selected()
            .map(|member| {
                Ok(ContextualGenerationMember {
                    identity: member.identity.clone(),
                    configuration: crate::plan::registration_hash(serde_json::json!([
                        "crabber.contextual-generation-member.v1",
                        member.identity,
                        crate::plan::registration_config(&member.config_hash),
                    ]))?,
                })
            })
            .collect::<Result<Vec<_>, ExtensionError>>()?;
        let graph = graph::Graph::new(&slot, members.len())?;
        let source = Arc::new(Source {
            generation,
            members,
            closing: tokio_util::sync::CancellationToken::default(),
            registration: Mutex::new(Weak::new()),
        });
        let (gate, ready) = oneshot::channel();
        let original = source.clone();
        let owner = slot.owner.clone();
        let main = tokio::spawn(super::INITIALIZING.scope(owner.clone(), async move {
            ready.await.ok()?;
            // Recheck in the actual gated body, after the scheduling handoff.
            if let Err(error) = provider.check_slot(&slot) {
                return Some(Err(error));
            }
            if slot.state.lock().unwrap().generation.as_ref() != Some(&original.generation) {
                return Some(Err(ExtensionError::ContextIdentityMismatch));
            }
            Some(provider.construct_slot(&slot).await)
        }));
        let registration = Arc::new(Self {
            source: source.clone(),
            graph,
            gate: Mutex::new(Some(gate)),
            host: AsyncMutex::new(None),
            decision: Mutex::new(None),
            main: AsyncMutex::new(Main {
                handle: Some(main),
                observed: None,
            }),
        });
        *source.registration.lock().unwrap() = Arc::downgrade(&registration);
        // A separate release gate prevents even future construction before both
        // actual handles are retained in their original registration.
        let (release, retained) = oneshot::channel();
        let call = tokio::spawn(super::INITIALIZING.scope(owner, async move {
            if retained.await.is_err() {
                return ContextualGenerationAdmission::Refused;
            }
            let mut future = host.register(ContextualGenerationWork { source });
            let outcome = future.as_mut().await;
            drop(future);
            outcome
        }));
        *registration.host.try_lock().expect("unpublished host call") = Some(call);
        // The caller publishes Registration under the slot lock before release.
        Ok((registration, release))
    }
    pub(super) fn record_started(&self, index: usize, cleanup: Arc<super::cleanup::RecordCleanup>) {
        self.graph.entered(index, cleanup);
        if self.source.closing.is_cancelled() {
            self.graph.signal();
        }
    }
    pub(super) fn record_mounted(&self, index: usize, mount: super::cleanup::RecordMount) {
        self.graph.mounted(index, mount);
        if self.source.closing.is_cancelled() {
            self.graph.signal();
        }
    }
    pub(super) fn record_failed(&self, index: usize) {
        self.graph.failed(index);
    }
    pub(super) async fn clean_members(&self) -> Result<(), ExtensionError> {
        self.graph.clean().await
    }
    pub(super) fn close(&self) {
        self.source.closing.cancel();
        drop(self.gate.lock().unwrap().take());
    }
    pub(super) fn blocked(&self) -> bool {
        matches!(
            *self.decision.lock().unwrap(),
            Some(Decision::Refused | Decision::Uncertain)
        )
    }
    pub(super) async fn run(
        &self,
        provider: &ContextualPlanProvider,
        slot: &Slot,
    ) -> Result<(), ExtensionError> {
        let mut host = self.host.lock().await;
        if self.decision.lock().unwrap().is_none() {
            let result = host
                .as_mut()
                .ok_or(ExtensionError::ContextInitializationIncomplete)?
                .await;
            let decision = match result {
                Ok(ContextualGenerationAdmission::Admitted(work))
                    if Arc::ptr_eq(&work.source, &self.source) =>
                {
                    Decision::Admitted
                }
                Ok(ContextualGenerationAdmission::Refused) => Decision::Refused,
                Ok(_) => Decision::Uncertain,
                Err(error) => {
                    if error.is_panic() {
                        crate::dispatch::discard_panic_payload(error.into_panic());
                    }
                    Decision::Uncertain
                }
            };
            *self.decision.lock().unwrap() = Some(decision);
            *host = None;
        }
        drop(host);
        let admitted = matches!(*self.decision.lock().unwrap(), Some(Decision::Admitted));
        if admitted && provider.check_slot(slot).is_ok() {
            if let Some(gate) = self.gate.lock().unwrap().take() {
                let _ = gate.send(());
            }
        } else {
            self.close();
        }
        let result = self.join_main().await;
        if !admitted {
            return Err(ExtensionError::ContextInitializationIncomplete);
        }
        provider.check_slot(slot)?;
        result?.ok_or(ExtensionError::ContextInitializationIncomplete)?
    }
    pub(super) async fn cleanup(&self) -> Result<(), ExtensionError> {
        self.close();
        // A returned constructor error is not an abnormal physical join.
        let _ = self.join_main().await?;
        if matches!(*self.decision.lock().unwrap(), Some(Decision::Uncertain)) {
            return Err(ExtensionError::ContextCleanupIncomplete);
        }
        Ok(())
    }
    async fn join_main(&self) -> Result<Option<Result<(), ExtensionError>>, ExtensionError> {
        let mut main = self.main.lock().await;
        if let Some(result) = &main.observed {
            return result.clone();
        }
        let result = match main
            .handle
            .as_mut()
            .ok_or(ExtensionError::ContextInitializationIncomplete)?
            .await
        {
            Ok(Some(result)) => Ok(Some(result)),
            Ok(None) => Ok(None),
            Err(error) => {
                if error.is_panic() {
                    crate::dispatch::discard_panic_payload(error.into_panic());
                }
                Err(ExtensionError::ContextInitializationIncomplete)
            }
        };
        main.handle = None;
        main.observed = Some(result.clone());
        result
    }
}
