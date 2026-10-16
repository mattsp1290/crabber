use super::cleanup::RecordCleanup;
use super::{
    ContextualPlanProvider, FactoryContext, FactoryRegistration, FactoryScope, INITIALIZING,
    Initialization, Owner, Record, Slot, SlotState,
};
use crate::Registrar;
use crate::{ComponentIdentity, Extension, ExtensionError, Scope, WorkspaceContext};
use async_trait::async_trait;
use crabber_core::SessionContext;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::{Arc, Mutex};
use tokio::sync::Mutex as AsyncMutex;

struct FrozenExtension {
    identity: ComponentIdentity,
    config_hash: String,
    extension: Arc<dyn Extension>,
}
#[async_trait]
impl Extension for FrozenExtension {
    fn id(&self) -> &str {
        &self.identity.id
    }
    fn version(&self) -> &str {
        &self.identity.version
    }
    fn config_hash(&self) -> String {
        self.config_hash.clone()
    }
    async fn install(&self, registrar: &mut Registrar) -> Result<(), ExtensionError> {
        self.extension.install(registrar).await
    }
    async fn shutdown(&self) {
        self.extension.shutdown().await;
    }
}

impl ContextualPlanProvider {
    pub(super) async fn ensure_owner(
        &self,
        owner: Owner,
        context: Option<SessionContext>,
    ) -> Result<(), ExtensionError> {
        let (slot, initialization) = {
            let mut owners = self.inner.owners.lock().unwrap();
            self.check_open()?;
            if let Some(context) = &context {
                owners.validate(context)?;
            }
            let slot = owners
                .slots
                .entry(owner.clone())
                .or_insert_with(|| {
                    Arc::new(Slot {
                        owner: owner.clone(),
                        context: context.clone(),
                        gate: AsyncMutex::new(()),
                        state: Mutex::new(SlotState::default()),
                    })
                })
                .clone();
            if matches!(owner, Owner::Session(_)) && slot.context != context {
                return Err(ExtensionError::ContextIdentityMismatch);
            }
            let mut state = slot.state.lock().unwrap();
            if state.closed {
                return Err(ExtensionError::ContextOwnerClosed);
            }
            if state.ready && state.initialization.is_none() {
                return Ok(());
            }
            let initialization = if let Some(initialization) = &state.initialization {
                initialization.clone()
            } else {
                let initialization = Arc::new(Initialization::default());
                state.initialization = Some(initialization.clone());
                let provider = self.clone();
                let slot = slot.clone();
                let ticket = initialization.clone();
                // Registered under the owners lock: global close cannot miss setup.
                let task = self.inner.initializers.tracker().spawn(async move {
                    let result = INITIALIZING
                        .scope(owner, async {
                            AssertUnwindSafe(provider.initialize_slot(slot.clone()))
                                .catch_unwind()
                                .await
                                .unwrap_or_else(|_| {
                                    Err(ExtensionError::ContextInitializationFailed {
                                        component: "contextual-profile".into(),
                                    })
                                })
                        })
                        .await;
                    *ticket.result.lock().unwrap() = Some(result);
                });
                initialization.retain(task);
                initialization
            };
            (slot.clone(), initialization)
        };
        initialization.join().await?;
        {
            let mut state = slot.state.lock().unwrap();
            if state
                .initialization
                .as_ref()
                .is_some_and(|current| Arc::ptr_eq(current, &initialization))
            {
                state.initialization = None;
            }
        }
        initialization
            .result
            .lock()
            .unwrap()
            .clone()
            .unwrap_or(Err(ExtensionError::ContextInitializationIncomplete))
    }

    async fn initialize_slot(&self, slot: Arc<Slot>) -> Result<(), ExtensionError> {
        let _gate = slot.gate.lock().await;
        self.check_slot(&slot)?;
        if slot.state.lock().unwrap().ready {
            return Ok(());
        }
        // A failed attempt's resources remain reachable until cleanup really ends.
        self.clean_records(&slot).await?;
        self.check_slot(&slot)?;
        let generation = self.new_generation(&slot)?;
        slot.state.lock().unwrap().generation = Some(generation);
        let result = AssertUnwindSafe(async {
            match &slot.owner {
                Owner::Shared => self.install_shared(&slot).await,
                Owner::Workspace(_) => self.install_factories(&slot, FactoryScope::Workspace).await,
                Owner::Session(_) => self.install_factories(&slot, FactoryScope::Session).await,
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|_| {
            Err(ExtensionError::ContextInitializationFailed {
                component: "contextual-profile".into(),
            })
        });
        if let Err(error) = result {
            self.clean_records(&slot).await?;
            return Err(error);
        }
        if let Err(error) = self.check_slot(&slot) {
            self.clean_records(&slot).await?;
            return Err(error);
        }
        slot.state.lock().unwrap().ready = true;
        Ok(())
    }

    fn check_slot(&self, slot: &Slot) -> Result<(), ExtensionError> {
        self.check_open()?;
        if slot.state.lock().unwrap().closed {
            Err(ExtensionError::ContextOwnerClosed)
        } else {
            Ok(())
        }
    }

    fn start_record(&self, slot: &Slot) -> Result<(usize, Arc<RecordCleanup>), ExtensionError> {
        let cleanup = Arc::new(match slot.owner {
            Owner::Shared => RecordCleanup::Shared(crate::CleanupOwner::new()),
            Owner::Workspace(_) | Owner::Session(_) => {
                RecordCleanup::Factory(crate::ToolResourceOwner::new())
            }
        });
        let mut state = slot.state.lock().unwrap();
        if state.closed || self.is_closed() {
            return Err(ExtensionError::ContextOwnerClosed);
        }
        let index = state.records.len();
        state.records.push(Record {
            cleanup: cleanup.clone(),
            mount: None,
        });
        Ok((index, cleanup))
    }

    async fn install_shared(&self, slot: &Slot) -> Result<(), ExtensionError> {
        for registration in &self.inner.shared {
            let extension = &registration.extension;
            self.check_slot(slot)?;
            let (index, _) = self.start_record(slot)?;
            let metadata = match &registration.metadata {
                Ok(metadata) => metadata,
                Err(error) => {
                    Self::signal_records(slot);
                    let _ = AssertUnwindSafe(extension.shutdown()).catch_unwind().await;
                    return Err(error.clone());
                }
            };
            let frozen = FrozenExtension {
                identity: metadata.identity.clone(),
                config_hash: metadata.config_hash.clone(),
                extension: extension.clone(),
            };
            let component = frozen.identity.id.clone();
            let Ok(mount) = self
                .inner
                .registry
                .mount(Arc::new(frozen), registration.scope.clone())
                .await
            else {
                Self::signal_records(slot);
                let _ = AssertUnwindSafe(extension.shutdown()).catch_unwind().await;
                return Err(ExtensionError::ContextInitializationFailed { component });
            };
            slot.state.lock().unwrap().records[index].mount = Some(mount);
        }
        Ok(())
    }

    async fn install_factories(
        &self,
        slot: &Slot,
        scope: FactoryScope,
    ) -> Result<(), ExtensionError> {
        let context = slot.context.as_ref().expect("factory owner has context");
        for registration in self.inner.factories.iter().filter(|f| f.scope == scope) {
            self.check_slot(slot)?;
            let (index, cleanup) = self.start_record(slot)?;
            let factory_context = FactoryContext {
                workspace: WorkspaceContext::from_persisted(
                    context.workspace_id(),
                    context.directory(),
                ),
                session_id: (scope == FactoryScope::Session).then(|| context.session_id().clone()),
                cleanup: cleanup.tracker(),
                generation: slot
                    .state
                    .lock()
                    .unwrap()
                    .generation
                    .clone()
                    .expect("generation selected before factory"),
            };
            let failed = || ExtensionError::ContextInitializationFailed {
                component: registration.identity.id.clone(),
            };
            let extension = AssertUnwindSafe(registration.factory.create(factory_context))
                .catch_unwind()
                .await
                .map_err(|_| failed())?
                .map_err(|_| failed())?;
            let frozen = std::panic::catch_unwind(AssertUnwindSafe(|| {
                freeze(registration, extension.clone())
            }))
            .map_err(|_| failed())
            .and_then(|result| result);
            let frozen = match frozen.and_then(|frozen| {
                self.check_slot(slot)?;
                Ok(frozen)
            }) {
                Ok(frozen) => frozen,
                Err(error) => {
                    Self::signal_records(slot);
                    let _ = AssertUnwindSafe(extension.shutdown()).catch_unwind().await;
                    return Err(error);
                }
            };
            let scope = match scope {
                FactoryScope::Workspace => Scope::Workspace(context.workspace_id().into()),
                FactoryScope::Session => Scope::SessionIn {
                    session_id: context.session_id().clone(),
                    workspace_id: context.workspace_id().into(),
                },
            };
            let extension: Arc<dyn Extension> = Arc::new(frozen);
            let Ok(mount) = self.inner.registry.mount(extension.clone(), scope).await else {
                Self::signal_records(slot);
                let _ = AssertUnwindSafe(extension.shutdown()).catch_unwind().await;
                return Err(failed());
            };
            slot.state.lock().unwrap().records[index].mount = Some(mount);
        }
        Ok(())
    }
}

fn freeze(
    registration: &FactoryRegistration,
    extension: Arc<dyn Extension>,
) -> Result<FrozenExtension, ExtensionError> {
    let id = extension.id();
    let version = extension.version();
    if id.is_empty()
        || id.len() > 128
        || id.chars().any(char::is_control)
        || version.is_empty()
        || version.len() > 128
        || version.chars().any(char::is_control)
    {
        return Err(ExtensionError::ContextInitializationFailed {
            component: registration.identity.id.clone(),
        });
    }
    let mut config = serde_json::json!({
        "contract":super::FACTORY_RESOURCE_CONTRACT, "factory":registration.identity,
        "resource_contract":crate::TOOL_RESOURCE_CONTRACT_VERSION,
        "resource_capacity":crate::MAX_TOOL_RESOURCE_TASKS,
        "factory_config":config_value(&registration.config_hash),
        "instance":{"id":id, "version":version, "config":config_value(&extension.config_hash())},
    });
    config.sort_all_objects();
    let config_hash = serde_json::to_string(&config).expect("factory metadata serializes");
    if config_hash.len() > 32768 {
        return Err(ExtensionError::ContextInitializationFailed {
            component: registration.identity.id.clone(),
        });
    }
    Ok(FrozenExtension {
        identity: registration.identity.clone(),
        config_hash,
        extension,
    })
}

fn config_value(value: &str) -> serde_json::Value {
    serde_json::from_str(value).unwrap_or_else(|_| serde_json::Value::String(value.into()))
}
