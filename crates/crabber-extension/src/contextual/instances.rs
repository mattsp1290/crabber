use super::cleanup::{FactoryCleanup, RecordCleanup, RecordMount};
use super::{
    ContextualPlanProvider, FactoryContext, FactoryRegistration, FactoryScope, Owner, Record, Slot,
};
use crate::Registrar;
use crate::{ComponentIdentity, Extension, ExtensionError, Scope, WorkspaceContext};
use async_trait::async_trait;
use crabber_core::SessionContext;
use futures::FutureExt;
use std::panic::AssertUnwindSafe;
use std::sync::Arc;

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
        context: Option<&SessionContext>,
    ) -> Result<(), ExtensionError> {
        self.ensure_owner_registered(owner, context, None).await
    }

    pub(super) async fn ensure_owner_registered(
        &self,
        owner: Owner,
        context: Option<&SessionContext>,
        binding: Option<super::native_dependency::Binding>,
    ) -> Result<(), ExtensionError> {
        let (slot, initialization) = self.select_owner(owner, context, binding.clone())?;
        if let Some(initialization) = initialization {
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
                .unwrap_or(Err(ExtensionError::ContextInitializationIncomplete))?;
        }
        self.check_slot(&slot)?;
        if let Some(binding) = binding {
            let generation = slot
                .state
                .lock()
                .unwrap()
                .generation
                .clone()
                .ok_or(ExtensionError::ContextInitializationIncomplete)?;
            self.inner
                .dependencies
                .register(&binding.context, &generation, &binding.services)
                .await?;
        }
        Ok(())
    }

    pub(super) async fn initialize_slot(
        &self,
        slot: Arc<Slot>,
        binding: Option<super::native_dependency::Binding>,
    ) -> Result<(), ExtensionError> {
        let _gate = slot.gate.lock().await;
        self.check_slot(&slot)?;
        if slot.state.lock().unwrap().ready {
            return Ok(());
        }
        if slot
            .state
            .lock()
            .unwrap()
            .producer
            .as_ref()
            .is_some_and(|p| p.blocked())
        {
            return Err(ExtensionError::ContextInitializationIncomplete);
        }
        // A failed attempt's resources remain reachable until cleanup really ends.
        self.clean_records(&slot).await?;
        self.check_slot(&slot)?;
        let pending = slot
            .state
            .lock()
            .unwrap()
            .generation
            .clone()
            .filter(|generation| {
                self.inner
                    .dependencies
                    .generation_uncertain(generation.id())
            });
        if pending.is_some() && binding.is_none() {
            return Err(ExtensionError::PreparationResourcesUnsupported);
        }
        let generation = match pending {
            Some(generation) => generation,
            None => self.new_generation(&slot)?,
        };
        slot.state.lock().unwrap().generation = Some(generation.clone());
        if let Some(binding) = binding {
            self.inner
                .dependencies
                .register(&binding.context, &generation, &binding.services)
                .await?;
            self.check_slot(&slot)?;
        }
        let result = if let Some(host) = self
            .inner
            .generation_host
            .as_ref()
            .filter(|_| !matches!(slot.owner, Owner::Shared))
        {
            let (registration, release) = super::producer::Registration::new(
                self.clone(),
                slot.clone(),
                generation,
                host.clone(),
            )?;
            let open = {
                let mut state = slot.state.lock().unwrap();
                state.producer = Some(registration.clone());
                if state.closed || self.is_closed() {
                    registration.close();
                    false
                } else {
                    true
                }
            };
            if open {
                let _ = release.send(());
            } else {
                drop(release);
            }
            registration.run(self, &slot).await
        } else {
            self.construct_slot(&slot).await
        };
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

    pub(super) async fn construct_slot(&self, slot: &Arc<Slot>) -> Result<(), ExtensionError> {
        AssertUnwindSafe(async {
            match &slot.owner {
                Owner::Shared => self.install_shared(slot).await,
                Owner::Workspace(_) => self.install_factories(slot, FactoryScope::Workspace).await,
                Owner::Session(_) => self.install_factories(slot, FactoryScope::Session).await,
            }
        })
        .catch_unwind()
        .await
        .unwrap_or_else(|payload| {
            crate::dispatch::discard_panic_payload(payload);
            Err(ExtensionError::ContextInitializationFailed {
                component: "contextual-profile".into(),
            })
        })
    }

    pub(super) fn check_slot(&self, slot: &Slot) -> Result<(), ExtensionError> {
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
                RecordCleanup::Factory(FactoryCleanup::new())
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
            failed_cleanup: false,
        });
        if let Some(producer) = &state.producer {
            producer.record_started(index, cleanup.clone());
        }
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
            slot.state.lock().unwrap().records[index].mount = Some(RecordMount::Legacy(mount));
        }
        Ok(())
    }

    async fn install_factories(
        &self,
        slot: &Arc<Slot>,
        scope: FactoryScope,
    ) -> Result<(), ExtensionError> {
        let context = slot.context.as_ref().expect("factory owner has context");
        for registration in self.inner.factories.iter().filter(|f| f.scope == scope) {
            self.check_slot(slot)?;
            let (index, cleanup) = self.start_record(slot)?;
            let generation = slot
                .state
                .lock()
                .unwrap()
                .generation
                .clone()
                .expect("selected generation");
            let factory_context = FactoryContext {
                provider_source: super::ContextualServiceBorrowSource::new(
                    self,
                    slot,
                    generation.clone(),
                    index,
                ),
                workspace: WorkspaceContext::from_persisted(
                    context.workspace_id(),
                    context.directory(),
                ),
                session_id: (scope == FactoryScope::Session).then(|| context.session_id().clone()),
                cleanup: cleanup.tracker(),
                generation,
            };
            let failed = || ExtensionError::ContextInitializationFailed {
                component: registration.identity.id.clone(),
            };
            let extension = AssertUnwindSafe(registration.factory.create(factory_context))
                .catch_unwind()
                .await
                .map_err(|payload| {
                    crate::dispatch::discard_panic_payload(payload);
                    failed()
                })?
                .map_err(|_| failed())?;
            let frozen = std::panic::catch_unwind(AssertUnwindSafe(|| {
                freeze(registration, extension.clone())
            }))
            .map_err(|payload| {
                crate::dispatch::discard_panic_payload(payload);
                failed()
            })
            .and_then(|result| result);
            let frozen = match frozen.and_then(|frozen| {
                self.check_slot(slot)?;
                Ok(frozen)
            }) {
                Ok(frozen) => frozen,
                Err(error) => {
                    Self::signal_records(slot);
                    Self::shutdown_factory(slot, index, &extension).await;
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
            let mount = match self
                .inner
                .registry
                .mount_owned_with_failure_signal(extension.clone(), scope, || {
                    Self::signal_records(slot);
                })
                .await
            {
                Ok(mount) => mount,
                Err(error) => {
                    if matches!(error, ExtensionError::MountCleanupIncomplete { .. }) {
                        let mut state = slot.state.lock().unwrap();
                        state.records[index].failed_cleanup = true;
                        if let Some(producer) = &state.producer {
                            producer.record_failed(index);
                        }
                    }
                    Self::signal_records(slot);
                    Self::shutdown_factory(slot, index, &extension).await;
                    return Err(failed());
                }
            };
            let mut state = slot.state.lock().unwrap();
            if let Some(producer) = &state.producer {
                producer.record_mounted(index, RecordMount::Owned(mount.clone()));
            }
            state.records[index].mount = Some(RecordMount::Owned(mount));
        }
        Ok(())
    }
    async fn shutdown_factory(slot: &Slot, index: usize, extension: &Arc<dyn Extension>) {
        let cleanup = slot.state.lock().unwrap().records[index].cleanup.clone();
        let (shutdown, joined) = tokio::join!(
            AssertUnwindSafe(extension.shutdown()).catch_unwind(),
            cleanup.wait_drained(),
        );
        if let Err(payload) = shutdown {
            crate::dispatch::discard_panic_payload(payload);
            let mut state = slot.state.lock().unwrap();
            state.records[index].failed_cleanup = true;
            if let Some(producer) = &state.producer {
                producer.record_failed(index);
            }
        }
        if joined.is_err() {
            let mut state = slot.state.lock().unwrap();
            state.records[index].failed_cleanup = true;
            if let Some(producer) = &state.producer {
                producer.record_failed(index);
            }
        }
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
        "mount_contract":super::FACTORY_MOUNT_CONTRACT,
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
