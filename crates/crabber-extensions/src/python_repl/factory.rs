//! Factory-scoped python-repl instances with construction-time frozen policy.
use super::{Options, OwnerReport, PythonRepl, SharedCapacity, config, manager::Manager};
use crate::contextual::{Binding, Instances, retain_close};
use crabber::{
    core::SessionId,
    extension::{
        ComponentIdentity, Extension, ExtensionError, ExtensionFactory, FactoryContext,
        FactoryScope,
    },
};
use std::sync::Arc;

/// Creates independent instances per session or workspace without spawning work.
/// Use `FactoryScope::Session` for independently closable sessions.
pub struct Factory {
    configuration: Arc<config::Configuration>,
    instance_hash: String,
    scope: FactoryScope,
    shared: Option<SharedCapacity>,
    instances: Instances<Manager>,
}
impl Factory {
    /// Validate and freeze executable/environment policy once, before admission.
    pub fn new(options: Options, scope: FactoryScope) -> Result<Self, ExtensionError> {
        let (configuration, instance_hash) = config::validate(options)?;
        Ok(Self {
            configuration: Arc::new(configuration),
            instance_hash,
            scope,
            shared: None,
            instances: Instances::default(),
        })
    }
    /// Add a nonqueueing aggregate budget across this factory's owner instances.
    pub fn with_shared_capacity(mut self, shared: SharedCapacity) -> Self {
        self.shared = Some(shared);
        self
    }
    /// Observe an instantiated owner, including survivors of bounded close.
    /// Pass no session for a workspace-scoped factory.
    pub fn owner_report(
        &self,
        workspace_id: &str,
        session_id: Option<&SessionId>,
    ) -> Option<OwnerReport> {
        self.instances
            .get(workspace_id, session_id)
            .map(|policy| policy.owner_report())
    }
}
#[async_trait::async_trait]
impl ExtensionFactory for Factory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "crabber-extensions/python-repl-factory".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
    fn config_hash(&self) -> String {
        crate::config_hash(&(
            "python-repl-factory-v1",
            crate::contextual::scope_name(self.scope),
            &self.instance_hash,
            self.shared.as_ref().map(SharedCapacity::limit),
        ))
    }
    fn scope(&self) -> FactoryScope {
        self.scope
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        let binding = Binding::from_factory(&context)?;
        let instance = Arc::new(PythonRepl::from_configuration(
            self.configuration.clone(),
            binding.clone(),
            self.shared.clone(),
            self.instance_hash.clone(),
        ));
        retain_close(&context, instance.clone(), PythonRepl::owner_report)?;
        self.instances.insert(binding, &instance.manager);
        Ok(instance)
    }
}
