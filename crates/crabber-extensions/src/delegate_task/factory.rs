//! Owner-scoped delegation with immutable runner configuration.
use super::{Configuration, DelegateTask, Options, OwnerReport, Policy, SharedCapacity};
use crate::contextual::{Binding, Instances, retain_close};
use crabber::{
    core::SessionId,
    extension::{
        ComponentIdentity, Extension, ExtensionError, ExtensionFactory, FactoryContext,
        FactoryScope,
    },
};
use std::sync::Arc;

/// Creates independent session/workspace task records and runner capacity.
pub struct Factory {
    configuration: Arc<Configuration>,
    instance_hash: String,
    scope: FactoryScope,
    shared: Option<SharedCapacity>,
    instances: Instances<Policy>,
}
impl Factory {
    /// Validate once without invoking the runner. Use Session scope for session close.
    pub fn new(options: Options, scope: FactoryScope) -> Result<Self, ExtensionError> {
        let validated = DelegateTask::new(options)?;
        Ok(Self {
            configuration: Arc::new(Configuration {
                runner: validated.policy.runner.clone(),
                limits: validated.policy.limits.clone(),
            }),
            instance_hash: validated.hash,
            scope,
            shared: None,
            instances: Instances::default(),
        })
    }
    /// Add a shared nonqueueing budget retained until every runner future exits.
    pub fn with_shared_capacity(mut self, shared: SharedCapacity) -> Self {
        self.shared = Some(shared);
        self
    }
    /// Observe a live owner policy, including retained stragglers after close.
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
            id: "crabber-extensions/delegate-task-factory".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
    fn config_hash(&self) -> String {
        crate::config_hash(&(
            "delegate-task-factory-v1",
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
        let instance = Arc::new(DelegateTask::from_configuration(
            self.configuration.clone(),
            Some(binding.clone()),
            self.shared.clone(),
            self.instance_hash.clone(),
        ));
        retain_close(&context, instance.clone(), DelegateTask::owner_report)?;
        self.instances.insert(binding, &instance.policy);
        Ok(instance)
    }
}
