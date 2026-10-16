//! A concrete provider bridge scope is supplied only after child activation.
use super::super::{Orchestrator, RuntimeError};
use crabber_core::{InputFingerprint, SessionContext};
use crabber_extension::{PreparationResourceContext, PreparationResourceLease};

#[derive(Clone)]
pub(in crate::orchestrator) struct PlanScope {
    pub contract: InputFingerprint,
    pub context: PreparationResourceContext,
    pub lease: PreparationResourceLease,
}
impl Orchestrator {
    pub(in crate::orchestrator) fn freeze_preparation_semantics(
        &self,
        mut config: serde_json::Value,
    ) -> Result<serde_json::Value, RuntimeError> {
        crabber_session::session_owner::preparation::bind_resource_contract(
            &mut config,
            self.preparation_scope.as_ref().map(|scope| &scope.contract),
        )?;
        config.sort_all_objects();
        Ok(config)
    }
    pub(in crate::orchestrator) fn with_preparation_scope(
        mut self,
        scope: Option<PlanScope>,
    ) -> Self {
        self.preparation_scope = scope;
        self
    }
    pub(in crate::orchestrator) fn declared_resource_contract(
        &self,
        context: &SessionContext,
    ) -> Result<Option<InputFingerprint>, RuntimeError> {
        self.plan_provider
            .preparation_resource_contract(context)
            .map(|declared| declared.map(|contract| contract.fingerprint()))
            .map_err(RuntimeError::ExtensionPlan)
    }
}
