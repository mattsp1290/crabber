use super::{ContextualPlanProvider, FactoryScope, Owner, Slot};
use crate::ExtensionError;
use crabber_core::{InputFingerprint, SessionId};
use serde::Serialize;

/// Correlation of one local physical generation, never a private grant. Restart
/// creates a different identity even when frozen behavior is unchanged.
#[derive(Debug, Clone, PartialEq, Eq, PartialOrd, Ord, Serialize)]
pub struct ServiceGenerationId(String);
impl ServiceGenerationId {
    #[must_use]
    pub fn as_str(&self) -> &str {
        &self.0
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
#[serde(tag = "scope")]
pub enum ServiceGenerationScope {
    Shared,
    Workspace {
        workspace_id: String,
        directory: String,
    },
    Session {
        session_id: SessionId,
        workspace_id: String,
        directory: String,
    },
}

/// Registration policy of this component only; declaration is not join proof.
/// Shared/base services and escaped callbacks retain independent lifetimes.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Serialize)]
pub enum ServiceGenerationCleanupPolicy {
    LegacySharedMount,
    SealedFactoryLegacyMount,
    /// Factory cleanup and mounted handler cleanup are sealed and retain joins.
    /// Actual initializer/factory/mount joins must still succeed before close.
    FullySealedFactory,
}

/// Frozen local generation metadata, separate from each session's dependency.
/// Reading or serializing it grants no initialization, lease or cleanup proof.
#[derive(Debug, Clone, PartialEq, Eq, Serialize)]
pub struct ServiceGenerationDescriptor {
    id: ServiceGenerationId,
    scope: ServiceGenerationScope,
    registration_fingerprint: InputFingerprint,
    cleanup_policy: ServiceGenerationCleanupPolicy,
}
impl ServiceGenerationDescriptor {
    #[must_use]
    pub fn id(&self) -> &ServiceGenerationId {
        &self.id
    }
    #[must_use]
    pub fn scope(&self) -> &ServiceGenerationScope {
        &self.scope
    }
    #[must_use]
    pub fn registration_fingerprint(&self) -> &InputFingerprint {
        &self.registration_fingerprint
    }
    #[must_use]
    pub fn cleanup_policy(&self) -> ServiceGenerationCleanupPolicy {
        self.cleanup_policy
    }
}

impl ContextualPlanProvider {
    /// Inspect already selected local generations without initializing services
    /// or acquiring plan/dependency authority. Closed generations remain visible.
    /// # Panics
    /// Panics if an earlier internal panic poisoned an ownership lock.
    #[must_use]
    pub fn service_generations(&self) -> Vec<ServiceGenerationDescriptor> {
        self.inner
            .owners
            .lock()
            .unwrap()
            .slots
            .values()
            .filter_map(|slot| slot.state.lock().unwrap().generation.clone())
            .collect()
    }

    pub(super) fn new_generation(
        &self,
        slot: &Slot,
    ) -> Result<ServiceGenerationDescriptor, ExtensionError> {
        let (scope, cleanup_policy, declarations) = match &slot.owner {
            Owner::Shared => {
                let base = self
                    .inner
                    .base_profile
                    .as_ref()
                    .map_or(serde_json::json!({"unavailable":true}), |base| {
                        base.all_declarations()
                    });
                let shared = self
                    .inner
                    .shared
                    .iter()
                    .map(|r| {
                        let Ok(metadata) = &r.metadata else {
                            return serde_json::json!([r.scope.declaration_value(), {"unavailable":true}]);
                        };
                        serde_json::json!([
                            r.scope.declaration_value(),
                            metadata.identity,
                            crate::plan::registration_config(&metadata.config_hash),
                        ])
                    })
                    .collect::<Vec<_>>();
                (
                    ServiceGenerationScope::Shared,
                    ServiceGenerationCleanupPolicy::LegacySharedMount,
                    serde_json::json!([base, shared]),
                )
            }
            owner => {
                let context = slot.context.as_ref().expect("factory owner has context");
                let (scope, factory_scope) = match owner {
                    Owner::Workspace(_) => (
                        ServiceGenerationScope::Workspace {
                            workspace_id: context.workspace_id().into(),
                            directory: context.directory().into(),
                        },
                        FactoryScope::Workspace,
                    ),
                    Owner::Session(_) => (
                        ServiceGenerationScope::Session {
                            session_id: context.session_id().clone(),
                            workspace_id: context.workspace_id().into(),
                            directory: context.directory().into(),
                        },
                        FactoryScope::Session,
                    ),
                    Owner::Shared => unreachable!(),
                };
                let factories = self
                    .inner
                    .factories
                    .iter()
                    .filter(|r| r.scope == factory_scope)
                    .map(|r| {
                        serde_json::json!([
                            r.identity,
                            crate::plan::registration_config(&r.config_hash),
                        ])
                    })
                    .collect::<Vec<_>>();
                (
                    scope,
                    ServiceGenerationCleanupPolicy::FullySealedFactory,
                    serde_json::json!([
                        super::FACTORY_RESOURCE_CONTRACT,
                        super::FACTORY_MOUNT_CONTRACT,
                        crate::TOOL_RESOURCE_CONTRACT_VERSION,
                        crate::MAX_TOOL_RESOURCE_TASKS,
                        factories,
                    ]),
                )
            }
        };
        let registration_fingerprint = crate::plan::registration_hash(serde_json::json!([
            "crabber.contextual-service-generation.v1",
            scope,
            cleanup_policy,
            declarations,
        ]))?;
        Ok(ServiceGenerationDescriptor {
            id: ServiceGenerationId(crabber_core::SessionOwnerId::new().to_string()),
            scope,
            registration_fingerprint,
            cleanup_policy,
        })
    }
}
