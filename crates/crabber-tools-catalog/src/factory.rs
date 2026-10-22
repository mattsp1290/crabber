//! Workspace-scoped standard-tool construction through host admission.
use crate::{Policies, StandardTools};
use async_trait::async_trait;
use crabber::{
    ExtensionError,
    extension::{ComponentIdentity, Extension, ExtensionFactory, FactoryContext, FactoryScope},
};
use crabber_tools_core::WorkspaceAdmission;
use std::sync::Arc;

/// One logical registration, with independent tool resources per workspace.
pub struct StandardToolsFactory {
    policies: Policies,
    admission: Arc<dyn WorkspaceAdmission>,
    hash: String,
}
impl StandardToolsFactory {
    /// Validate root-free policies. Admission remains the host's authority.
    pub fn new(
        policies: Policies,
        admission: Arc<dyn WorkspaceAdmission>,
    ) -> Result<Self, ExtensionError> {
        policies.validate()?;
        let hash = policies.config_identity();
        Ok(Self {
            policies,
            admission,
            hash,
        })
    }
}
#[async_trait]
impl ExtensionFactory for StandardToolsFactory {
    fn identity(&self) -> ComponentIdentity {
        ComponentIdentity {
            id: "crabber-tools/standard-factory".into(),
            version: env!("CARGO_PKG_VERSION").into(),
        }
    }
    fn config_hash(&self) -> String {
        self.hash.clone()
    }
    fn scope(&self) -> FactoryScope {
        FactoryScope::Workspace
    }
    async fn create(&self, context: FactoryContext) -> Result<Arc<dyn Extension>, ExtensionError> {
        let admission = self.admission.clone();
        let workspace = context.workspace().clone();
        let root = tokio::task::spawn_blocking(move || {
            let root = admission.admit(&workspace)?;
            if root
                .matches_session_directory(workspace.directory())
                .is_err()
            {
                return Err(ExtensionError::Plan(
                    "admitted root does not match session directory".into(),
                ));
            }
            Ok(root)
        })
        .await
        .map_err(|_| ExtensionError::Plan("workspace admission failed".into()))??;
        Ok(Arc::new(StandardTools::new(root, self.policies.clone())?))
    }
}
