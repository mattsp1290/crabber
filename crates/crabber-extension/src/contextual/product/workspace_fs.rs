//! Unique Box filesystem products. Registered disposal is not physical policy.
mod construction;
mod facade;
mod install;
mod node;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope, WorkspaceFs};
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(crate) use node::Node;
use std::sync::Arc;
/// Move-only original filesystem. Its implementation is never exposed directly.
/// Projected facades retain explicit references; reads require an actual ambient
/// callback owner and keep the original borrowed future inside its retained task.
/// Internal Arcs, global state and unregistered resources remain opaque.
pub struct ContextualWorkspaceFsProduct {
    body: Box<dyn WorkspaceFs>,
}
impl ContextualWorkspaceFsProduct {
    #[must_use]
    pub fn new(body: Box<dyn WorkspaceFs>) -> Self {
        Self { body }
    }
}
/// Distinct opt-in filesystem producer; hidden internal resources remain unknown.
pub trait ContextualWorkspaceFsProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_workspace_fs(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualWorkspaceFsProduct, ExtensionError>>;
}
/// Original node and graph only; no acquisition reference is pinned.
#[derive(Clone)]
pub struct ContextualWorkspaceFsProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box destructor and constructor/registered graph joins.
/// A successful read or completed Run alone never creates this observation.
/// This does not certify hidden references, global resources or physical closure.
#[derive(Clone)]
pub struct DisposedContextualWorkspaceFsProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualWorkspaceFsProductObservation {
    /// Wait without closing; timeout retains the same original tasks.
    /// # Errors
    /// Refuses pending references, original invocation/destructor failures or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualWorkspaceFsProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualWorkspaceFsProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualWorkspaceFsProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
