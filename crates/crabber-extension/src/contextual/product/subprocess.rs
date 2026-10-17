//! Unique Box subprocess products. Registered disposal is not physical policy.
mod construction;
mod facade;
mod install;
mod node;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope, Subprocess};
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(crate) use node::Node;
use std::sync::Arc;
/// Move-only original subprocess. Its implementation is never exposed directly.
/// Projected facades retain explicit references; calls require an actual ambient
/// callback owner and keep the original borrowed future inside its retained task.
/// Internal Arcs, global state, OS child processes and unregistered resources remain opaque.
pub struct ContextualSubprocessProduct {
    body: Box<dyn Subprocess>,
}
impl ContextualSubprocessProduct {
    #[must_use]
    pub fn new(body: Box<dyn Subprocess>) -> Self {
        Self { body }
    }
}
/// Distinct opt-in subprocess producer; hidden internal resources remain unknown.
pub trait ContextualSubprocessProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_subprocess(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualSubprocessProduct, ExtensionError>>;
}
/// Original node and graph only; no acquisition reference is pinned.
#[derive(Clone)]
pub struct ContextualSubprocessProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box destructor and constructor/registered graph joins.
/// A successful call or completed Run alone never creates this observation.
/// This does not certify hidden references, global resources or physical closure.
#[derive(Clone)]
pub struct DisposedContextualSubprocessProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualSubprocessProductObservation {
    /// Wait without closing; timeout retains the same original tasks.
    /// # Errors
    /// Refuses pending references, original invocation/destructor failures or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualSubprocessProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualSubprocessProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualSubprocessProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
