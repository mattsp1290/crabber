//! Unique Box `ToolGuard` products. Registered disposal is not physical policy.
mod cell;
mod construction;
mod facade;
mod install;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope, ToolGuard};
pub(crate) use cell::Node;
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(in crate::contextual) use registration::ToolGuardRegistration;
pub(in crate::contextual) use registration::capture;
use std::sync::Arc;
/// Move-only original `ToolGuard`. Its implementation is never exposed directly.
/// Projected facades retain explicit references; checks require an actual ambient
/// callback owner and keep the original borrowed call on its original synchronous stack.
/// Internal Arcs, global state and unregistered resources remain opaque.
pub struct ContextualToolGuardProduct {
    body: Box<dyn ToolGuard>,
}
impl ContextualToolGuardProduct {
    #[must_use]
    pub fn new(body: Box<dyn ToolGuard>) -> Self {
        Self { body }
    }
}
/// Distinct opt-in `ToolGuard` producer; hidden internal resources remain unknown.
pub trait ContextualToolGuardProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_tool_guard(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolGuardProduct, ExtensionError>>;
}
/// Original node and graph only; no acquisition reference is pinned.
#[derive(Clone)]
pub struct ContextualToolGuardProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box destructor and constructor/registered graph joins.
/// A successful check or completed Run alone never creates this observation.
/// This does not certify hidden references, global resources or physical closure.
#[derive(Clone)]
pub struct DisposedContextualToolGuardProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualToolGuardProductObservation {
    /// Wait without closing; timeout retains the same original tasks.
    /// # Errors
    /// Refuses pending references, original invocation/destructor failures or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualToolGuardProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualToolGuardProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualToolGuardProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
