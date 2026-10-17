//! Unique Box reader resolver products. Registered disposal is not physical policy.
mod child;
mod construction;
mod facade;
mod install;
mod node;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope};
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(crate) use node::Node;
pub(in crate::contextual) use registration::capture;
use std::sync::Arc;
/// Unique original resolver producer. Each returned reader is installed before
/// disposing the literal resolve future; original Arc-returning resolvers remain
/// unchanged and cannot supply unique-object disposal observations.
#[async_trait::async_trait]
pub trait ContextualWorkspaceReaderResolverBody: Send + Sync {
    async fn resolve_box(
        &self,
        workspace: &crate::WorkspaceContext,
    ) -> Result<ContextualWorkspaceReaderProduct, crate::WorkspaceReadError>;
}
/// Move-only returned reader. Facade Arc clones share one explicit allocation
/// lease; the final clone seals only this reader's own original invocation subset.
/// Reads require its original creator's actual caller capability while that
/// owner remains open. A later caller must resolve its own reader; references
/// neither delay the creator seal nor gain authority from workspace routing.
/// Box/reference disposal remains independent generation evidence, not proof
/// that opaque internal services or the caller's whole resource graph stopped.
pub struct ContextualWorkspaceReaderProduct {
    body: Box<dyn crate::WorkspaceReader>,
}
impl ContextualWorkspaceReaderProduct {
    #[must_use]
    pub fn new(body: Box<dyn crate::WorkspaceReader>) -> Self {
        Self { body }
    }
}

/// Move-only original reader resolver. Its implementation is never exposed directly.
/// Projected facades retain explicit references; reads require an actual ambient
/// callback owner and keep the original borrowed future inside its retained task.
/// Internal Arcs, global state and unregistered resources remain opaque.
pub struct ContextualWorkspaceReaderResolverProduct {
    body: Box<dyn ContextualWorkspaceReaderResolverBody>,
}
impl ContextualWorkspaceReaderResolverProduct {
    #[must_use]
    pub fn new(body: Box<dyn ContextualWorkspaceReaderResolverBody>) -> Self {
        Self { body }
    }
}
/// Distinct opt-in reader resolver producer; hidden internal resources remain unknown.
pub trait ContextualWorkspaceReaderResolverProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_workspace_reader_resolver(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualWorkspaceReaderResolverProduct, ExtensionError>>;
}
/// Original node and graph only; no acquisition reference is pinned.
#[derive(Clone)]
pub struct ContextualWorkspaceReaderResolverProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box destructor and constructor/registered graph joins.
/// A successful read or completed Run alone never creates this observation.
/// This does not certify hidden references, global resources or physical closure.
#[derive(Clone)]
pub struct DisposedContextualWorkspaceReaderResolverProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualWorkspaceReaderResolverProductObservation {
    /// Wait without closing; timeout retains the same original tasks.
    /// # Errors
    /// Refuses pending references, original invocation/destructor failures or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualWorkspaceReaderResolverProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualWorkspaceReaderResolverProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualWorkspaceReaderResolverProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
