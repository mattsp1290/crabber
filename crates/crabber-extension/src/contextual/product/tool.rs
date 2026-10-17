//! Box-owned ordinary Tool nodes; original references and invocations stay owned.
mod cell;
mod construction;
mod install;
pub(in crate::contextual) use install::install;
mod facade;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope, ToolExecutor};
pub(crate) use cell::Node;

use crabber_core::ToolInfo;
use futures::future::BoxFuture;
pub(in crate::contextual) use registration::ToolRegistration;
use std::sync::Arc;

/// Move-only actual Tool return. No original executor reference is exposed.
pub struct ContextualToolProduct {
    info: ToolInfo,
    executor: Box<dyn ToolExecutor>,
}
impl ContextualToolProduct {
    #[must_use]
    pub fn new(info: ToolInfo, executor: Box<dyn ToolExecutor>) -> Self {
        Self { info, executor }
    }
}
/// Separate ordinary-only Box producer. Resumable declarations/capabilities are
/// refused, never hidden behind an ordinary facade. Opaque internals remain unknown.
pub trait ContextualToolProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_tool(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualToolProduct, ExtensionError>>;
}
/// Original typed node, independent of projection and acquisition waiters.
#[derive(Clone)]
pub struct ContextualToolProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box and original graph joined; not physical/global closure.
#[derive(Clone)]
pub struct DisposedContextualToolProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualToolProductObservation {
    /// Wait without initiating close. Retains all original tasks across timeout.
    /// # Errors
    /// Refuses incomplete/failed original references, callbacks or destructors.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualToolProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualToolProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualToolProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
