//! Non-exposing actual Box roots. Disposal is a component, not physical policy.
mod cell;
mod construction;
pub(crate) mod provider;
mod registration;
pub(crate) mod tool;
use super::{FactoryContext, FactoryScope};
use crate::{ComponentIdentity, ExtensionError};
pub(super) use construction::create_member;
use futures::future::BoxFuture;
use std::sync::Arc;

pub(super) use cell::Cell;
/// An opaque owned value. Its actual Box disposal says nothing about hidden
/// shared references, factory objects, globals or arbitrary background effects.
pub trait ContextualProductRoot: Send + Sync + 'static {}
impl<T: Send + Sync + 'static> ContextualProductRoot for T {}
/// Separate opt-in factory family. No object, callback or registration is exposed
/// by this root-only route. Existing Arc factories remain unchanged.
pub trait ContextualProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_root(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<Box<dyn ContextualProductRoot>, ExtensionError>>;
}
/// Retains the exact original root and original constructor/graph observations.
/// This pins no acquisition; it grants no native or physical-close authority.
#[derive(Clone)]
pub struct ContextualProductRootObservation {
    pub(super) graph: super::ContextualGenerationObservation,
    pub(super) cell: Arc<Cell>,
    pub(super) member: usize,
}
/// Exact original Box destructor and original constructor graph joined. This is
/// not a complete object graph, physical generation close or resource policy.
#[derive(Clone)]
pub struct DisposedContextualProductRoot {
    graph: super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualProductRootObservation {
    /// Wait without initiating close. Timeout retains every original handle.
    /// # Errors
    /// Reports original task/destructor failure, no returned root or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualProductRoot, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.cell.disposed().await?;
            Ok(DisposedContextualProductRoot {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualProductRoot {
    #[must_use]
    pub fn generation(&self) -> &super::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &super::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
