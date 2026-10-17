//! Unique Box prompt products. Registered disposal is not physical policy.
mod cell;
mod construction;
pub(crate) mod facade;
mod install;
mod registration;
use crate::{ComponentIdentity, ExtensionError, FactoryContext, FactoryScope};
pub(crate) use cell::Node;
use futures::future::BoxFuture;
pub(in crate::contextual) use install::install;
pub(in crate::contextual) use registration::PromptRegistration;
pub(in crate::contextual) use registration::capture;
use std::sync::Arc;
/// Move-only original native or typed prompt callback. Only the product facade
/// is projected; hidden references and factory resources remain unknown.
pub struct ContextualPromptProduct {
    pub(super) name: String,
    pub(super) order: i32,
    pub(super) body: Body,
}
/// Move-only native callback storage. This alias grants no invocation authority.
pub type ContextualPromptCallback = Box<
    dyn Fn(
            crate::PromptAttemptContext,
        ) -> BoxFuture<'static, Result<Option<String>, ExtensionError>>
        + Send
        + Sync,
>;
pub(super) enum Body {
    Native(ContextualPromptCallback),
    SystemPrompt(
        crate::MiddlewareDescriptor,
        Box<dyn crate::SystemPromptMiddleware>,
    ),
}
impl ContextualPromptProduct {
    #[must_use]
    pub fn native(name: String, order: i32, body: ContextualPromptCallback) -> Self {
        Self {
            name,
            order,
            body: Body::Native(body),
        }
    }
    #[must_use]
    pub fn system_prompt(
        name: String,
        order: i32,
        descriptor: crate::MiddlewareDescriptor,
        body: Box<dyn crate::SystemPromptMiddleware>,
    ) -> Self {
        Self {
            name,
            order,
            body: Body::SystemPrompt(descriptor, body),
        }
    }
}
/// Distinct prompt producer; selecting it supplies no native or physical policy.
pub trait ContextualPromptProductFactory: Send + Sync {
    fn identity(&self) -> ComponentIdentity;
    fn config_hash(&self) -> String;
    fn scope(&self) -> FactoryScope;
    fn create_prompt(
        &self,
        context: FactoryContext,
    ) -> BoxFuture<'_, Result<ContextualPromptProduct, ExtensionError>>;
}
/// Original node and graph only; no acquisition reference is pinned.
#[derive(Clone)]
pub struct ContextualPromptProductObservation {
    pub(in crate::contextual) graph: super::super::ContextualGenerationObservation,
    pub(in crate::contextual) node: Arc<Node>,
    pub(in crate::contextual) member: usize,
}
/// Actual original Box destructor and constructor/registered graph joins.
/// A successful check or completed Run alone never creates this observation.
/// This does not certify hidden references, global resources or physical closure.
#[derive(Clone)]
pub struct DisposedContextualPromptProduct {
    graph: super::super::JoinedContextualGenerationGraph,
    member: usize,
}
impl ContextualPromptProductObservation {
    /// Wait without closing; timeout retains the same original tasks.
    /// # Errors
    /// Refuses pending references, original invocation/destructor failures or timeout.
    pub async fn disposed(
        &self,
        bound: std::time::Duration,
    ) -> Result<DisposedContextualPromptProduct, ExtensionError> {
        tokio::time::timeout(bound, async {
            let graph = self.graph.registered_graph_join(bound).await?;
            self.node.disposed().await?;
            Ok(DisposedContextualPromptProduct {
                graph,
                member: self.member,
            })
        })
        .await
        .map_err(|_| ExtensionError::ContextCleanupIncomplete)?
    }
}
impl DisposedContextualPromptProduct {
    #[must_use]
    pub fn generation(&self) -> &crate::ServiceGenerationDescriptor {
        self.graph.generation()
    }
    #[must_use]
    pub fn member(&self) -> &crate::ContextualGenerationMember {
        &self.graph.members()[self.member]
    }
}
