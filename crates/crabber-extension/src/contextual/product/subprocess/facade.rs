use super::node::{Node, Reference};
use crate::{ExtensionError, Subprocess, registry::InvocationSource};
use async_trait::async_trait;
use serde_json::Value;
use std::sync::Weak;
pub(super) struct Template;
#[async_trait]
impl Subprocess for Template {
    async fn run(&self, _: &str, _: &[String]) -> Result<Value, String> {
        Err(ExtensionError::ContextOwnerClosed.to_string())
    }
}
pub(super) struct Process {
    pub(super) node: Weak<Node>,
    pub(super) source: InvocationSource,
    pub(super) _reference: Reference,
}
#[async_trait]
impl Subprocess for Process {
    async fn run(&self, program: &str, arguments: &[String]) -> Result<Value, String> {
        if crate::registry::current_callback_invocations().is_none() {
            return Err(ExtensionError::ContextOwnerClosed.to_string());
        }
        let node = self
            .node
            .upgrade()
            .ok_or_else(|| ExtensionError::ContextOwnerClosed.to_string())?;
        let program = program.to_owned();
        let arguments = arguments.to_vec();
        self.source
            .invoke(move || {
                Box::pin(async move {
                    let original = node.borrow().map_err(|error| error.to_string())?;
                    match super::super::provider::original::original(
                        || original.value().0.run(&program, &arguments),
                        std::convert::identity,
                        &tokio_util::sync::CancellationToken::new(),
                    )
                    .await
                    {
                        Ok(Some(result)) => result,
                        // Fail the retained Source, rather than report successful
                        // observation of an original poll/Drop failure.
                        _ => panic!("original subprocess observation failed"),
                    }
                })
            })
            .await
            .map_err(|error| error.to_string())?
    }
}
