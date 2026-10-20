use super::{
    Body,
    cell::{Node, Reference},
};
use crate::{
    CallbackInvocationCapability, ExtensionError, ModelAttemptContext, PromptAttemptContext,
    WorkspaceReaderResolver,
};
use futures::future::BoxFuture;
use std::sync::{Arc, Weak};
/// One projected allocation; Arc clones share its exact original reference.
pub(crate) struct Prompt {
    pub(super) node: Weak<Node>,
    pub(super) _reference: Reference,
}
impl Prompt {
    pub(crate) async fn invoke(
        &self,
        invocation: PromptAttemptContext,
        resolver: Option<Arc<dyn WorkspaceReaderResolver>>,
        creator: &CallbackInvocationCapability,
    ) -> Result<Option<String>, ()> {
        if !creator.matches_current() {
            return Err(());
        }
        let node = self.node.upgrade().ok_or(())?;
        let original = node.borrow().map_err(|_| ())?;
        let cancel = invocation.cancellation().clone();
        let observed = match &original.value().0.body {
            Body::Native(callback) => {
                crate::dispatch::original::original(
                    || -> BoxFuture<'_, _> {
                        if !creator.matches_current() {
                            return Box::pin(async { Err(ExtensionError::ContextOwnerClosed) });
                        }
                        callback(invocation)
                    },
                    |result| result.map_err(|_| ()),
                    &cancel,
                )
                .await
            }
            Body::SystemPrompt(_, callback) => {
                let typed = ModelAttemptContext::from_prompt_attempt(&invocation, resolver)
                    .with_cancellation(invocation.cancellation().clone())
                    .with_cleanup(invocation.cleanup().clone());
                crate::dispatch::original::original(
                    || -> BoxFuture<'_, _> {
                        if !creator.matches_current() {
                            return Box::pin(async { Err("prompt caller closed".into()) });
                        }
                        callback.contribute(typed)
                    },
                    |result| result.map_err(|_| ()),
                    &cancel,
                )
                .await
            }
        };
        let result = match observed {
            Ok(Some(result)) => result,
            Ok(None) => Err(()),
            Err(()) => panic!("original product prompt future observation failed"),
        };
        drop(original);
        if !creator.matches_current() {
            return Err(());
        }
        result
    }
}
