//! Synchronous guard admission; no detached task stands in for the original call.
use super::InvocationSource;
use crate::{GuardContext, GuardDecision, ToolGuard};
use serde_json::Value;
use std::sync::Arc;

pub(in crate::registry) fn project(
    inner: &Arc<dyn ToolGuard>,
    identity: &str,
    source: InvocationSource,
) -> Arc<dyn ToolGuard> {
    Arc::new(Guard {
        inner: inner.clone(),
        identity: identity.into(),
        source,
    })
}
struct Guard {
    inner: Arc<dyn ToolGuard>,
    identity: String,
    source: InvocationSource,
}
impl Guard {
    fn invoke(&self, call: impl FnOnce(&dyn ToolGuard) -> GuardDecision) -> GuardDecision {
        self.source
            .invoke_inline(|| {
                if self.inner.id() == self.identity {
                    call(self.inner.as_ref())
                } else {
                    GuardDecision::Deny
                }
            })
            .unwrap_or(GuardDecision::Deny)
    }
}
impl ToolGuard for Guard {
    fn id(&self) -> &str {
        &self.identity
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        self.invoke(|inner| inner.check(name, arguments))
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        self.invoke(|inner| inner.check_with_context(context))
    }
}
