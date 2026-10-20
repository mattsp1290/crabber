use super::cell::{Node, Reference};
use crate::registry::{InvocationSource, current_callback_invocations};
use crate::{GuardContext, GuardDecision, ToolGuard};
use serde_json::Value;
use std::sync::Weak;
pub(super) struct Template(pub(super) String);
impl ToolGuard for Template {
    fn id(&self) -> &str {
        &self.0
    }
    fn check(&self, _: &str, _: &Value) -> GuardDecision {
        GuardDecision::Deny
    }
}
pub(super) struct Guard {
    pub(super) node: Weak<Node>,
    pub(super) source: InvocationSource,
    pub(super) identity: String,
    pub(super) _reference: Reference,
}
impl Guard {
    fn invoke(&self, call: impl FnOnce(&dyn ToolGuard) -> GuardDecision) -> GuardDecision {
        let Some(capability) = current_callback_invocations() else {
            return GuardDecision::Deny;
        };
        if !capability.matches_current() {
            return GuardDecision::Deny;
        }
        let Some(node) = self.node.upgrade() else {
            return GuardDecision::Deny;
        };
        let decision = self
            .source
            .invoke_inline(|| {
                if !capability.matches_current() {
                    return GuardDecision::Deny;
                }
                let Ok(original) = node.borrow() else {
                    return GuardDecision::Deny;
                };
                if original.value().0.id() != self.identity || !capability.matches_current() {
                    return GuardDecision::Deny;
                }
                call(original.value().0.as_ref())
            })
            .unwrap_or(GuardDecision::Deny);
        if capability.matches_current() {
            decision
        } else {
            GuardDecision::Deny
        }
    }
}
impl ToolGuard for Guard {
    fn id(&self) -> &str {
        &self.identity
    }
    fn check(&self, name: &str, arguments: &Value) -> GuardDecision {
        self.invoke(|body| body.check(name, arguments))
    }
    fn check_with_context(&self, context: GuardContext<'_>) -> GuardDecision {
        self.invoke(|body| body.check_with_context(context))
    }
}
