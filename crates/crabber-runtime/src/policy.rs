use async_trait::async_trait;
use crabber_core::ToolInfo;
use serde_json::Value;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
    Ask,
}

pub trait PermissionPolicy: Send + Sync {
    fn decide(&self, tool: &ToolInfo, arguments: &Value) -> PermissionDecision;
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PermissionRule {
    pub pattern: String,
    pub action: PermissionDecision,
}

#[derive(Clone)]
pub struct StaticPolicy {
    default: PermissionDecision,
    rules: Vec<PermissionRule>,
}

impl StaticPolicy {
    #[must_use]
    pub fn new(default: PermissionDecision) -> Self {
        Self {
            default,
            rules: Vec::new(),
        }
    }

    #[must_use]
    pub fn with_rule(mut self, tool: impl Into<String>, decision: PermissionDecision) -> Self {
        self.rules.push(PermissionRule {
            pattern: tool.into(),
            action: decision,
        });
        self
    }
}

impl Default for StaticPolicy {
    fn default() -> Self {
        Self::new(PermissionDecision::Deny)
    }
}

impl PermissionPolicy for StaticPolicy {
    fn decide(&self, tool: &ToolInfo, _arguments: &Value) -> PermissionDecision {
        self.rules
            .iter()
            .rev()
            .find(|rule| glob_match(&rule.pattern, &tool.name))
            .map_or(self.default, |rule| rule.action)
    }
}

#[async_trait]
pub trait ApprovalRequester: Send + Sync {
    async fn approve(&self, tool: &ToolInfo, arguments: &Value) -> bool;
}

pub struct DefaultDenyApprover;

#[async_trait]
impl ApprovalRequester for DefaultDenyApprover {
    async fn approve(&self, _tool: &ToolInfo, _arguments: &Value) -> bool {
        false
    }
}

#[async_trait]
pub trait ToolPipeline: Send + Sync {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String>;
    async fn transform_result(&self, _tool: &ToolInfo, result: Value) -> Result<Value, String>;
}

pub struct IdentityToolPipeline;

#[async_trait]
impl ToolPipeline for IdentityToolPipeline {
    async fn prepare(&self, _tool: &ToolInfo, arguments: Value) -> Result<Value, String> {
        Ok(arguments)
    }
    async fn transform_result(&self, _tool: &ToolInfo, result: Value) -> Result<Value, String> {
        Ok(result)
    }
}

fn glob_match(pattern: &str, text: &str) -> bool {
    let p = pattern.as_bytes();
    let t = text.as_bytes();
    let mut matches = vec![false; t.len() + 1];
    matches[0] = true;
    for &piece in p {
        if piece == b'*' {
            for i in 1..=t.len() {
                matches[i] |= matches[i - 1];
            }
        } else {
            for i in (1..=t.len()).rev() {
                matches[i] = matches[i - 1] && (piece == b'?' || piece == t[i - 1]);
            }
            matches[0] = false;
        }
    }
    matches[t.len()]
}
#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn glob_permissions() {
        let info = ToolInfo {
            name: "shell.run".into(),
            description: String::new(),
            parameters: Value::Null,
            retry_safe: false,
            required_permissions: vec![],
        };
        let policy = StaticPolicy::new(PermissionDecision::Deny)
            .with_rule("shell.*", PermissionDecision::Ask)
            .with_rule("shell.run", PermissionDecision::Allow);
        assert_eq!(
            policy.decide(&info, &Value::Null),
            PermissionDecision::Allow
        );
        assert!(glob_match("shell.?un", "shell.run"));
        assert!(!glob_match("shell.?un", "shell.done"));
    }
}
