use async_trait::async_trait;
use crabber_core::ToolInfo;
use serde_json::Value;
use std::collections::BTreeMap;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum PermissionDecision {
    Allow,
    Deny,
    Ask,
}

pub trait PermissionPolicy: Send + Sync {
    fn decide(&self, tool: &ToolInfo, arguments: &Value) -> PermissionDecision;
}

#[derive(Clone)]
pub struct StaticPolicy {
    default: PermissionDecision,
    rules: BTreeMap<String, PermissionDecision>,
}

impl StaticPolicy {
    #[must_use]
    pub fn new(default: PermissionDecision) -> Self {
        Self {
            default,
            rules: BTreeMap::new(),
        }
    }

    #[must_use]
    pub fn with_rule(mut self, tool: impl Into<String>, decision: PermissionDecision) -> Self {
        self.rules.insert(tool.into(), decision);
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
        self.rules.get(&tool.name).copied().unwrap_or(self.default)
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
