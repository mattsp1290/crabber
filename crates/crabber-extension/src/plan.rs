use async_trait::async_trait;
use crabber_core::{SessionId, ToolInfo};
use serde::{Deserialize, Serialize};
use serde_json::Value;
use sha2::{Digest, Sha256};
use std::{fmt, sync::Arc};

#[derive(Debug, Clone, PartialEq, Eq, Serialize, Deserialize)]
pub struct ComponentIdentity {
    pub id: String,
    pub version: String,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PlanFingerprint(pub [u8; 32]);

impl fmt::Display for PlanFingerprint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        for byte in self.0 {
            write!(f, "{byte:02x}")?;
        }
        Ok(())
    }
}

/// Computes a stable digest of the sorted component identities.
///
/// # Panics
///
/// Panics only if serializing the fixed identity fields fails.
#[must_use]
pub fn compute_fingerprint(components: &[ComponentIdentity]) -> PlanFingerprint {
    let mut canonical = components.to_vec();
    canonical.sort_by(|a, b| (&a.id, &a.version).cmp(&(&b.id, &b.version)));
    let bytes = serde_json::to_vec(&canonical).expect("component identities serialize");
    PlanFingerprint(Sha256::digest(bytes).into())
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PromptSection {
    pub name: String,
    pub order: i32,
    pub text: String,
}

#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ExtensionError {
    #[error("extension plan failed: {0}")]
    Plan(String),
    #[error("tool execution failed: {0}")]
    Tool(String),
}

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError>;
}

pub struct ToolDefinition {
    pub info: ToolInfo,
    pub executor: Arc<dyn ToolExecutor>,
}

#[derive(Clone)]
pub struct RunPlan {
    pub fingerprint: PlanFingerprint,
    pub tools: Vec<Arc<ToolDefinition>>,
    pub prompts: Vec<Arc<PromptSection>>,
}

impl RunPlan {
    #[must_use]
    pub fn fingerprint(&self) -> &PlanFingerprint {
        &self.fingerprint
    }

    pub fn release(self) {}
}

#[async_trait]
pub trait RunPlanProvider: Send + Sync {
    async fn acquire_plan(&self, session: &SessionId) -> Result<RunPlan, ExtensionError>;
}

#[derive(Clone)]
pub struct StaticPlanProvider {
    plan: RunPlan,
}

impl StaticPlanProvider {
    /// Creates a fixed plan with prompts sorted by order and name.
    ///
    /// # Panics
    ///
    /// Panics only if serializing a tool's fixed metadata fails.
    #[must_use]
    pub fn new(tools: Vec<Arc<ToolDefinition>>, mut prompts: Vec<Arc<PromptSection>>) -> Self {
        prompts.sort_by(|a, b| (a.order, &a.name).cmp(&(b.order, &b.name)));
        let mut identities = tools
            .iter()
            .map(|tool| ComponentIdentity {
                id: format!("tool:{}", tool.info.name),
                version: serde_json::to_string(&tool.info).expect("tool metadata serializes"),
            })
            .collect::<Vec<_>>();
        identities.extend(prompts.iter().map(|prompt| ComponentIdentity {
            id: format!("prompt:{}", prompt.name),
            version: serde_json::json!({"order":prompt.order,"text":prompt.text}).to_string(),
        }));
        Self {
            plan: RunPlan {
                fingerprint: compute_fingerprint(&identities),
                tools,
                prompts,
            },
        }
    }
}

#[async_trait]
impl RunPlanProvider for StaticPlanProvider {
    async fn acquire_plan(&self, _session: &SessionId) -> Result<RunPlan, ExtensionError> {
        Ok(self.plan.clone())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn fingerprint_is_order_independent() {
        let a = ComponentIdentity {
            id: "a".into(),
            version: "1".into(),
        };
        let b = ComponentIdentity {
            id: "b".into(),
            version: "1".into(),
        };
        assert_eq!(
            compute_fingerprint(&[a.clone(), b.clone()]),
            compute_fingerprint(&[b, a])
        );
    }

    #[test]
    fn static_plan_fingerprint_tracks_prompt_text() {
        let first = StaticPlanProvider::new(
            Vec::new(),
            vec![Arc::new(PromptSection {
                name: "system".into(),
                order: 0,
                text: "one".into(),
            })],
        );
        let second = StaticPlanProvider::new(
            Vec::new(),
            vec![Arc::new(PromptSection {
                name: "system".into(),
                order: 0,
                text: "two".into(),
            })],
        );
        assert_ne!(first.plan.fingerprint, second.plan.fingerprint);
    }
}
