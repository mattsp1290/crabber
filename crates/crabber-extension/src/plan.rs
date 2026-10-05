use crate::RESULT_TRANSFORM_CONTRACT_VERSION;
use crate::dispatch::Dispatcher;
use crate::registry::ToolGuard;
use crate::{MountedPromptContributor, ToolContext};
use async_trait::async_trait;
use crabber_core::{SessionId, ToolInfo};
use crabber_providers::ProviderAdapter;
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

/// Id of the synthetic component that carries the result-transform contract
/// version into every fingerprint (D9).
const RESULT_TRANSFORM_CONTRACT_COMPONENT_ID: &str = "contract:crabber/tool/result-transform";

/// Computes a stable digest of the sorted component identities plus the
/// result-transform contract version, so plans frozen under another contract
/// version never match (D9). A caller component with the contract id is kept
/// beside the contract component, never merged into it.
///
/// # Panics
///
/// Panics only if serializing the fixed identity fields fails.
#[must_use]
pub fn compute_fingerprint(components: &[ComponentIdentity]) -> PlanFingerprint {
    let mut canonical = components.to_vec();
    canonical.push(ComponentIdentity {
        id: RESULT_TRANSFORM_CONTRACT_COMPONENT_ID.into(),
        version: RESULT_TRANSFORM_CONTRACT_VERSION.to_string(),
    });
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
    #[error("extension rejected point {0}")]
    Rejected(&'static str),
    #[error("tool name collision: {0}")]
    ToolCollision(String),
    #[error("prompt contributor name collision: {0}")]
    PromptContributorCollision(String),
    #[error("around handler did not call next")]
    NextNotCalled,
    #[error("around handler called next twice")]
    NextCalledTwice,
    #[error("around handler next expired")]
    NextExpired,
    #[error("around handler next called outside its callback")]
    NextOutsideCallback,
    #[error("mount cannot close itself from its callback")]
    SelfClose,
    #[error("missing host capability: {0}")]
    MissingCapability(&'static str),
    #[error("mount close timed out: {extension}")]
    MountCloseTimeout { extension: String },
    #[error("extension registry is closed")]
    RegistryClosed,
}

#[async_trait]
pub trait ToolExecutor: Send + Sync {
    async fn execute(&self, arguments: Value) -> Result<Value, ExtensionError>;
    async fn execute_with_context(
        &self,
        _context: ToolContext,
        arguments: Value,
    ) -> Result<Value, ExtensionError> {
        self.execute(arguments).await
    }
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
    pub prompt_contributors: Vec<MountedPromptContributor>,
    pub guards: Vec<Arc<dyn ToolGuard>>,
    pub restrictions: Vec<Vec<String>>,
    pub dispatcher: Dispatcher,
    pub components: Vec<ComponentIdentity>,
    pub providers: Vec<Arc<dyn ProviderAdapter>>,
    _lease: Option<Arc<PlanLease>>,
}

struct PlanLease(Box<dyn Fn() + Send + Sync>);
impl Drop for PlanLease {
    fn drop(&mut self) {
        (self.0)();
    }
}

impl RunPlan {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn from_registry(
        fingerprint: PlanFingerprint,
        tools: Vec<Arc<ToolDefinition>>,
        prompts: Vec<Arc<PromptSection>>,
        prompt_contributors: Vec<MountedPromptContributor>,
        guards: Vec<Arc<dyn ToolGuard>>,
        restrictions: Vec<Vec<String>>,
        dispatcher: Dispatcher,
        providers: Vec<Arc<dyn ProviderAdapter>>,
        components: Vec<ComponentIdentity>,
        release: impl Fn() + Send + Sync + 'static,
    ) -> Self {
        Self {
            fingerprint,
            tools,
            prompts,
            prompt_contributors,
            guards,
            restrictions,
            dispatcher,
            providers,
            components,
            _lease: Some(Arc::new(PlanLease(Box::new(release)))),
        }
    }
    #[must_use]
    pub fn fingerprint(&self) -> &PlanFingerprint {
        &self.fingerprint
    }

    pub fn release(self) {
        drop(self);
    }
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
            .map(|tool| {
                // Sort nested JSON even when a consumer enables preserve_order.
                // Serialize the struct directly to retain its stable field order.
                let mut info = tool.info.clone();
                info.parameters.sort_all_objects();
                ComponentIdentity {
                    id: format!("tool:{}", info.name),
                    version: serde_json::to_string(&info).expect("tool metadata serializes"),
                }
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
                prompt_contributors: Vec::new(),
                guards: Vec::new(),
                restrictions: Vec::new(),
                dispatcher: Dispatcher::new(Vec::new()),
                components: identities,
                providers: Vec::new(),
                _lease: None,
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

    struct MetadataOnlyTool;

    #[async_trait]
    impl ToolExecutor for MetadataOnlyTool {
        async fn execute(&self, _: Value) -> Result<Value, ExtensionError> {
            panic!("metadata test must not execute")
        }
    }

    #[test]
    fn canonical_tool_identity_retains_default_build_serialization() {
        let provider = StaticPlanProvider::new(
            vec![Arc::new(ToolDefinition {
                info: ToolInfo {
                    name: "tool".into(),
                    description: "metadata".into(),
                    parameters: serde_json::json!({"type":"object","properties":{}}),
                    retry_safe: false,
                    required_permissions: vec![],
                },
                executor: Arc::new(MetadataOnlyTool),
            })],
            Vec::new(),
        );
        // Before keyed admission, default serde_json builds serialized ToolInfo
        // fields in declaration order and nested JSON objects in sorted order.
        let retained_identity = ComponentIdentity {
            id: "tool:tool".into(),
            version: r#"{"name":"tool","description":"metadata","parameters":{"properties":{},"type":"object"},"retry_safe":false,"required_permissions":[]}"#.into(),
        };
        assert_eq!(provider.plan.components, vec![retained_identity.clone()]);
        assert_eq!(
            provider.plan.fingerprint,
            compute_fingerprint(&[retained_identity])
        );
    }

    /// Re-pinned for D9 (epic crabber-miia): the fingerprint covers component
    /// identities plus the result-transform contract component at version 2,
    /// so both hashes differ from the pre-contract values (1e222d9f..., 3defff03...,
    /// pinned at 5e3046a). Workspace values are not inputs.
    #[test]
    fn fingerprint_of_fixed_components_is_pinned() {
        let components = [
            ComponentIdentity {
                id: "example/native".into(),
                version: "1".into(),
            },
            ComponentIdentity {
                id: "example/other".into(),
                version: "2.0.0".into(),
            },
        ];
        assert_eq!(
            compute_fingerprint(&components).to_string(),
            "77ef3495189564ce7fadd89fa4b63e88b3dec1b1a92dbad900d4dc81a4cf5025"
        );
        let provider = StaticPlanProvider::new(
            Vec::new(),
            vec![Arc::new(PromptSection {
                name: "system".into(),
                order: 0,
                text: "pinned".into(),
            })],
        );
        assert_eq!(
            provider.plan.fingerprint.to_string(),
            "25d4c815b08bdb22373d9e9c93fe71a140279d68feacd4f92c56c43d40e5efce"
        );
    }

    fn pre_contract_fingerprint(components: &[ComponentIdentity]) -> String {
        let mut canonical = components.to_vec();
        canonical.sort_by(|a, b| (&a.id, &a.version).cmp(&(&b.id, &b.version)));
        let bytes = serde_json::to_vec(&canonical).unwrap();
        PlanFingerprint(Sha256::digest(bytes).into()).to_string()
    }

    #[test]
    fn contract_component_participates_in_fingerprint() {
        let components = [ComponentIdentity {
            id: "example/native".into(),
            version: "1".into(),
        }];
        let old = pre_contract_fingerprint(&components);
        assert_ne!(compute_fingerprint(&components).to_string(), old);
        assert_ne!(
            compute_fingerprint(&[]).to_string(),
            pre_contract_fingerprint(&[])
        );
    }

    /// A caller component with the contract's id and version is appended
    /// beside the contract component, not deduplicated, so it cannot
    /// reproduce a pre-contract fingerprint or the contract-free digest.
    #[test]
    fn caller_component_cannot_shadow_contract_component() {
        let contract = ComponentIdentity {
            id: RESULT_TRANSFORM_CONTRACT_COMPONENT_ID.into(),
            version: RESULT_TRANSFORM_CONTRACT_VERSION.to_string(),
        };
        let spoofed = [contract.clone()];
        assert_ne!(
            compute_fingerprint(&spoofed).to_string(),
            pre_contract_fingerprint(&spoofed)
        );
        assert_ne!(compute_fingerprint(&spoofed), compute_fingerprint(&[]));
        let other_version = [ComponentIdentity {
            version: "1".into(),
            ..contract
        }];
        assert_ne!(
            compute_fingerprint(&other_version).to_string(),
            pre_contract_fingerprint(&other_version)
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
