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
impl JsonToolExecutor for MetadataOnlyTool {
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

/// Pins component identities plus result-transform version 3 and the
/// native tool resource contract (version 3, capacity 64), and interaction
/// contract version 2 (no live human hooks). Workspace routing
/// values remain excluded; old execution plans fail closed on restoration.
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
        "8983ee967d21a9dc43e2823567cd348126325fcccc9e5a76543c1a6a4ec7f9a6"
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
        "dab6ef3e01dbff45e62355e88c71f6dfc778a5693efc6c833c726b0ee25032f9"
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
