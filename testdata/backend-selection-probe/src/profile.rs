use crabber_core::{
    AdmissionOptions, InputEnvelope, InputFingerprint, ModelControls, ProviderBindingDescriptor,
    ProviderBindingLimits, SessionId, SessionRevision,
};
use crabber_extension::ComponentIdentity;
use crabber_session::{
    AdmissionExecutionCapsule, AdmissionRequestData, AdmissionSelectionPolicy, AdmitRequest,
    BoundKeyedAdmitRequest, ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT, KeyedAdmitRequest,
    PROVIDER_BINDING_COMPONENT,
};
use sha2::{Digest, Sha256};
use std::time::Duration;

pub const RESPONSE: &str = "finite native response Ω\u{1} literal \\u0000";
pub const WORKSPACE: &str = "backend-selection";
pub const DIRECTORY: &str = "/backend-selection/host-admitted";
const BEHAVIOR: &[u8] = b"crabber.pending-initial.scripted-behavior.v2:one-turn:no-tools:no-prompts:no-guards:no-provider-factories:no-host-callbacks:no-compaction:no-heartbeat:canonical-attempt-prelude:permission-before-run-started:five-lifecycle-events:one-assistant-message:drain-before-conclusion:actual-main-and-resource-join:atomic-native-driver-retirement-and-paired-close:v2-no-retroactive-retirement";

pub fn resource_contract() -> InputFingerprint {
    InputFingerprint::new("d".repeat(64)).unwrap()
}

struct ScriptedProfile {
    provider: ProviderBindingDescriptor,
    behavior: InputFingerprint,
    semantics: serde_json::Value,
    config: String,
    plan: String,
}

fn scripted_profile() -> crate::Result<ScriptedProfile> {
    let leaf = "1:pristine-cursor0:single-stream:ordered-script:explicit-resource-drain";
    let mut recipe = Sha256::new();
    for value in [
        "crabber.pending-initial.scripted-recipe.v1",
        leaf,
        "library",
        "text",
    ] {
        recipe.update(u64::try_from(value.len())?.to_be_bytes());
        recipe.update(value.as_bytes());
    }
    recipe.update(1_u64.to_be_bytes());
    recipe.update(u64::try_from(RESPONSE.len())?.to_be_bytes());
    recipe.update(RESPONSE.as_bytes());
    let provider = ProviderBindingDescriptor::new(
        "library",
        "text",
        "crabber.library-scripted.initial",
        leaf,
        InputFingerprint::new(format!("{:x}", recipe.finalize()))?,
        ProviderBindingLimits::default(),
    )?;
    let behavior = InputFingerprint::new(format!("{:x}", Sha256::digest(BEHAVIOR)))?;
    let components = vec![
        ComponentIdentity {
            id: PROVIDER_BINDING_COMPONENT.into(),
            version: serde_json::to_string(&provider)?,
        },
        ComponentIdentity {
            id: HOST_BEHAVIOR_COMPONENT.into(),
            version: behavior.as_str().into(),
        },
        ComponentIdentity {
            id: "contract:crabber/pending-initial-scripted.v2".into(),
            version: "2".into(),
        },
    ];
    let empty: &[()] = &[];
    let mut semantics = serde_json::to_value((
        "crabber.runtime.admission.v1",
        "library",
        "text",
        None::<()>,
        None::<()>,
        "PendingInitialScriptedOneTurn",
        0.0,
        0_u32,
        1_u32,
        empty,
        empty,
        empty,
        &components,
        empty,
        empty,
        ModelControls::default(),
    ))?;
    crabber_session::session_owner::preparation::bind_resource_contract(
        &mut semantics,
        Some(&resource_contract()),
    )?;
    let config = crabber_session::admission_config_hash(&semantics);
    let plan = crabber_extension::compute_fingerprint(&components).to_string();
    Ok(ScriptedProfile {
        provider,
        behavior,
        semantics,
        config,
        plan,
    })
}

pub fn bound(
    session: &SessionId,
    revision: SessionRevision,
) -> crate::Result<BoundKeyedAdmitRequest> {
    let ScriptedProfile {
        provider,
        behavior,
        semantics,
        config,
        plan,
    } = scripted_profile()?;
    let input = InputEnvelope::text("original native input", revision)?;
    let data = AdmissionRequestData {
        selection_policy: AdmissionSelectionPolicy::Explicit,
        session_id: session.clone(),
        workspace_id: WORKSPACE.into(),
        directory: DIRECTORY.into(),
        title: "native selection proof".into(),
        text: String::new(),
        provider_id: "library".into(),
        model_id: "text".into(),
        system_prompt: None,
        max_output_tokens: None,
        model_controls: ModelControls::default(),
    };
    let mut messages = input.materialize_messages(session, time::OffsetDateTime::now_utc())?;
    let request = AdmitRequest {
        input_envelope: Some(input.clone()),
        additional_messages: messages.split_off(1),
        expected_revision: Some(revision),
        session_id: Some(session.clone()),
        workspace_id: WORKSPACE.into(),
        directory: DIRECTORY.into(),
        title: data.title.clone(),
        user_message: messages.remove(0),
        config_hash: config.clone(),
        plan_fingerprint: plan.clone(),
        owner: "backend-selection-host".into(),
        lease: Duration::from_secs(30),
    };
    let mut binding = ExecutionBindingCapsule {
        input_fingerprint: None,
        input_envelope: Some(input.clone()),
        version: 3,
        provider,
        request: data.clone(),
        runtime_semantics: semantics.clone(),
        config_hash: InputFingerprint::new(config.clone())?,
        plan_fingerprint: InputFingerprint::new(plan.clone())?,
        behavior_fingerprint: behavior.clone(),
    };
    binding.input_fingerprint = binding.computed_input_fingerprint()?;
    let placeholder = InputFingerprint::new("a".repeat(64))?;
    let mut keyed = KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key: crabber_core::AdmissionKey::new("backend-selection-native")?,
            fingerprint: binding
                .input_fingerprint
                .clone()
                .ok_or("typed fingerprint missing")?,
            behavior_fingerprint: behavior.clone(),
        },
        execution: Some(AdmissionExecutionCapsule {
            input_envelope: Some(input),
            version: 2,
            request: data,
            runtime_semantics: semantics,
            config_hash: config,
            plan_fingerprint: plan,
            fingerprint: binding
                .input_fingerprint
                .clone()
                .ok_or("typed fingerprint missing")?,
            behavior_fingerprint: behavior,
            semantic_digest: placeholder,
        }),
    };
    let digest = keyed.semantic_digest()?;
    keyed
        .execution
        .as_mut()
        .ok_or("execution missing")?
        .semantic_digest = digest;
    binding.validate_keyed(&keyed)?;
    Ok(BoundKeyedAdmitRequest { keyed, binding })
}
