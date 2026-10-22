//! Retained-admission inputs shared by private fault and transition fixtures.
use super::{
    AdmissionExecutionCapsule, AdmissionKey, AdmissionOptions, AdmissionRequestData, AdmitRequest,
    BoundKeyedAdmitRequest, ContentBlock, Duration, ExecutionBindingCapsule,
    HOST_BEHAVIOR_COMPONENT, InputFingerprint, KeyedAdmitRequest, Message, MessageId,
    PROVIDER_BINDING_COMPONENT, Part, PartId, PartKind, ProviderBindingDescriptor,
    ProviderBindingLimits, Role, SessionId, admission_config_hash,
};
use serde_json::json;
use time::OffsetDateTime;
fn fingerprint(c: char) -> InputFingerprint {
    InputFingerprint::new(c.to_string().repeat(64)).unwrap()
}

pub(crate) fn fixture(
    session: &SessionId,
    now: OffsetDateTime,
    backend: &str,
    behavior: char,
) -> BoundKeyedAdmitRequest {
    let data = AdmissionRequestData {
        selection_policy: crate::AdmissionSelectionPolicy::Explicit,
        session_id: session.clone(),
        workspace_id: "bound-workspace".into(),
        directory: "/bound/workspace".into(),
        title: "private-title".into(),
        text: "private-input".into(),
        provider_id: "provider".into(),
        model_id: "model".into(),
        system_prompt: Some("private-system".into()),
        max_output_tokens: Some(32),
        model_controls: crabber_core::ModelControls::default(),
    };
    let provider = ProviderBindingDescriptor::new(
        "provider",
        "model",
        backend,
        "version-1",
        fingerprint('c'),
        ProviderBindingLimits {
            context_tokens: Some(1024),
            output_tokens: Some(64),
        },
    )
    .unwrap();
    let behavior_fingerprint = fingerprint(behavior);
    let semantics = json!(["crabber.runtime.admission.v1", "provider", "model", data.system_prompt, data.max_output_tokens, "Sequential", 0.85, 8, 64, [], [], [], [
        {"id": PROVIDER_BINDING_COMPONENT, "version": serde_json::to_string(&provider).unwrap()},
        {"id": HOST_BEHAVIOR_COMPONENT, "version": behavior_fingerprint.as_str()},
    ], [], [], {"reasoning_effort": null}]);
    let config_hash = InputFingerprint::new(admission_config_hash(&semantics)).unwrap();
    let binding = ExecutionBindingCapsule {
        input_fingerprint: None,
        input_envelope: None,
        version: 2,
        provider,
        request: data.clone(),
        runtime_semantics: semantics.clone(),
        config_hash: config_hash.clone(),
        plan_fingerprint: fingerprint('d'),
        behavior_fingerprint: behavior_fingerprint.clone(),
    };
    let message_id = MessageId::new();
    let request = AdmitRequest {
        input_envelope: None,
        additional_messages: Vec::new(),
        expected_revision: None,
        session_id: Some(session.clone()),
        workspace_id: data.workspace_id.clone(),
        directory: data.directory.clone(),
        title: data.title.clone(),
        user_message: Message {
            id: message_id.clone(),
            session_id: session.clone(),
            run_id: None,
            role: Role::User,
            parent_id: None,
            parts: vec![Part {
                id: PartId::new(),
                message_id,
                ordinal: 0,
                kind: PartKind::UserInputText,
                content: ContentBlock::Text {
                    text: data.text.clone(),
                },
            }],
            created_at: now,
        },
        config_hash: config_hash.as_str().to_owned(),
        plan_fingerprint: binding.plan_fingerprint.as_str().to_owned(),
        owner: "private-owner".into(),
        lease: Duration::from_secs(30),
    };
    let mut keyed = KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key: AdmissionKey::new("bound-admission").unwrap(),
            fingerprint: fingerprint('a'),
            behavior_fingerprint,
        },
        execution: None,
    };
    keyed.execution = Some(AdmissionExecutionCapsule {
        input_envelope: None,
        version: 1,
        request: data,
        runtime_semantics: semantics,
        config_hash: config_hash.as_str().to_owned(),
        plan_fingerprint: binding.plan_fingerprint.as_str().to_owned(),
        fingerprint: keyed.options.fingerprint.clone(),
        behavior_fingerprint: keyed.options.behavior_fingerprint.clone(),
        semantic_digest: keyed.semantic_digest().unwrap(),
    });
    BoundKeyedAdmitRequest { keyed, binding }
}
