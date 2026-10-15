//! Canonical typed admission semantics, independently verified by the store.
use crate::{KeyedAdmitRequest, PROVIDER_BINDING_COMPONENT, StoreError, admission_config_hash};
use crabber_core::{InputFingerprint, ProviderBindingDescriptor, SessionContext};

pub(crate) fn semantic_digest(keyed: &KeyedAdmitRequest) -> Result<InputFingerprint, StoreError> {
    let request = &keyed.request;
    let input = request
        .input_envelope
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?;
    let execution = keyed
        .execution
        .as_ref()
        .ok_or(StoreError::HostUnsupported)?;
    if execution.version != 2
        || execution.input_envelope.as_ref() != Some(input)
        || request.title.len() > 16384
    {
        return Err(StoreError::AdmissionConflict);
    }
    let components = execution
        .runtime_semantics
        .get(12)
        .and_then(serde_json::Value::as_array)
        .ok_or(StoreError::AdmissionConflict)?;
    let mut candidates = components.iter().filter(|component| {
        component.get("id").and_then(serde_json::Value::as_str) == Some(PROVIDER_BINDING_COMPONENT)
    });
    let encoded = candidates
        .next()
        .and_then(|component| component.get("version"))
        .and_then(serde_json::Value::as_str)
        .ok_or(StoreError::AdmissionConflict)?;
    if candidates.next().is_some() || encoded.len() > 4096 {
        return Err(StoreError::AdmissionConflict);
    }
    let provider: ProviderBindingDescriptor =
        serde_json::from_str(encoded).map_err(|_| StoreError::AdmissionConflict)?;
    let runtime = runtime_digest(
        &request.config_hash,
        &request.plan_fingerprint,
        &request.title,
    )?;
    let session = request
        .session_id
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?;
    let context = SessionContext::new(
        session.clone(),
        request.workspace_id.clone(),
        request.directory.clone(),
    );
    input.fingerprint(
        &context,
        &provider,
        &runtime,
        &keyed.options.behavior_fingerprint,
    )
}

pub(crate) fn runtime_digest(
    config: &str,
    plan: &str,
    title: &str,
) -> Result<InputFingerprint, StoreError> {
    InputFingerprint::new(config)?;
    InputFingerprint::new(plan)?;
    if title.len() > 16384 {
        return Err(StoreError::AdmissionConflict);
    }
    InputFingerprint::new(admission_config_hash(&serde_json::json!([
        "crabber.typed-admission-config.v2",
        config,
        plan,
        title,
    ])))
}
