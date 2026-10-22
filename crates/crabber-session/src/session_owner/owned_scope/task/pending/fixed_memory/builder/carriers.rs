//! Independent named carriers are metered before their owned fields are copied.
use super::{FixedMemoryInitialInput, InputFingerprint, PreparationCommitError, Proof};
use crate::session_owner::owned_scope::FixedInitialProfile;
use crabber_core::{AdmissionKey, InputEnvelope, SessionId};
use serde::Serialize;
fn measured(value: &impl Serialize, proof: &mut Proof) -> Result<(), PreparationCommitError> {
    proof
        .measure(value)
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity.into())
}
pub(super) fn request(
    input: &FixedMemoryInitialInput,
    profile: &FixedInitialProfile,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    // Explicit selection is omitted by AdmissionRequestData's serde contract.
    // Title moves, but charging the complete new named carrier also accounts
    // for every copied identity/provider field before its first allocation.
    #[derive(Serialize)]
    struct Request<'a> {
        session_id: &'a SessionId,
        workspace_id: &'a str,
        directory: &'a str,
        title: &'a str,
        text: &'a str,
        provider_id: &'a str,
        model_id: &'a str,
        system_prompt: Option<&'a str>,
        max_output_tokens: Option<u32>,
        model_controls: crabber_core::ModelControls,
    }
    measured(
        &Request {
            session_id: input.context.session_id(),
            workspace_id: input.context.workspace_id(),
            directory: input.context.directory(),
            title: &input.title,
            text: "",
            provider_id: profile.provider.provider_id(),
            model_id: profile.provider.model_id(),
            system_prompt: None,
            max_output_tokens: None,
            model_controls: Default::default(),
        },
        proof,
    )
}
pub(super) fn keyed(
    binding: &crate::ExecutionBindingCapsule,
    key: &AdmissionKey,
    fingerprint: &InputFingerprint,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    #[derive(Serialize)]
    struct Options<'a> {
        key: &'a AdmissionKey,
        fingerprint: &'a InputFingerprint,
        behavior_fingerprint: &'a InputFingerprint,
    }
    #[derive(Serialize)]
    struct Capsule<'a> {
        #[serde(skip_serializing_if = "Option::is_none")]
        input_envelope: Option<&'a InputEnvelope>,
        version: u32,
        request: &'a crate::AdmissionRequestData,
        runtime_semantics: &'a serde_json::Value,
        config_hash: &'a str,
        plan_fingerprint: &'a str,
        fingerprint: &'a InputFingerprint,
        behavior_fingerprint: &'a InputFingerprint,
        semantic_digest: &'a InputFingerprint,
    }
    measured(
        &Options {
            key,
            fingerprint,
            behavior_fingerprint: &binding.behavior_fingerprint,
        },
        proof,
    )?;
    // Each repeated fingerprint/behavior field is a separate owned clone in
    // these actual carriers, so the named views bill every occurrence.
    measured(
        &Capsule {
            input_envelope: binding.input_envelope.as_ref(),
            version: 2,
            request: &binding.request,
            runtime_semantics: &binding.runtime_semantics,
            config_hash: binding.config_hash.as_str(),
            plan_fingerprint: binding.plan_fingerprint.as_str(),
            fingerprint,
            behavior_fingerprint: &binding.behavior_fingerprint,
            semantic_digest: fingerprint,
        },
        proof,
    )
}
