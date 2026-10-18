//! Corroborate metadata against the actual pristine carrier before native Open.
//! These hashes grant no task, resource, dispatch, or storage authority.
use crate::{
    ExecutionBindingCapsule, HOST_BEHAVIOR_COMPONENT, PROVIDER_BINDING_COMPONENT,
    SELECTION_POLICY_COMPONENT,
    public_session::v2::proof::{Failure, Proof},
    session_owner::{
        SessionOwnerError,
        preparation::{JoinedPreparation, PreparationCommitError, PreparedOperation},
    },
};
use crabber_core::{InputFingerprint, ProviderBindingDescriptor, ProviderBindingLimits};
use crabber_extension::{ComponentIdentity, LibraryScriptedSource, compute_fingerprint};
use sha2::{Digest, Sha256};
#[cfg(test)]
mod binding_tests;
#[cfg(test)]
pub(crate) mod fixture;

const BACKEND: &str = "crabber.library-scripted.initial";
const LEAF: &str = "1:pristine-cursor0:single-stream:ordered-script:explicit-resource-drain";
const PROFILE_COMPONENT: &str = "contract:crabber/pending-initial-scripted.v2";
const PROFILE_VERSION: &str = "2";
const BEHAVIOR: &[u8] = b"crabber.pending-initial.scripted-behavior.v2:one-turn:no-tools:no-prompts:no-guards:no-provider-factories:no-host-callbacks:no-compaction:no-heartbeat:canonical-attempt-prelude:permission-before-run-started:five-lifecycle-events:one-assistant-message:drain-before-conclusion:actual-main-and-resource-join:atomic-native-driver-retirement-and-paired-close:v2-no-retroactive-retirement";

fn conflict() -> PreparationCommitError {
    SessionOwnerError::SemanticConflict.into()
}

fn budget(error: &Failure) -> PreparationCommitError {
    match error {
        Failure::Limited(_) => SessionOwnerError::Capacity.into(),
        Failure::Error(_) => SessionOwnerError::UnknownStoreOutcome.into(),
    }
}

fn field(hash: &mut Sha256, value: &[u8]) -> Result<(), PreparationCommitError> {
    let length = u64::try_from(value.len()).map_err(|_| conflict())?;
    hash.update(length.to_be_bytes());
    hash.update(value);
    Ok(())
}

fn describe(
    provider: &str,
    model: &str,
    responses: &[String],
    proof: &mut Proof,
) -> Result<ProviderBindingDescriptor, PreparationCommitError> {
    // Charge borrowed originals before hashing or allocating their description.
    // The concrete source already enforces these bounds; recheck them here so
    // this corroboration has no dependency on an unchecked descriptor or cast.
    let mut bytes = 8_usize;
    for route in [provider, model] {
        if route.trim().is_empty() || route.len() > 128 || route.chars().any(char::is_control) {
            return Err(conflict());
        }
        bytes = bytes.checked_add(8 + route.len()).ok_or_else(conflict)?;
    }
    if responses.len() > 64 {
        return Err(conflict());
    }
    let mut total = 0_usize;
    for response in responses {
        if response.len() > 64 * 1024 {
            return Err(conflict());
        }
        total = total.checked_add(response.len()).ok_or_else(conflict)?;
        bytes = bytes.checked_add(8 + response.len()).ok_or_else(conflict)?;
    }
    if total > 1024 * 1024 {
        return Err(conflict());
    }
    proof.charge(bytes).map_err(|error| budget(&error))?;
    let mut hash = Sha256::new();
    field(&mut hash, b"crabber.pending-initial.scripted-recipe.v1")?;
    field(&mut hash, LEAF.as_bytes())?;
    field(&mut hash, provider.as_bytes())?;
    field(&mut hash, model.as_bytes())?;
    hash.update(
        u64::try_from(responses.len())
            .map_err(|_| conflict())?
            .to_be_bytes(),
    );
    for response in responses {
        field(&mut hash, response.as_bytes())?;
    }
    let config = InputFingerprint::new(format!("{:x}", hash.finalize())).map_err(|_| conflict())?;
    proof
        .measure(&(
            provider,
            model,
            BACKEND,
            LEAF,
            &config,
            ProviderBindingLimits::default(),
        ))
        .map_err(|error| budget(&error))?;
    ProviderBindingDescriptor::new(
        provider,
        model,
        BACKEND,
        LEAF,
        config,
        // Byte bounds do not establish either kind of token limit.
        ProviderBindingLimits::default(),
    )
    .map_err(|_| conflict())
}

struct Profile {
    behavior: InputFingerprint,
    semantics: serde_json::Value,
    plan: String,
}

fn profile(
    descriptor: &ProviderBindingDescriptor,
    policy: &crate::AdmissionSelectionPolicy,
    resource_contract: Option<&InputFingerprint>,
    proof: &mut Proof,
) -> Result<Profile, PreparationCommitError> {
    let behavior =
        InputFingerprint::new(format!("{:x}", Sha256::digest(BEHAVIOR))).map_err(|_| conflict())?;
    proof.measure(descriptor).map_err(|error| budget(&error))?;
    proof
        .measure(&(
            PROVIDER_BINDING_COMPONENT,
            HOST_BEHAVIOR_COMPONENT,
            behavior.as_str(),
            PROFILE_COMPONENT,
            PROFILE_VERSION,
        ))
        .map_err(|error| budget(&error))?;
    let mut components = vec![
        ComponentIdentity {
            id: PROVIDER_BINDING_COMPONENT.into(),
            version: serde_json::to_string(descriptor).map_err(|_| conflict())?,
        },
        ComponentIdentity {
            id: HOST_BEHAVIOR_COMPONENT.into(),
            version: behavior.as_str().into(),
        },
        ComponentIdentity {
            id: PROFILE_COMPONENT.into(),
            version: PROFILE_VERSION.into(),
        },
    ];
    if !policy.is_explicit() {
        proof
            .measure(&(SELECTION_POLICY_COMPONENT, policy))
            .map_err(|error| budget(&error))?;
        components.push(ComponentIdentity {
            id: SELECTION_POLICY_COMPONENT.into(),
            version: serde_json::to_string(policy).map_err(|_| conflict())?,
        });
    }
    let empty: &[()] = &[];
    let wire = (
        "crabber.runtime.admission.v1",
        descriptor.provider_id(),
        descriptor.model_id(),
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
    );
    proof.measure(&wire).map_err(|error| budget(&error))?;
    let mut expected = serde_json::to_value(wire).map_err(|_| conflict())?;
    if let Some(contract) = resource_contract {
        proof
            .charge("crabber.runtime.preparation.resources.v1".len() + contract.as_str().len() + 8)
            .map_err(|error| budget(&error))?;
    }
    crate::session_owner::preparation::bind_resource_contract(&mut expected, resource_contract)?;
    proof.measure(&components).map_err(|error| budget(&error))?;
    Ok(Profile {
        behavior,
        semantics: expected,
        plan: compute_fingerprint(&components).to_string(),
    })
}

fn verify_profile(
    binding: &ExecutionBindingCapsule,
    descriptor: &ProviderBindingDescriptor,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    let expected = profile(
        descriptor,
        &binding.request.selection_policy,
        original
            .observed_grant()
            .original_reservation()
            .resource_contract
            .as_ref(),
        proof,
    )?;
    if binding.version != 3
        || binding.provider != *descriptor
        || binding.behavior_fingerprint != expected.behavior
        || binding.request.system_prompt.is_some()
        || binding.request.max_output_tokens.is_some()
        || binding
            .input_envelope
            .as_ref()
            .is_none_or(|input| !input.frontend_tools().is_empty())
        || binding.runtime_semantics != expected.semantics
        || binding.config_hash.as_str() != crate::admission_config_hash(&expected.semantics)
        || binding.plan_fingerprint.as_str() != expected.plan
    {
        return Err(conflict());
    }
    Ok(())
}

pub(super) fn verify(
    source: &LibraryScriptedSource,
    original: &JoinedPreparation,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    let (selection, responses) = source.recipe();
    let descriptor = describe(
        &selection.provider_id,
        &selection.model_id,
        responses,
        proof,
    )?;
    let binding = match original.observed_operation() {
        PreparedOperation::Admit(bound) => {
            proof
                .measure(&bound.binding)
                .map_err(|error| budget(&error))?;
            proof
                .measure(&bound.request)
                .map_err(|error| budget(&error))?;
            bound.binding.validate(&bound.request)?;
            &bound.binding
        }
        PreparedOperation::AdmitKeyed(bound) => {
            proof
                .measure(&bound.binding)
                .map_err(|error| budget(&error))?;
            proof
                .measure(&bound.keyed)
                .map_err(|error| budget(&error))?;
            bound.binding.validate_keyed(&bound.keyed)?;
            &bound.binding
        }
        _ => return Err(conflict()),
    };
    // Preserve authenticated policy, captured revision, request and original
    // resource declaration. Never rewrite an existing preparation to fit.
    original.verify_original_intent()?;
    verify_profile(binding, &descriptor, original, proof)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn recipe_order_boundaries_and_unicode_bytes_are_distinct() {
        let fingerprint = |items: &[&str]| {
            let responses = items
                .iter()
                .map(|value| (*value).to_owned())
                .collect::<Vec<_>>();
            let source = LibraryScriptedSource::new(
                crabber::Selection {
                    provider_id: "provider".into(),
                    model_id: "model".into(),
                },
                responses,
            )
            .unwrap();
            let (selection, responses) = source.recipe();
            describe(
                &selection.provider_id,
                &selection.model_id,
                responses,
                &mut Proof::bounded(4096),
            )
            .unwrap()
            .config_fingerprint()
            .clone()
        };
        assert_ne!(fingerprint(&["ab", "c"]), fingerprint(&["a", "bc"]));
        assert_ne!(fingerprint(&["a", "b"]), fingerprint(&["b", "a"]));
        assert_ne!(fingerprint(&["é"]), fingerprint(&["e\u{301}"]));
        assert_ne!(fingerprint(&[]), fingerprint(&[""]));
    }

    #[test]
    fn complete_borrowed_recipe_is_charged_before_description() {
        let responses = vec!["x".repeat(64 * 1024); 16];
        let mut full = Proof::bounded(2 * 1024 * 1024);
        let descriptor = describe("provider", "model", &responses, &mut full).unwrap();
        assert_eq!(descriptor.limits(), ProviderBindingLimits::default());
        let used = full.used();
        assert!(used > 1024 * 1024);
        assert!(describe("provider", "model", &responses, &mut Proof::bounded(used)).is_ok());
        assert_eq!(
            describe(
                "provider",
                "model",
                &responses,
                &mut Proof::bounded(used - 1)
            ),
            Err(SessionOwnerError::Capacity.into())
        );
    }

    #[test]
    fn tail_response_and_route_are_part_of_the_descriptor() {
        let first = vec!["first".into(), "tail".into()];
        let changed = vec!["first".into(), "changed".into()];
        let describe = |provider, model, recipe: &[String]| {
            super::describe(provider, model, recipe, &mut Proof::bounded(4096)).unwrap()
        };
        assert_ne!(describe("p", "m", &first), describe("p", "m", &changed));
        assert_ne!(describe("p", "m", &first), describe("other", "m", &first));
        assert_ne!(describe("p", "m", &first), describe("p", "other", &first));
    }

    #[test]
    fn optional_resource_marker_charges_its_complete_encoded_extension() {
        let descriptor = describe("p", "m", &[], &mut Proof::bounded(4096)).unwrap();
        let contract = InputFingerprint::new("d".repeat(64)).unwrap();
        let policy = crate::AdmissionSelectionPolicy::Explicit;
        let mut without = Proof::bounded(16 * 1024);
        let original = profile(&descriptor, &policy, None, &mut without).unwrap();
        let mut with = Proof::bounded(16 * 1024);
        let declared = profile(&descriptor, &policy, Some(&contract), &mut with).unwrap();
        assert_eq!(
            with.used() - without.used(),
            serde_json::to_vec(&declared.semantics).unwrap().len()
                - serde_json::to_vec(&original.semantics).unwrap().len(),
        );
        assert!(
            profile(
                &descriptor,
                &policy,
                Some(&contract),
                &mut Proof::bounded(with.used())
            )
            .is_ok()
        );
        assert!(matches!(
            profile(
                &descriptor,
                &policy,
                Some(&contract),
                &mut Proof::bounded(with.used() - 1)
            ),
            Err(PreparationCommitError::Owner(SessionOwnerError::Capacity)),
        ));
    }
}
