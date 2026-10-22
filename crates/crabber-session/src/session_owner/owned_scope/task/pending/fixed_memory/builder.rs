//! Actual typed admission values; no fixture, supplied profile or factory.
mod carriers;
mod projection;
use super::FixedMemoryInitialInput;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::{PreparationCommitError, PreparedOperation};
use crate::{
    AdmissionExecutionCapsule, AdmissionRequestData, AdmitRequest, BoundAdmitRequest,
    BoundKeyedAdmitRequest, ExecutionBindingCapsule, KeyedAdmitRequest, StoreError,
};
use crabber_core::{AdmissionKind, AdmissionOptions, InputFingerprint};
use crabber_extension::LibraryScriptedSource;
use std::time::Duration;
use time::OffsetDateTime;

pub(super) struct PreparedValues {
    pub operation: PreparedOperation,
    pub resource_contract: InputFingerprint,
    pub behavior: InputFingerprint,
}

pub(super) fn validate(
    input: &FixedMemoryInitialInput,
    source: &LibraryScriptedSource,
    proof: &mut Proof,
) -> Result<(), PreparationCommitError> {
    let context = &input.context;
    proof
        .measure(&(
            context.session_id(),
            context.workspace_id(),
            context.directory(),
            &input.title,
            &input.input,
            &input.key,
        ))
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    crate::public_run::validate_session(context.session_id())?;
    if context.workspace_id().is_empty()
        || context.workspace_id().len() > 512
        || context.directory().is_empty()
        || context.directory().len() > 16384
        || input.title.len() > 16384
        || !matches!(input.input.admission_kind(), AdmissionKind::Ordinary)
        || !input.input.frontend_tools().is_empty()
    {
        return Err(StoreError::HostUnsupported.into());
    }
    let (selection, responses) = source.recipe();
    proof
        .measure(&(&selection.provider_id, &selection.model_id, responses))
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    Ok(())
}

pub(super) fn build(
    input: FixedMemoryInitialInput,
    source: &LibraryScriptedSource,
    now: OffsetDateTime,
    proof: &mut Proof,
) -> Result<PreparedValues, PreparationCommitError> {
    validate(&input, source, proof)?;
    let contract = crate::session_owner::owned_scope::fixed_initial_resource_contract(proof)?;
    let profile: crate::session_owner::owned_scope::FixedInitialProfile =
        crate::session_owner::owned_scope::fixed_initial_profile(
            source,
            &crate::AdmissionSelectionPolicy::Explicit,
            Some(&contract),
            proof,
        )?;
    // admission_config_hash owns an independent canonical JSON copy.
    proof
        .measure(&profile.semantics)
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    let config = InputFingerprint::new(crate::admission_config_hash(&profile.semantics))
        .map_err(|_| StoreError::AdmissionConflict)?;
    let mut messages =
        projection::materialize(&input.input, input.context.session_id(), now, proof)?;
    if messages.is_empty() {
        return Err(StoreError::AdmissionConflict.into());
    }
    let first = messages.remove(0);
    carriers::request(&input, &profile, proof)?;
    let request_data = AdmissionRequestData {
        selection_policy: crate::AdmissionSelectionPolicy::Explicit,
        session_id: input.context.session_id().clone(),
        workspace_id: input.context.workspace_id().into(),
        directory: input.context.directory().into(),
        title: input.title,
        text: String::new(),
        provider_id: profile.provider.provider_id().into(),
        model_id: profile.provider.model_id().into(),
        system_prompt: None,
        max_output_tokens: None,
    };
    proof
        .measure(&input.input)
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    let mut binding = ExecutionBindingCapsule {
        input_fingerprint: None,
        input_envelope: Some(input.input.clone()),
        version: 3,
        provider: profile.provider,
        request: request_data,
        runtime_semantics: profile.semantics,
        config_hash: config,
        plan_fingerprint: InputFingerprint::new(profile.plan)
            .map_err(|_| StoreError::AdmissionConflict)?,
        behavior_fingerprint: profile.behavior,
    };
    binding.input_fingerprint = binding.computed_input_fingerprint()?;
    proof
        .measure(&(
            input.context.session_id(),
            input.context.workspace_id(),
            input.context.directory(),
            &binding.request.title,
            &binding.config_hash,
            &binding.plan_fingerprint,
        ))
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    let request = AdmitRequest {
        expected_revision: Some(input.input.expected_revision()),
        additional_messages: messages,
        input_envelope: Some(input.input),
        session_id: Some(input.context.session_id().clone()),
        workspace_id: input.context.workspace_id().into(),
        directory: input.context.directory().into(),
        title: binding.request.title.clone(),
        user_message: first,
        config_hash: binding.config_hash.as_str().into(),
        plan_fingerprint: binding.plan_fingerprint.as_str().into(),
        owner: "crabber-fixed-memory-initial".into(),
        lease: Duration::from_secs(30),
    };
    let operation = match input.key {
        None => {
            binding.validate(&request)?;
            proof
                .charge(std::mem::size_of::<BoundAdmitRequest>())
                .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
            PreparedOperation::Admit(Box::new(BoundAdmitRequest { request, binding }))
        }
        Some(key) => keyed(request, binding, key, proof)?,
    };
    proof
        .measure(match &operation {
            PreparedOperation::Admit(value) => &value.binding.behavior_fingerprint,
            PreparedOperation::AdmitKeyed(value) => &value.binding.behavior_fingerprint,
            _ => unreachable!("closed initial value builder"),
        })
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    let behavior = match &operation {
        PreparedOperation::Admit(value) => value.binding.behavior_fingerprint.clone(),
        PreparedOperation::AdmitKeyed(value) => value.binding.behavior_fingerprint.clone(),
        _ => unreachable!("closed initial value builder"),
    };
    Ok(PreparedValues {
        operation,
        resource_contract: contract,
        behavior,
    })
}

fn keyed(
    request: AdmitRequest,
    binding: ExecutionBindingCapsule,
    key: crabber_core::AdmissionKey,
    proof: &mut Proof,
) -> Result<PreparedOperation, PreparationCommitError> {
    let fingerprint = binding
        .input_fingerprint
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?;
    carriers::keyed(&binding, &key, fingerprint, proof)?;
    let keyed = KeyedAdmitRequest {
        request,
        options: AdmissionOptions {
            key,
            fingerprint: fingerprint.clone(),
            behavior_fingerprint: binding.behavior_fingerprint.clone(),
        },
        execution: Some(AdmissionExecutionCapsule {
            input_envelope: binding.input_envelope.clone(),
            version: 2,
            request: binding.request.clone(),
            runtime_semantics: binding.runtime_semantics.clone(),
            config_hash: binding.config_hash.as_str().into(),
            plan_fingerprint: binding.plan_fingerprint.as_str().into(),
            fingerprint: fingerprint.clone(),
            behavior_fingerprint: binding.behavior_fingerprint.clone(),
            semantic_digest: fingerprint.clone(),
        }),
    };
    let digest = keyed.semantic_digest()?;
    if keyed
        .execution
        .as_ref()
        .ok_or(StoreError::AdmissionConflict)?
        .semantic_digest
        != digest
    {
        return Err(StoreError::AdmissionConflict.into());
    }
    binding.validate_keyed(&keyed)?;
    proof
        .charge(std::mem::size_of::<BoundKeyedAdmitRequest>())
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    Ok(PreparedOperation::AdmitKeyed(Box::new(
        BoundKeyedAdmitRequest { keyed, binding },
    )))
}

pub(super) fn reservation(
    values: &PreparedValues,
    context: &crabber_core::SessionContext,
    proof: &mut Proof,
) -> Result<crate::session_owner::preparation::ReservePreparation, PreparationCommitError> {
    use crate::session_owner::preparation::{
        OWNED_PREPARATION_COMPONENT, PreparationKey, PreparationPurpose, ReservePreparation,
    };
    use crate::session_owner::{ReserveSessionOwner, SessionOwnerBinding, SessionOwnerKind};
    let (request, input, options) = match &values.operation {
        PreparedOperation::Admit(bound) => (
            &bound.binding.request,
            bound.binding.input_envelope.as_ref(),
            None,
        ),
        PreparedOperation::AdmitKeyed(bound) => (
            &bound.binding.request,
            bound.binding.input_envelope.as_ref(),
            Some(&bound.keyed.options),
        ),
        _ => return Err(StoreError::HostUnsupported.into()),
    };
    let input = input.ok_or(StoreError::AdmissionConflict)?;
    proof
        .measure(&(
            request,
            input,
            options,
            &values.resource_contract,
            &values.behavior,
        ))
        .map_err(|_| crate::session_owner::SessionOwnerError::Capacity)?;
    let intent = crate::session_owner::preparation::admission_intent(request, input, options)?;
    let key = options.map_or_else(
        || PreparationKey::Unkeyed(crabber_core::ControlId::new()),
        |options| PreparationKey::Admission(options.key.clone()),
    );
    let reserve = ReservePreparation {
        owner: ReserveSessionOwner {
            id: crabber_core::SessionOwnerOperationId::new(),
            binding: SessionOwnerBinding {
                session_id: context.session_id().clone(),
                workspace_id: context.workspace_id().into(),
                directory: context.directory().into(),
                id: crabber_core::SessionOwnerId::new(),
                kind: SessionOwnerKind::Preparation,
                component_id: OWNED_PREPARATION_COMPONENT.into(),
                behavior_fingerprint: values.behavior.clone(),
                resource_contract: None,
                parent: None,
            },
            owner: "crabber-fixed-memory-preparation".into(),
            lease_ms: 30_000,
        },
        key: key.clone(),
        source: None,
        resource_contract: Some(values.resource_contract.clone()),
        expected_revision: input.expected_revision(),
        purpose: PreparationPurpose::OrdinaryAdmission,
        intent,
    };
    Ok(reserve.bind_key(key)?)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn real_value_builder_requires_its_exact_cumulative_allowance() {
        let session = crabber_core::SessionId::new();
        for keyed in [false, true] {
            let source = LibraryScriptedSource::new(
                crabber_providers::Selection {
                    provider_id: "library".into(),
                    model_id: "text".into(),
                },
                vec!["answer".into()],
            )
            .unwrap();
            let input = || FixedMemoryInitialInput {
                context: crabber_core::SessionContext::new(
                    session.clone(),
                    "workspace",
                    "/workspace",
                ),
                title: "owned value builder".into(),
                input: crabber_core::InputEnvelope::text(
                    "body\n\"\\".repeat(256),
                    crabber_core::SessionRevision::INITIAL,
                )
                .unwrap(),
                key: keyed.then(|| crabber_core::AdmissionKey::new("exact-builder").unwrap()),
            };
            let mut measured = Proof::bounded(32 << 20);
            build(input(), &source, OffsetDateTime::UNIX_EPOCH, &mut measured).unwrap();
            let n = measured.used();
            let value = build(
                input(),
                &source,
                OffsetDateTime::UNIX_EPOCH,
                &mut Proof::bounded(n),
            )
            .unwrap();
            assert_eq!(
                matches!(value.operation, PreparedOperation::AdmitKeyed(_)),
                keyed
            );
            let mut short = Proof::bounded(n - 1);
            assert!(build(input(), &source, OffsetDateTime::UNIX_EPOCH, &mut short).is_err());
            assert!(short.charge(0).is_err());
            // This production value builder accepts no Store and performs no
            // Session/preparation I/O; refusal cannot have published a native row.
        }
    }
}
