//! Borrowed original intent hashing and exact operation binding before replay.
use super::{
    PreparationCommitError as Error, PreparationGrant, PreparationKey, PreparationPurpose,
    PreparedOperation,
};
use crate::session_owner::SessionOwnerError;
use crate::{AdmissionRequestData, BoundKeyedAdmitRequest, QueueAction};
use crabber_core::{AdmissionOptions, InputEnvelope, InputFingerprint};
use sha2::{Digest, Sha256};
use std::io::Write;
struct Writer {
    digest: Sha256,
    bytes: usize,
    cap: usize,
}
impl Write for Writer {
    fn write(&mut self, bytes: &[u8]) -> std::io::Result<usize> {
        self.bytes = self
            .bytes
            .checked_add(bytes.len())
            .filter(|count| *count <= self.cap)
            .ok_or_else(|| std::io::Error::other("preparation intent exceeds bound"))?;
        self.digest.update(bytes);
        Ok(bytes.len())
    }
    fn flush(&mut self) -> std::io::Result<()> {
        Ok(())
    }
}
/// Hash the original semantic request/input/options without materializing a
/// second JSON tree. This digest never replaces the native operation digest.
/// # Errors
/// Refuses an oversized or unencodable original intent.
pub fn admission_intent(
    request: &AdmissionRequestData,
    input: &InputEnvelope,
    options: Option<&AdmissionOptions>,
) -> Result<InputFingerprint, SessionOwnerError> {
    let mut writer = Writer {
        digest: Sha256::new(),
        bytes: 0,
        cap: 16 << 20,
    };
    serde_json::to_writer(
        &mut writer,
        &("crabber.preparation.intent.v1", request, input, options),
    )
    .map_err(|_| SessionOwnerError::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", writer.digest.finalize()))
        .map_err(|_| SessionOwnerError::InvalidRequest)
}
/// Freeze the already prepared winner without generated projection IDs or leases.
/// # Errors
/// Refuses an invalid or oversized retained binding/capsule.
pub fn queue_admission_intent(
    bound: &BoundKeyedAdmitRequest,
) -> Result<InputFingerprint, SessionOwnerError> {
    let mut writer = Writer {
        digest: Sha256::new(),
        bytes: 0,
        cap: crate::queue::MAX_RECORD,
    };
    serde_json::to_writer(
        &mut writer,
        &(
            "crabber.preparation.queue-admit.v1",
            &bound.binding,
            &bound.keyed.execution,
            &bound.keyed.options,
        ),
    )
    .map_err(|_| SessionOwnerError::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", writer.digest.finalize()))
        .map_err(|_| SessionOwnerError::InvalidRequest)
}
fn binding_scope(
    grant: &PreparationGrant,
    binding: &crate::ExecutionBindingCapsule,
) -> Result<(), Error> {
    let owner = &grant.original_reservation().owner.binding;
    if binding.request.session_id != owner.session_id
        || binding.request.workspace_id != owner.workspace_id
        || binding.request.directory != owner.directory
        || binding.behavior_fingerprint != owner.behavior_fingerprint
        || super::resource_contract(&binding.runtime_semantics)?
            != grant.original_reservation().resource_contract
    {
        return conflict();
    }
    Ok(())
}
fn conflict<T>() -> Result<T, Error> {
    Err(SessionOwnerError::SemanticConflict.into())
}
fn keyed_intent(
    grant: &PreparationGrant,
    bound: &BoundKeyedAdmitRequest,
) -> Result<InputFingerprint, Error> {
    bound.binding.validate_keyed(&bound.keyed)?;
    binding_scope(grant, &bound.binding)?;
    admission_intent(
        &bound.binding.request,
        bound
            .binding
            .input_envelope
            .as_ref()
            .ok_or(SessionOwnerError::InvalidRequest)?,
        Some(&bound.keyed.options),
    )
    .map_err(Into::into)
}
pub(crate) fn validate(
    grant: &PreparationGrant,
    operation: &PreparedOperation,
) -> Result<(), Error> {
    grant.original_reservation().fingerprint()?;
    let reserved = &grant.original_reservation();
    let intent = match operation {
        PreparedOperation::ExpiredRunResume(prepared) => {
            prepared.validate()?;
            super::source_validation::validate(
                &super::PreparationSource::ExpiredRun(prepared.source.clone()),
                reserved,
            )?;
            binding_scope(grant, &prepared.binding)?;
            prepared.source.fingerprint()?
        }
        PreparedOperation::AdmissionRecovery(recovery) => {
            verify_admission_recovery(grant, recovery)?
        }
        PreparedOperation::Admit(bound) => verify_admit(grant, bound)?,
        PreparedOperation::AdmitKeyed(bound) => verify_admit_keyed(grant, bound)?,
        PreparedOperation::Queue { request, binding } => {
            verify_queue(grant, request, binding.as_deref())?
        }
        PreparedOperation::ApprovalClaim(claim) => verify_approval_claim(grant, claim)?,
        PreparedOperation::ToolClaim(claim) => verify_tool_claim(grant, claim)?,
        PreparedOperation::FrontendClaim(claim) => verify_frontend_claim(grant, claim)?,
        PreparedOperation::FrontendRecovery(recovery) => verify_frontend_recovery(grant, recovery)?,
        PreparedOperation::ApprovalRecovery(recovery) => verify_approval_recovery(grant, recovery)?,
        PreparedOperation::ToolRecovery(recovery) => verify_tool_recovery(grant, recovery)?,
    };
    if intent != reserved.intent {
        return conflict();
    }
    Ok(())
}

fn verify_admit(
    grant: &PreparationGrant,
    bound: &crate::BoundAdmitRequest,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    bound.binding.validate(&bound.request)?;
    binding_scope(grant, &bound.binding)?;
    if !matches!(reserved.purpose, PreparationPurpose::OrdinaryAdmission)
        || !matches!(reserved.key, PreparationKey::Unkeyed(_))
        || bound.binding.request.session_id != *session
        || bound.request.expected_revision != Some(reserved.expected_revision)
    {
        return conflict();
    }
    Ok(admission_intent(
        &bound.binding.request,
        bound
            .binding
            .input_envelope
            .as_ref()
            .ok_or(SessionOwnerError::InvalidRequest)?,
        None,
    )?)
}

fn verify_admit_keyed(
    grant: &PreparationGrant,
    bound: &BoundKeyedAdmitRequest,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    if !matches!(reserved.purpose, PreparationPurpose::OrdinaryAdmission)
        || reserved.key != PreparationKey::Admission(bound.keyed.options.key.clone())
        || bound.binding.request.session_id != *session
        || bound.keyed.request.expected_revision != Some(reserved.expected_revision)
    {
        return conflict();
    }
    keyed_intent(grant, bound)
}

fn verify_queue(
    grant: &PreparationGrant,
    request: &crate::QueueMutation,
    binding: Option<&BoundKeyedAdmitRequest>,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    let PreparationPurpose::Queue { authority } = &reserved.purpose else {
        return conflict();
    };
    if reserved.key != PreparationKey::Queue(request.id.clone())
        || request.session_id != *session
        || request.expected_revision != reserved.expected_revision
    {
        return conflict();
    }
    let bound = match (&request.action, binding) {
        (
            QueueAction::Prepare {
                authority: actual,
                prepared,
            },
            None,
        ) if actual == authority => prepared.as_ref(),
        (
            QueueAction::Admit {
                authority: actual, ..
            },
            Some(bound),
        ) if actual == authority => bound,
        _ => return conflict(),
    };
    if bound.binding.request.session_id != *session {
        return conflict();
    }
    match request.action {
        QueueAction::Admit { .. } => {
            bound.binding.validate_keyed(&bound.keyed)?;
            binding_scope(grant, &bound.binding)?;
            Ok(queue_admission_intent(bound)?)
        }
        _ => keyed_intent(grant, bound),
    }
}

fn verify_approval_claim(
    grant: &PreparationGrant,
    claim: &crate::ApprovalContinuationClaim,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    if reserved.key != PreparationKey::Approval(claim.id.clone())
        || claim.session_id != *session
        || claim.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::ApprovalClaim(target) }
                    if *run_id == claim.run_id && *target == claim.continuation)
    {
        return conflict();
    }
    Ok(claim.fingerprint()?)
}

fn verify_tool_claim(
    grant: &PreparationGrant,
    claim: &crate::ToolContinuationClaim,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    if reserved.key != PreparationKey::ToolContinuation(claim.id.clone())
        || claim.session_id != *session
        || claim.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::ToolClaim(target) }
                    if *run_id == claim.run_id && *target == claim.continuation)
    {
        return conflict();
    }
    Ok(claim.fingerprint()?)
}

fn verify_approval_recovery(
    grant: &PreparationGrant,
    recovery: &crate::ApprovalContinuationRecovery,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    if reserved.key != PreparationKey::Approval(recovery.id.clone())
        || recovery.original.session_id != *session
        || recovery.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::ApprovalRecovery { continuation, attempt } }
                    if *run_id == recovery.original.run_id && *continuation == recovery.original.continuation && *attempt == recovery.attempt)
    {
        return conflict();
    }
    Ok(recovery.fingerprint()?)
}

fn verify_tool_recovery(
    grant: &PreparationGrant,
    recovery: &crate::ToolContinuationRecovery,
) -> Result<InputFingerprint, Error> {
    let reserved = &grant.original_reservation();
    let session = &reserved.owner.binding.session_id;
    if reserved.key != PreparationKey::ToolContinuation(recovery.id.clone())
        || recovery.original.session_id != *session
        || recovery.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::ToolRecovery { continuation, attempt } }
                    if *run_id == recovery.original.run_id && *continuation == recovery.original.continuation && *attempt == recovery.attempt)
    {
        return conflict();
    }
    Ok(recovery.fingerprint()?)
}

fn verify_frontend_claim(
    grant: &PreparationGrant,
    request: &crate::FrontendContinuationClaim,
) -> Result<InputFingerprint, Error> {
    let reserved = grant.original_reservation();
    let claim = &request.claim;
    if reserved.key != PreparationKey::ToolContinuation(claim.id.clone())
        || claim.session_id != reserved.owner.binding.session_id
        || claim.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::FrontendClaim(target) } if *run_id == claim.run_id && *target == claim.continuation)
    {
        return conflict();
    }
    Ok(request.fingerprint()?)
}
fn verify_frontend_recovery(
    grant: &PreparationGrant,
    request: &crate::FrontendContinuationRecovery,
) -> Result<InputFingerprint, Error> {
    let reserved = grant.original_reservation();
    let claim = &request.original.claim;
    if reserved.key != PreparationKey::ToolContinuation(request.id.clone())
        || claim.session_id != reserved.owner.binding.session_id
        || request.expected_revision != reserved.expected_revision
        || !matches!(&reserved.purpose, PreparationPurpose::ExistingRun { run_id, work: super::ExistingWorkTarget::FrontendRecovery { continuation, attempt } } if *run_id == claim.run_id && *continuation == claim.continuation && *attempt == request.attempt)
    {
        return conflict();
    }
    Ok(request.fingerprint()?)
}

fn verify_admission_recovery(
    grant: &PreparationGrant,
    recovery: &super::PreparedAdmissionRecovery,
) -> Result<InputFingerprint, Error> {
    recovery.validate()?;
    let source = &recovery.source;
    let reserved = grant.original_reservation();
    super::source_validation::validate(
        &super::PreparationSource::AdmissionRecovery(source.clone()),
        reserved,
    )?;
    binding_scope(grant, &recovery.binding.binding)?;
    Ok(source.fingerprint()?)
}

/// Hash the exact retained capsule without materializing another private tree.
pub(crate) fn execution_binding_digest(
    binding: &crate::ExecutionBindingCapsule,
) -> Result<InputFingerprint, SessionOwnerError> {
    let mut writer = Writer {
        digest: Sha256::new(),
        bytes: 0,
        cap: crate::execution_binding::MAX_BINDING_BYTES,
    };
    serde_json::to_writer(
        &mut writer,
        &("crabber.preparation.recovery-binding.v1", binding),
    )
    .map_err(|_| SessionOwnerError::InvalidRequest)?;
    InputFingerprint::new(format!("{:x}", writer.digest.finalize()))
        .map_err(|_| SessionOwnerError::InvalidRequest)
}
