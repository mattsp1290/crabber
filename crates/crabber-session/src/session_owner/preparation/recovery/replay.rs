//! Original immutable recovery sources authenticate metadata before current state.
use super::super::{PreparationGrant, PreparedAdmissionRecovery, proof};
use crate::admission_journal::Init;
use crate::native_parent::{CompletionOperation, CompletionSource, RunIssuance, RunState};
use crate::session_owner::{SessionOwnerError as Error, SessionOwnerOperation, audit::Audit};
use crabber_core::{AdmissionReceipt, RunStatus, SessionOwnerOperationId};

pub(crate) fn audit_ids(grant: &PreparationGrant) -> Result<Vec<SessionOwnerOperationId>, Error> {
    let ids = super::super::identity::ids(
        &grant.original_reservation().key,
        &grant.original_reservation().owner.binding.session_id,
    )?;
    let mut result = vec![ids.reserve, ids.begin, ids.join];
    if grant.retained_resources_authority().is_some() {
        let child = super::super::PreparationResourceLink::for_grant(grant)?.identities()?;
        result.extend([child.reserve, child.begin]);
    }
    Ok(result)
}

pub(crate) struct NativeSources<'a> {
    pub previous: &'a RunIssuance,
    pub next: &'a RunIssuance,
    pub initial: &'a RunIssuance,
    pub init: &'a Init,
    pub receipt: &'a AdmissionReceipt,
}

pub(crate) fn verify(
    grant: &PreparationGrant,
    recovery: &PreparedAdmissionRecovery,
    audits: &[Audit],
    native: &NativeSources<'_>,
) -> Result<AdmissionReceipt, super::super::PreparationCommitError> {
    recovery.validate()?;
    verify_audits(grant, audits)?;
    let source = &recovery.source;
    let request = source.request();
    for issuance in [native.previous, native.next, native.initial] {
        issuance
            .validate()
            .map_err(|_| Error::UnknownStoreOutcome)?;
        if issuance.state.session_id != request.session_id
            || issuance.state.run_id != request.expected_fence.run_id
        {
            return Err(Error::SemanticConflict.into());
        }
    }
    let old = RunState::new(
        &request.session_id,
        &request.expected_fence.run_id,
        RunStatus::Running,
        &request.expected_fence.claim_token,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    if native.previous.sequence != source.expected_issuance()
        || native.previous.state != old
        || native.next.sequence != source.expected_issuance() + 1
        || native.next.version != 3
        || native.next.state.status != RunStatus::Running
        || native.next.state.token == old.token
        || native.next.completion_source.as_ref()
            != Some(&CompletionSource {
                root_join_id: audits[2].receipt.operation_id.clone(),
                root_join_digest: audits[2].receipt.semantic_digest.clone(),
                native_operation: CompletionOperation::AdmissionRecovery,
                native_semantic_digest: source.fingerprint()?,
            })
    {
        return Err(Error::SemanticConflict.into());
    }
    native
        .init
        .verify_recovery(request, native.initial, native.receipt)?;
    Ok(native.receipt.clone())
}

pub(crate) fn verify_audits(grant: &PreparationGrant, audits: &[Audit]) -> Result<(), Error> {
    let ids = audit_ids(grant)?;
    if audits.len() != ids.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    for (audit, id) in audits.iter().zip(ids) {
        audit.validate()?;
        if audit.receipt.operation_id != id
            || audit.receipt.session_id != grant.original_reservation().owner.binding.session_id
        {
            return Err(Error::SemanticConflict);
        }
    }
    proof::verify_join_audit(grant, Some(&audits[2]))?;
    let reserve = &audits[0];
    let begin = &audits[1];
    crate::session_owner::audit::verify_current(&begin.record, reserve, begin)?;
    crate::session_owner::audit::verify_current(&audits[2].record, reserve, &audits[2])?;
    if reserve.receipt.semantic_digest != grant.original_reservation().fingerprint()?
        || reserve.receipt.owner.binding != grant.original_reservation().owner.binding
        || begin.receipt.operation != SessionOwnerOperation::Begin
        || begin.record.snapshot.target != *grant.owner_authority().target()
    {
        return Err(Error::SemanticConflict);
    }
    if audits.len() == 5 {
        let child_reserve = &audits[3];
        let child_begin = &audits[4];
        crate::session_owner::audit::verify_current(
            &child_begin.record,
            child_reserve,
            child_begin,
        )?;
        super::super::resource_source::verify(&child_begin.record, reserve, begin)?;
        if child_begin.receipt.operation != SessionOwnerOperation::Begin
            || Some(&child_begin.record.snapshot.target)
                != grant
                    .retained_resources_authority()
                    .map(crate::session_owner::SessionOwnerAuthority::target)
        {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(())
}
