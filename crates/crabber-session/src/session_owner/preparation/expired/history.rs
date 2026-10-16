//! Pure original generic-claim association, shared by cold metadata/history reads.
//! This proves neither actual worker join nor whole provider/resource policy.
use super::{
    Error, ExpiredRunTarget,
    replay::{Native, verify_native},
};
use crate::native_parent::{CompletionOperation, CompletionSource, RunIssuance};
use crate::session_owner::{
    ReserveSessionOwner, SessionOwnerOperation, SessionOwnerState, SessionOwnerTermination,
    audit::{Audit, RevisionState, verify_current},
    preparation::{
        ExistingWorkTarget, ExpiredRunReceipt, PreparationKey, PreparationPurpose,
        PreparationResourceLink,
    },
};
use crabber_core::SessionOwnerOperationId;
/// Selection metadata only. Callers authenticate all sources in `verify`.
pub(crate) fn audit_ids(
    reserve: &Audit,
    begin: &Audit,
    join: &Audit,
) -> Result<Vec<SessionOwnerOperationId>, Error> {
    if join.record.retained_resources.is_none() {
        return Ok(Vec::new());
    }
    let link = PreparationResourceLink {
        session_id: reserve.receipt.session_id.clone(),
        root: begin.record.snapshot.target.clone(),
        root_reserve: reserve.receipt.operation_id.clone(),
        root_begin: begin.receipt.operation_id.clone(),
        root_digest: reserve.receipt.semantic_digest.clone(),
    };
    let ids = link.identities()?;
    Ok(vec![ids.reserve, ids.begin])
}
/// Legacy/unminted domains return None. Supported malformed/missing sources fail
/// closed. Caller loads exact adjacent issuers and original source carriers under
/// one bounded coherent view, never reconstructing a current Run or a grant.
pub(crate) fn verify(
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &[Audit],
    native: Native<'_>,
) -> Result<Option<ExpiredRunReceipt>, Error> {
    if next
        .completion_source
        .as_ref()
        .is_none_or(|source| source.native_operation != CompletionOperation::ExpiredRunResume)
    {
        return Ok(None);
    }
    if !matches!(audits.len(), 3 | 5) {
        return Err(Error::UnknownStoreOutcome);
    }
    for issuer in [previous, next] {
        bounded(issuer, 2048)?;
        issuer.validate().map_err(unknown)?;
    }
    for audit in audits {
        bounded(audit, 32768)?;
        audit.validate()?;
    }
    let reserve = &audits[0];
    let begin = &audits[1];
    let join = &audits[2];
    verify_current(&begin.record, reserve, begin)?;
    verify_current(&join.record, reserve, join)?;
    let origin = reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let PreparationPurpose::ExistingRun {
        run_id,
        work: ExistingWorkTarget::ExpiredRun(target),
    } = &origin.purpose
    else {
        return Err(Error::SemanticConflict);
    };
    let session = &reserve.receipt.session_id;
    let ids = super::super::identity::ids(&origin.key, session)?;
    let original = ReserveSessionOwner {
        id: reserve.receipt.operation_id.clone(),
        binding: reserve.receipt.owner.binding.clone(),
        owner: reserve.record.owner.clone(),
        lease_ms: 1,
    };
    let digest = target.fingerprint(session, run_id, origin.expected_revision)?;
    if origin.key
        != (PreparationKey::ExpiredRun {
            run_id: run_id.clone(),
            issuance: target.issuance,
        })
        || reserve.receipt.operation_id != ids.reserve
        || begin.receipt.operation_id != ids.begin
        || join.receipt.operation_id != ids.join
        || reserve.receipt.semantic_digest != origin.fingerprint(&original)?
        || origin.intent != digest
        || begin.receipt.operation != SessionOwnerOperation::Begin
        || join.receipt.operation != SessionOwnerOperation::Join
        || begin.receipt.owner.state != SessionOwnerState::Active
        || join.receipt.owner.state != SessionOwnerState::Joined
        || begin.receipt.owner.target != join.receipt.owner.target
        || join.receipt.owner.termination != Some(SessionOwnerTermination::Returned)
    {
        return Err(Error::SemanticConflict);
    }
    issuer_pair(
        previous,
        next,
        target,
        session,
        run_id,
        &CompletionSource {
            root_join_id: ids.join.clone(),
            root_join_digest: join.receipt.semantic_digest.clone(),
            native_operation: CompletionOperation::ExpiredRunResume,
            native_semantic_digest: digest,
        },
    )?;
    child(audits)?;
    verify_native(session, run_id, target, previous.state.status, native)?;
    let RevisionState::Present(revision) =
        join.publication.ok_or(Error::UnknownStoreOutcome)?.after
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    Ok(Some(ExpiredRunReceipt {
        session_id: session.clone(),
        run_id: run_id.clone(),
        operation_id: ids.join,
        issuance: next.sequence,
        revision,
    }))
}
fn issuer_pair(
    previous: &RunIssuance,
    next: &RunIssuance,
    target: &ExpiredRunTarget,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    expected: &CompletionSource,
) -> Result<(), Error> {
    if previous.sequence != target.issuance
        || previous.state.token != target.fence_digest
        || next.sequence != target.issuance + 1
        || next.version != 3
        || crate::native_parent::issuance_digest(previous).map_err(unknown)? != target.issuer_digest
        || previous.state.session_id != *session
        || next.state.session_id != *session
        || previous.state.run_id != *run
        || next.state.run_id != *run
        || previous.state.status != next.state.status
        || previous.state.token == next.state.token
        || next.completion_source.as_ref() != Some(expected)
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
fn child(audits: &[Audit]) -> Result<(), Error> {
    let expected = audit_ids(&audits[0], &audits[1], &audits[2])?;
    if expected.len() + 3 != audits.len() {
        return Err(Error::UnknownStoreOutcome);
    }
    if audits.len() == 5 {
        let reserve = &audits[3];
        let begin = &audits[4];
        verify_current(&begin.record, reserve, begin)?;
        super::super::resource_source::verify(&begin.record, &audits[0], &audits[1])?;
        if reserve.receipt.operation_id != expected[0]
            || begin.receipt.operation_id != expected[1]
            || begin.receipt.operation != SessionOwnerOperation::Begin
            || audits[2].record.retained_resources.as_ref() != Some(&begin.record.snapshot.target)
        {
            return Err(Error::SemanticConflict);
        }
    }
    Ok(())
}
fn bounded(value: &impl serde::Serialize, cap: usize) -> Result<(), Error> {
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
fn unknown(_: impl std::fmt::Debug) -> Error {
    Error::UnknownStoreOutcome
}

pub(crate) fn identities(
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    issuance: u64,
) -> Result<super::super::identity::Ids, Error> {
    crate::session_owner::identifier(&session.0, 512, false)?;
    super::super::identity::ids(
        &PreparationKey::ExpiredRun {
            run_id: run.clone(),
            issuance,
        },
        session,
    )
}
pub(crate) fn target(audits: &[Audit]) -> Result<&ExpiredRunTarget, Error> {
    let reserve = audits.first().ok_or(Error::UnknownStoreOutcome)?;
    let origin = reserve
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let PreparationPurpose::ExistingRun {
        run_id,
        work: ExistingWorkTarget::ExpiredRun(target),
    } = &origin.purpose
    else {
        return Err(Error::SemanticConflict);
    };
    target.fingerprint(
        &reserve.receipt.session_id,
        run_id,
        origin.expected_revision,
    )?;
    Ok(target)
}
