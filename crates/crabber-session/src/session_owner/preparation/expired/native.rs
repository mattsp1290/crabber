//! Exact source and original-CAS guard for the real native claim transaction.
use super::{Error, ExpiredRunPreparation};
use crate::session_owner::preparation::{
    ExpiredRunOutcome, ExpiredRunReceipt, PreparationKey, PreparationSource,
    proof::VerifiedPreparation,
};
use crabber_core::{Run, RunFence, SessionRevision};
pub(crate) fn completion_source(
    source: &ExpiredRunPreparation,
    proof: &VerifiedPreparation<'_>,
    revision: SessionRevision,
) -> Result<crate::native_parent::CompletionSource, Error> {
    super::super::source_validation::validate(
        &PreparationSource::ExpiredRun(source.clone()),
        proof.grant.original_reservation(),
    )?;
    let request = source.request();
    let key = PreparationKey::ExpiredRun {
        run_id: request.expected_fence.run_id.clone(),
        issuance: source.expected_issuance(),
    };
    let (effective, completion) = crate::native_parent::CompletionSource::control(
        Some(proof),
        crate::native_parent::CompletionOperation::ExpiredRunResume,
        &key,
        (&request.session_id, &request.expected_fence.run_id),
        source.expected_revision(),
        &source.fingerprint()?,
    )?;
    if effective != revision {
        return Err(Error::RevisionConflict);
    }
    completion.ok_or(Error::UnknownStoreOutcome)
}
pub(crate) fn replacement(
    run: &Run,
    source: &ExpiredRunPreparation,
    now: time::OffsetDateTime,
) -> Result<Run, Error> {
    let mut next = run.clone();
    next.owner.clone_from(&source.request().owner);
    next.claim_token = uuid::Uuid::new_v4().to_string();
    next.lease_until = now
        .checked_add(
            time::Duration::try_from(source.request().lease).map_err(|_| Error::InvalidRequest)?,
        )
        .ok_or(Error::InvalidRequest)?;
    next.updated_at = now;
    crate::ensure_storable(&next).map_err(|_| Error::InvalidRequest)?;
    Ok(next)
}
pub(crate) fn claimed(
    source: &ExpiredRunPreparation,
    completion: &crate::native_parent::CompletionSource,
    revision: SessionRevision,
    run: &Run,
) -> ExpiredRunOutcome {
    ExpiredRunOutcome::Claimed {
        receipt: ExpiredRunReceipt {
            session_id: run.session_id.clone(),
            run_id: run.id.clone(),
            operation_id: completion.root_join_id.clone(),
            issuance: source.expected_issuance() + 1,
            revision,
        },
        fence: RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        },
    }
}

/// Select only a fresh native-source-bound Begin. Replay is reconciled by the
/// adapter before consulting this original record or current native state.
pub(crate) fn begin_target<'a>(
    record: &'a crate::session_owner::record::Record,
    request: &crate::session_owner::MutateSessionOwner,
) -> Option<(&'a crabber_core::RunId, &'a super::ExpiredRunTarget)> {
    if !matches!(
        request.command,
        crate::session_owner::SessionOwnerCommand::Begin
    ) {
        return None;
    }
    let origin = record.preparation.as_ref()?;
    match &origin.purpose {
        super::super::PreparationPurpose::ExistingRun {
            run_id,
            work: super::super::ExistingWorkTarget::ExpiredRun(target),
        } => Some((run_id, target)),
        _ => None,
    }
}
pub(crate) fn begin_request(
    run: &Run,
    target: &super::ExpiredRunTarget,
) -> Result<super::InspectExpiredRunPreparation, Error> {
    if crate::snapshot::encoded_bytes(run, crate::MAX_RUN_BOUNDARY_BYTES + 32768).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(super::InspectExpiredRunPreparation {
        session_id: run.session_id.clone(),
        expected_fence: RunFence {
            run_id: run.id.clone(),
            claim_token: run.claim_token.clone(),
        },
        owner: target.owner.clone(),
        lease: target.lease,
    })
}
