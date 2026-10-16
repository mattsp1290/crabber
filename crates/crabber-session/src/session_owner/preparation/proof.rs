//! Pure validation of independently selected, bounded preparation publications.
use super::PreparationGrant;
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error, SessionOwnerOperation,
    SessionOwnerState, SessionOwnerTermination,
    audit::{Audit, RevisionState},
    record::Record,
};
use crabber_core::SessionRevision;
use time::OffsetDateTime;

pub(crate) const MAX_PUBLICATIONS: usize = 64;

/// This proof never escapes the transaction that authenticated it.
#[derive(Clone, Copy)]
pub(crate) struct VerifiedPreparation<'a> {
    pub grant: &'a PreparationGrant,
    pub effective_revision: SessionRevision,
}
impl VerifiedPreparation<'_> {
    pub fn control_revision(
        &self,
        key: &super::PreparationKey,
        session: &crabber_core::SessionId,
        run: &crabber_core::RunId,
        original_revision: SessionRevision,
    ) -> Result<SessionRevision, Error> {
        if self.grant.original_reservation().key != *key
            || self.grant.original_reservation().owner.binding.session_id != *session
            || self.grant.original_reservation().expected_revision != original_revision
            || !matches!(&self.grant.original_reservation().purpose, super::PreparationPurpose::ExistingRun { run_id, .. } if run_id == run)
        {
            return Err(Error::SemanticConflict);
        }
        Ok(self.effective_revision)
    }
    pub fn verify_queue(&self, request: &crate::QueueMutation) -> Result<(), crate::StoreError> {
        let super::PreparationPurpose::Queue { authority } =
            &self.grant.original_reservation().purpose
        else {
            return Err(crate::StoreError::AdmissionConflict);
        };
        let (crate::QueueAction::Prepare {
            authority: actual, ..
        }
        | crate::QueueAction::Admit {
            authority: actual, ..
        }) = &request.action
        else {
            return Err(crate::StoreError::AdmissionConflict);
        };
        if actual != authority
            || request.session_id != self.grant.original_reservation().owner.binding.session_id
            || request.expected_revision != self.grant.original_reservation().expected_revision
        {
            return Err(crate::StoreError::AdmissionConflict);
        }
        Ok(())
    }
    pub fn verify_admission(&self, request: &crate::AdmitRequest) -> Result<(), crate::StoreError> {
        if request
            .session_id
            .as_ref()
            .unwrap_or(&request.user_message.session_id)
            != &self.grant.original_reservation().owner.binding.session_id
            || request.workspace_id != self.grant.original_reservation().owner.binding.workspace_id
            || request.directory != self.grant.original_reservation().owner.binding.directory
            || request.expected_revision
                != Some(self.grant.original_reservation().expected_revision)
        {
            return Err(crate::StoreError::AdmissionConflict);
        }
        Ok(())
    }
}

pub(crate) fn verify<'a>(
    grant: &'a PreparationGrant,
    record: &Record,
    audits: &[Audit],
    revision: RevisionState,
    now: OffsetDateTime,
) -> Result<VerifiedPreparation<'a>, Error> {
    let request = &grant.original_reservation();
    if record.snapshot.state != SessionOwnerState::Active
        || record.lease_until <= now
        || !grant.owner_authority().authenticates(
            &record.snapshot.binding.session_id,
            &record.snapshot.target,
            &record.token,
        )
        || record.preparation.as_ref() != Some(&crate::session_owner::preparation::origin(request))
        || record.snapshot.binding != request.owner.binding
    {
        return Err(Error::StaleOwner);
    }
    if audits.is_empty() || audits.len() > MAX_PUBLICATIONS {
        return Err(Error::Capacity);
    }
    let mut expected = None;
    let mut sequence = 0;
    let mut saw_begin = false;
    for audit in audits {
        audit.validate()?;
        if audit.record.sequence <= sequence
            || audit.record.snapshot.target.id != record.snapshot.target.id
            || audit.record.snapshot.binding != record.snapshot.binding
            || audit.record.preparation != record.preparation
        {
            return Err(Error::UnknownStoreOutcome);
        }
        let publication = audit.publication.ok_or(Error::Unsupported)?;
        if !publication.public_change {
            return Err(Error::UnknownStoreOutcome);
        }
        if let Some(previous) = expected {
            if publication.before != previous {
                return Err(Error::RevisionConflict);
            }
        } else {
            let original = match publication.before {
                RevisionState::Absent(_) => SessionRevision::INITIAL,
                RevisionState::Present(value) => value,
            };
            if audit.receipt.operation != SessionOwnerOperation::Reserve
                || audit.receipt.operation_id != request.owner.id
                || audit.receipt.semantic_digest != request.fingerprint()?
                || original != request.expected_revision
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        match audit.receipt.operation {
            SessionOwnerOperation::Reserve if sequence == 0 => (),
            SessionOwnerOperation::ReclaimUnstarted if !saw_begin => (),
            SessionOwnerOperation::Begin if !saw_begin => {
                if audit.receipt.operation_id
                    != crate::session_owner::preparation::identity::ids(
                        &request.key,
                        &request.owner.binding.session_id,
                    )?
                    .begin
                {
                    return Err(Error::UnknownStoreOutcome);
                }
                saw_begin = true;
            }
            _ => return Err(Error::InvalidTransition),
        }
        expected = Some(publication.after);
        sequence = audit.record.sequence;
    }
    if !saw_begin || expected != Some(revision) {
        return Err(Error::RevisionConflict);
    }
    let effective_revision = match revision {
        RevisionState::Absent(_) => SessionRevision::INITIAL,
        RevisionState::Present(value) => value.next().map_err(|_| Error::UnknownStoreOutcome)?,
    };
    Ok(VerifiedPreparation {
        grant,
        effective_revision,
    })
}

pub(crate) fn join_request(grant: &PreparationGrant) -> Result<MutateSessionOwner, Error> {
    let ids = super::identity::ids(
        &grant.original_reservation().key,
        &grant.original_reservation().owner.binding.session_id,
    )?;
    Ok(MutateSessionOwner {
        id: ids.join,
        session_id: grant
            .original_reservation()
            .owner
            .binding
            .session_id
            .clone(),
        target: grant.owner_authority().target().clone(),
        command: SessionOwnerCommand::Join {
            termination: SessionOwnerTermination::Returned,
        },
    })
}

/// Replayed native metadata cannot conceal an unretired preparation. Read only
/// the original Join receipt; current owner corruption cannot invalidate an
/// already committed exact retry or turn its metadata into fresh authority.
fn verify_join_receipt(
    grant: &PreparationGrant,
    receipt: Option<&crate::session_owner::SessionOwnerReceipt>,
) -> Result<(), Error> {
    let receipt = receipt.ok_or(Error::InvalidTransition)?;
    let expected = join_request(grant)?;
    if receipt.session_id != expected.session_id
        || receipt.operation_id != expected.id
        || receipt.semantic_digest != expected.fingerprint()?
        || receipt.operation != SessionOwnerOperation::Join
        || receipt.owner.binding != grant.original_reservation().owner.binding
        || receipt.owner.target != expected.target
        || receipt.owner.state != SessionOwnerState::Joined
    {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}

pub(crate) fn verify_join_audit(
    grant: &PreparationGrant,
    audit: Option<&Audit>,
) -> Result<(), Error> {
    let audit = audit.ok_or(Error::InvalidTransition)?;
    audit.validate()?;
    if audit.record.preparation.as_ref()
        != Some(&crate::session_owner::preparation::origin(
            grant.original_reservation(),
        ))
        || audit.record.origin != grant.original_reservation().owner.id
    {
        return Err(Error::SemanticConflict);
    }
    verify_join_receipt(grant, Some(&audit.receipt))
}
