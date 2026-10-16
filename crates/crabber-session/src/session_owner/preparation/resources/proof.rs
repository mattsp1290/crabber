//! Bounded two-owner publication lineage, with no label-only exemptions.
use super::PreparationResourceLink;
use crate::session_owner::preparation::{PreparationGrant, proof::VerifiedPreparation};
use crate::session_owner::{
    SessionOwnerError as Error, SessionOwnerOperation as Operation, SessionOwnerState,
    audit::{Audit, RevisionState, verify_current},
    record::Record,
};
use time::OffsetDateTime;

pub(crate) fn verify<'a>(
    grant: &'a PreparationGrant,
    root: &Record,
    child: &Record,
    audits: &[Audit],
    revision: RevisionState,
    now: OffsetDateTime,
) -> Result<VerifiedPreparation<'a>, Error> {
    super::proof::authenticate_root(grant, root, now)?;
    let authority = grant
        .retained_resources_authority()
        .ok_or(Error::StaleOwner)?;
    let link = PreparationResourceLink::for_grant(grant)?;
    if child.resource.as_ref() != Some(&link)
        || child.snapshot.state != SessionOwnerState::Active
        || child.lease_until <= now
        || child.cancellation.is_some()
        || !authority.authenticates(
            &child.snapshot.binding.session_id,
            &child.snapshot.target,
            &child.token,
        )
    {
        return Err(Error::StaleOwner);
    }
    if audits.is_empty() || audits.len() > super::proof::MAX_PUBLICATIONS {
        return Err(Error::Capacity);
    }
    let ids = link.identities()?;
    let find = |id: &crabber_core::SessionOwnerOperationId| {
        audits
            .iter()
            .find(|audit| audit.receipt.operation_id == *id)
            .ok_or(Error::UnknownStoreOutcome)
    };
    super::resource_source::verify(child, find(&link.root_reserve)?, find(&link.root_begin)?)?;
    verify_current(
        &find(&ids.begin)?.record,
        find(&ids.reserve)?,
        find(&ids.begin)?,
    )?;
    let mut ordered: Vec<_> = audits.iter().collect();
    for audit in &ordered {
        audit.validate()?;
    }
    ordered.sort_by_key(|audit| key(audit.publication.map(|value| value.before)));
    let mut expected = None;
    let mut root_sequence = 0;
    let mut child_sequence = 0;
    let mut began = false;
    for audit in ordered {
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
                RevisionState::Absent(_) => crabber_core::SessionRevision::INITIAL,
                RevisionState::Present(value) => value,
            };
            if audit.receipt.operation_id != link.root_reserve
                || audit.receipt.semantic_digest != grant.original_reservation().fingerprint()?
                || original != grant.original_reservation().expected_revision
            {
                return Err(Error::UnknownStoreOutcome);
            }
        }
        if audit.record.snapshot.target.id == root.snapshot.target.id {
            root_publication(grant, root, audit, &mut root_sequence, &mut began)?;
        } else if audit.record.snapshot.target.id == ids.owner && began {
            child_publication(&link, audit, &mut child_sequence)?;
        } else {
            return Err(Error::InvalidTransition);
        }
        expected = Some(publication.after);
    }
    if !began
        || child_sequence != if child.owned_scope.is_some() { 3 } else { 2 }
        || expected != Some(revision)
    {
        return Err(Error::RevisionConflict);
    }
    let effective_revision = match revision {
        RevisionState::Absent(_) => crabber_core::SessionRevision::INITIAL,
        RevisionState::Present(value) => value.next().map_err(|_| Error::UnknownStoreOutcome)?,
    };
    Ok(VerifiedPreparation {
        grant,
        effective_revision,
        resource_handoff: Some(super::proof::VerifiedResourceHandoff {
            link,
            child: child.snapshot.target.clone(),
        }),
    })
}
fn key(revision: Option<RevisionState>) -> (u8, u64) {
    match revision {
        None => (0, 0),
        Some(RevisionState::Absent(value)) => (1, value),
        Some(RevisionState::Present(value)) => (2, value.get()),
    }
}
fn root_publication(
    grant: &PreparationGrant,
    root: &Record,
    audit: &Audit,
    sequence: &mut u64,
    began: &mut bool,
) -> Result<(), Error> {
    if audit.record.sequence <= *sequence
        || audit.record.snapshot.binding != root.snapshot.binding
        || audit.record.preparation != root.preparation
    {
        return Err(Error::UnknownStoreOutcome);
    }
    match audit.receipt.operation {
        Operation::Reserve if *sequence == 0 => (),
        Operation::ReclaimUnstarted if !*began => (),
        Operation::Begin if !*began => {
            if audit.receipt.operation_id != grant.original_reservation().reconciliation()?.begin_id
            {
                return Err(Error::UnknownStoreOutcome);
            }
            *began = true;
        }
        _ => return Err(Error::InvalidTransition),
    }
    *sequence = audit.record.sequence;
    Ok(())
}
fn child_publication(
    link: &PreparationResourceLink,
    audit: &Audit,
    sequence: &mut u64,
) -> Result<(), Error> {
    let ids = link.identities()?;
    if audit.record.resource.as_ref() != Some(link) {
        return Err(Error::UnknownStoreOutcome);
    }
    match (audit.receipt.operation, *sequence) {
        (Operation::Reserve, 0)
            if audit.receipt.operation_id == ids.reserve && audit.record.sequence == 1 => {}
        (Operation::Begin, 1)
            if audit.receipt.operation_id == ids.begin && audit.record.sequence == 2 => {}
        (Operation::ActivateScope, 2)
            if audit.record.owned_scope.as_ref().is_some_and(|scope| {
                scope.id == audit.receipt.operation_id
                    && scope.source.reserve == ids.reserve
                    && scope.source.begin == ids.begin
                    && scope.source.target == audit.record.snapshot.target
            }) => {}
        _ => return Err(Error::InvalidTransition),
    }
    *sequence += 1;
    Ok(())
}
