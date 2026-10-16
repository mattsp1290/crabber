//! Original root source corroboration is independent of mutable child labels.
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error, SessionOwnerOperation,
    audit::{Audit, verify_current},
    record::Record,
};

pub(crate) fn verify(
    child: &Record,
    root_reserve: &Audit,
    root_begin: &Audit,
) -> Result<(), Error> {
    let link = child.resource.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    verify_current(&root_begin.record, root_reserve, root_begin)?;
    let root = &root_reserve.record.snapshot.binding;
    let binding = &child.snapshot.binding;
    if root_reserve.receipt.operation_id != link.root_reserve
        || root_reserve.receipt.semantic_digest != link.root_digest
        || root_begin.receipt.operation_id != link.root_begin
        || root_begin.receipt.operation != SessionOwnerOperation::Begin
        || root_begin.record.snapshot.target != link.root
        || root_begin.record.preparation.is_none()
        || root.session_id != binding.session_id
        || root.workspace_id != binding.workspace_id
        || root.directory != binding.directory
        || root.behavior_fingerprint != binding.behavior_fingerprint
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let command = MutateSessionOwner {
        id: link.root_begin.clone(),
        session_id: binding.session_id.clone(),
        target: link.root.clone(),
        command: SessionOwnerCommand::Begin,
    };
    if command.fingerprint()? != root_begin.receipt.semantic_digest {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn receipt(
    grant: &super::PreparationGrant,
    reserved: &Audit,
    began: &Audit,
    root_reserve: &Audit,
    root_begin: &Audit,
) -> Result<super::PreparationResourceReceipt, Error> {
    let link = super::PreparationResourceLink::for_grant(grant)?;
    let request = super::PreparationResourceLink::reservation(grant)?;
    if reserved.receipt.semantic_digest != link.fingerprint(&request)?
        || reserved.record.resource.as_ref() != Some(&link)
    {
        return Err(Error::SemanticConflict);
    }
    verify_current(&began.record, reserved, began)?;
    verify(&reserved.record, root_reserve, root_begin)?;
    let command = MutateSessionOwner {
        id: link.identities()?.begin,
        session_id: link.session_id.clone(),
        target: began.record.snapshot.target.clone(),
        command: SessionOwnerCommand::Begin,
    };
    if began.receipt.operation != SessionOwnerOperation::Begin
        || began.receipt.operation_id != command.id
        || began.receipt.semantic_digest != command.fingerprint()?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(super::PreparationResourceReceipt {
        link,
        reserved: reserved.receipt.clone(),
        began: began.receipt.clone(),
    })
}
