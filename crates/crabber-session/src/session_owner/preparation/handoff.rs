//! Native handoff is a different immutable statement from ordinary cleanup.
use super::PreparationResourceLink;
use crate::session_owner::{
    MutateSessionOwner, ReserveSessionOwner, SessionOwnerCommand, SessionOwnerError as Error,
    SessionOwnerTarget, SessionOwnerTermination, digest, record::Record,
};
use crabber_core::{InputFingerprint, SessionOwnerOperationId};

pub(crate) fn source(record: &Record) -> Result<PreparationResourceLink, Error> {
    let origin = record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    Ok(PreparationResourceLink {
        session_id: record.snapshot.binding.session_id.clone(),
        root: record.snapshot.target.clone(),
        root_reserve: record.origin.clone(),
        root_begin: super::identity::ids(&origin.key, &record.snapshot.binding.session_id)?.begin,
        root_digest: origin.fingerprint(&ReserveSessionOwner {
            id: record.origin.clone(),
            binding: record.snapshot.binding.clone(),
            owner: record.owner.clone(),
            lease_ms: 1,
        })?,
    })
}
pub(crate) fn fingerprint(
    command: &MutateSessionOwner,
    link: &PreparationResourceLink,
    child: &SessionOwnerTarget,
) -> Result<InputFingerprint, Error> {
    if command.session_id != link.session_id
        || command.target != link.root
        || !matches!(
            command.command,
            SessionOwnerCommand::Join {
                termination: SessionOwnerTermination::Returned
            }
        )
        || child.id != link.identities()?.owner
        || child.generation != crabber_core::SessionOwnerGeneration::INITIAL
    {
        return Err(Error::UnknownStoreOutcome);
    }
    digest(&(
        "crabber.preparation.native-handoff.v1",
        command.fingerprint()?,
        link,
        child,
    ))
}
pub(crate) fn record_fingerprint(
    record: &Record,
    id: &SessionOwnerOperationId,
) -> Result<InputFingerprint, Error> {
    let command = MutateSessionOwner {
        id: id.clone(),
        session_id: record.snapshot.binding.session_id.clone(),
        target: record.snapshot.target.clone(),
        command: SessionOwnerCommand::Join {
            termination: record
                .snapshot
                .termination
                .ok_or(Error::UnknownStoreOutcome)?,
        },
    };
    match &record.retained_resources {
        Some(child) => fingerprint(&command, &source(record)?, child),
        None => command.fingerprint(),
    }
}
