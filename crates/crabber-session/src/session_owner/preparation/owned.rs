//! Correlate actual registered main observations with the original child.
use super::{PreparationGrant, PreparationResourceLink};
use crate::session_owner::{
    MutateSessionOwner, SessionOwnerCommand, SessionOwnerError as Error,
    owned_scope::OwnedScopeSource,
};

pub(super) fn verify_child_source(
    grant: &PreparationGrant,
    actual: &OwnedScopeSource,
) -> Result<(), Error> {
    let link = PreparationResourceLink::for_grant(grant)?;
    let ids = link.identities()?;
    let reservation = PreparationResourceLink::reservation(grant)?;
    let authority = grant
        .retained_resources_authority()
        .ok_or(Error::InvalidTransition)?;
    let begin = MutateSessionOwner {
        id: ids.begin.clone(),
        session_id: link.session_id.clone(),
        target: authority.target().clone(),
        command: SessionOwnerCommand::Begin,
    };
    let expected = OwnedScopeSource {
        session: link.session_id.clone(),
        target: authority.target().clone(),
        reserve: ids.reserve,
        reserve_digest: link.fingerprint(&reservation)?,
        begin: ids.begin,
        begin_digest: begin.fingerprint()?,
        binding: crate::session_owner::validation::digest(&reservation.binding)?,
    };
    if actual != &expected {
        return Err(Error::SemanticConflict);
    }
    Ok(())
}
