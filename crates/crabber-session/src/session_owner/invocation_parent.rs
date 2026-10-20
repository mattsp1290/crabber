//! Same original detached owner is the only authority for a nested handoff.
use super::{
    ReserveSessionOwner, SessionOwnerAuthority, SessionOwnerError as Error, SessionOwnerState,
    record::Record,
};
use crate::public_session::v2::proof::Proof;

#[path = "invocation_parent/original.rs"]
mod original;
pub(crate) use original::original;
use original::relation;
pub(crate) fn fresh<'a>(
    request: &ReserveSessionOwner,
    parent: &'a Record,
    authority: &SessionOwnerAuthority,
    now: time::OffsetDateTime,
    proof: &mut Proof,
) -> Result<&'a crate::native_parent::Witness, Error> {
    relation(&request.binding, parent)?;
    crate::native_enrollment::caller::measure_owner_authority(authority, proof)
        .map_err(|_| Error::Capacity)?;
    if !authority.authenticates(
        &parent.snapshot.binding.session_id,
        &parent.snapshot.target,
        &parent.token,
    ) || parent.snapshot.state != SessionOwnerState::Active
        || parent.lease_until <= now
    {
        return Err(Error::StaleOwner);
    }
    if parent.cancellation.is_some() {
        return Err(Error::CancellationPending);
    }
    let witness = parent.parent.as_ref().ok_or(Error::UnknownStoreOutcome)?;
    if witness.source.kind != crate::native_parent::Kind::Phase {
        return Err(Error::Unsupported);
    }
    proof.measure(witness).map_err(|_| Error::Capacity)?;
    Ok(witness)
}
