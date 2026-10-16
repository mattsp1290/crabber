//! Exact optional resource linkage; absent fields retain the legacy codec.
use super::{Error, Record, ReserveSessionOwner, SessionOwnerGeneration, SessionOwnerState};
use crate::session_owner::preparation;
pub(super) fn validate(record: &Record) -> Result<(), Error> {
    if let Some(resource) = &record.resource {
        if record.version != 2 || record.preparation.is_some() || record.parent.is_some() {
            return Err(Error::UnknownStoreOutcome);
        }
        resource
            .fingerprint(&ReserveSessionOwner {
                id: record.origin.clone(),
                binding: record.snapshot.binding.clone(),
                owner: record.owner.clone(),
                lease_ms: 1,
            })
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(target) = &record.retained_resources {
        if record.snapshot.state != SessionOwnerState::Joined {
            return Err(Error::UnknownStoreOutcome);
        }
        let link = preparation::handoff::source(record)?;
        if target.id != link.identities()?.owner
            || target.generation != SessionOwnerGeneration::INITIAL
        {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    Ok(())
}
