//! Exact optional resource/service linkage; absent fields retain the legacy codec.
use super::{Error, Record, ReserveSessionOwner, SessionOwnerGeneration, SessionOwnerState};
use crate::session_owner::preparation;
pub(super) fn validate(record: &Record) -> Result<(), Error> {
    if record.unkeyed_input.is_some()
        && (record.version != 2
            || record.retained_resources.is_none()
            || record.snapshot.state != SessionOwnerState::Joined
            || record.preparation.as_ref().is_none_or(|origin| {
                !matches!(origin.key, preparation::PreparationKey::Unkeyed(_))
                    || origin.purpose != preparation::PreparationPurpose::OrdinaryAdmission
            }))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    if let Some(driver) = &record.driver {
        if record.version != 2
            || record.preparation.is_some()
            || record.resource.is_some()
            || record.service.is_some()
            || record.retained_resources.is_some()
            || record.parent.is_some()
        {
            return Err(Error::UnknownStoreOutcome);
        }
        driver
            .fingerprint(&ReserveSessionOwner {
                id: record.origin.clone(),
                binding: record.snapshot.binding.clone(),
                owner: record.owner.clone(),
                lease_ms: 1,
            })
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(service) = &record.service {
        if record.version != 2
            || record.driver.is_some()
            || record.preparation.is_some()
            || record.resource.is_some()
            || record.retained_resources.is_some()
            || record.parent.is_some()
            || record.snapshot.target.generation != SessionOwnerGeneration::INITIAL
        {
            return Err(Error::UnknownStoreOutcome);
        }
        service
            .fingerprint(&ReserveSessionOwner {
                id: record.origin.clone(),
                binding: record.snapshot.binding.clone(),
                owner: record.owner.clone(),
                lease_ms: 1,
            })
            .map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if let Some(resource) = &record.resource {
        if record.version != 2
            || record.driver.is_some()
            || record.preparation.is_some()
            || record.parent.is_some()
        {
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
