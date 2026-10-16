//! Exact immutable request/observation sources supplement the independent latest audit.
use super::Error;
use crate::session_owner::{
    SessionOwnerOperation as Operation, SessionOwnerState,
    audit::{Audit, RevisionState},
    record::Record,
};

pub(crate) fn operation(audit: &Audit) -> Result<(), Error> {
    if !matches!(
        audit.receipt.operation,
        Operation::RequestCancel | Operation::ObserveCancel
    ) {
        return Ok(());
    }
    let value = audit
        .record
        .cancellation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if audit.record.snapshot.state == SessionOwnerState::Joined {
        return Err(Error::UnknownStoreOutcome);
    }
    let (id, digest, timestamp) = match audit.receipt.operation {
        Operation::RequestCancel => {
            if value.observed.is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
            let before = audit.publication.ok_or(Error::UnknownStoreOutcome)?.before;
            if !match (before, value.request.expected_revision) {
                (RevisionState::Absent(_), None) => true,
                (RevisionState::Present(actual), Some(expected)) => actual == expected,
                _ => false,
            } {
                return Err(Error::UnknownStoreOutcome);
            }
            (
                &value.request.id,
                value.request.fingerprint()?,
                value.requested_at,
            )
        }
        Operation::ObserveCancel => {
            let observed = value.observed.as_ref().ok_or(Error::UnknownStoreOutcome)?;
            (
                &observed.request.id,
                observed.request.fingerprint()?,
                observed.observed_at,
            )
        }
        _ => unreachable!(),
    };
    if audit.receipt.operation_id != *id
        || audit.receipt.semantic_digest != digest
        || audit.record.snapshot.updated_at != timestamp
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn sources(
    record: &Record,
    requested: Option<&Audit>,
    observed: Option<&Audit>,
) -> Result<(), Error> {
    let Some(value) = &record.cancellation else {
        return if requested.is_none() && observed.is_none() {
            Ok(())
        } else {
            Err(Error::UnknownStoreOutcome)
        };
    };
    let source = requested.ok_or(Error::UnknownStoreOutcome)?;
    source.validate()?;
    let original = source
        .record
        .cancellation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    if source.receipt.operation != Operation::RequestCancel
        || source.record.snapshot.binding != record.snapshot.binding
        || original.request != value.request
        || original.requested_at != value.requested_at
    {
        return Err(Error::UnknownStoreOutcome);
    }
    match (&value.observed, observed) {
        (None, None) => Ok(()),
        (Some(accepted), Some(source)) => {
            source.validate()?;
            let original = source
                .record
                .cancellation
                .as_ref()
                .ok_or(Error::UnknownStoreOutcome)?;
            if source.receipt.operation != Operation::ObserveCancel
                || source.record.snapshot.binding != record.snapshot.binding
                || original.request != value.request
                || original.requested_at != value.requested_at
                || original.observed.as_ref() != Some(accepted)
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok(())
        }
        _ => Err(Error::UnknownStoreOutcome),
    }
}
