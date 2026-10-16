//! Pure checks over immutable metadata; never constructs an opaque join.
use super::{Error, JoinSource, OwnedMainDisposition};
use crate::session_owner::{SessionOwnerCommand, record::Record};
use crabber_core::{InputFingerprint, SessionOwnerOperationId};
pub(crate) fn verify_record(source: &JoinSource, record: &Record) -> Result<(), Error> {
    let request = source.request()?;
    super::source::matches_record(&source.activation.source, record)?;
    if (record.owned_scope.as_ref() != Some(&source.activation)
        && !(record.owned_scope.is_none()
            && source.disposition == OwnedMainDisposition::NoDispatch))
        || record.retained_resources.is_some()
        || request.session_id != record.snapshot.binding.session_id
        || request.target != record.snapshot.target
        || record.snapshot.state != crate::session_owner::SessionOwnerState::Joined
        || !matches!(request.command, SessionOwnerCommand::Join {termination} if record.snapshot.termination==Some(termination))
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

pub(crate) fn record_fingerprint(
    record: &Record,
    id: &SessionOwnerOperationId,
) -> Result<InputFingerprint, Error> {
    let source = record
        .owned_join
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    verify_record(source, record)?;
    if source.request()?.id != *id {
        return Err(Error::UnknownStoreOutcome);
    }
    source.fingerprint()
}
