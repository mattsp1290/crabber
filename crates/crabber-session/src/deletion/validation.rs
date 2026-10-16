//! Bounded routing and safe error translation, without storage or authority.
use super::DeletionError;
use crabber_core::SessionId;

pub(crate) fn validate_session(session: &SessionId) -> Result<(), DeletionError> {
    if session.0.is_empty() || session.0.len() > 512 || session.0.chars().any(char::is_control) {
        return Err(DeletionError::InvalidRequest);
    }
    Ok(())
}

#[allow(clippy::needless_pass_by_value)] // Used directly by Result::map_err.
pub(crate) fn management_error(error: crate::StoreError) -> crate::ManagementError {
    match error {
        crate::StoreError::SessionDeleted => crate::ManagementError::Deleted,
        _ => crate::ManagementError::UnknownStoreOutcome,
    }
}
#[allow(clippy::needless_pass_by_value)] // Used directly by Result::map_err.
pub(crate) fn owner_error(error: crate::StoreError) -> crate::session_owner::SessionOwnerError {
    match error {
        crate::StoreError::SessionDeleted => crate::session_owner::SessionOwnerError::Deleted,
        _ => crate::session_owner::SessionOwnerError::UnknownStoreOutcome,
    }
}
#[allow(clippy::needless_pass_by_value)] // Used directly by Result::map_err.
pub(crate) fn fork_error(error: crate::StoreError) -> crate::ForkError {
    match error {
        crate::StoreError::SessionDeleted => crate::ForkError::Deleted,
        _ => crate::ForkError::UnknownStoreOutcome,
    }
}
