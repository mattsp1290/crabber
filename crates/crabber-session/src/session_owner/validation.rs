use crate::session_owner::{SessionOwnerError, SessionOwnerResolver};
pub(crate) fn identifier(text: &str, max: usize, empty: bool) -> Result<(), SessionOwnerError> {
    if (!empty && text.is_empty()) || text.len() > max || text.chars().any(char::is_control) {
        return Err(SessionOwnerError::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn lease(owner: &str, ms: u64) -> Result<(), SessionOwnerError> {
    identifier(owner, 512, false)?;
    if ms == 0 || ms > 900_000 {
        return Err(SessionOwnerError::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn resolver(claim: &SessionOwnerResolver) -> Result<(), SessionOwnerError> {
    identifier(&claim.resolver_id, 512, false)?;
    identifier(&claim.authority_claim, 4096, false)
}

pub(crate) fn context_text(text: &str, max: usize) -> Result<(), SessionOwnerError> {
    if text.len() > max {
        return Err(SessionOwnerError::InvalidRequest);
    }
    crate::ensure_storable_text(text).map_err(|_| SessionOwnerError::InvalidRequest)
}
