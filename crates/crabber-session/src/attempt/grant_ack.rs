use super::{AttemptAuthority, AttemptError};
/// Borrowed identity for ACK hashing. Excludes the ACK seal recursively.
pub(crate) fn grant_ack_identity(authority: &AttemptAuthority) -> impl serde::Serialize + '_ {
    (&authority.snapshot, &authority.token, &authority.run_token)
}
/// Full authority accounting includes an existing ACK seal.
pub(crate) fn grant_ack_meter(authority: &AttemptAuthority) -> impl serde::Serialize + '_ {
    (
        &authority.snapshot,
        &authority.token,
        &authority.run_token,
        &authority.tool_ack_commitment,
    )
}
pub(crate) fn validate_grant_ack_authority(
    authority: &AttemptAuthority,
) -> Result<(), AttemptError> {
    if crate::snapshot::encoded_bytes(&authority.snapshot, 4096).is_none()
        || [&authority.token, &authority.run_token]
            .into_iter()
            .any(|token| {
                token.is_empty()
                    || token.len() > 512
                    || !token.bytes().all(|byte| byte.is_ascii_graphic())
            })
    {
        return Err(AttemptError::InvalidRequest);
    }
    Ok(())
}
