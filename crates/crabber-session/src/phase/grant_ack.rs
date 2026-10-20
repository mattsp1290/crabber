use super::{
    InputFingerprint, PhaseAuthority, PhaseError, PhaseQuiescence, PhaseState, PhaseTarget,
};
/// Exact private phase identity for ACK hashing; no token getter.
pub(crate) fn grant_ack_identity(authority: &PhaseAuthority) -> impl serde::Serialize + '_ {
    (
        &authority.snapshot,
        &authority.token,
        crate::attempt::grant_ack_identity(&authority.attempt),
    )
}
pub(crate) fn grant_ack_meter(authority: &PhaseAuthority) -> impl serde::Serialize + '_ {
    (
        &authority.snapshot,
        &authority.token,
        crate::attempt::grant_ack_meter(&authority.attempt),
    )
}
pub(crate) fn validate_grant_ack_authority(
    authority: &PhaseAuthority,
    parent: &crate::AttemptAuthority,
    target: &PhaseTarget,
    call_binding: &InputFingerprint,
) -> Result<(), PhaseError> {
    crate::attempt::validate_grant_ack_authority(&authority.attempt)
        .map_err(|_| PhaseError::InvalidRequest)?;
    if crate::snapshot::encoded_bytes(&authority.snapshot, 4096).is_none()
        || authority.token.is_empty()
        || authority.token.len() > 512
        || !authority.token.bytes().all(|byte| byte.is_ascii_graphic())
        || authority.snapshot.target != *target
        || authority.snapshot.call_binding != *call_binding
        || authority.snapshot.session_id != parent.snapshot.session_id
        || authority.snapshot.run_id != parent.snapshot.run_id
        || authority.snapshot.state != PhaseState::Claimed
        || authority.snapshot.quiescence != PhaseQuiescence::Pending
        || authority.attempt.snapshot != parent.snapshot
        || authority.attempt.token != parent.token
        || authority.attempt.run_token != parent.run_token
    {
        return Err(PhaseError::SemanticConflict);
    }
    Ok(())
}
