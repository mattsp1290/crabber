use crate::ToolContinuationError;
use crabber_core::InputFingerprint;
use serde::Serialize;
use sha2::{Digest, Sha256};
pub(crate) type Error = ToolContinuationError;
pub(crate) fn identity(text: &str, max: usize) -> Result<(), Error> {
    if text.is_empty() || text.len() > max || text.chars().any(char::is_control) {
        return Err(Error::InvalidRequest);
    }
    Ok(())
}
pub(crate) fn digest(value: &impl Serialize) -> Result<InputFingerprint, Error> {
    if crate::snapshot::encoded_bytes(value, 4 * 1024 * 1024).is_none() {
        return Err(Error::InvalidRequest);
    }
    let mut value = serde_json::to_value(value).map_err(|_| Error::InvalidRequest)?;
    value.sort_all_objects();
    InputFingerprint::new(format!(
        "{:x}",
        Sha256::digest(value.to_string().as_bytes())
    ))
    .map_err(|_| Error::InvalidRequest)
}
pub(crate) fn activated_run(
    run: &crabber_core::Run,
    owner: &str,
    lease_ms: u64,
    now: time::OffsetDateTime,
) -> Result<crabber_core::Run, Error> {
    identity(owner, 512)?;
    if lease_ms == 0 || lease_ms > 900_000 {
        return Err(Error::InvalidRequest);
    }
    let mut activated = run.clone();
    owner.clone_into(&mut activated.owner);
    activated.claim_token = uuid::Uuid::new_v4().to_string();
    activated.lease_until = now
        .checked_add(time::Duration::milliseconds(
            i64::try_from(lease_ms).map_err(|_| Error::InvalidRequest)?,
        ))
        .ok_or(Error::InvalidRequest)?;
    activated.updated_at = now;
    activated.status = crabber_core::RunStatus::Running;
    Ok(activated)
}

/// Project the native recovery control without replacing the compound frontend
/// digest. Callers still authenticate the complete original frontend results.
pub(crate) fn frontend_recovery(
    request: &crate::FrontendContinuationRecovery,
) -> crate::ToolContinuationRecovery {
    crate::ToolContinuationRecovery {
        id: request.id.clone(),
        expected_revision: request.expected_revision,
        original: request.original.claim.clone(),
        attempt: request.attempt.clone(),
        owner: request.owner.clone(),
        lease_ms: request.lease_ms,
    }
}
