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

/// Original semantic fields only. Native source readers supply the revision
/// authenticated by the original preparation, without inventing worker data.
pub(crate) fn claim_fingerprint(
    id: &crabber_core::ControlId,
    session: &crabber_core::SessionId,
    run: &crabber_core::RunId,
    continuation: &crate::ContinuationTarget,
    revision: crabber_core::SessionRevision,
    plan: &InputFingerprint,
) -> Result<InputFingerprint, Error> {
    for text in [&id.0, &continuation.id.0, &run.0] {
        identity(text, 128)?;
    }
    identity(&session.0, 512)?;
    digest(&(
        "crabber.tool-continuation.claim.v1",
        id,
        session,
        run,
        continuation,
        revision,
        plan,
    ))
}

pub(crate) fn recovery_fingerprint(
    id: &crabber_core::ControlId,
    revision: crabber_core::SessionRevision,
    original_id: &crabber_core::ControlId,
    original_digest: &InputFingerprint,
    attempt: &crate::AttemptTarget,
) -> Result<InputFingerprint, Error> {
    identity(&id.0, 128)?;
    identity(&original_id.0, 128)?;
    identity(&attempt.id.0, 128)?;
    digest(&(
        "crabber.tool-continuation.recover-unstarted.v1",
        id,
        revision,
        original_id,
        original_digest,
        attempt,
    ))
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

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn original_source_rehash_preserves_v1_canonical_bytes() {
        let id = crabber_core::ControlId::from("claim");
        let claim = claim_fingerprint(
            &id,
            &crabber_core::SessionId("session".into()),
            &crabber_core::RunId("run".into()),
            &crate::ContinuationTarget {
                id: crabber_core::ContinuationId::from("continuation"),
                generation: crabber_core::ContinuationGeneration::INITIAL,
            },
            crabber_core::SessionRevision::new(7).unwrap(),
            &InputFingerprint::new("a".repeat(64)).unwrap(),
        )
        .unwrap();
        assert_eq!(
            claim.as_str(),
            "9eaa6af9249cc3c1519cbe8a5c8ac0e4ddcd5a68770eebcf7a6e6d93a7f69fa9"
        );
        let recovery = recovery_fingerprint(
            &crabber_core::ControlId::from("recovery"),
            crabber_core::SessionRevision::new(9).unwrap(),
            &id,
            &claim,
            &crate::AttemptTarget {
                id: crabber_core::AttemptId::from("attempt"),
                generation: crabber_core::AttemptGeneration::new(2).unwrap(),
            },
        )
        .unwrap();
        assert_eq!(
            recovery.as_str(),
            "9a4f8c8dc8961aa19bb2db1e62737dbb921310701108fc2074e0298727360714"
        );
    }
}
