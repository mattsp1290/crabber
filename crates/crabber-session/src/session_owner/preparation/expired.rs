//! Pure coherent expired-run source inspection; no execution or owner grant.
use crate::session_owner::SessionOwnerError as Error;
use crabber_core::{InputFingerprint, RunFence, SessionId, SessionRevision};

/// Exact native ownership to replace after Store-clock expiry. Inspection is
/// observation only: reservation and completion independently revalidate it.
#[derive(Clone)]
pub struct InspectExpiredRunPreparation {
    pub session_id: SessionId,
    pub expected_fence: RunFence,
    pub owner: String,
    pub lease: std::time::Duration,
}
impl InspectExpiredRunPreparation {
    /// Validate bounded routing before any storage read or private clone.
    /// # Errors
    /// Refuses malformed identities or an invalid replacement lease.
    pub fn validate(&self) -> Result<(), Error> {
        for (value, cap) in [
            (self.session_id.0.as_str(), 512),
            (self.expected_fence.run_id.0.as_str(), 128),
            (self.expected_fence.claim_token.as_str(), 512),
            (self.owner.as_str(), 512),
        ] {
            crate::session_owner::identifier(value, cap, false)?;
        }
        if self.lease.is_zero() || time::Duration::try_from(self.lease).is_err() {
            return Err(Error::InvalidRequest);
        }
        Ok(())
    }
}
/// Original immutable source location. These are metadata, never authority.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub enum ExpiredRunOrigin {
    Suspended(crabber_core::ControlId),
    KeyedInitial(crabber_core::AdmissionKey),
    UnkeyedAdmission(crabber_core::SessionOwnerOperationId),
}
/// Bounded original source metadata retained in the private preparation audit.
/// No native token or dispatch capability is stored here.
#[derive(Debug, Clone, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ExpiredRunTarget {
    pub issuance: u64,
    pub issuer_digest: InputFingerprint,
    pub fence_digest: InputFingerprint,
    pub binding_digest: InputFingerprint,
    pub native_source: InputFingerprint,
    pub origin: ExpiredRunOrigin,
    pub owner: String,
    pub lease: std::time::Duration,
}
impl ExpiredRunTarget {
    /// Canonical original source metadata, for trusted adapter verification.
    /// Hashing caller-supplied metadata neither authenticates storage nor grants work.
    /// # Errors
    /// Refuses malformed, oversized or unrepresentable original targets.
    pub fn fingerprint(
        &self,
        session: &SessionId,
        run: &crabber_core::RunId,
        revision: SessionRevision,
    ) -> Result<InputFingerprint, Error> {
        crate::session_owner::identifier(&self.owner, 512, false)?;
        match &self.origin {
            ExpiredRunOrigin::Suspended(id) => crate::session_owner::identifier(&id.0, 128, false)?,
            ExpiredRunOrigin::UnkeyedAdmission(id) => {
                crate::session_owner::identifier(&id.0, 128, false)?;
            }
            ExpiredRunOrigin::KeyedInitial(key) => {
                crate::session_owner::identifier(key.as_str(), 512, false)?;
            }
        }
        if self.lease.is_zero()
            || time::Duration::try_from(self.lease).is_err()
            || self.issuance == 0
            || self.issuance >= i64::MAX as u64
            || crate::snapshot::encoded_bytes(self, 4096).is_none()
        {
            return Err(Error::InvalidRequest);
        }
        crate::session_owner::digest(&(
            "crabber.preparation.expired-run.v1",
            session,
            run,
            revision,
            self,
        ))
    }
}
/// Authenticated original source, native issuer and captured public revision.
/// This is deliberately neither serializable nor an execution capability.
#[derive(Clone)]
pub struct ExpiredRunPreparation {
    request: InspectExpiredRunPreparation,
    revision: SessionRevision,
    target: ExpiredRunTarget,
}
impl ExpiredRunPreparation {
    /// Trusted adapter inspection SPI. Call only after coherently authenticating
    /// the original native source, actual current issuer/fence/frozen capsule,
    /// and Store-clock expiry. Native mutation independently repeats the checks.
    /// # Errors
    /// Refuses malformed routing, source metadata or a mismatched replacement.
    pub fn from_verified_source(
        request: InspectExpiredRunPreparation,
        revision: SessionRevision,
        target: ExpiredRunTarget,
    ) -> Result<Self, Error> {
        let value = Self {
            request,
            revision,
            target,
        };
        value.fingerprint()?;
        Ok(value)
    }
    #[must_use]
    pub fn request(&self) -> &InspectExpiredRunPreparation {
        &self.request
    }
    #[must_use]
    pub fn expected_revision(&self) -> SessionRevision {
        self.revision
    }
    #[must_use]
    pub fn expected_issuance(&self) -> u64 {
        self.target.issuance
    }
    /// Original source association; metadata only, never current authority.
    #[must_use]
    pub fn target(&self) -> &ExpiredRunTarget {
        &self.target
    }
    /// Hash the exact source/CAS without rewriting historical input envelopes.
    /// # Errors
    /// Refuses invalid original source or replacement metadata.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        self.request.validate()?;
        if self.target.owner != self.request.owner || self.target.lease != self.request.lease {
            return Err(Error::SemanticConflict);
        }
        let status = match self.target.origin {
            ExpiredRunOrigin::Suspended(_) => crabber_core::RunStatus::Paused,
            ExpiredRunOrigin::KeyedInitial(_) | ExpiredRunOrigin::UnkeyedAdmission(_) => {
                crabber_core::RunStatus::Running
            }
        };
        let original = crate::native_parent::RunState::new(
            &self.request.session_id,
            &self.request.expected_fence.run_id,
            status,
            &self.request.expected_fence.claim_token,
        )
        .map_err(|_| Error::InvalidRequest)?;
        if original.token != self.target.fence_digest {
            return Err(Error::SemanticConflict);
        }

        self.target.fingerprint(
            &self.request.session_id,
            &self.request.expected_fence.run_id,
            self.revision,
        )
    }
    /// Canonical bounded frozen-binding digest for coherent adapter source reads.
    /// # Errors
    /// Refuses an oversized or unencodable execution capsule.
    pub fn fingerprint_binding(
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<InputFingerprint, Error> {
        super::intent::execution_binding_digest(binding)
    }
    /// Verify the exact frozen capsule before any restore or provider hook.
    /// # Errors
    /// Refuses changed or oversized execution bindings.
    pub fn verify_binding(&self, binding: &crate::ExecutionBindingCapsule) -> Result<(), Error> {
        if super::intent::execution_binding_digest(binding)? != self.target.binding_digest {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    /// Compare freshly authenticated source metadata without refreshing R.
    /// # Errors
    /// Refuses native fence, capsule or original-source drift.
    pub fn verify_source(&self, actual: &Self) -> Result<(), Error> {
        if self.request.session_id != actual.request.session_id
            || self.request.expected_fence.run_id != actual.request.expected_fence.run_id
            || self.request.expected_fence.claim_token != actual.request.expected_fence.claim_token
            || self.target != actual.target
        {
            return Err(Error::StaleOwner);
        }
        Ok(())
    }
}
/// Actual frozen capsule returned by the retained restoration task.
#[derive(Clone)]
pub struct PreparedExpiredRunResume {
    pub source: ExpiredRunPreparation,
    pub binding: Box<crate::ExecutionBindingCapsule>,
}
impl PreparedExpiredRunResume {
    /// # Errors
    /// Refuses a changed frozen execution capsule or original scope.
    pub fn validate(&self) -> Result<(), Error> {
        self.source.fingerprint()?;
        self.source.verify_binding(&self.binding)?;
        if self.binding.request.session_id != self.source.request.session_id {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}
impl std::fmt::Debug for InspectExpiredRunPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("InspectExpiredRunPreparation([redacted])")
    }
}
impl std::fmt::Debug for ExpiredRunPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ExpiredRunPreparation([redacted])")
    }
}
impl std::fmt::Debug for PreparedExpiredRunResume {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedExpiredRunResume([redacted])")
    }
}

mod suspension;
pub(crate) use suspension::suspension_digest;

pub(crate) mod history;
pub(crate) mod native;
pub(crate) mod replay;
pub(crate) use native::{claimed, completion_source, replacement};

#[cfg(all(test, any(feature = "postgres", feature = "sqlite")))]
#[path = "expired/process.rs"]
pub(crate) mod process;

pub(crate) mod unkeyed;
