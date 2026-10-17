//! Private original recovery intent, captured coherently before factory work.
mod guard;
pub(crate) mod replay;
use crate::{AdmissionExecutionError as Error, ClaimUnstartedAdmissionRequest};
use crabber_core::{InputFingerprint, SessionRevision};
pub(crate) use guard::completion_source;

/// Metadata from an authenticated, expired Unstarted admission source. This
/// carries no fence grant: every fresh reservation and completion must reread
/// the exact source under the Store mutation gate. It is never serialized.
#[derive(Clone)]
pub struct AdmissionRecoveryPreparation {
    request: ClaimUnstartedAdmissionRequest,
    expected_revision: SessionRevision,
    expected_issuance: u64,
    execution_binding_digest: InputFingerprint,
}
impl std::fmt::Debug for AdmissionRecoveryPreparation {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("AdmissionRecoveryPreparation([redacted])")
    }
}
impl AdmissionRecoveryPreparation {
    /// Compute the bounded original binding fingerprint for a trusted adapter's
    /// coherent source read. This does not authenticate storage or grant work.
    /// # Errors
    /// Rejects an oversized or unencodable private capsule.
    pub fn fingerprint_binding(
        binding: &crate::ExecutionBindingCapsule,
    ) -> Result<InputFingerprint, Error> {
        super::intent::execution_binding_digest(binding).map_err(|_| Error::SemanticConflict)
    }

    /// Trusted adapter inspection SPI. Call only after coherently verifying
    /// the original receipt/Init/capsule, current native issuer/fence, and expired
    /// Unstarted eligibility with the Store clock. This constructor grants no
    /// execution authority and does not itself authenticate storage.
    /// # Errors
    /// Rejects an invalid scope, digest, lease, or absent/overflowed issuer.
    pub fn from_verified_source(
        request: ClaimUnstartedAdmissionRequest,
        expected_revision: SessionRevision,
        expected_issuance: u64,
        execution_binding_digest: InputFingerprint,
    ) -> Result<Self, Error> {
        let value = Self {
            request,
            expected_revision,
            expected_issuance,
            execution_binding_digest,
        };
        value.fingerprint()?;
        Ok(value)
    }
    #[must_use]
    pub fn request(&self) -> &ClaimUnstartedAdmissionRequest {
        &self.request
    }
    #[must_use]
    pub fn expected_revision(&self) -> SessionRevision {
        self.expected_revision
    }
    #[must_use]
    pub fn expected_issuance(&self) -> u64 {
        self.expected_issuance
    }
    /// Bound an inspection request before issuing storage reads. This validates
    /// only input representation and confers no source or execution authority.
    /// # Errors
    /// Rejects oversized identities, malformed capsule digests or invalid leases.
    pub fn validate_request(r: &ClaimUnstartedAdmissionRequest) -> Result<(), Error> {
        for (text, cap) in [
            (r.session_id.0.as_str(), 512),
            (r.key.as_str(), 512),
            (r.expected_owner.as_str(), 512),
            (r.owner.as_str(), 512),
            (r.expected_fence.claim_token.as_str(), 512),
        ] {
            crate::session_owner::identifier(text, cap, false)
                .map_err(|_| Error::SemanticConflict)?;
        }
        crate::attempt::validate_identifier(&r.expected_fence.run_id.0)
            .map_err(|_| Error::SemanticConflict)?;
        InputFingerprint::new(r.capsule_digest.clone()).map_err(|_| Error::SemanticConflict)?;
        if r.lease.is_zero() || time::Duration::try_from(r.lease).is_err() {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
    /// Hash the exact original source/CAS. Lease duration is bookkeeping;
    /// expected native fence and both original/replacement owners stay bound.
    /// # Errors
    /// Rejects malformed or oversized original source metadata.
    pub fn fingerprint(&self) -> Result<InputFingerprint, Error> {
        Self::validate_request(&self.request)?;
        let r = &self.request;
        if self.expected_issuance == 0 || self.expected_issuance >= i64::MAX as u64 {
            return Err(Error::SemanticConflict);
        }
        crate::session_owner::digest(&(
            "crabber.preparation.admission-recovery.v1",
            &r.session_id,
            &r.key,
            &r.expected_fence.run_id,
            &r.expected_fence.claim_token,
            &r.expected_owner,
            &r.fingerprint,
            &r.behavior_fingerprint,
            &r.semantic_digest,
            &r.capsule_digest,
            &r.owner,
            self.expected_revision,
            self.expected_issuance,
            &self.execution_binding_digest,
        ))
        .map_err(|_| Error::SemanticConflict)
    }
}

/// Actual restored binding returned by the retained recovery preparation task.
/// The original request/capsule is preserved; its historical input revision is
/// not rewritten to the newly captured preparation revision.
#[derive(Clone)]
pub struct PreparedAdmissionRecovery {
    pub source: AdmissionRecoveryPreparation,
    pub binding: Box<crate::BoundKeyedAdmitRequest>,
}
impl std::fmt::Debug for PreparedAdmissionRecovery {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("PreparedAdmissionRecovery([redacted])")
    }
}
impl PreparedAdmissionRecovery {
    /// # Errors
    /// Rejects a restored request or frozen execution capsule for another source.
    pub fn validate(&self) -> Result<(), Error> {
        self.source.fingerprint()?;
        let original = self.source.request();
        let bound = &self.binding;
        bound
            .binding
            .validate_keyed(&bound.keyed)
            .map_err(|_| Error::SemanticConflict)?;
        let execution = bound
            .keyed
            .execution
            .as_ref()
            .ok_or(Error::MissingEvidence)?;
        if bound.keyed.options.key != original.key
            || bound.binding.request.session_id != original.session_id
            || bound.keyed.request.owner != original.owner
            || bound.keyed.options.fingerprint != original.fingerprint
            || bound.keyed.options.behavior_fingerprint != original.behavior_fingerprint
            || bound
                .keyed
                .semantic_digest()
                .map_err(|_| Error::SemanticConflict)?
                != original.semantic_digest
            || execution.digest()? != original.capsule_digest
            || super::intent::execution_binding_digest(&bound.binding)
                .map_err(|_| Error::SemanticConflict)?
                != self.source.execution_binding_digest
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}

impl AdmissionRecoveryPreparation {
    /// Check a retained capsule against this exact original coherent source.
    /// This pure adapter SPI grants no dispatch or current-state authority.
    /// # Errors
    /// Rejects a changed or oversized private binding.
    pub fn verify_binding(&self, binding: &crate::ExecutionBindingCapsule) -> Result<(), Error> {
        if super::intent::execution_binding_digest(binding).map_err(|_| Error::SemanticConflict)?
            != self.execution_binding_digest
        {
            return Err(Error::SemanticConflict);
        }
        Ok(())
    }
}

impl AdmissionRecoveryPreparation {
    pub(crate) fn measure_original(
        &self,
        proof: &mut crate::public_session::v2::proof::Proof,
    ) -> Result<(), crate::public_session::v2::proof::Failure> {
        crate::native_enrollment::caller::recovery::measure_request(&self.request, proof)?;
        proof.measure(&(
            self.expected_revision,
            self.expected_issuance,
            &self.execution_binding_digest,
        ))
    }
}
