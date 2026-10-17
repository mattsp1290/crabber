//! External grants keep private fields opaque. Native wrappers measure their
//! actual full contents; this scaffold only bounds the two constructor tokens.
//! It does not claim to account for an inaccessible malformed hidden snapshot.
use crate::AttemptAuthority;
use crate::public_session::v2::proof::{Failure, Proof};
#[path = "../../../crabber-session/src/native_enrollment/caller/verify.rs"]
mod verify;
pub(crate) use verify::{Attempt, Caller, Fresh, Phase, measure_record, relevant};

pub(crate) fn measure_authority(
    authority: Option<&AttemptAuthority>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if authority.is_some() {
        // from_store caps each token at 512 ASCII-graphic bytes. JSON can
        // double each byte; include quotes, separator and tuple brackets.
        proof.charge(2 * (2 * 512 + 2) + 3)?;
    }
    Ok(())
}
#[cfg(test)]
#[path = "../../../crabber-session/src/native_enrollment/caller/probe.rs"]
mod probe;
#[cfg(test)]
pub(crate) use probe::Probe;
#[cfg(test)]
#[path = "../../../crabber-session/src/native_enrollment/caller/contract.rs"]
pub(crate) mod contract;

pub(crate) fn measure_phase_record(
    record: &crate::phase::PhaseRecord,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(&record.snapshot, &record.token, &record.output))
}
pub(crate) fn measure_phase_authority(
    authority: Option<&crate::PhaseAuthority>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if authority.is_some() {
        // Three opaque tokens: phase plus the embedded parent attempt/run pair.
        // Actual native wrappers measure full private snapshots and tokens.
        proof.charge(3 * (2 * 512 + 2) + 4)?;
    }
    Ok(())
}

#[path = "../../../crabber-session/src/native_enrollment/caller/legacy.rs"]
mod legacy;
pub(crate) use legacy::LegacyTool;
#[path = "../../../crabber-session/src/native_enrollment/caller/admission_begin.rs"]
mod admission_begin;
pub(crate) use admission_begin::AdmissionBegin;
#[path = "../../../crabber-session/src/native_enrollment/caller/initial.rs"]
pub(crate) mod initial;

pub(crate) fn measure_owner_authority(
    authority: &crate::session_owner::SessionOwnerAuthority,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(authority.session_id(), authority.target()))?;
    // This scaffold accounts only the documented 512-byte constructor token;
    // real authority validation remains the unchanged opaque authenticator.
    proof.charge(2 * 512 + 2)
}

#[path = "../../../crabber-session/src/native_enrollment/caller/recovery.rs"]
pub(crate) mod recovery;

// The public source exposes request/revision/issuance; its remaining private
// InputFingerprint is constructor-validated to exactly 64 ASCII hex bytes.
// Only the native module above can measure those actual private fields.
pub(crate) fn measure_recovery_source(
    source: &crate::session_owner::preparation::AdmissionRecoveryPreparation,
    proof: &mut Proof,
) -> Result<(), Failure> {
    recovery::measure_request(source.request(), proof)?;
    proof.measure(&(source.expected_revision(), source.expected_issuance()))?;
    proof.charge(64 + 2 + 1)
}
