//! External grants keep private fields opaque. Native wrappers measure their
//! actual full contents; this scaffold bounds constructor tokens and the optional
//! native 64-hex ACK seal.
//! It does not claim to account for an inaccessible malformed hidden snapshot.
use crate::AttemptAuthority;
use crate::public_session::v2::proof::{Failure, Proof};
#[path = "../../../crabber-session/src/native_enrollment/caller/verify.rs"]
mod verify;
pub(crate) use verify::{
    Approval, ApprovalCompletion, ApprovalInput, ApprovalResult, Attempt, Caller, Fresh, Phase,
    measure_record, relevant,
};

pub(crate) fn measure_authority(
    authority: Option<&AttemptAuthority>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if authority.is_some() {
        // from_store caps each token at 512 ASCII-graphic bytes. JSON can
        // double each byte; include quotes, separator and tuple brackets.
        proof.charge(2 * (2 * 512 + 2) + 3 + 67)?;
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
        proof.charge(3 * (2 * 512 + 2) + 4 + 67)?;
    }
    Ok(())
}

#[path = "../../../crabber-session/src/native_enrollment/caller/legacy.rs"]
mod legacy;
pub(crate) use legacy::LegacyTool;
#[path = "../../../crabber-session/src/native_enrollment/caller/admission_begin.rs"]
mod admission_begin;
pub(crate) use admission_begin::AdmissionBegin;
// Retain the canonical closed variants for read/negative verification. Their
// positive Pending constructors are intentionally absent from this fixture.
#[path = "../../../crabber-session/src/native_enrollment/caller/initial.rs"]
#[allow(dead_code)]
pub(crate) mod initial;

pub(crate) fn measure_owner_authority(
    authority: &crate::session_owner::SessionOwnerAuthority,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&(authority.session_id(), authority.target()))?;
    // The constructor caps tokens at 512 ASCII-graphic bytes. Include maximal
    // JSON escaping, quotes, and the added third-tuple-element separator.
    // Real authority validation remains the unchanged opaque authenticator.
    proof.charge(2 * 512 + 3)
}

#[test]
fn owner_authority_meter_covers_maximum_escaped_constructor_token_exactly() {
    use crate::session_owner::{SessionOwnerAuthority, SessionOwnerTarget};
    use crabber_core::{SessionId, SessionOwnerGeneration, SessionOwnerId};
    let session = SessionId::new();
    let target = SessionOwnerTarget {
        id: SessionOwnerId::new(),
        generation: SessionOwnerGeneration::INITIAL,
    };
    for character in ['"', '\\'] {
        let token = character.to_string().repeat(512);
        let authority =
            SessionOwnerAuthority::from_store(session.clone(), target.clone(), token.clone())
                .unwrap();
        // Serialize the exact constructor values, without any private getter.
        // This tests representation accounting, never native permission.
        let actual = serde_json::to_vec(&(&session, &target, &token))
            .unwrap()
            .len();
        let mut exact = Proof::bounded(actual);
        measure_owner_authority(&authority, &mut exact).unwrap();
        assert_eq!(exact.used(), actual);
        let mut short = Proof::bounded(actual - 1);
        assert!(matches!(
            measure_owner_authority(&authority, &mut short),
            Err(Failure::Limited(_))
        ));
        assert_eq!(short.used(), actual);
        assert!(
            SessionOwnerAuthority::from_store(
                session.clone(),
                target.clone(),
                character.to_string().repeat(513),
            )
            .is_err()
        );
    }
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

#[path = "../../../crabber-session/src/native_enrollment/caller/tool.rs"]
pub(crate) mod tool;
