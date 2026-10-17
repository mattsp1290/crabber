//! External grants keep private fields opaque. Native wrappers measure their
//! actual full contents; this scaffold only bounds the two constructor tokens.
//! It does not claim to account for an inaccessible malformed hidden snapshot.
use crate::AttemptAuthority;
use crate::public_session::v2::proof::{Failure, Proof};
#[path = "../../../crabber-session/src/native_enrollment/caller/verify.rs"]
mod verify;
pub(crate) use verify::{Attempt, Caller, Fresh, measure_record, relevant};

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
