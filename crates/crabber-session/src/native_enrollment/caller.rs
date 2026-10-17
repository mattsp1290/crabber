//! Raw-only actual native caller routing. No enrollment or positive evidence.
use crate::AttemptAuthority;
use crate::public_session::v2::proof::{Failure, Proof};
#[path = "caller/verify.rs"]
mod verify;
pub(crate) use verify::{Attempt, Caller, Fresh, measure_record, relevant};

pub(crate) fn measure_authority(
    authority: Option<&AttemptAuthority>,
    proof: &mut Proof,
) -> Result<(), Failure> {
    if let Some(authority) = authority {
        proof.measure(&(&authority.snapshot, &authority.token, &authority.run_token))?;
    }
    Ok(())
}

#[cfg(test)]
#[path = "caller/probe.rs"]
mod probe;
#[cfg(test)]
pub(crate) use probe::Probe;

#[cfg(test)]
#[path = "caller/contract.rs"]
pub(crate) mod contract;
