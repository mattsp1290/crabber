//! Raw-only actual native caller routing. No enrollment or positive evidence.
use crate::AttemptAuthority;
use crate::public_session::v2::proof::{Failure, Proof};
#[path = "caller/verify.rs"]
mod verify;
pub(crate) use verify::{Attempt, Caller, Fresh, Phase, measure_record, relevant};

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

#[path = "caller/legacy.rs"]
mod legacy;
pub(crate) use legacy::LegacyTool;

#[path = "caller/admission_begin.rs"]
mod admission_begin;
pub(crate) use admission_begin::AdmissionBegin;
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
    authority.map_or(Ok(()), |a| crate::phase::measure_native_authority(a, proof))
}
