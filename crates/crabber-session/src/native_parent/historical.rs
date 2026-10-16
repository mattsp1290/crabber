//! Original native Begin association after its parent has become terminal.
//! The adapter supplies the independently retained immutable witness and issuer;
//! no current Run, fake Started phase or reconstructed authority is accepted.
use super::{Error, Kind, RunIssuance, Witness};
use crate::PhaseReceipt;
use crabber_core::{EventRecord, RunStatus};

impl Witness {
    pub(crate) fn verify_historical_phase(
        &self,
        receipt: &PhaseReceipt,
        event: &EventRecord,
        issuance: &RunIssuance,
    ) -> Result<(), Error> {
        self.validate()?;
        super::phase_source(receipt, event)?;
        issuance.validate()?;
        let source = &self.source;
        if self.version != 2
            || self.run_issuance_digest.as_ref() != Some(&super::issuance_digest(issuance)?)
            || source.kind != Kind::Phase
            || source.session_id != receipt.phase.session_id
            || source.run_id != receipt.phase.run_id
            || source.operation_id != receipt.operation_id
            || source.event_cursor != receipt.event_cursor
            || source.semantic_digest != receipt.semantic_digest
            || source.attempt != receipt.phase.target.attempt
            || source.phase.as_ref() != Some(&receipt.phase.target)
            || source.call_binding.as_ref() != Some(&receipt.phase.call_binding)
            || issuance.sequence != self.run_issuance
            || issuance.state.session_id != source.session_id
            || issuance.state.run_id != source.run_id
            || issuance.state.status != RunStatus::Running
        {
            return Err(Error);
        }
        Ok(())
    }
}
