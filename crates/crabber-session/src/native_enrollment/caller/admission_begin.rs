//! Actual one-shot keyed execution admission has Init/Begin sources, no invented event.
use super::verify::Scope;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{Run, RunFence};
pub(crate) struct AdmissionBegin<'a> {
    pub fence: &'a RunFence,
    pub run: &'a Run,
    pub record: &'a crate::AdmissionExecutionRecord,
    pub sources: Option<(
        &'a crate::admission_journal::Init,
        &'a crate::admission_journal::Begin,
    )>,
}
impl<'a> AdmissionBegin<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        proof.measure(&(self.fence, self.run, self.record, self.sources))?;
        if self.fence.run_id != self.run.id
            || self.fence.claim_token != self.run.claim_token
            || self.record.receipt.run_id != self.run.id
            || self.record.receipt.session_id != self.run.session_id
            || self.record.state != crate::AdmissionExecutionState::Started
        {
            return Err(unknown("actual raw admission Begin"));
        }
        if let Some((init, begin)) = self.sources {
            init.verify(self.record).map_err(unknown)?;
            begin.verify(init).map_err(unknown)?;
            begin.issuance.verify(self.run).map_err(unknown)?;
        }
        Ok(Scope {
            session: &self.run.session_id,
            run: &self.run.id,
        })
    }
}
