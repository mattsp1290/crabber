//! Actual compatibility admissions retain their exact accepted input and result.
use super::verify::Scope;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use crabber_core::{
    EventCursor, EventRecord, Run, RunFence, ToolCallId, ToolCallRecord, ToolCallStatus,
};

pub(crate) struct LegacyTool<'a> {
    pub fence: &'a RunFence,
    pub run: &'a Run,
    pub id: &'a ToolCallId,
    pub requested: &'a EventRecord,
    pub call: &'a ToolCallRecord,
    pub cursor: EventCursor,
}
impl<'a> LegacyTool<'a> {
    pub(super) fn verify(&self, proof: &mut Proof) -> Result<Scope<'a>, Failure> {
        proof.measure(&(
            self.fence,
            self.run,
            self.id,
            self.requested,
            self.call,
            self.cursor,
        ))?;
        if self.fence.run_id != self.run.id
            || self.fence.claim_token != self.run.claim_token
            || self.call.id != *self.id
            || self.call.run_id != self.run.id
            || self.call.status != ToolCallStatus::Running
            || self.requested.session_id != self.run.session_id
            || self.requested.run_id != self.run.id
            || self.cursor.0 == 0
        {
            return Err(unknown("actual raw legacy tool admission"));
        }
        Ok(Scope {
            session: &self.run.session_id,
            run: &self.run.id,
        })
    }
}
