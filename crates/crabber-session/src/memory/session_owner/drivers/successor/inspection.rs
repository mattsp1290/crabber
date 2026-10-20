//! Exact selected original evidence, never today's head or current authority.
use super::{Error, MemoryStore, Proof, source};
use crate::session_owner::preparation::{
    InspectPreparedApprovalDriver, PreparedApprovalDriverMetadata,
};

impl MemoryStore {
    pub(in crate::memory) fn inspect_approval_driver(
        &self,
        request: &InspectPreparedApprovalDriver,
    ) -> Result<PreparedApprovalDriverMetadata, Error> {
        self.inspect_approval_driver_bounded(request, 32 << 20)
    }
    fn inspect_approval_driver_bounded(
        &self,
        request: &InspectPreparedApprovalDriver,
        bytes: usize,
    ) -> Result<PreparedApprovalDriverMetadata, Error> {
        crate::session_owner::preparation::approval_driver::validate(request)?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        let proof = &mut Proof::bounded(bytes);
        inspect(&state, request, proof)
    }
}
fn inspect(
    state: &crate::memory::State,
    request: &InspectPreparedApprovalDriver,
    proof: &mut Proof,
) -> Result<PreparedApprovalDriverMetadata, Error> {
    let selected = source::original(state, &request.request, proof, None)?;
    let receipt = selected.approval_receipt.ok_or(Error::Unsupported)?;
    let scope = crate::session_owner::preparation::approval_driver::scope(&receipt)?;
    if !state
        .continuation_inputs
        .protocol(&scope, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::Unsupported);
    }
    let record = state
        .approvals
        .get(&scope.continuation.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let answers: Vec<_> = record
        .questions
        .iter()
        .filter_map(|q| q.answer.as_ref())
        .take(65)
        .map(crate::continuation_input::Input::ApprovalAnswer)
        .collect();
    crate::memory::continuation_input::answers(state, &scope, &answers, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    crate::session_owner::preparation::approval_driver::metadata(request, receipt)
}

#[cfg(test)]
#[path = "inspection/tests.rs"]
mod tests;
