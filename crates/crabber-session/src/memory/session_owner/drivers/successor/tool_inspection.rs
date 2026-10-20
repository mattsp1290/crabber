//! Pure selected original Tool/Frontend metadata; no fresh authority.
use super::{Error, Proof, source};
use crate::session_owner::preparation::{InspectPreparedToolDriver, PreparedToolDriverMetadata};
impl crate::MemoryStore {
    pub(in crate::memory) fn inspect_tool_driver(
        &self,
        request: &InspectPreparedToolDriver,
    ) -> Result<PreparedToolDriverMetadata, Error> {
        crate::session_owner::preparation::tool_driver::validate(request)?;
        let state = self.state.lock().map_err(|_| Error::UnknownStoreOutcome)?;
        inspect(&state, request, &mut Proof::bounded(32 << 20))
    }
}
fn inspect(
    state: &crate::memory::State,
    request: &InspectPreparedToolDriver,
    proof: &mut Proof,
) -> Result<PreparedToolDriverMetadata, Error> {
    let selected = source::original(state, &request.request, proof, None)?;
    let receipt = selected.tool_receipt.ok_or(Error::Unsupported)?;
    crate::session_owner::preparation::tool_driver::metadata(
        request,
        receipt,
        selected.tool_payload_digest.ok_or(Error::Unsupported)?,
    )
}
