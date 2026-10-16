//! Original fork metadata stays independent of current source/target state.
use super::{ForkError as Error, ForkReceipt, ForkSession};
use serde::{Deserialize, Serialize};

#[derive(Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Audit {
    pub request: ForkSession,
    pub receipt: ForkReceipt,
}
impl Audit {
    pub(crate) fn validate(&self) -> Result<(), Error> {
        let request = &self.request;
        let receipt = &self.receipt;
        let target = &receipt.target;
        if crate::snapshot::encoded_bytes(self, 32768).is_none()
            || request
                .fingerprint()
                .map_err(|_| Error::UnknownStoreOutcome)?
                != receipt.semantic_digest
            || receipt.operation_id != request.id
            || receipt.source_session_id != request.source_session_id
            || receipt.source_revision != request.expected_source_revision
            || receipt.boundary != request.boundary
            || target.session_id != request.target_session_id
            || target.workspace_id != request.workspace_id
            || target.directory != request.directory
            || target.title != request.title
            || target.defaults != request.defaults
            || target.archived
            || target.created_at != target.updated_at
            || target.revision
                != crabber_core::SessionRevision::INITIAL
                    .next()
                    .map_err(|_| Error::UnknownStoreOutcome)?
            || receipt.messages > request.limits.messages
            || receipt.imported_calls > request.limits.tool_calls
        {
            return Err(Error::UnknownStoreOutcome);
        }
        Ok(())
    }
}
