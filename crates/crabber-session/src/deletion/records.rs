//! Minimal permanent evidence, outside the payload-bearing receipt graph.
use super::{DeleteSession, DeletionError, DeletionReceipt};
use crabber_core::{SessionId, SessionRevision};
use serde::{Deserialize, Serialize};

#[cfg(any(feature = "postgres", feature = "sqlite"))]
pub(crate) fn timestamp(seconds: i64, nanos: i64) -> Result<time::OffsetDateTime, DeletionError> {
    time::OffsetDateTime::from_unix_timestamp(seconds)
        .map_err(|_| DeletionError::UnknownStoreOutcome)?
        .replace_nanosecond(u32::try_from(nanos).map_err(|_| DeletionError::UnknownStoreOutcome)?)
        .map_err(|_| DeletionError::UnknownStoreOutcome)
}

#[derive(Debug, Clone, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub(crate) struct Tombstone {
    pub format_version: u8,
    pub expected_revision: SessionRevision,
    pub receipt: DeletionReceipt,
}

impl Tombstone {
    pub(crate) fn verify(&self, session: &SessionId) -> Result<(), DeletionError> {
        let receipt = &self.receipt;
        crate::management::validate_scope(&receipt.session_id, &receipt.operation_id)
            .map_err(|_| DeletionError::UnknownStoreOutcome)?;
        let request = DeleteSession {
            id: receipt.operation_id.clone(),
            session_id: receipt.session_id.clone(),
            expected_revision: self.expected_revision,
        };
        if self.format_version != 1
            || receipt.session_id != *session
            || self.expected_revision.next().ok() != Some(receipt.final_revision)
            || request.fingerprint().ok().as_ref() != Some(&receipt.semantic_digest)
        {
            return Err(DeletionError::UnknownStoreOutcome);
        }
        Ok(())
    }
}
