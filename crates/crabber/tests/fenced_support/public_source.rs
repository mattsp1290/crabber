//! Explicit unsupported transport for native-only bound source publication.
use crate::{
    EventCursor, EventRecord, PublicSourceAuthority, PublicSourceMutation, PublicStateError,
    PublicStateReceipt, RunFence, memory::MemoryStore,
};

// Match canonical private receiver hooks; this embedded fixture has no native issuer.
#[allow(clippy::unused_self)]
impl MemoryStore {
    pub(crate) fn read_public_source_publication(
        &self,
        _: &crate::SessionId,
        _: &crabber::core::ControlId,
    ) -> Result<Option<crabber::session::PublicSourcePublication>, PublicStateError> {
        Err(PublicStateError::Unsupported)
    }

    pub(crate) fn read_continuation_source(
        &self,
        _: &RunFence,
        _: &crate::AttemptAuthority,
    ) -> Result<crabber::core::continuation_source::ResumeSnapshot, PublicStateError> {
        Err(PublicStateError::Unsupported)
    }

    pub(crate) fn write_public_source(
        &self,
        _: PublicSourceMutation,
        _: &PublicSourceAuthority,
        _: Option<&RunFence>,
    ) -> Result<PublicStateReceipt, PublicStateError> {
        Err(PublicStateError::Unsupported)
    }
}

pub(crate) fn verify_source_event(
    _: &PublicStateReceipt,
    _: &EventRecord,
) -> Result<(), PublicStateError> {
    Err(PublicStateError::UnknownStoreOutcome)
}
pub(crate) fn before_event(event: &EventRecord) -> Result<Option<EventCursor>, PublicStateError> {
    if event
        .payload
        .get("version")
        .and_then(serde_json::Value::as_u64)
        == Some(1)
    {
        Ok(None)
    } else {
        Err(PublicStateError::UnknownStoreOutcome)
    }
}
pub(crate) fn verify_before_event(
    _: &EventRecord,
    _: &EventRecord,
) -> Result<(), PublicStateError> {
    Err(PublicStateError::UnknownStoreOutcome)
}
