//! Borrowed former full-record shape for exact logical capacity accounting.
use crate::queue::{
    BoundKeyedAdmitRequest, Claim, ControlId, QueueInput, QueueSummary, Record, StoreError, corrupt,
};
use serde::Serialize;
/// Exactly the old full Record wire shape; the current projection is version2.
#[derive(Serialize)]
pub(crate) struct RecordView<'a> {
    enqueue_operation: &'a ControlId,
    prepare_operation: &'a Option<ControlId>,
    summary: &'a QueueSummary,
    input: &'a QueueInput,
    claim: &'a Option<Claim>,
    prepared: Option<&'a BoundKeyedAdmitRequest>,
}
impl Record {
    pub(crate) fn view<'a>(
        &'a self,
        prepared: Option<&'a BoundKeyedAdmitRequest>,
    ) -> RecordView<'a> {
        RecordView {
            enqueue_operation: &self.enqueue_operation,
            prepare_operation: &self.prepare_operation,
            summary: &self.summary,
            input: &self.input,
            claim: &self.claim,
            prepared,
        }
    }
    pub(crate) fn former_encoded_len(
        &self,
        prepared: Option<&BoundKeyedAdmitRequest>,
    ) -> Result<usize, StoreError> {
        if self.prepared.is_some() != prepared.is_some() {
            return Err(corrupt());
        }
        crate::snapshot::encoded_bytes(&self.view(prepared), crate::queue::MAX_RECORD)
            .ok_or_else(corrupt)
    }
}
/// Canonical current JSON adds exactly `"version":2,` before the old fields.
/// Callers independently validate the selected codec/reference/original routes.
/// These scalar sizes account capacity; they do not authenticate private input.
#[cfg(any(feature = "postgres", feature = "sqlite", test))]
pub(crate) fn former_encoded_len(
    stored_bytes: usize,
    reference_bytes: Option<usize>,
    original_prepared_bytes: Option<usize>,
) -> Result<usize, StoreError> {
    let base = stored_bytes.checked_sub(12).ok_or_else(corrupt)?;
    let result = match (reference_bytes, original_prepared_bytes) {
        (None, None) => base,
        (Some(reference), Some(original)) if reference > 0 && original > 0 => base
            .checked_sub(reference)
            .and_then(|n| n.checked_add(original))
            .ok_or_else(corrupt)?,
        _ => return Err(corrupt()),
    };
    if result > crate::queue::MAX_RECORD {
        return Err(corrupt());
    }
    Ok(result)
}
