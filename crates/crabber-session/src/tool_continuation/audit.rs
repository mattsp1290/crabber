use crate::tool_continuation::canonical::digest;
use crate::tool_continuation::{
    ToolContinuationError as Error, ToolContinuationReceipt, ToolContinuationSuspension,
};
use crabber_core::ControlId;
use time::OffsetDateTime;
pub(crate) fn audit(
    receipt: &ToolContinuationReceipt,
    now: OffsetDateTime,
) -> crabber_core::EventRecord {
    crabber_core::EventRecord {
        cursor: Some(receipt.event_cursor),
        session_id: receipt.session_id.clone(),
        run_id: receipt.run_id.clone(),
        turn_id: None,
        kind: crabber_core::EventKind::ContinuationChanged,
        payload: serde_json::json!({"version":1,"kind":"tool_continuation","operation_id":receipt.operation_id,"semantic_digest":receipt.semantic_digest,"revision":receipt.revision,"outcome":receipt.outcome}),
        correlation: Some(receipt.operation_id.0.clone()),
        live_only: false,
        created_at: now,
    }
}
pub(crate) fn verify_receipt(
    receipt: &ToolContinuationReceipt,
    event: &crabber_core::EventRecord,
) -> Result<(), Error> {
    let mut canonical = audit(receipt, event.created_at);
    if let Some(handoff) = crate::continuation_handoff::Handoff::from_event(event)
        .map_err(|()| Error::UnknownStoreOutcome)?
    {
        if !matches!(
            receipt.outcome,
            crate::ToolContinuationOutcome::Suspended { .. }
        ) {
            return Err(Error::UnknownStoreOutcome);
        }
        canonical.payload["handoff"] =
            serde_json::to_value(handoff).map_err(|_| Error::UnknownStoreOutcome)?;
    }
    if event.session_id != receipt.session_id
        || event.run_id != receipt.run_id
        || event.cursor != Some(receipt.event_cursor)
        || event.kind != crabber_core::EventKind::ContinuationChanged
        || event.live_only
        || event.correlation.as_deref() != Some(receipt.operation_id.0.as_str())
        || event.payload != canonical.payload
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
pub(crate) fn checkpoint(batch: &ToolContinuationSuspension) -> serde_json::Value {
    serde_json::json!({"version":5,"tool_continuation":batch.continuation,"turns_used":batch.turns_used})
}
pub(crate) fn derived_id(domain: &str, key: &ControlId) -> ControlId {
    ControlId::from(
        digest(&(domain, key))
            .expect("serializable identities")
            .as_str(),
    )
}
