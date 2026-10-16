//! Frontend origin is immutable evidence, not an executable `ToolDefinition`.
use super::{MemoryExecution, OperationReceipt, State};
use crate::frontend_continuation::FrontendOriginRecord;
use crate::{FrontendCallStaging, FrontendStagingReceipt, ToolContinuationError as Error};
use crabber_core::{ControlId, Run, SessionId, ToolCallId};
#[path = "frontend/evidence.rs"]
mod evidence;
pub(super) use evidence::declared;
#[path = "frontend/stage.rs"]
mod stage;
pub(super) fn receipt(
    state: &State,
    session: &SessionId,
    key: &ControlId,
) -> Result<Option<FrontendStagingReceipt>, Error> {
    let Some(saved) = state
        .operation_receipts
        .get(&(session.clone(), key.clone()))
    else {
        return Ok(None);
    };
    let OperationReceipt::FrontendStaging(receipt) = saved else {
        return Err(Error::SemanticConflict);
    };
    if receipt.session_id != *session || receipt.operation_id != *key {
        return Err(Error::UnknownStoreOutcome);
    }
    crate::frontend_continuation::verify_staging_receipt(
        receipt,
        state
            .events
            .get(receipt.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    Ok(Some(receipt.clone()))
}
pub(super) fn origin(
    state: &State,
    run: &Run,
    call: &ToolCallId,
) -> Result<Option<FrontendOriginRecord>, Error> {
    let count = state
        .events
        .iter()
        .filter(|event| {
            event.session_id == run.session_id
                && event.run_id == run.id
                && event.kind == crabber_core::EventKind::ToolCallPending
                && event.payload.get("frontend_origin").is_some()
                && event.payload["call_id"] == serde_json::json!(call)
        })
        .take(2)
        .count();
    let Some(record) = state.frontend_origins.get(call) else {
        let declared = if let Some(call) = state.calls.get(call) {
            evidence::declared(state, run, &call.name)?
        } else {
            false
        };
        return if count == 0 && !declared {
            Ok(None)
        } else {
            Err(Error::UnknownStoreOutcome)
        };
    };
    if count != 1 {
        return Err(Error::UnknownStoreOutcome);
    }
    if record.request.run_id != run.id || record.request.session_id != run.session_id {
        return Err(Error::UnknownStoreOutcome);
    }
    record.verify(
        state
            .events
            .get(record.receipt.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    let saved = receipt(state, &run.session_id, &record.receipt.operation_id)
        .map_err(|_| Error::UnknownStoreOutcome)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if saved != record.receipt {
        return Err(Error::UnknownStoreOutcome);
    }
    evidence::binding(state, run, &record.request)?;
    let call = state.calls.get(call).ok_or(Error::UnknownStoreOutcome)?;
    if crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
        != crate::phase::call_binding(&record.request.call)
            .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(Some(record.clone()))
}

pub(super) fn verify_frozen(
    state: &State,
    run: &Run,
    frozen: &crate::FrozenFrontendCall,
) -> Result<(), Error> {
    let original = origin(state, run, &frozen.target.call_id)?.ok_or(Error::UnknownStoreOutcome)?;
    if original.request.descriptor != frozen.descriptor
        || original.request.descriptor_fingerprint != frozen.descriptor_fingerprint
        || original.request.call.arguments != frozen.arguments
        || crate::phase::call_binding(&original.request.call)
            .map_err(|_| Error::UnknownStoreOutcome)?
            != frozen.call_binding
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

#[cfg(test)]
#[path = "frontend/tests.rs"]
mod tests;
