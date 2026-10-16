//! Frontend origin is immutable evidence, not an executable `ToolDefinition`.
use super::{MemoryExecution, OperationReceipt, State};
use crate::frontend_continuation::FrontendOriginRecord;
use crate::public_session::v2::proof::{Proof, ToolReadError};
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
    receipt_with_proof(state, session, key, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn receipt_with_proof(
    state: &State,
    session: &SessionId,
    key: &ControlId,
    proof: &mut Proof,
) -> Result<Option<FrontendStagingReceipt>, ToolReadError> {
    let Some(saved) = state
        .operation_receipts
        .get(&(session.clone(), key.clone()))
    else {
        return Ok(None);
    };
    let OperationReceipt::FrontendStaging(receipt) = saved else {
        return Err(Error::SemanticConflict.into());
    };
    if receipt.session_id != *session || receipt.operation_id != *key {
        return Err(Error::UnknownStoreOutcome.into());
    }
    proof.measure(receipt).map_err(ToolReadError::Budget)?;
    proof
        .measure(
            state
                .events
                .get(receipt.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(ToolReadError::Budget)?;
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
    origin_with_proof(state, run, call, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn origin_with_proof(
    state: &State,
    run: &Run,
    call: &ToolCallId,
    proof: &mut Proof,
) -> Result<Option<FrontendOriginRecord>, ToolReadError> {
    for event in state
        .events
        .iter()
        .filter(|e| {
            e.session_id == run.session_id
                && e.run_id == run.id
                && e.kind == crabber_core::EventKind::ToolCallPending
                && e.payload.get("frontend_origin").is_some()
                && e.payload["call_id"] == serde_json::json!(call)
        })
        .take(2)
    {
        proof.measure(event).map_err(ToolReadError::Budget)?;
    }
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
            evidence::declared_with_proof(state, run, &call.name, proof)?
        } else {
            false
        };
        return if count == 0 && !declared {
            Ok(None)
        } else {
            Err(Error::UnknownStoreOutcome.into())
        };
    };
    proof.measure(record).map_err(ToolReadError::Budget)?;
    if count != 1 {
        return Err(Error::UnknownStoreOutcome.into());
    }
    if record.request.run_id != run.id || record.request.session_id != run.session_id {
        return Err(Error::UnknownStoreOutcome.into());
    }
    proof
        .measure(
            state
                .events
                .get(record.receipt.event_cursor)
                .ok_or(Error::UnknownStoreOutcome)?,
        )
        .map_err(ToolReadError::Budget)?;
    record.verify(
        state
            .events
            .get(record.receipt.event_cursor)
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    let saved = receipt_with_proof(state, &run.session_id, &record.receipt.operation_id, proof)
        .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?
        .ok_or(Error::UnknownStoreOutcome)?;
    if saved != record.receipt {
        return Err(Error::UnknownStoreOutcome.into());
    }
    evidence::binding_with_proof(state, run, &record.request, proof)?;
    let call = state.calls.get(call).ok_or(Error::UnknownStoreOutcome)?;
    proof.measure(call).map_err(ToolReadError::Budget)?;
    if crate::phase::call_binding(call).map_err(|_| Error::UnknownStoreOutcome)?
        != crate::phase::call_binding(&record.request.call)
            .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(Some(record.clone()))
}

pub(super) fn verify_frozen(
    state: &State,
    run: &Run,
    frozen: &crate::FrozenFrontendCall,
) -> Result<(), Error> {
    verify_frozen_with_proof(state, run, frozen, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn verify_frozen_with_proof(
    state: &State,
    run: &Run,
    frozen: &crate::FrozenFrontendCall,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    let original = origin_with_proof(state, run, &frozen.target.call_id, proof)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if original.request.descriptor != frozen.descriptor
        || original.request.descriptor_fingerprint != frozen.descriptor_fingerprint
        || original.request.call.arguments != frozen.arguments
        || crate::phase::call_binding(&original.request.call)
            .map_err(|_| Error::UnknownStoreOutcome)?
            != frozen.call_binding
    {
        return Err(Error::UnknownStoreOutcome.into());
    }
    Ok(())
}

#[cfg(test)]
#[path = "frontend/tests.rs"]
mod tests;
