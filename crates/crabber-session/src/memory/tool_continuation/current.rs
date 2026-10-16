use super::{
    Error, OperationReceipt, State, ToolContinuationOutcome, ToolContinuationRecord, domain,
    event_at, receipt,
};

pub(super) fn verify(state: &State, record: &ToolContinuationRecord) -> Result<(), Error> {
    let mut audits = domain::CurrentAudits::default();
    let mut releases = 0_u8;
    for event in state
        .events
        .iter()
        .filter(|event| event.session_id == record.session_id && event.run_id == record.run_id)
    {
        if let Some(key) = domain::audit_key(event, record)? {
            let saved =
                receipt(state, &record.session_id, &key)?.ok_or(Error::UnknownStoreOutcome)?;
            if event.cursor != Some(saved.event_cursor) {
                return Err(Error::UnknownStoreOutcome);
            }
            audits.insert(saved, record)?;
        }
        if domain::terminal_candidate(event, record) {
            verify_native(state, event, record)?;
            releases += 1;
        }
        if is_successor(event, record) {
            verify_successor(state, event, record)?;
            releases += 1;
        }
        if releases > 1 {
            return Err(Error::UnknownStoreOutcome);
        }
    }
    if (state.tool_barriers.get(&record.session_id) == Some(&record.batch.continuation.id))
        != (record.state != crate::ToolContinuationState::Released)
    {
        return Err(Error::UnknownStoreOutcome);
    }
    audits.verify(record, releases == 1)?;
    super::cancel::verify(state, record)
}
fn is_successor(event: &crabber_core::EventRecord, record: &ToolContinuationRecord) -> bool {
    event.kind == crabber_core::EventKind::ContinuationChanged
        && event
            .payload
            .get("kind")
            .and_then(serde_json::Value::as_str)
            == Some("tool_continuation")
        && event
            .payload
            .pointer("/outcome/operation")
            .and_then(serde_json::Value::as_str)
            == Some("suspended")
        && event
            .payload
            .pointer("/outcome/continuation/generation")
            .and_then(serde_json::Value::as_u64)
            == record.batch.continuation.generation.get().checked_add(1)
}

fn verify_native(
    state: &State,
    event: &crabber_core::EventRecord,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let key = crabber_core::ControlId::from(
        event
            .correlation
            .as_deref()
            .ok_or(Error::UnknownStoreOutcome)?,
    );
    let Some(OperationReceipt::Attempt(native)) = state
        .operation_receipts
        .get(&(record.session_id.clone(), key))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    super::super::attempt::verify(state, native).map_err(|_| Error::UnknownStoreOutcome)?;
    let latest = state
        .attempts
        .values()
        .filter(|attempt| attempt.snapshot.run_id == record.run_id)
        .max_by_key(|attempt| attempt.snapshot.target.generation)
        .ok_or(Error::UnknownStoreOutcome)?;
    latest.validate().map_err(|_| Error::UnknownStoreOutcome)?;
    let run = state
        .runs
        .get(&record.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let expected = match native.attempt.state {
        crate::AttemptState::Completed => crabber_core::RunStatus::Completed,
        crate::AttemptState::Failed => crabber_core::RunStatus::Failed,
        crate::AttemptState::Interrupted => crabber_core::RunStatus::Interrupted,
        _ => return Err(Error::UnknownStoreOutcome),
    };
    if event.cursor != Some(native.event_cursor)
        || latest.snapshot != native.attempt
        || run.status != expected
        || run.id != record.run_id
        || run.session_id != record.session_id
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}

fn verify_successor(
    state: &State,
    event: &crabber_core::EventRecord,
    record: &ToolContinuationRecord,
) -> Result<(), Error> {
    let target: crate::ContinuationTarget = serde_json::from_value(
        event
            .payload
            .pointer("/outcome/continuation")
            .cloned()
            .ok_or(Error::UnknownStoreOutcome)?,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?;
    let successor = state
        .tool_continuations
        .get(&target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    successor.validate()?;
    let saved = receipt(state, &record.session_id, &successor.batch.id)?
        .ok_or(Error::UnknownStoreOutcome)?;
    if successor.session_id != record.session_id
        || successor.run_id != record.run_id
        || successor.batch.continuation != target
        || successor.batch.attempt != record.batch.next_attempt
        || saved.semantic_digest
            != successor
                .batch
                .fingerprint(&record.session_id, &record.run_id)?
        || saved.outcome
            != (ToolContinuationOutcome::Suspended {
                continuation: target,
            })
        || event.cursor != Some(saved.event_cursor)
        || event_at(state, saved.event_cursor)?.created_at != successor.created_at
    {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(())
}
