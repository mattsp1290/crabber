//! Independent immutable source selection, never mutable question/state pointers.
use super::{Error, OperationReceipt, State};
use crate::approval::{
    ApprovalRecord,
    current::{CurrentProof, NativeSource, Release, Source},
};
use crabber_core::{EventKind, EventRecord, Run, SessionId};

fn operation(event: &EventRecord) -> Option<&str> {
    event.payload.get("outcome")?.get("operation")?.as_str()
}
fn belongs(event: &EventRecord, record: &ApprovalRecord) -> bool {
    event.session_id == record.session_id
        && event.run_id == record.run_id
        && event
            .payload
            .get("outcome")
            .and_then(|v| v.get("continuation"))
            .is_some_and(|v| {
                v.get("id").and_then(serde_json::Value::as_str)
                    == Some(record.batch.continuation.id.0.as_str())
                    && v.get("generation").and_then(serde_json::Value::as_u64)
                        == Some(record.batch.continuation.generation.get())
            })
}
fn selected<'a>(
    state: &'a State,
    record: &ApprovalRecord,
    answers: bool,
) -> Result<Vec<&'a EventRecord>, Error> {
    let limit = if answers { 65 } else { 1 };
    let events: Vec<_> = state
        .events
        .iter()
        .rev()
        .filter(|event| {
            belongs(event, record)
                && if answers {
                    event.kind == EventKind::InteractionChanged
                        && operation(event) == Some("answered")
                } else {
                    event.kind == EventKind::ContinuationChanged
                        && operation(event) != Some("answered")
                }
        })
        .take(limit)
        .collect();
    if answers && events.len() > 64 {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(events)
}
fn source(state: &State, event: &EventRecord) -> Result<Source, Error> {
    let fail = || Error::UnknownStoreOutcome;
    if crate::snapshot::encoded_bytes(event, 65536).is_none() {
        return Err(fail());
    }
    let id = event.correlation.as_deref().ok_or_else(fail)?;
    crate::attempt::validate_identifier(id).map_err(|_| fail())?;
    let Some(OperationReceipt::Approval(saved)) = state
        .operation_receipts
        .get(&(event.session_id.clone(), crabber_core::ControlId(id.into())))
    else {
        return Err(fail());
    };
    if saved.event_cursor != event.cursor.ok_or_else(fail)?
        || crate::snapshot::encoded_bytes(saved, 32768).is_none()
    {
        return Err(fail());
    }
    let source = Source {
        receipt: saved.clone(),
        event: event.clone(),
    };
    source.verify()?;
    Ok(source)
}

fn successor<'a>(
    state: &'a State,
    record: &ApprovalRecord,
) -> Result<Option<(&'a ApprovalRecord, &'a EventRecord)>, Error> {
    let generation = record
        .batch
        .continuation
        .generation
        .next()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let event = state.events.iter().rev().find(|e| {
        e.session_id == record.session_id
            && e.run_id == record.run_id
            && e.kind == EventKind::ContinuationChanged
            && operation(e) == Some("suspended")
            && e.payload
                .pointer("/outcome/continuation/generation")
                .and_then(serde_json::Value::as_u64)
                == Some(generation.get())
    });
    event
        .map(|event| {
            let id = event
                .payload
                .pointer("/outcome/continuation/id")
                .and_then(serde_json::Value::as_str)
                .ok_or(Error::UnknownStoreOutcome)?;
            crate::attempt::validate_identifier(id).map_err(|_| Error::UnknownStoreOutcome)?;
            let record = state
                .approvals
                .get(&crabber_core::ContinuationId(id.into()))
                .ok_or(Error::UnknownStoreOutcome)?;
            if record.batch.continuation.id.0 != id
                || record.batch.continuation.generation != generation
            {
                return Err(Error::UnknownStoreOutcome);
            }
            Ok((record, event))
        })
        .transpose()
}
fn native<'a>(state: &'a State, record: &ApprovalRecord) -> Result<&'a EventRecord, Error> {
    state
        .events
        .iter()
        .rev()
        .find(|e| {
            e.session_id == record.session_id
                && e.run_id == record.run_id
                && e.kind == EventKind::ExecutionAttemptChanged
                && matches!(
                    e.payload.get("version").and_then(serde_json::Value::as_u64),
                    Some(2 | 3)
                )
                && e.payload
                    .pointer("/attempt/target/id")
                    .and_then(serde_json::Value::as_str)
                    == Some(record.batch.next_attempt.id.0.as_str())
        })
        .ok_or(Error::UnknownStoreOutcome)
}
pub(super) fn verify(
    state: &State,
    run: &Run,
    record: &ApprovalRecord,
    releasing: bool,
) -> Result<(), Error> {
    let answers = selected(state, record, true)?
        .into_iter()
        .map(|event| source(state, event))
        .collect::<Result<_, _>>()?;
    let latest = source(
        state,
        selected(state, record, false)?
            .first()
            .copied()
            .ok_or(Error::UnknownStoreOutcome)?,
    )?;
    let release = if record.state == crate::ApprovalContinuationState::Released
        && record.termination.is_none()
    {
        if let Some((next, event)) = successor(state, record)? {
            super::origin::verify(state, run, next)?;
            Some(Release::Successor {
                pub_source: source(state, event)?,
                attempt: next.batch.attempt.clone(),
            })
        } else {
            let event = native(state, record)?;
            if crate::snapshot::encoded_bytes(event, 65536).is_none() {
                return Err(Error::UnknownStoreOutcome);
            }
            let id = event
                .correlation
                .as_deref()
                .ok_or(Error::UnknownStoreOutcome)?;
            crate::attempt::validate_identifier(id).map_err(|_| Error::UnknownStoreOutcome)?;
            let id = crabber_core::ControlId(id.into());
            let Some(OperationReceipt::Attempt(saved)) = state
                .operation_receipts
                .get(&(record.session_id.clone(), id))
            else {
                return Err(Error::UnknownStoreOutcome);
            };
            if crate::snapshot::encoded_bytes(event, 65536).is_none()
                || crate::snapshot::encoded_bytes(saved, 8192).is_none()
            {
                return Err(Error::UnknownStoreOutcome);
            }
            let latest = state
                .attempts
                .values()
                .filter(|a| a.snapshot.run_id == run.id)
                .max_by_key(|a| a.snapshot.target.generation)
                .ok_or(Error::UnknownStoreOutcome)?;
            if crate::snapshot::encoded_bytes(&latest.snapshot, 8192).is_none() {
                return Err(Error::UnknownStoreOutcome);
            }
            Some(Release::Native(NativeSource {
                receipt: saved.clone(),
                event: event.clone(),
                latest: latest.snapshot.clone(),
            }))
        }
    } else {
        None
    };
    record.verify_current(
        run,
        &CurrentProof {
            answers,
            latest,
            release,
        },
        state.approval_barriers.get(&record.session_id),
        releasing,
    )
}
/// Validate the last independently published suspension, including absence of its barrier.
pub(in super::super) fn session(state: &State, session: &SessionId) -> Result<(), Error> {
    let event = state.events.iter().rev().find(|e| {
        e.session_id == *session
            && e.kind == EventKind::ContinuationChanged
            && operation(e) == Some("suspended")
    });
    let Some(event) = event else {
        return if state.approval_barriers.contains_key(session)
            || state.approvals.values().any(|r| r.session_id == *session)
        {
            Err(Error::UnknownStoreOutcome)
        } else {
            Ok(())
        };
    };
    let suspended = source(state, event)?;
    let crate::ApprovalOutcome::Suspended { continuation } = &suspended.receipt.outcome else {
        return Err(Error::UnknownStoreOutcome);
    };
    let record = state
        .approvals
        .get(&continuation.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    let run = state
        .runs
        .get(&suspended.receipt.run_id)
        .ok_or(Error::UnknownStoreOutcome)?;
    super::verify_record(state, run, record)
}

pub(in super::super) fn has_source(state: &State, run: &Run) -> bool {
    state.events.iter().any(|e| {
        e.session_id == run.session_id
            && e.run_id == run.id
            && e.kind == EventKind::ContinuationChanged
            && operation(e) == Some("suspended")
    })
}

pub(in super::super) fn preflight(
    state: &State,
    run: &Run,
    record: &ApprovalRecord,
    proof: &mut crate::public_session::budget::Budget,
) -> Result<(), crate::public_session::budget::Failure> {
    use crate::public_session::budget::unknown;
    for event in selected(state, record, true)
        .map_err(unknown)?
        .into_iter()
        .chain(selected(state, record, false).map_err(unknown)?)
    {
        measure_source(state, event, proof)?;
    }
    if record.state == crate::ApprovalContinuationState::Released && record.termination.is_none() {
        if let Some((next, event)) = successor(state, record).map_err(unknown)? {
            measure_source(state, event, proof)?;
            proof.measure(next)?;
            super::super::public_header::preflight(state, next, proof)?;
        } else {
            let event = native(state, record).map_err(unknown)?;
            proof.measure(event)?;
            let id = event
                .correlation
                .as_deref()
                .ok_or_else(|| unknown("native source"))?;
            crate::attempt::validate_identifier(id).map_err(unknown)?;
            let Some(OperationReceipt::Attempt(receipt)) = state.operation_receipts.get(&(
                record.session_id.clone(),
                crabber_core::ControlId(id.into()),
            )) else {
                return Err(unknown("native receipt"));
            };
            proof.measure(receipt)?;
            let latest = state
                .attempts
                .values()
                .filter(|a| a.snapshot.run_id == run.id)
                .max_by_key(|a| a.snapshot.target.generation)
                .ok_or_else(|| unknown("latest attempt"))?;
            proof.measure(&latest.snapshot)?;
        }
    }
    Ok(())
}
fn measure_source(
    state: &State,
    event: &EventRecord,
    proof: &mut crate::public_session::budget::Budget,
) -> Result<(), crate::public_session::budget::Failure> {
    use crate::public_session::budget::unknown;
    proof.measure(event)?;
    let id = event
        .correlation
        .as_deref()
        .ok_or_else(|| unknown("source operation"))?;
    crate::attempt::validate_identifier(id).map_err(unknown)?;
    let Some(OperationReceipt::Approval(saved)) = state
        .operation_receipts
        .get(&(event.session_id.clone(), crabber_core::ControlId(id.into())))
    else {
        return Err(unknown("source receipt"));
    };
    proof.measure(saved)
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn oversized_current_sources_are_rejected_before_copying() {
        let runtime = tokio::runtime::Builder::new_current_thread()
            .enable_all()
            .build()
            .unwrap();
        let clock = std::sync::Arc::new(crabber_core::ManualClock::new(
            time::OffsetDateTime::UNIX_EPOCH,
        ));
        let store = super::super::MemoryStore::with_clock(clock.clone());
        runtime.block_on(crate::approval_contract::current::retired(&store, &clock));
        let saved = store.state.lock().unwrap().clone();
        let record = saved.approvals.values().next().unwrap().clone();
        let run = saved.runs[&record.run_id].clone();
        for case in 0..3 {
            let mut state = saved.clone();
            if case == 2 {
                let latest = state
                    .attempts
                    .values_mut()
                    .filter(|a| a.snapshot.run_id == run.id)
                    .max_by_key(|a| a.snapshot.target.generation)
                    .unwrap();
                latest.snapshot.target.id.0 = "x".repeat(1024 * 1024);
            } else {
                let mut event = if case == 0 {
                    native(&state, &record).unwrap().clone()
                } else {
                    selected(&state, &record, false).unwrap()[0].clone()
                };
                if case == 0 {
                    event.correlation = Some("x".repeat(1024 * 1024));
                } else {
                    event.kind = EventKind::ContinuationChanged;
                    event.payload["outcome"]["operation"] = "suspended".into();
                    event.payload["outcome"]["continuation"]["id"] = "x".repeat(1024 * 1024).into();
                    event.payload["outcome"]["continuation"]["generation"] = record
                        .batch
                        .continuation
                        .generation
                        .next()
                        .unwrap()
                        .get()
                        .into();
                }
                event.cursor = Some(state.events.next_cursor().unwrap());
                state.events.append(event).unwrap();
            }
            let measured = allocation_counter::measure(|| {
                assert!(verify(&state, &run, &record, false).is_err());
                let mut budget = crate::public_session::budget::Budget::new(
                    65536,
                    crate::PublicSessionLimit::ProofBytes,
                );
                assert!(preflight(&state, &run, &record, &mut budget).is_err());
            });
            assert!(measured.bytes_max < 131_072, "case {case}: {measured:?}");
        }
    }
}
