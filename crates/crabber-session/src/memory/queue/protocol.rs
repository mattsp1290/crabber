//! Borrowed queue closure before copies, using the caller's selected-source budget.
use super::{Audit, Record, State, StoreError, corrupt, current, verify_sources};
use crate::public_session::v2::proof::{Proof, ReadError};
use crate::queue::source::{Scope, Source};
use crabber_core::{ControlId, SessionId};
type Result<T> = std::result::Result<T, ReadError<StoreError>>;
pub(in crate::memory) fn domain(error: ReadError<StoreError>) -> StoreError {
    error.into_domain_or(StoreError::Validation(
        "queue proof limit or source failure".into(),
    ))
}
pub(super) fn audit<'a>(
    state: &'a State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<&'a Audit> {
    let key = (session.clone(), id.clone());
    let audit = state.queue_audits.get(&key).ok_or_else(corrupt)?;
    let Some(crate::memory::OperationReceipt::Queue(receipt)) = state.operation_receipts.get(&key)
    else {
        return Err(corrupt().into());
    };
    proof
        .measure(&(&key, audit, receipt))
        .map_err(ReadError::Budget)?;
    crate::queue::verify_audit(audit, receipt)?;
    if audit.request.session_id != *session || audit.request.id != *id {
        return Err(corrupt().into());
    }
    Ok(audit)
}
pub(in crate::memory) fn admission_prefix<'a>(
    state: &'a State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Option<Vec<&'a Audit>>> {
    if !state
        .operation_receipts
        .contains_key(&(session.clone(), id.clone()))
    {
        proof.charge(1).map_err(ReadError::Budget)?;
        return if state
            .queue_audits
            .contains_key(&(session.clone(), id.clone()))
            || state
                .queue_publications
                .contains_operation(session, id, proof)?
        {
            Err(corrupt().into())
        } else {
            Ok(None)
        };
    }
    let selected = audit(state, session, id, proof)?;
    state
        .queue_publications
        .prefix(selected, proof, |id, proof| {
            audit(state, session, id, proof)
        })
        .map(Some)
}
pub(super) fn selected(
    state: &State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<()> {
    let audit = audit(state, session, id, proof)?;
    state.queue_publications.selected(audit, proof)?;
    Ok(())
}
pub(in crate::memory) fn consistent(
    state: &State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<()> {
    state
        .queue_publications
        .consistent(session, proof, |scope| {
            state
                .queue
                .get(&(scope.session.clone(), scope.queue.clone()))
                .is_some_and(|record| {
                    record.summary.session_id == scope.session
                        && record.summary.queue_id == scope.queue
                })
        })?;
    for ((routing, id), audit) in &state.queue_audits {
        if routing != session {
            continue;
        }
        proof
            .measure(&(
                (routing, id),
                &audit.receipt,
                &audit.request.session_id,
                &audit.request.id,
            ))
            .map_err(ReadError::Budget)?;
        if audit.request.session_id != *session
            || audit.request.id != *id
            || audit.receipt.session_id != *session
            || audit.receipt.operation_id != *id
        {
            return Err(corrupt().into());
        }
        if state
            .queue_publications
            .original_receipt(&audit.receipt, proof)?
        {
            let Some(crate::memory::OperationReceipt::Queue(saved)) =
                state.operation_receipts.get(&(session.clone(), id.clone()))
            else {
                return Err(corrupt().into());
            };
            proof.measure(saved).map_err(ReadError::Budget)?;
            if *saved != audit.receipt {
                return Err(corrupt().into());
            }
        }
    }
    Ok(())
}
pub(super) fn history<'a>(
    state: &'a State,
    scope: &Scope,
    proof: &mut Proof,
) -> Result<Option<&'a Source>> {
    consistent(state, &scope.session, proof)?;
    let head = state
        .queue_publications
        .history(scope, proof, |id, proof| {
            audit(state, &scope.session, id, proof)
        })?;
    Ok(head)
}
pub(super) fn record<'a>(state: &'a State, scope: &Scope, proof: &mut Proof) -> Result<&'a Record> {
    crate::memory::deletion::ensure_live(state, &scope.session)?;
    let head = history(state, scope, proof)?;
    let record = state
        .queue
        .get(&(scope.session.clone(), scope.queue.clone()))
        .ok_or(StoreError::NotFound)?;
    proof.measure(&(scope, record)).map_err(ReadError::Budget)?;
    record.validate(&scope.session, &scope.queue, record.summary.sequence)?;
    if let Some(head) = head
        && record.summary
            != state
                .queue_audits
                .get(&(scope.session.clone(), head.operation.clone()))
                .ok_or_else(corrupt)?
                .receipt
                .summary
    {
        return Err(corrupt().into());
    }
    if let Some(receipt) = state
        .receipts
        .get(&(scope.session.clone(), record.input.admission.key.clone()))
    {
        proof.measure(receipt).map_err(ReadError::Budget)?;
    }
    current(state, &record.summary, &record.input.admission.key)?;
    let mut selected = |id: &ControlId| {
        if head.is_none() {
            audit(state, &scope.session, id, proof)
        } else {
            state
                .queue_audits
                .get(&(scope.session.clone(), id.clone()))
                .ok_or_else(|| corrupt().into())
        }
    };
    let enqueue = selected(&record.enqueue_operation)?;
    let prepare = record
        .prepare_operation
        .as_ref()
        .map(&mut selected)
        .transpose()?;
    let claim = record
        .claim
        .as_ref()
        .map(|c| selected(&c.operation))
        .transpose()?;
    verify_sources(record, enqueue, prepare, claim)?;
    Ok(record)
}

pub(super) fn context<'a>(
    state: &'a State,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<(
    &'a crabber_core::Session,
    Option<&'a crate::management::Metadata>,
)> {
    crate::memory::deletion::ensure_live(state, session)?;
    let stored = state.sessions.get(session).ok_or(StoreError::NotFound)?;
    let metadata = state.management.get(session);
    proof
        .measure(&(session, stored, metadata))
        .map_err(ReadError::Budget)?;
    crate::queue::validate_session(
        stored,
        metadata.unwrap_or(&crate::management::Metadata::default()),
    )?;
    Ok((stored, metadata))
}
