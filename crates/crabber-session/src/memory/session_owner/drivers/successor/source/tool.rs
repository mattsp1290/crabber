//! Borrow selected original requests, independently of later mutable selectors.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, State, audit, bounded, issuer};
use crate::native_parent::RunIssuance;
use crate::session_owner::audit::Audit;
use crate::session_owner::driver::successor::source::{self, SelectedClaim, claim::Native, tool};
use crate::session_owner::preparation::PreparationKey;
use crabber_core::{ControlId, SessionId};

pub(super) fn original(
    state: &State,
    request: &RegisterSuccessorExecutionDriver,
    previous: &RunIssuance,
    next: &RunIssuance,
    audits: &[Audit],
    proof: &mut Proof,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    let session = request.context.session_id();
    let run = &request.fence.run_id;
    let origin = audits[0]
        .record
        .preparation
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    let PreparationKey::ToolContinuation(id) = &origin.key else {
        return Err(Error::SemanticConflict);
    };
    let (receipt, event) = control(state, session, id, proof)?;
    let (target, _, original_id) = tool::targets(receipt)?;
    let record = state
        .tool_continuations
        .get(&target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(
        &(
            record.version,
            &record.session_id,
            &record.run_id,
            &record.batch,
            &record.claim,
            &record.frontend_claim,
        ),
        4 << 20,
        proof,
    )?;
    if record.version != 1
        || record.session_id != *session
        || record.run_id != *run
        || record.batch.continuation != *target
    {
        return Err(Error::UnknownStoreOutcome);
    }
    let (suspension, suspension_event) = control(state, session, &record.batch.id, proof)?;
    if !state
        .continuation_inputs
        .verify(
            crate::continuation_input::Input::ToolSuspension(&record.batch),
            crate::continuation_input::Receipt::Tool(suspension),
            suspension_event,
            proof,
        )
        .map_err(|_| Error::UnknownStoreOutcome)?
    {
        return Err(Error::Unsupported);
    }
    let scope = crate::continuation_input::Scope {
        domain: crate::continuation_input::Domain::Tool,
        session: session.clone(),
        run: run.clone(),
        continuation: target.clone(),
    };
    proof.charge(8).map_err(|_| Error::UnknownStoreOutcome)?;
    if record.questions.len() > 64 {
        return Err(Error::UnknownStoreOutcome);
    }
    let answers: Vec<_> = record
        .questions
        .iter()
        .filter_map(|q| q.answer.as_ref())
        .take(65)
        .map(crate::continuation_input::Input::ToolAnswer)
        .collect();
    bounded(&answers, 4 << 20, proof)?;
    crate::memory::continuation_input::answers(state, &scope, &answers, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let original = original_id
        .map(|id| original_claim(state, request, id, proof))
        .transpose()?;
    let evidence = tool::Evidence {
        receipt,
        event,
        batch: &record.batch,
        suspension,
        suspension_event,
        claim: record.claim.as_ref().ok_or(Error::UnknownStoreOutcome)?,
        frontend: record.frontend_claim.as_deref(),
        original: original
            .as_ref()
            .map(|(previous, next, audits, receipt, event)| tool::Claim {
                previous,
                next,
                audits,
                receipt,
                event,
            }),
    };
    let input = super::approval::initial_input(state, request, proof)?;
    source::selected(
        request,
        previous,
        next,
        audits,
        Native::Tool { evidence, input },
        binding,
    )
}

type Original<'a> = (
    &'a RunIssuance,
    &'a RunIssuance,
    Vec<Audit>,
    &'a crate::ToolContinuationReceipt,
    &'a crabber_core::EventRecord,
);
fn original_claim<'a>(
    state: &'a State,
    request: &RegisterSuccessorExecutionDriver,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Original<'a>, Error> {
    let session = request.context.session_id();
    let join = crate::session_owner::preparation::identity::ids(
        &PreparationKey::ToolContinuation(id.clone()),
        session,
    )?
    .join;
    let mut sequence = None;
    for ((owner, run, slot), value) in &state.native_run_issuances {
        if owner == session
            && *run == request.fence.run_id
            && value
                .completion_source
                .as_ref()
                .is_some_and(|source| source.root_join_id == join)
        {
            bounded(value, 2048, proof)?;
            if sequence.replace(*slot).is_some() {
                return Err(Error::UnknownStoreOutcome);
            }
        }
    }
    let sequence = sequence.ok_or(Error::UnknownStoreOutcome)?;
    let next = issuer(state, request, sequence, proof)?;
    let previous = issuer(
        state,
        request,
        sequence
            .checked_sub(1)
            .filter(|n| *n > 0)
            .ok_or(Error::UnknownStoreOutcome)?,
        proof,
    )?;
    let joined = audit(state, session, &join, proof)?;
    let mut audits = Vec::with_capacity(5);
    for id in source::claim::audit_ids(joined)? {
        audits.push(audit(state, session, &id, proof)?.clone());
    }
    let (receipt, event) = control(state, session, id, proof)?;
    Ok((previous, next, audits, receipt, event))
}

fn control<'a>(
    state: &'a State,
    session: &SessionId,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<
    (
        &'a crate::ToolContinuationReceipt,
        &'a crabber_core::EventRecord,
    ),
    Error,
> {
    let Some(crate::memory::OperationReceipt::ToolContinuation(receipt)) =
        state.operation_receipts.get(&(session.clone(), id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    bounded(receipt, 32768, proof)?;
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(event, 65536, proof)?;
    crate::tool_continuation::verify_receipt(receipt, event)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    if receipt.operation_id != *id || receipt.session_id != *session {
        return Err(Error::SemanticConflict);
    }
    Ok((receipt, event))
}
