//! Borrow selected original requests, independently of later mutable selectors.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, State, bounded};
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
    with_native(
        state,
        super::scope::Scope { session, run },
        id,
        proof,
        |native| source::selected(request, previous, next, audits, native, binding),
    )
}
fn with_native<T>(
    state: &State,
    selected_scope: super::scope::Scope<'_>,
    id: &ControlId,
    proof: &mut Proof,
    use_native: impl FnOnce(Native<'_>) -> Result<T, Error>,
) -> Result<T, Error> {
    let session = selected_scope.session;
    let run = selected_scope.run;
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
        .map(|id| original_claim(state, selected_scope, id, proof))
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
    let input = super::approval::input_for(state, session, run, proof)?;
    use_native(Native::Tool { evidence, input })
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
    scope: super::scope::Scope<'_>,
    id: &ControlId,
    proof: &mut Proof,
) -> Result<Original<'a>, Error> {
    let pair = super::scope::pair(
        state,
        scope,
        &PreparationKey::ToolContinuation(id.clone()),
        proof,
    )?;
    let (receipt, event) = control(state, scope.session, id, proof)?;
    Ok((pair.previous, pair.next, pair.audits, receipt, event))
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

pub(in crate::memory::session_owner::drivers::successor) fn verify_scope(
    state: &State,
    scope: super::scope::Scope<'_>,
    id: &ControlId,
    binding: &crate::ExecutionBindingCapsule,
    proof: &mut Proof,
) -> Result<source::ContinuationFacts, Error> {
    let pair = super::scope::pair(
        state,
        scope,
        &PreparationKey::ToolContinuation(id.clone()),
        proof,
    )?;
    with_native(state, scope, id, proof, |native| {
        if let Native::Tool { input, .. } = &native {
            input.verify(scope.session, scope.run, Some(binding))?;
        }
        let verified =
            source::AuthenticatedClaim::verify(pair.previous, pair.next, &pair.audits, native)?;
        verified.verify_binding(binding)?;
        let (continuation, attempt) = verified.tool.ok_or(Error::SemanticConflict)?;
        Ok(source::ContinuationFacts {
            revision: verified.revision,
            head: verified.head,
            head_digest: verified.head_digest,
            continuation,
            attempt,
        })
    })
}
