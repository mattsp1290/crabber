//! Borrow only the exact original approval batch and its immutable native sources.
use super::{Error, Proof, RegisterSuccessorExecutionDriver, State, bounded, issuer};
use crate::native_parent::RunIssuance;
use crate::session_owner::driver::successor::source::approval::Evidence;
use crate::session_owner::driver::successor::source::input::Input;
use crate::session_owner::driver::successor::source::{self, SelectedClaim, claim::Native};
use crate::session_owner::{audit::Audit, preparation::PreparationKey};

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
    let PreparationKey::Approval(id) = &origin.key else {
        return Err(Error::SemanticConflict);
    };
    let (receipt, event) = control(state, session, id, proof)?;
    let work = source::approval::work(receipt)?;
    let (crate::session_owner::preparation::ExistingWorkTarget::ApprovalClaim(target)
    | crate::session_owner::preparation::ExistingWorkTarget::ApprovalRecovery {
        continuation: target,
        ..
    }) = &work
    else {
        return Err(Error::SemanticConflict);
    };
    let record = state
        .approvals
        .get(&target.id)
        .ok_or(Error::UnknownStoreOutcome)?;
    if record.version != 1
        || record.session_id != *session
        || record.run_id != *run
        || record.batch.continuation != *target
    {
        return Err(Error::UnknownStoreOutcome);
    }
    bounded(&record.batch, 4 << 20, proof)?;
    let (suspension, suspension_event) = control(state, session, &record.batch.id, proof)?;
    let pause_id = crate::approval::derived_id("suspend-attempt", &record.batch.id);
    let (pause_parent, pause_event) = super::attempt(state, session, &pause_id, proof)?;
    let pause_frame = state
        .events
        .iter()
        .rev()
        .find(|event| {
            event.session_id == *session
                && event.cursor.is_some_and(|c| c < pause_parent.event_cursor)
        })
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(pause_frame, 65536, proof)?;
    let (claim, native_id) = source::approval::parent_operation(next, id)?;
    let (parent, parent_event) = super::attempt(state, session, &native_id, proof)?;
    let original = if let crate::ApprovalOutcome::ReclaimedUnstarted {
        original_claim_id, ..
    } = &receipt.outcome
    {
        Some(control(state, session, original_claim_id, proof)?)
    } else {
        None
    };
    let status = if claim {
        Some(issuer(
            state,
            request,
            next.sequence
                .checked_add(1)
                .ok_or(Error::UnknownStoreOutcome)?,
            proof,
        )?)
    } else {
        None
    };
    let evidence = Evidence {
        receipt,
        event,
        parent,
        parent_event,
        batch: &record.batch,
        suspension,
        suspension_event,
        pause_parent,
        pause_event,
        pause_frame,
        original,
        status,
    };
    let input = initial_input(state, request, proof)?;
    source::selected(
        request,
        previous,
        next,
        audits,
        Native::Approval { evidence, input },
        binding,
    )
}
fn control<'a>(
    state: &'a State,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
    proof: &mut Proof,
) -> Result<(&'a crate::ApprovalReceipt, &'a crabber_core::EventRecord), Error> {
    let Some(crate::memory::OperationReceipt::Approval(receipt)) =
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
    crate::approval::verify_receipt(receipt, event).map_err(|_| Error::UnknownStoreOutcome)?;
    if receipt.operation_id != *id || receipt.session_id != *session {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((receipt, event))
}

fn initial_input<'a>(
    state: &'a State,
    request: &RegisterSuccessorExecutionDriver,
    proof: &mut Proof,
) -> Result<Input<'a>, Error> {
    let run = &request.fence.run_id;
    if issuer(state, request, 1, proof)?
        .ownership_handoff
        .is_none()
    {
        return Err(Error::Unsupported);
    }
    let (current, original_issuer, witness, initial_audits) =
        super::super::super::sources(state, run, proof).map_err(|error| {
            if matches!(error, Error::Unsupported) {
                Error::UnknownStoreOutcome
            } else {
                error
            }
        })?;
    for a in [
        initial_audits.reserve,
        initial_audits.begin,
        initial_audits.join,
        initial_audits.child_reserve,
        initial_audits.child_begin,
    ] {
        if crate::snapshot::encoded_bytes(a, 32768).is_none() {
            return Err(Error::UnknownStoreOutcome);
        }
        a.validate()?;
    }
    if crate::snapshot::encoded_bytes(current, 16384).is_none() {
        return Err(Error::UnknownStoreOutcome);
    }
    let initial = issuer(state, request, 1, proof)?;
    if initial != original_issuer {
        return Err(Error::UnknownStoreOutcome);
    }
    witness.verify_sources(current, initial, &initial_audits)?;
    Ok(if witness.is_unkeyed() {
        Input::Unkeyed(witness)
    } else {
        let init = super::super::super::initial_journal(state, run, initial, proof)?;
        let record = state
            .admission_executions
            .get(run)
            .ok_or(Error::Unsupported)?;
        bounded(record, 16 << 20, proof)?;
        Input::Keyed {
            init,
            initial,
            capsule: &record.capsule,
            receipt: &record.receipt,
            key: &record.key,
        }
    })
}
