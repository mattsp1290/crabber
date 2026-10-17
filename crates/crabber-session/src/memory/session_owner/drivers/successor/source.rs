//! Selected protected sources, charged while borrowed before any private clone.
#[path = "source/approval.rs"]
pub(super) mod approval;
use super::{Error, Proof, RegisterSuccessorExecutionDriver, State};
use crate::native_parent::{CompletionOperation, RunIssuance};
use crate::session_owner::{
    audit::Audit,
    driver::successor::source::{self, SelectedClaim, claim::Native},
    preparation::{ExpiredRunOrigin, expired::replay::Native as Expired},
};
pub(super) fn original(
    state: &State,
    request: &RegisterSuccessorExecutionDriver,
    proof: &mut Proof,
    binding: Option<&crate::ExecutionBindingCapsule>,
) -> Result<SelectedClaim, Error> {
    let session = request.context.session_id();
    let run = &request.fence.run_id;
    let protocol = state
        .native_run_protocols
        .contains(&(session.clone(), run.clone()));
    super::super::measure(proof, &(session, run, protocol))?;
    if !protocol {
        return Err(Error::Unsupported);
    }
    let next = issuer(state, request, request.claim.issuance, proof)?;
    let previous = issuer(state, request, request.claim.issuance - 1, proof)?;
    if next
        .completion_source
        .as_ref()
        .is_none_or(|source| source.root_join_id != request.claim.root_join_id)
    {
        return Err(Error::SemanticConflict);
    }
    let join = audit(state, session, &request.claim.root_join_id, proof)?;
    let ids = source::claim::audit_ids(join)?;
    let mut audits = Vec::with_capacity(5);
    for id in ids {
        audits.push(audit(state, session, &id, proof)?.clone());
    }
    let operation = next
        .completion_source
        .as_ref()
        .ok_or(Error::Unsupported)?
        .native_operation;
    match operation {
        CompletionOperation::AdmissionRecovery => {
            let initial = issuer(state, request, 1, proof)?;
            let init = super::super::initial_journal(state, run, initial, proof)?;
            let receipt = state
                .receipts
                .get(&(session.clone(), init.driver_key().clone()))
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(receipt, 8192, proof)?;
            source::selected(
                request,
                previous,
                next,
                &audits,
                Native::Admission {
                    init,
                    initial,
                    receipt,
                },
                binding,
            )
        }
        CompletionOperation::ExpiredRunResume => {
            expired(state, request, previous, next, &audits, proof, binding)
        }
        CompletionOperation::ApprovalClaim | CompletionOperation::ApprovalRecovery => {
            approval::original(state, request, previous, next, &audits, proof, binding)
        }
        _ => Err(Error::Unsupported),
    }
}
fn expired(
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
    let target = crate::session_owner::preparation::expired::history::target(audits)?;
    let parent = if let ExpiredRunOrigin::InitialUnstarted { reservation, .. } = &target.origin {
        let (receipt, event) = attempt(state, session, reservation, proof)?;
        Some(
            crate::session_owner::preparation::expired::unstarted::Original {
                receipt: receipt.clone(),
                event: event.clone(),
            },
        )
    } else {
        None
    };
    let origin = if let ExpiredRunOrigin::InitialUnstarted { admission, .. } = &target.origin {
        crate::session_owner::preparation::expired::unstarted::admission(admission)
    } else {
        target.origin.clone()
    };
    let verify = |native| {
        source::selected(
            request,
            previous,
            next,
            audits,
            Native::Expired(if let Some(original) = &parent {
                Expired::Unstarted {
                    initial: Box::new(native),
                    original,
                }
            } else {
                native
            }),
            binding,
        )
    };
    match origin {
        ExpiredRunOrigin::InitialUnstarted { .. } => Err(Error::UnknownStoreOutcome),
        ExpiredRunOrigin::Suspended(id) => {
            let (receipt, event) = attempt(state, session, &id, proof)?;
            verify(Expired::Suspended(receipt, event))
        }
        ExpiredRunOrigin::UnkeyedAdmission(_) => {
            let (current, _, witness, sources) = super::super::sources(state, run, proof)?;
            if crate::snapshot::encoded_bytes(current, 16384).is_none() {
                return Err(Error::UnknownStoreOutcome);
            }
            for audit in [
                sources.reserve,
                sources.begin,
                sources.join,
                sources.child_reserve,
                sources.child_begin,
            ] {
                if crate::snapshot::encoded_bytes(audit, 32768).is_none() {
                    return Err(Error::UnknownStoreOutcome);
                }
                audit.validate()?;
            }
            let initial = issuer(state, request, 1, proof)?;
            let original = crate::session_owner::preparation::expired::unkeyed::Original {
                session: current.clone(),
                witness: witness.clone(),
                initial: initial.clone(),
                audits: [
                    sources.reserve.clone(),
                    sources.begin.clone(),
                    sources.join.clone(),
                    sources.child_reserve.clone(),
                    sources.child_begin.clone(),
                ],
            };
            verify(Expired::Unkeyed(&original))
        }
        ExpiredRunOrigin::KeyedInitial(key) => {
            let initial = issuer(state, request, 1, proof)?;
            let init = super::super::initial_journal(state, run, initial, proof)?;
            let source = state
                .admission_execution_sources
                .get(&(run.clone(), crate::admission_journal::Kind::Begin))
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(source, 8192, proof)?;
            let crate::admission_journal::Source::Begin(begin) = source else {
                return Err(Error::UnknownStoreOutcome);
            };
            let activated_issuer = issuer(state, request, begin.issuance.sequence, proof)?;
            let receipt = state
                .receipts
                .get(&(session.clone(), key))
                .ok_or(Error::UnknownStoreOutcome)?;
            bounded(receipt, 8192, proof)?;
            verify(Expired::Initial {
                init,
                begin,
                receipt,
                initial,
                begun: activated_issuer,
            })
        }
    }
}
fn attempt<'a>(
    state: &'a State,
    session: &crabber_core::SessionId,
    id: &crabber_core::ControlId,
    proof: &mut Proof,
) -> Result<(&'a crate::AttemptReceipt, &'a crabber_core::EventRecord), Error> {
    let Some(crate::memory::OperationReceipt::Attempt(receipt)) =
        state.operation_receipts.get(&(session.clone(), id.clone()))
    else {
        return Err(Error::UnknownStoreOutcome);
    };
    bounded(receipt, 8192, proof)?;
    let event = state
        .events
        .get(receipt.event_cursor)
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(event, 65536, proof)?;
    if receipt.operation_id != *id || receipt.attempt.session_id != *session {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok((receipt, event))
}
fn audit<'a>(
    state: &'a State,
    session: &crabber_core::SessionId,
    id: &crabber_core::SessionOwnerOperationId,
    proof: &mut Proof,
) -> Result<&'a Audit, Error> {
    let audit = state
        .session_owners
        .source_audit(session, id)
        .ok_or(Error::UnknownStoreOutcome)?;
    bounded(audit, 32768, proof)?;
    audit.validate()?;
    if audit.receipt.session_id != *session || audit.receipt.operation_id != *id {
        return Err(Error::UnknownStoreOutcome);
    }
    Ok(audit)
}
fn issuer<'a>(
    state: &'a State,
    request: &RegisterSuccessorExecutionDriver,
    sequence: u64,
    proof: &mut Proof,
) -> Result<&'a RunIssuance, Error> {
    crate::memory::native_parent::source_protocol::original(
        state,
        request.context.session_id(),
        &request.fence.run_id,
        sequence,
        proof,
    )
    .map_err(|_| Error::UnknownStoreOutcome)?
    .ok_or(Error::UnknownStoreOutcome)
}
fn bounded(value: &impl serde::Serialize, cap: usize, proof: &mut Proof) -> Result<(), Error> {
    super::super::measure(proof, value)?;
    crate::snapshot::encoded_bytes(value, cap)
        .map(|_| ())
        .ok_or(Error::UnknownStoreOutcome)
}
