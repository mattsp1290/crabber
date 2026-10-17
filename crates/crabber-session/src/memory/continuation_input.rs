//! Actual original acceptance birth under the `MemoryStore` transaction lock.
use super::State;
use crate::continuation_input::{Error, Input, Receipt, Source};
pub(super) fn accept(
    state: &mut State,
    input: Input<'_>,
    receipt: Receipt<'_>,
) -> Result<(), Error> {
    let event = state.events.get(receipt.cursor()).ok_or(Error)?;
    let source = Source::new(input, receipt, event)?;
    if state.tombstones.contains_key(&source.scope.session) {
        return Err(Error);
    }
    state.continuation_inputs.publish(source)
}

pub(super) fn answers(
    state: &State,
    scope: &crate::continuation_input::Scope,
    inputs: &[Input<'_>],
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::public_session::v2::proof::ReadError<Error>> {
    if !state.continuation_inputs.protocol(scope, proof)? {
        return Ok(());
    }
    if inputs.len() > 64 {
        return Err(Error.into());
    }
    let mut expected = Vec::new();
    for input in inputs {
        let (subject, operation) = input.answer_identity()?;
        let saved = state
            .operation_receipts
            .get(&(scope.session.clone(), operation.clone()))
            .ok_or(Error)?;
        let receipt = match (input, saved) {
            (Input::ApprovalAnswer(_), super::OperationReceipt::Approval(r)) => {
                Receipt::Approval(r)
            }
            (Input::ToolAnswer(_), super::OperationReceipt::ToolContinuation(r)) => {
                Receipt::Tool(r)
            }
            _ => return Err(Error.into()),
        };
        let event = state.events.get(receipt.cursor()).ok_or(Error)?;
        proof
            .measure(&receipt)
            .map_err(crate::public_session::v2::proof::ReadError::Budget)?;
        proof
            .measure(event)
            .map_err(crate::public_session::v2::proof::ReadError::Budget)?;
        state
            .continuation_inputs
            .verify(*input, receipt, event, proof)?;
        expected.push((subject.0.clone(), operation.clone()));
    }
    expected.sort();
    if state.continuation_inputs.subjects(scope, proof)? != expected {
        return Err(Error.into());
    }
    Ok(())
}

/// Checks retained birth membership without cloning private record bodies.
pub(super) fn consistent(
    state: &State,
    domain: crate::continuation_input::Domain,
    session: &crabber_core::SessionId,
    run: Option<&crabber_core::RunId>,
    proof: &mut crate::public_session::v2::proof::Proof,
) -> Result<(), crate::public_session::v2::proof::ReadError<Error>> {
    use crate::continuation_input::Domain;
    state
        .continuation_inputs
        .consistent(domain, session, run, proof, |scope| {
            let actual = match domain {
                Domain::Approval => state
                    .approvals
                    .get(&scope.continuation.id)
                    .map(|r| (&r.session_id, &r.run_id, &r.batch.continuation)),
                Domain::Tool => state
                    .tool_continuations
                    .get(&scope.continuation.id)
                    .map(|r| (&r.session_id, &r.run_id, &r.batch.continuation)),
            };
            actual.is_some_and(|(session, run, target)| {
                session == &scope.session && run == &scope.run && target == &scope.continuation
            })
        })
}

#[cfg(test)]
#[path = "continuation_input/tests.rs"]
mod tests;

pub(super) fn accept_approval_suspension(
    state: &mut State,
    batch: &crate::ApprovalSuspension,
    receipt: &crate::ApprovalReceipt,
) -> Result<(), Error> {
    accept(
        state,
        Input::ApprovalSuspension(batch),
        Receipt::Approval(receipt),
    )
}
