use super::{Error, FrontendCallStaging, Run, State};
use crate::public_session::v2::proof::{Proof, ToolReadError};
pub(super) fn binding(
    state: &State,
    run: &Run,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    binding_with_proof(state, run, request, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn binding_with_proof(
    state: &State,
    run: &Run,
    request: &FrontendCallStaging,
    proof: &mut Proof,
) -> Result<(), ToolReadError> {
    let binding =
        super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?
            .ok_or(Error::UnknownStoreOutcome)?;
    let input = binding
        .capsule
        .input_envelope
        .as_ref()
        .ok_or(Error::UnknownStoreOutcome)?;
    crate::frontend_continuation::verify_declaration(request, input)?;
    let source = state
        .messages
        .for_session(&run.session_id)
        .iter()
        .find(|message| message.id == request.message_id)
        .ok_or(Error::SemanticConflict)?;
    proof.measure(source).map_err(ToolReadError::Budget)?;
    Ok(crate::frontend_continuation::verify_assistant_source(
        request, source,
    )?)
}

pub(in super::super) fn declared(state: &State, run: &Run, name: &str) -> Result<bool, Error> {
    declared_with_proof(state, run, name, &mut Proof::legacy())
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(super) fn declared_with_proof(
    state: &State,
    run: &Run,
    name: &str,
    proof: &mut Proof,
) -> Result<bool, ToolReadError> {
    let binding =
        super::super::execution_binding::read_with_proof(state, &run.session_id, &run.id, proof)
            .map_err(|e| e.map_domain(|_| Error::UnknownStoreOutcome))?;
    Ok(binding
        .as_ref()
        .and_then(|binding| binding.capsule.input_envelope.as_ref())
        .is_some_and(|input| input.frontend_tools().iter().any(|tool| tool.name == name)))
}
