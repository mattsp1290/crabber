use super::{Error, FrontendCallStaging, Run, State};
pub(super) fn binding(
    state: &State,
    run: &Run,
    request: &FrontendCallStaging,
) -> Result<(), Error> {
    let binding = super::super::execution_binding::read(state, &run.session_id, &run.id)
        .map_err(|_| Error::UnknownStoreOutcome)?
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
    crate::frontend_continuation::verify_assistant_source(request, source)
}

pub(in super::super) fn declared(state: &State, run: &Run, name: &str) -> Result<bool, Error> {
    let binding = super::super::execution_binding::read(state, &run.session_id, &run.id)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    Ok(binding
        .as_ref()
        .and_then(|binding| binding.capsule.input_envelope.as_ref())
        .is_some_and(|input| input.frontend_tools().iter().any(|tool| tool.name == name)))
}
