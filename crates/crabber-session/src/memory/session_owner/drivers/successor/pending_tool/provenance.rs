use super::{Error, Proof, State, source};
pub(super) fn check(
    state: &State,
    run: &crabber_core::Run,
    record: &crate::tool_continuation::ToolContinuationRecord,
    binding: &crate::ExecutionBindingCapsule,
    current: &crate::native_parent::RunIssuance,
    proof: &mut Proof,
) -> Result<(), Error> {
    let (id, _) = record
        .latest_claim_source()
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let scope = crate::session_owner::driver::successor::source::scope::Scope {
        session: &run.session_id,
        run: &run.id,
    };
    // Recovery evidence independently verifies the original Claim's own root too.
    let facts = source::tool::verify_scope(state, scope, &id, binding, proof)?;
    if facts.continuation != record.batch.continuation || facts.attempt != record.batch.next_attempt
    {
        return Err(Error::SemanticConflict);
    }
    source::scope::ancestry(state, scope, &facts, current, proof)
}
