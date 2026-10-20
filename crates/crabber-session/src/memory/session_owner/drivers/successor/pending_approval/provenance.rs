use super::{Error, Proof, State, source};
pub(super) fn check(
    state: &State,
    run: &crabber_core::Run,
    record: &crate::approval::ApprovalRecord,
    binding: &crate::ExecutionBindingCapsule,
    original: &crabber_core::ControlId,
    current: &crate::native_parent::RunIssuance,
    proof: &mut Proof,
) -> Result<(), Error> {
    let scope = crate::session_owner::driver::successor::source::scope::Scope {
        session: &run.session_id,
        run: &run.id,
    };
    let original_facts = source::approval::verify_scope(state, scope, original, binding, proof)?;
    let selected = crate::memory::approval::current::selected(state, record, false)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let [event] = selected.as_slice() else {
        return Err(Error::UnknownStoreOutcome);
    };
    let latest = crate::memory::approval::current::source_with_proof(state, event, proof)
        .map_err(|_| Error::UnknownStoreOutcome)?;
    let facts = if latest.receipt.operation_id == *original {
        original_facts
    } else {
        source::approval::verify_scope(state, scope, &latest.receipt.operation_id, binding, proof)?
    };
    if facts.continuation != record.batch.continuation
        || facts.attempt != record.batch.next_attempt
        || facts.revision != latest.receipt.revision
    {
        return Err(Error::SemanticConflict);
    }
    source::scope::ancestry(state, scope, &facts, current, proof)
}
