use super::{Error, Proof, source, unknown};
use crate::transaction::Transaction;
use sqlx::Sqlite;
pub(super) async fn check(
    tx: &mut Transaction<'_, Sqlite>,
    run: &crabber_core::Run,
    record: &crate::tool_continuation::ToolContinuationRecord,
    binding: &crate::ExecutionBindingCapsule,
    current: &crate::native_parent::RunIssuance,
    proof: &mut Proof,
) -> Result<(), Error> {
    let (id, _) = record.latest_claim_source().map_err(unknown)?;
    let scope = crate::session_owner::driver::successor::source::scope::Scope {
        session: &run.session_id,
        run: &run.id,
    };
    let facts = Box::pin(source::tool::verify_scope(tx, scope, &id, binding, proof)).await?;
    if facts.continuation != record.batch.continuation || facts.attempt != record.batch.next_attempt
    {
        return Err(Error::SemanticConflict);
    }
    source::scope::ancestry(tx, scope, &facts, current, proof).await
}
