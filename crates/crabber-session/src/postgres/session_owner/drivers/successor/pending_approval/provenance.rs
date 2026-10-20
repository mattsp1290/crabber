use super::{Error, Proof, source, unknown};
use crate::postgres::transaction::Transaction;
use sqlx::Postgres;
pub(super) async fn check(
    tx: &mut Transaction<'_, Postgres>,
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
    let original_facts = Box::pin(source::approval::verify_scope(
        tx, scope, original, binding, proof,
    ))
    .await?;
    let selected = crate::postgres::approval::current::rows_with_proof(
        tx,
        record,
        crate::postgres::approval::current::Select::Latest,
        proof,
    )
    .await
    .map_err(unknown)?;
    let [row] = selected.as_slice() else {
        return Err(Error::UnknownStoreOutcome);
    };
    let latest = crate::postgres::approval::current::source(row).map_err(unknown)?;
    let facts = if latest.receipt.operation_id == *original {
        original_facts
    } else {
        Box::pin(source::approval::verify_scope(
            tx,
            scope,
            &latest.receipt.operation_id,
            binding,
            proof,
        ))
        .await?
    };
    if facts.continuation != record.batch.continuation
        || facts.attempt != record.batch.next_attempt
        || facts.revision != latest.receipt.revision
    {
        return Err(Error::SemanticConflict);
    }
    source::scope::ancestry(tx, scope, &facts, current, proof).await
}
