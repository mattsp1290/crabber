//! One selected outer-worker join cannot certify complete Run membership.
use super::super::Record;
use super::source;
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, ReadError, unknown};
use sqlx::Postgres;
pub(in crate::postgres) async fn classified(
    tx: &mut Transaction<'_, Postgres>,
    record: &Record,
    proof: &mut Proof,
) -> Result<bool, Failure> {
    if !proof.active() {
        return Err(unknown("driver join requires bounded proof"));
    }
    proof.measure(record)?;
    let Some(link) = crate::session_owner::coverage::execution_driver(record).map_err(unknown)?
    else {
        return Ok(false);
    };
    super::super::protocol::require_record(tx, record, proof)
        .await
        .map_err(ReadError::public)?;
    let original = source::original_read(
        tx,
        &record.snapshot.binding.session_id,
        link.run_id(),
        proof,
    )
    .await
    .map_err(ReadError::public)?;
    original.verify_link(link).map_err(unknown)?;
    if original.session.workspace_id != record.snapshot.binding.workspace_id
        || original.session.directory != record.snapshot.binding.directory
    {
        return Err(unknown("driver original context"));
    }
    Ok(true)
}

#[cfg(test)]
#[path = "history_tests.rs"]
mod tests;
