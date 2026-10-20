//! Explicit original unkeyed input, never inferred from a missing keyed journal.
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::session_owner::preparation::admission::Witness;
use crate::sql_deletion::readiness::{Result, unknown};
use sqlx::Postgres;
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    original: &Witness,
    binding: Option<&crate::RetainedExecutionBinding>,
    keyed: bool,
    proof: &mut Proof,
) -> Result<bool> {
    if keyed {
        return Err(unknown("mixed unkeyed/keyed original input"));
    }
    let receipt = has_keyed_receipt(tx, original, proof).await?;
    if receipt {
        return Err(unknown("unkeyed original has keyed receipt"));
    }
    let binding = binding.ok_or_else(|| unknown("original unkeyed binding missing"))?;
    if binding.session_id != *original.session_id() || binding.run_id != *original.run_id() {
        return Err(unknown("original unkeyed binding routing"));
    }
    // admission::verify independently authenticated Witness and all original
    // owner publications; the binding's entire original capsule is now compared.
    original
        .verify_unkeyed_binding(&binding.capsule)
        .map_err(unknown)?;
    Ok(true)
}

pub(super) async fn has_keyed_receipt(
    tx: &mut Transaction<'_, Postgres>,
    original: &Witness,
    proof: &mut Proof,
) -> Result<bool> {
    proof.charge(1)?;
    let receipt: bool =
        sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM admission_receipts WHERE run_id=$1)")
            .bind(&original.run_id().0)
            .fetch_one(&mut **tx)
            .await
            .map_err(unknown)?;
    Ok(receipt)
}
