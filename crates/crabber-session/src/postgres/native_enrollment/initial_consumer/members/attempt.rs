use super::{domain, read, records, same};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use sqlx::Postgres;

pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    expected: &domain::AttemptOriginal,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let actual = crate::postgres::attempt::records::receipt_with_proof(
        tx,
        &expected.receipt.attempt.session_id,
        &expected.receipt.operation_id,
        proof,
    )
    .await
    .map_err(read)?
    .ok_or_else(|| unknown("missing native Attempt receipt"))?;
    same(&actual, &expected.receipt)?;
    records::event(tx, &expected.event, proof).await?;
    if let Some(value) = &expected.publication {
        let issuer = crate::postgres::native_parent::source_protocol::original(
            tx,
            &value.issuer.state.session_id,
            &value.issuer.state.run_id,
            value.issuer.sequence,
            proof,
        )
        .await
        .map_err(read)?
        .ok_or_else(|| unknown("missing captured consumer issuer"))?;
        let source = crate::postgres::native_parent::source_protocol::source(
            tx,
            &value.issuer.state.session_id,
            &value.issuer.state.run_id,
            value.issuer.sequence,
            proof,
        )
        .await
        .map_err(read)?
        .ok_or_else(|| unknown("missing captured consumer issuer source"))?;
        same(&issuer, &value.issuer)?;
        same(&source, &value.source)?;
    }
    Ok(())
}
pub(super) async fn current(
    tx: &mut Transaction<'_, Postgres>,
    expected: &domain::StoredAttempt,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.charge(8)?;
    let count: i64 = sqlx::query_scalar("SELECT count(*) FROM execution_attempts WHERE run_id=$1")
        .bind(&expected.snapshot.run_id.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if count != 1 {
        return Err(unknown("consumer missing/additional Attempt target"));
    }
    let (actual, _) = crate::postgres::attempt::records::load_with_proof(
        tx,
        &expected.snapshot.run_id,
        Some(&expected.snapshot.target.id),
        proof,
    )
    .await
    .map_err(read)?
    .ok_or_else(|| unknown("missing current consumer Attempt"))?;
    if actual.snapshot != expected.snapshot
        || actual.owner != expected.owner
        || actual.token != expected.token
        || actual.run_token != expected.run_token
        || actual.lease_until != expected.lease_until
    {
        return Err(unknown(
            "consumer current Attempt differs from latest original",
        ));
    }
    Ok(())
}
