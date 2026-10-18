use super::{Failure, Proof, Sqlite, Transaction, domain, read, same, unknown};
use sqlx::Row;
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    expected: &domain::KeyedBeginOriginal,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let actual = crate::sqlite::admission_execution::load_record_with_proof(
        tx,
        &expected.record.receipt.run_id,
        proof,
    )
    .await
    .map_err(read)?
    .ok_or_else(|| unknown("missing native keyed record"))?;
    let init = crate::sqlite::admission_journal::read_with_proof(tx, &actual, proof)
        .await
        .map_err(read)?
        .ok_or_else(|| unknown("missing native keyed Init"))?;
    same(&actual, &expected.record)?;
    same(&init, &expected.init)?;
    // read_with_proof authenticates both actual immutable sources and their
    // selected historical issuers. Retain/corroborate the actual Begin bytes too.
    crate::sqlite::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(data_json AS BLOB))+length(CAST(session_id AS BLOB))+length(CAST(kind AS BLOB)) AS BIGINT) FROM admission_execution_sources WHERE run_id=$1 AND kind='begin'").bind(&actual.receipt.run_id.0),proof).await?;

    let row=sqlx::query("SELECT data_json,session_id FROM admission_execution_sources WHERE run_id=$1 AND kind='begin'").bind(&actual.receipt.run_id.0).fetch_optional(&mut **tx).await.map_err(unknown)?.ok_or_else(||unknown("missing native keyed Begin"))?;
    let text: String = row.try_get("data_json").map_err(unknown)?;
    let source = crate::admission_journal::Source::decode(&text).map_err(unknown)?;
    if row.try_get::<String, _>("session_id").map_err(unknown)? != actual.receipt.session_id.0 {
        return Err(unknown("native keyed Begin route"));
    }
    let crate::admission_journal::Source::Begin(begin) = source else {
        return Err(unknown("native keyed Begin kind"));
    };
    same(&begin, &expected.begin)
}
