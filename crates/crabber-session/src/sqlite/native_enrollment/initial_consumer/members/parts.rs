//! Message body and its separately persisted native Part rows are distinct carriers.
use super::{Failure, Proof, Sqlite, Transaction, same, unknown};
use sqlx::Row;
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    expected: &crabber_core::Message,
    proof: &mut Proof,
) -> Result<(), Failure> {
    proof.measure(&expected.id)?;
    let (count,bytes):(i64,i64)=sqlx::query_as("SELECT count(*),COALESCE(sum(length(CAST(data AS BLOB))+length(CAST(id AS BLOB))+length(CAST(message_id AS BLOB))+8),0) FROM parts WHERE message_id=$1").bind(&expected.id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    proof.charge(16)?;
    proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
    if usize::try_from(count).map_err(unknown)? != expected.parts.len() {
        return Err(unknown("consumer native Part roster"));
    }
    let rows=sqlx::query("SELECT id,message_id,ordinal,data AS data FROM parts WHERE message_id=$1 ORDER BY ordinal,id").bind(&expected.id.0).fetch_all(&mut **tx).await.map_err(unknown)?;
    if rows.len() != expected.parts.len() {
        return Err(unknown("consumer Part membership changed"));
    }
    for row in rows {
        let text: String = row.try_get("data").map_err(unknown)?;
        let actual: crabber_core::Part = serde_json::from_str(&text).map_err(unknown)?;
        if row.try_get::<String, _>("id").map_err(unknown)? != actual.id.0
            || row.try_get::<String, _>("message_id").map_err(unknown)? != actual.message_id.0
            || row.try_get::<i64, _>("ordinal").map_err(unknown)? != i64::from(actual.ordinal)
        {
            return Err(unknown("consumer native Part route"));
        }
        let original = expected
            .parts
            .iter()
            .find(|part| part.id == actual.id)
            .ok_or_else(|| unknown("consumer additional native Part"))?;
        same(&actual, original)?;
    }
    Ok(())
}
