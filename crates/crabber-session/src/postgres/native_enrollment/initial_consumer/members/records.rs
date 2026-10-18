use super::{parts, read, same};
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::{Failure, Proof, unknown};
use sqlx::Postgres;

use crabber_core::{EventRecord, Message, Run};
use sqlx::Row;
pub(super) async fn event(
    tx: &mut Transaction<'_, Postgres>,
    expected: &EventRecord,
    proof: &mut Proof,
) -> Result<(), Failure> {
    let cursor = expected
        .cursor
        .ok_or_else(|| unknown("missing original event cursor"))?;
    let actual = crate::postgres::read_proof::event(tx, &expected.session_id, cursor, false, proof)
        .await
        .map_err(read)?;
    same(&actual, expected)
}
pub(super) async fn message(
    tx: &mut Transaction<'_, Postgres>,
    expected: &Message,
    proof: &mut Proof,
) -> Result<(), Failure> {
    crate::postgres::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(data::text)+octet_length(id)+octet_length(session_id)+COALESCE(octet_length(run_id),0) AS BIGINT) FROM messages WHERE id=$1").bind(&expected.id.0),proof).await?;
    let row =
        sqlx::query("SELECT data::text AS data,id,session_id,run_id FROM messages WHERE id=$1")
            .bind(&expected.id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(unknown)?
            .ok_or_else(|| unknown("missing native assistant message"))?;
    let text: String = row.try_get("data").map_err(unknown)?;
    let actual: Message = serde_json::from_str(&text).map_err(unknown)?;
    if row.try_get::<String, _>("id").map_err(unknown)? != actual.id.0
        || row.try_get::<String, _>("session_id").map_err(unknown)? != actual.session_id.0
        || row
            .try_get::<Option<String>, _>("run_id")
            .map_err(unknown)?
            .as_deref()
            != actual.run_id.as_ref().map(|id| id.0.as_str())
    {
        return Err(unknown("native message indexed route"));
    }
    same(&actual, expected)?;
    parts::verify(tx, expected, proof).await
}
pub(super) async fn run(
    tx: &mut Transaction<'_, Postgres>,
    expected: &Run,
    proof: &mut Proof,
) -> Result<(), Failure> {
    crate::postgres::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(data::text)+octet_length(id)+octet_length(session_id) AS BIGINT) FROM runs WHERE id=$1").bind(&expected.id.0),proof).await?;
    let row = sqlx::query("SELECT data::text AS data,id,session_id FROM runs WHERE id=$1")
        .bind(&expected.id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .ok_or_else(|| unknown("missing native consumer Run"))?;
    let text: String = row.try_get("data").map_err(unknown)?;
    let actual: Run = serde_json::from_str(&text).map_err(unknown)?;
    if row.try_get::<String, _>("id").map_err(unknown)? != actual.id.0
        || row.try_get::<String, _>("session_id").map_err(unknown)? != actual.session_id.0
    {
        return Err(unknown("native Run indexed route"));
    }
    same(&actual, expected)
}
