//! Fresh effect guards require the complete protected source membership.
use super::{
    Proof, ReadError, Result, RunId, RunIssuance, RunState, SessionId, bad, head, history, original,
};
use crate::postgres::transaction::Transaction;
use sqlx::{Postgres, Row};
pub(in crate::postgres) async fn read(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<RunIssuance>> {
    if !history(tx, session, run, proof).await? {
        return Err(bad("unprotected fresh Run"));
    }
    let selected = head(tx, session, run, proof)
        .await?
        .ok_or_else(|| bad("current head"))?;
    super::super::super::read_proof::charge(tx, sqlx::query_scalar("SELECT CAST(octet_length(claim_token)+octet_length(status) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2").bind(&session.0).bind(&run.0), proof).await.map_err(ReadError::Budget)?;
    let row = sqlx::query("SELECT CASE WHEN octet_length(claim_token)<=512 THEN claim_token END AS token,CASE WHEN octet_length(status)<=32 THEN status END AS status,(data->'id'=to_jsonb(id) AND data->'session_id'=to_jsonb(session_id) AND data->'status'=to_jsonb(status) AND data->'claim_token'=to_jsonb(claim_token)) AS valid FROM runs WHERE session_id=$1 AND id=$2").bind(&session.0).bind(&run.0).fetch_one(&mut **tx).await.map_err(bad)?;
    if row.try_get::<Option<bool>, _>("valid").map_err(bad)? != Some(true) {
        return Err(bad("current native Run projection"));
    }
    let token = row
        .try_get::<Option<String>, _>("token")
        .map_err(bad)?
        .ok_or_else(|| bad("current token"))?;
    let status = row
        .try_get::<Option<String>, _>("status")
        .map_err(bad)?
        .ok_or_else(|| bad("current status"))?;
    let status = serde_json::from_value(serde_json::Value::String(status)).map_err(bad)?;
    let actual = RunState::new(session, run, status, &token).map_err(bad)?;
    let issuer = original(tx, session, run, selected.sequence, proof)
        .await?
        .ok_or_else(|| bad("current issuer"))?;
    if issuer.state != actual {
        return Err(bad("current Run state"));
    }
    Ok(Some(issuer))
}
