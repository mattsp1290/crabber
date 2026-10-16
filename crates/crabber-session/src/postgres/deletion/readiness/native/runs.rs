//! Narrow current fence and the independent immutable issuance chain.
//! Missing legacy issuance never becomes positive ownership coverage.
mod terminal;
use crate::native_parent::{RunIssuance, RunState};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{RunId, RunStatus, SessionId};
use sqlx::{Postgres, Row, Transaction};
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<()> {
    super::super::super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(TOKEN_COST).bind(&session.0).bind(&run.0),
        proof,
    )
    .await?;
    let token: Option<String> = sqlx::query_scalar(TOKEN)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let expected = RunState::new(
        session,
        run,
        status,
        &token.ok_or_else(|| unknown("current Run token"))?,
    )
    .map_err(unknown)?;
    if let Some(latest) = journal(tx, session, run, proof).await?
        && latest != expected
    {
        return Err(unknown("current Run issuance"));
    }
    terminal::verify(tx, session, run, status, proof).await?;
    Ok(())
}
async fn journal(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<Option<RunState>> {
    let mut after = 0_i64;
    let mut latest = None;
    loop {
        let rows = sqlx::query(JOURNAL)
            .bind(&session.0)
            .bind(&run.0)
            .bind(after)
            .bind(i64::try_from(proof.budget().remaining()).map_err(unknown)?)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        if rows.is_empty() {
            return Ok(latest);
        }
        for row in &rows {
            proof.charge(
                usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?)
                    .map_err(unknown)?,
            )?;
        }
        for row in rows {
            let sequence = row.try_get::<i64, _>("sequence").map_err(unknown)?;
            let raw = row
                .try_get::<Option<String>, _>("record")
                .map_err(unknown)?
                .ok_or_else(|| unknown("Run issuance bound"))?;
            let source: RunIssuance = serde_json::from_str(&raw).map_err(unknown)?;
            source.validate().map_err(unknown)?;
            if sequence
                != after
                    .checked_add(1)
                    .ok_or_else(|| unknown("issuance sequence"))?
                || i64::try_from(source.sequence).map_err(unknown)? != sequence
                || source.state.session_id != *session
                || source.state.run_id != *run
            {
                return Err(unknown("Run issuance scope"));
            }
            after = sequence;
            latest = Some(source.state);
        }
    }
}
const TOKEN_COST: &str =
    "SELECT CAST(octet_length(claim_token) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const TOKEN: &str = "SELECT CASE WHEN octet_length(claim_token)<=512 AND jsonb_typeof(data->'claim_token')='string' AND data->>'claim_token'=claim_token AND data->>'status'=status THEN claim_token END FROM runs WHERE session_id=$1 AND id=$2";
const JOURNAL: &str = "WITH candidates AS (SELECT sequence,data_json,CAST(octet_length(data_json)+8 AS BIGINT) AS bytes FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence>$3 ORDER BY sequence LIMIT 16), bounded AS (SELECT *,SUM(bytes) OVER(ORDER BY sequence) AS used FROM candidates) SELECT sequence,bytes,CASE WHEN used<=$4 AND octet_length(data_json)<=2048 THEN data_json END AS record FROM bounded ORDER BY sequence";
