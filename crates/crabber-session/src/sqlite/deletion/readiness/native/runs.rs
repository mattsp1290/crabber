//! Narrow current fence and the independent immutable issuance chain.
//! Missing legacy issuance never becomes positive ownership coverage.
mod claims;
mod sources;
mod terminal;
use crate::deletion::history::{NoninitialClaims, RunHistoryReport};
use crate::native_parent::{RunIssuance, RunState};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crate::transaction::Transaction;
use crabber_core::{RunId, RunStatus, SessionId};
use sqlx::{Row, Sqlite};
/// Independent source facts remain distinct from actual resource joins.
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<RunHistoryReport> {
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
    let protected = super::super::super::super::native_parent::source_protocol::history(
        tx, session, run, proof,
    )
    .await
    .map_err(crate::public_session::v2::proof::ReadError::public)?;
    let (initial, latest, claims) = journal(tx, session, run, proof).await?;
    if let Some(latest) = latest
        && latest != expected
    {
        return Err(unknown("current Run issuance"));
    }
    let admitted = sources::verify(tx, session, run, initial.as_ref(), proof).await?;
    let terminal = terminal::verify(tx, session, run, status, proof).await?;
    let abandoned =
        super::super::super::super::abandon_history::verify(tx, session, run, proof).await?;
    Ok(RunHistoryReport::sources(
        admitted,
        claims.with_membership(protected),
        terminal || abandoned,
    ))
}
async fn journal(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<(
    Option<RunIssuance>,
    Option<RunState>,
    crate::deletion::history::ClaimHistory,
)> {
    let mut after = 0_i64;
    let mut latest: Option<RunIssuance> = None;
    let mut initial = None;
    let mut claims = NoninitialClaims::default();
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
            let claims = claims.finish(initial.is_some());
            return Ok((initial, latest.map(|issuer| issuer.state), claims));
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
            let cursor = Box::pin(claims::source(tx, latest.as_ref(), &source, proof)).await?;
            claims
                .observe(latest.as_ref(), &source, cursor)
                .map_err(unknown)?;
            after = sequence;
            if sequence == 1 {
                initial = Some(source.clone());
            }
            latest = Some(source);
        }
    }
}
const TOKEN_COST: &str = "SELECT CAST(length(CAST(claim_token AS BLOB)) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const TOKEN: &str = "SELECT CASE WHEN length(CAST(claim_token AS BLOB))<=512 AND json_type(data,'$.claim_token')='text' AND json_extract(data,'$.claim_token')=claim_token AND json_extract(data,'$.status')=status THEN claim_token END FROM runs WHERE session_id=$1 AND id=$2";
const JOURNAL: &str = "WITH candidates AS (SELECT sequence,data_json,CAST(length(CAST(data_json AS BLOB))+8 AS BIGINT) AS bytes FROM native_run_issuances WHERE session_id=$1 AND run_id=$2 AND sequence>$3 ORDER BY sequence LIMIT 16), bounded AS (SELECT *,SUM(bytes) OVER(ORDER BY sequence) AS used FROM candidates) SELECT sequence,bytes,CASE WHEN used<=$4 AND length(CAST(data_json AS BLOB))<=2048 THEN data_json END AS record FROM bounded ORDER BY sequence";

#[cfg(test)]
mod report_tests;
