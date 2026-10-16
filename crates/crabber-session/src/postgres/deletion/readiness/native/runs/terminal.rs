//! Exact terminal domains share one private-fields projection and allowance.
mod approval;
mod waiting;
use crate::attempt::terminal::{self, TerminalFacts};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{EventCursor, RunId, RunStatus, SessionId, Usage};
use serde::Deserialize;
use sqlx::{Postgres, Row, Transaction};
#[derive(Deserialize)]
#[serde(deny_unknown_fields)]
struct Fields {
    status: RunStatus,
    error: Option<String>,
    usage: Usage,
}
pub(super) async fn verify(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<bool> {
    let attempted = attempt(tx, session, run, status, proof).await?;
    let approved = approval::verify(tx, session, run, status, proof).await?;
    let cancelled = waiting::verify(tx, session, run, status, proof).await?;
    Ok(attempted || approved || cancelled)
}
async fn attempt(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<bool> {
    let Some((current, _)) =
        super::super::super::super::super::attempt::load_host_attempt_with_proof(
            tx, run, None, proof,
        )
        .await?
    else {
        return Ok(false);
    };
    let event = super::super::source::latest(
        tx,
        session,
        run,
        &current.snapshot.target.id.0,
        super::super::source::Kind::Attempt,
        proof,
    )
    .await?;
    if !terminal::candidate(&event) {
        return Ok(false);
    }
    let operation = super::super::source::operation(&event)?;
    let receipt = super::super::super::super::super::attempt::host_receipt_with_proof(
        tx, session, &operation, proof,
    )
    .await?
    .ok_or_else(|| unknown("original terminal receipt"))?;
    if receipt.attempt != current.snapshot {
        return Err(unknown("terminal current attempt"));
    }
    let saved = terminal::settlement(&receipt, &event).map_err(unknown)?;
    let cursor = saved
        .cursor
        .ok_or_else(|| unknown("terminal source cursor"))?;
    source_cursor(tx, session, run, receipt.event_cursor, cursor).await?;
    let original = super::super::super::super::super::read_proof::event_with_limit(
        tx, session, cursor, false, 32768, proof,
    )
    .await?;
    let fields = fields(tx, session, run, status, proof).await?;
    terminal::verify_scope(
        TerminalFacts {
            session,
            run,
            status,
            error: fields.error.as_deref(),
            usage: &fields.usage,
        },
        &receipt,
        &event,
        &original,
    )
    .map_err(unknown)?;
    Ok(true)
}
async fn fields(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    proof: &mut Proof,
) -> Result<Fields> {
    super::super::super::super::super::read_proof::charge(
        tx,
        sqlx::query_scalar(COST).bind(&session.0).bind(&run.0),
        proof,
    )
    .await?;
    let row = sqlx::query(FIELDS)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let raw = row
        .try_get::<Option<String>, _>("fields")
        .map_err(unknown)?
        .ok_or_else(|| unknown("terminal field bound"))?;
    let fields: Fields = serde_json::from_str(&raw).map_err(unknown)?;
    if fields.status != status {
        return Err(unknown("terminal state projection"));
    }
    Ok(fields)
}
const COST: &str = r"SELECT CAST(COALESCE(octet_length((data->'error')::text),0)+COALESCE(octet_length((data->'usage')::text),0)+COALESCE(octet_length(data->>'status'),0) AS BIGINT) FROM runs WHERE session_id=$1 AND id=$2";
const FIELDS: &str = r"SELECT CASE WHEN jsonb_typeof(data->'error') IN ('null','string') AND COALESCE(octet_length(data->>'error'),0)<=65536 AND octet_length((jsonb_build_object('status',data->'status','error',data->'error','usage',data->'usage'))::text)<=524288 THEN (jsonb_build_object('status',data->'status','error',data->'error','usage',data->'usage'))::text END AS fields FROM runs WHERE session_id=$1 AND id=$2";

async fn source_cursor(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    before: EventCursor,
    expected: EventCursor,
) -> Result<()> {
    let cursor: Option<i64> = sqlx::query_scalar(SETTLEMENT)
        .bind(&session.0)
        .bind(&run.0)
        .bind(i64::try_from(before.0).map_err(unknown)?)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    if cursor != Some(i64::try_from(expected.0).map_err(unknown)?) {
        return Err(unknown("independent latest terminal source"));
    }
    Ok(())
}
const SETTLEMENT: &str = r#"WITH decoded AS (SELECT seq,CASE WHEN data->>'encoding'='crabber.event.json-text.v1' THEN CASE WHEN split_part(data->>'record', ',"payload":',1) ~ ',"kind":"run_settled"$' THEN (data->>'record')::json ELSE '{}'::json END ELSE data::json END AS d FROM events WHERE session_id=$1 AND run_id=$2 AND seq<$3) SELECT seq FROM decoded WHERE d->>'kind'='run_settled' ORDER BY seq DESC LIMIT 1"#;

#[cfg(test)]
mod tests;
