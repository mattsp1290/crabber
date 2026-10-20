//! Narrow exact original abandonment domain, with a single caller proof budget.
use crate::abandonment::history::{self, Commit, Event, RunView};
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crate::transaction::Transaction;
use crabber_core::{EventCursor, RunId, SessionId};
use sqlx::{Row, Sqlite};
/// False is explicitly unproven; it never certifies cleanup or ownership.
pub(super) async fn verify(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: &RunId,
    proof: &mut Proof,
) -> Result<bool> {
    let record = super::admission_execution::load_record_with_proof(tx, run, proof).await?;
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(COST).bind(&session.0).bind(&run.0),
        proof,
    )
    .await?;
    let row = sqlx::query(PROJECTION)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?;
    let Some(row) = row else {
        return Ok(false);
    };
    if !row.try_get::<bool, _>("coherent").map_err(unknown)? {
        return Err(unknown("current Run projection"));
    }
    let parse = |key| -> Result<String> {
        row.try_get::<Option<String>, _>(key)
            .map_err(unknown)?
            .ok_or_else(|| unknown("abandonment projection bound"))
    };
    let current: RunView = serde_json::from_str(&parse("current_run")?).map_err(unknown)?;
    let commit: Commit = serde_json::from_str(&parse("commit_record")?).map_err(unknown)?;
    let event: Event = serde_json::from_str(&parse("source_event")?).map_err(unknown)?;
    let source_seq = row
        .try_get::<Option<i64>, _>("source_seq")
        .map_err(unknown)?
        .ok_or_else(|| unknown("original terminal source"))?;
    if source_seq != row.try_get::<i64, _>("committed_seq").map_err(unknown)?
        || current.session_id != *session
        || current.id != *run
    {
        return Err(unknown("original abandonment scope"));
    }
    let Some(marker) = &commit.no_effect_v1 else {
        if event.evidence.no_effect_v1.is_some() || commit.event.evidence.no_effect_v1.is_some() {
            return Err(unknown("removed no-effect source"));
        }
        return Ok(false);
    };
    let record = record.ok_or_else(|| unknown("original execution"))?;
    if record.receipt.session_id != *session
        || record.state != crate::AdmissionExecutionState::Unstarted
    {
        return Err(unknown("unstarted execution scope"));
    }
    let init = super::admission_journal::read_with_proof(tx, &record, proof)
        .await?
        .ok_or_else(|| unknown("unstarted source cohort"))?;
    verify_input(tx, &current, &record, proof).await?;
    let original = super::admission_journal::issuance_with_proof(
        tx,
        session,
        run,
        Some(marker.issuance.sequence),
        proof,
    )
    .await?;
    let revoked = super::admission_journal::issuance_with_proof(
        tx,
        session,
        run,
        Some(
            marker
                .issuance
                .sequence
                .checked_add(1)
                .ok_or_else(|| unknown("issuer overflow"))?,
        ),
        proof,
    )
    .await?;
    let terminal =
        super::admission_journal::issuance_with_proof(tx, session, run, None, proof).await?;
    history::verify(
        &current,
        &commit,
        &event,
        EventCursor(u64::try_from(source_seq).map_err(unknown)?),
        &init,
        [&original, &revoked, &terminal],
    )
    .map_err(unknown)
}
async fn verify_input(
    tx: &mut Transaction<'_, Sqlite>,
    current: &RunView,
    record: &crate::AdmissionExecutionRecord,
    proof: &mut Proof,
) -> Result<()> {
    let session = &current.session_id;
    let run = &current.id;
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(RECEIPT_COST)
            .bind(&session.0)
            .bind(record.key.as_str()),
        proof,
    )
    .await?;
    let original: Option<String> = sqlx::query_scalar(RECEIPT)
        .bind(&session.0)
        .bind(record.key.as_str())
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    let original: crabber_core::AdmissionReceipt =
        serde_json::from_str(&original.ok_or_else(|| unknown("original admission receipt"))?)
            .map_err(unknown)?;
    if original != record.receipt {
        return Err(unknown("original admission identity"));
    }
    let count = record
        .capsule
        .input_envelope
        .as_ref()
        .map_or(1, |input| input.messages().len());
    let messages =
        super::execution_binding::original_messages_with_proof(tx, session, run, count, proof)
            .await?;
    record
        .validate_messages_scope(
            session,
            run,
            &current.config_hash,
            &current.plan_fingerprint,
            &current.owner,
            &messages,
        )
        .map_err(unknown)?;
    let observed:i64=sqlx::query_scalar("SELECT count(*) FROM (SELECT 1 FROM messages WHERE session_id=$1 AND run_id=$2 LIMIT $3) originals")
        .bind(&session.0).bind(&run.0).bind(i64::try_from(count+1).map_err(unknown)?).fetch_one(&mut **tx).await.map_err(unknown)?;
    let dirty:bool=sqlx::query_scalar("SELECT EXISTS(SELECT 1 FROM tool_calls WHERE run_id=$1) OR EXISTS(SELECT 1 FROM execution_attempts WHERE run_id=$1) OR EXISTS(SELECT 1 FROM tool_phases WHERE run_id=$1) OR EXISTS(SELECT 1 FROM epochs WHERE run_id=$1 AND id<>$2)").bind(&run.0).bind(&current.epoch_id.0).fetch_one(&mut **tx).await.map_err(unknown)?;
    if observed != i64::try_from(count).map_err(unknown)? || dirty {
        return Err(unknown("contradictory no-effect history"));
    }
    Ok(())
}
const COST: &str = r"SELECT CAST(length(CAST(r.data AS BLOB))+length(CAST(a.data AS BLOB))+COALESCE(length(CAST(e.data AS BLOB)),0) AS BIGINT) FROM runs r JOIN abandonment_commits a ON a.run_id=r.id LEFT JOIN events e ON e.seq=(SELECT seq FROM events e0 WHERE e0.session_id=r.session_id AND e0.run_id=r.id AND json_extract(e0.data,'$.kind')='run_settled' ORDER BY seq DESC LIMIT 1) WHERE r.session_id=$1 AND r.id=$2";
const PROJECTION: &str = include_str!("abandon_history.sql");
const RECEIPT_COST: &str = r"SELECT CAST(length(CAST(data AS BLOB)) AS BIGINT) FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";
const RECEIPT: &str = r"SELECT CASE WHEN length(CAST(data AS BLOB))<=8192 THEN data END FROM admission_receipts WHERE session_id=$1 AND admission_key=$2";

#[cfg(test)]
#[path = "abandon_history/tests.rs"]
mod tests;
