//! FIFO public queue projection; never fetches QueueInput or preparation capsules.
use super::reads;
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, unknown},
};
use crate::{PublicSessionLimitV2, QueueReceipt, QueueState, QueueSummary};
use crabber_core::{QueueId, SessionId};
use sqlx::{Row, Sqlite, Transaction};
pub(in crate::sqlite) async fn pending(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<Vec<QueueSummary>, Failure> {
    let mismatch: bool = sqlx::query_scalar(SCOPE)
        .bind(&session.0)
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if mismatch {
        return Err(unknown("queue membership"));
    }
    let mut result = Vec::new();
    let mut after = 0_i64;
    loop {
        let rows = sqlx::query(IDS)
            .bind(&session.0)
            .bind(after)
            .fetch_all(&mut **tx)
            .await
            .map_err(unknown)?;
        if rows.is_empty() {
            break;
        }
        for row in &rows {
            proof.charge(
                usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?)
                    .map_err(unknown)?,
            )?;
        }
        for row in rows {
            let id = row
                .try_get::<Option<String>, _>("id")
                .map_err(unknown)?
                .ok_or_else(|| unknown("queue routing"))?;
            let sequence: i64 = row.try_get("sequence").map_err(unknown)?;
            if sequence <= after {
                return Err(unknown("queue order"));
            }
            let bytes: i64 = sqlx::query_scalar(COST)
                .bind(&session.0)
                .bind(&id)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?;
            proof.charge(usize::try_from(bytes).map_err(unknown)?)?;
            let query = reads::SUMMARY.replacen("SELECT ", "SELECT CASE WHEN length(CAST(json_extract(a.data_json,'$.receipt') AS BLOB))<=16384 THEN json_extract(a.data_json,'$.receipt') END AS immutable_receipt, ", 1);
            let row = sqlx::query(&query)
                .bind(&session.0)
                .bind(&id)
                .fetch_one(&mut **tx)
                .await
                .map_err(unknown)?;
            let summary = reads::summary(&row, session).map_err(unknown)?;
            let source: QueueReceipt = serde_json::from_str(
                &row.try_get::<Option<String>, _>("immutable_receipt")
                    .map_err(unknown)?
                    .ok_or_else(|| unknown("queue immutable source"))?,
            )
            .map_err(unknown)?;
            if source.summary != summary
                || source.operation_id.0
                    != row
                        .try_get::<Option<String>, _>("latest_operation")
                        .map_err(unknown)?
                        .ok_or_else(|| unknown("queue source routing"))?
                || source.summary.queue_id != QueueId(id)
                || i64::try_from(source.summary.sequence).ok() != Some(sequence)
            {
                return Err(unknown("queue immutable summary"));
            }
            if matches!(summary.state, QueueState::Queued | QueueState::Claimed) {
                if result.len() >= usize::from(limit) {
                    return Err(Failure::Limited(PublicSessionLimitV2::QueueEntries));
                }
                safe.measure(&summary)?;
                result.push(summary);
            }
            after = sequence;
        }
    }
    Ok(result)
}
const IDS: &str = r"SELECT CAST(length(CAST(q.queue_id AS BLOB)) AS BIGINT) AS bytes,CASE WHEN length(CAST(q.queue_id AS BLOB))<=128 THEN q.queue_id END AS id,q.sequence FROM future_queue q WHERE q.session_id=$1 AND q.sequence>$2 ORDER BY q.sequence LIMIT 32";
const SCOPE: &str = r"SELECT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=$1 AND NOT EXISTS(SELECT 1 FROM future_queue q WHERE q.session_id=a.session_id AND q.queue_id=a.queue_id) UNION ALL SELECT 1 FROM future_queue q WHERE q.session_id=$1 AND NOT EXISTS(SELECT 1 FROM future_queue_audit a WHERE a.session_id=q.session_id AND a.queue_id=q.queue_id))";
const COST: &str = r"SELECT CAST(length(CAST(q.summary_json AS BLOB))+COALESCE(length(CAST(r.data_json AS BLOB)),0)+COALESCE(length(CAST(CAST(ar.data AS TEXT) AS BLOB)),0)+length(CAST(q.queue_id AS BLOB))+length(CAST(q.state AS BLOB))+COALESCE(length(CAST(a.operation_id AS BLOB)),0)+COALESCE(length(CAST(json_extract(a.data_json,'$.receipt') AS BLOB)),0) AS BIGINT) AS bytes FROM future_queue q LEFT JOIN future_queue_audit a ON a.session_id=q.session_id AND a.queue_id=q.queue_id AND a.revision=(SELECT MAX(x.revision) FROM future_queue_audit x WHERE x.session_id=q.session_id AND x.queue_id=q.queue_id) LEFT JOIN operation_receipts r ON r.session_id=a.session_id AND r.operation_id=a.operation_id AND r.kind='future_queue' AND r.event_seq IS NULL LEFT JOIN admission_receipts ar ON ar.session_id=q.session_id AND ar.admission_key=q.admission_key WHERE q.session_id=$1 AND q.queue_id=$2";
