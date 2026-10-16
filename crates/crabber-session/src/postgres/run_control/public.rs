//! Latest-Run control metadata with the same immutable acceptance/progress checks.
use super::reads;
use crate::public_session::{
    budget::Budget,
    v2::proof::{Failure, Proof, unknown},
};
use crate::{PublicSessionLimitV2, RunControlSummary};
use crabber_core::{RunId, RunStatus, SessionId};
use sqlx::{Postgres, Row, Transaction};
pub(in crate::postgres) async fn controls(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: &RunId,
    status: RunStatus,
    limit: u16,
    safe: &mut Budget,
    proof: &mut Proof,
) -> Result<Vec<RunControlSummary>, Failure> {
    let bytes: Option<i64> = sqlx::query_scalar(BIND_COST)
        .bind(&run.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(unknown)?
        .flatten();
    proof.charge(
        usize::try_from(bytes.ok_or_else(|| unknown("control binding"))?).map_err(unknown)?,
    )?;
    let rows = sqlx::query(COST)
        .bind(&session.0)
        .bind(&run.0)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    if rows.len() > 64 {
        return Err(unknown("control capacity"));
    }
    for row in &rows {
        proof.charge(
            usize::try_from(row.try_get::<i64, _>("proof_bytes").map_err(unknown)?)
                .map_err(unknown)?,
        )?;
        safe.charge(
            usize::try_from(row.try_get::<i64, _>("safe_bytes").map_err(unknown)?)
                .map_err(unknown)?,
        )?;
    }
    let binding = reads::binding(tx, session, run).await.map_err(unknown)?;
    let controls = reads::summaries(tx, &binding.target)
        .await
        .map_err(unknown)?;
    for control in &controls {
        crate::run_control::verify_terminal(control, status).map_err(unknown)?;
    }
    if controls.len() > usize::from(limit) {
        return Err(Failure::Limited(PublicSessionLimitV2::RunControls));
    }
    Ok(controls)
}
const BIND_COST: &str = r"SELECT CAST(octet_length(session_id)+octet_length(data->>'config_hash')+octet_length(data->>'plan_fingerprint') AS BIGINT) FROM runs WHERE id=$1";
const COST: &str = r"SELECT CAST(octet_length(q.summary_json) AS BIGINT) AS safe_bytes,
 CAST(octet_length(q.summary_json)+COALESCE(octet_length(p.summary_json),0)+octet_length(q.operation_id)+COALESCE(octet_length(o.data_json),0)+COALESCE(octet_length(o.semantic_digest),0)+11+COALESCE(octet_length(a.data::text),0)+COALESCE(octet_length(l.data::text),0) AS BIGINT) AS proof_bytes
 FROM (SELECT * FROM run_controls WHERE session_id=$1 AND run_id=$2 ORDER BY sequence LIMIT 65) q
 LEFT JOIN run_control_progress p ON p.session_id=q.session_id AND p.operation_id=q.operation_id AND p.event_seq=(SELECT MAX(x.event_seq) FROM run_control_progress x WHERE x.session_id=q.session_id AND x.operation_id=q.operation_id)
 LEFT JOIN operation_receipts o ON o.session_id=q.session_id AND o.operation_id=q.operation_id
 LEFT JOIN events a ON a.session_id=o.session_id AND a.seq=o.event_seq
 LEFT JOIN events l ON l.session_id=p.session_id AND l.seq=p.event_seq";
