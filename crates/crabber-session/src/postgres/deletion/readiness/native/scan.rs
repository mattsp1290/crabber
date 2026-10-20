//! Finite keyset routing pages; actual carriers are charged before decoding.
use crate::postgres::transaction::Transaction;
use crate::public_session::v2::proof::Proof;
use crate::sql_deletion::readiness::{Result, unknown};
use crabber_core::{RunId, SessionId};
use sqlx::{Postgres, Row};
#[derive(Clone, Copy)]
pub(super) enum Kind {
    Run,
    Attempt,
    Phase,
    Call,
}
pub(super) async fn page(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    run: Option<&RunId>,
    after: &str,
    kind: Kind,
    proof: &mut Proof,
) -> Result<Vec<String>> {
    let source = match kind {
        Kind::Run => "SELECT id FROM runs WHERE session_id=$1 AND id COLLATE \"C\">$2",
        Kind::Attempt => {
            "SELECT a.id FROM execution_attempts a JOIN runs r ON r.id=a.run_id WHERE r.session_id=$1 AND a.id COLLATE \"C\">$2 AND a.run_id=$3"
        }
        Kind::Phase => {
            "SELECT p.id FROM tool_phases p JOIN runs r ON r.id=p.run_id WHERE r.session_id=$1 AND p.id COLLATE \"C\">$2 AND p.run_id=$3"
        }
        Kind::Call => {
            "SELECT c.id FROM tool_calls c JOIN runs r ON r.id=c.run_id WHERE r.session_id=$1 AND c.id COLLATE \"C\">$2 AND c.run_id=$3"
        }
    };
    let max = if matches!(kind, Kind::Call) { 512 } else { 128 };
    let query = format!(
        "WITH candidates AS (SELECT id FROM ({source}) routed ORDER BY id COLLATE \"C\" LIMIT 32), bounded AS (SELECT id,CAST(octet_length(id) AS BIGINT) AS bytes,SUM(octet_length(id)) OVER(ORDER BY id COLLATE \"C\") AS used FROM candidates) SELECT bytes,CASE WHEN used<=${budget} AND bytes<={max} THEN id END AS id FROM bounded ORDER BY id COLLATE \"C\"",
        budget = if run.is_some() { 4 } else { 3 }
    );
    let mut query = sqlx::query(&query).bind(&session.0).bind(after);
    if let Some(run) = run {
        query = query.bind(&run.0);
    }
    let rows = query
        .bind(i64::try_from(proof.budget().remaining()).map_err(unknown)?)
        .fetch_all(&mut **tx)
        .await
        .map_err(unknown)?;
    let mut result = Vec::with_capacity(rows.len());
    for row in rows {
        let bytes =
            usize::try_from(row.try_get::<i64, _>("bytes").map_err(unknown)?).map_err(unknown)?;
        proof.charge(bytes)?;
        let id = row
            .try_get::<Option<String>, _>("id")
            .map_err(unknown)?
            .ok_or_else(|| unknown("routing bound"))?;
        if matches!(kind, Kind::Call) {
            crate::fork::history::validate_id(&id).map_err(unknown)?;
        } else {
            crate::attempt::validate_identifier(&id).map_err(unknown)?;
        }
        result.push(id);
    }
    Ok(result)
}
