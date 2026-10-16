//! Page every scoped record; never trust a mutable Released/barrier predicate.
use super::{Error, Row, Sqlite, ToolContinuationRecord, Transaction, read, unknown};
use crate::public_session::v2::proof::{Proof, ToolReadError};
use crabber_core::{Run, RunId, SessionId};
pub(in super::super) struct Records {
    pub latest: Option<ToolContinuationRecord>,
    pub active: Option<ToolContinuationRecord>,
}
pub(super) async fn run(tx: &mut Transaction<'_, Sqlite>, run: &Run) -> Result<Records, Error> {
    scan(tx, &run.session_id, Some(&run.id)).await
}
pub(in super::super) async fn pending(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
) -> Result<bool, Error> {
    scan(tx, session, None)
        .await
        .map(|records| records.active.is_some())
}
pub(super) async fn scan(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: Option<&RunId>,
) -> Result<Records, Error> {
    scan_with_proof(tx, session, run, &mut Proof::legacy())
        .await
        .map_err(|error| error.into_domain_or(Error::UnknownStoreOutcome))
}
pub(in super::super) async fn scan_with_proof(
    tx: &mut Transaction<'_, Sqlite>,
    session: &SessionId,
    run: Option<&RunId>,
    proof: &mut Proof,
) -> Result<Records, ToolReadError> {
    let mut records = Records {
        latest: None,
        active: None,
    };
    let mut after: Option<String> = None;
    let mut count = 0_usize;
    loop {
        super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE(SUM(bytes),0) AS BIGINT) FROM (SELECT length(CAST(id AS BLOB))+length(CAST(run_id AS BLOB)) AS bytes FROM tool_continuations WHERE session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR run_id=$2) AND (CAST($3 AS TEXT) IS NULL OR id COLLATE BINARY>$3) ORDER BY id COLLATE BINARY LIMIT 16) bounded_ids").bind(&session.0).bind(run.map(|r|r.0.as_str())).bind(&after),proof).await.map_err(ToolReadError::Budget)?;
        let rows = sqlx::query("SELECT CASE WHEN length(CAST(id AS BLOB))<=128 THEN id END AS id,CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END AS run_id FROM tool_continuations WHERE session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR run_id=$2) AND (CAST($3 AS TEXT) IS NULL OR id COLLATE BINARY>$3) ORDER BY id COLLATE BINARY LIMIT 16")
            .bind(&session.0).bind(run.map(|run| run.0.as_str())).bind(&after).fetch_all(&mut **tx).await.map_err(unknown)?;
        if rows.is_empty() {
            break;
        }
        for row in rows {
            let id = row
                .get::<Option<String>, _>("id")
                .ok_or(Error::UnknownStoreOutcome)?;
            let run_id = RunId(
                row.get::<Option<String>, _>("run_id")
                    .ok_or(Error::UnknownStoreOutcome)?,
            );
            let record = read::load_with_proof(
                tx,
                session,
                &run_id,
                Some(&crabber_core::ContinuationId(id.clone())),
                proof,
            )
            .await?
            .ok_or(Error::UnknownStoreOutcome)?;
            count = count.checked_add(1).ok_or(Error::UnknownStoreOutcome)?;
            if record.state != crate::ToolContinuationState::Released {
                if records.active.is_some() {
                    return Err(Error::UnknownStoreOutcome.into());
                }
                records.active = Some(record.clone());
            }
            if records.latest.as_ref().is_none_or(|old| {
                old.batch.continuation.generation < record.batch.continuation.generation
            }) {
                records.latest = Some(record);
            }
            after = Some(id);
        }
    }
    let carrier = super::current::CARRIER;
    let query = format!(
        "SELECT COUNT(*) FROM events e WHERE e.session_id=$1 AND (CAST($2 AS TEXT) IS NULL OR e.run_id=$2) AND json_extract({carrier},'$.payload.kind')='tool_continuation' AND json_extract({carrier},'$.payload.outcome.operation')='suspended'"
    );
    let origins: i64 = sqlx::query_scalar(&query)
        .bind(&session.0)
        .bind(run.map(|run| run.0.as_str()))
        .fetch_one(&mut **tx)
        .await
        .map_err(unknown)?;
    if usize::try_from(origins).ok() != Some(count) {
        return Err(Error::UnknownStoreOutcome.into());
    }
    super::super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(length(CAST(run_id AS BLOB))+length(CAST(continuation_id AS BLOB)) AS BIGINT) FROM tool_continuation_barriers WHERE session_id=$1").bind(&session.0),proof).await.map_err(ToolReadError::Budget)?;
    let barrier: Option<(Option<String>, Option<String>)> = sqlx::query_as("SELECT CASE WHEN length(CAST(run_id AS BLOB))<=128 THEN run_id END,CASE WHEN length(CAST(continuation_id AS BLOB))<=128 THEN continuation_id END FROM tool_continuation_barriers WHERE session_id=$1")
        .bind(&session.0).fetch_optional(&mut **tx).await.map_err(unknown)?;
    if let Some((owner, id)) = barrier {
        let owner = owner.ok_or(Error::UnknownStoreOutcome)?;
        let id = id.ok_or(Error::UnknownStoreOutcome)?;
        if run.is_none_or(|run| run.0 == owner)
            && records
                .active
                .as_ref()
                .is_none_or(|record| record.batch.continuation.id.0 != id)
        {
            return Err(Error::UnknownStoreOutcome.into());
        }
    }
    Ok(records)
}
