//! Bounded original admission history; scalar preflight and CASE carriers share the caller Proof.
use super::{db, decode_record};
use crate::postgres::transaction::Transaction;
use crate::{StoreError, public_session::v2::proof::Proof};
use crabber_core::{ContextEpoch, Message, Run, Session, SessionId};
use sqlx::{Postgres, Row};
pub(super) async fn project(
    tx: &mut Transaction<'_, Postgres>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Vec<Message>, StoreError> {
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(COALESCE((SELECT SUM(octet_length(data::text)) FROM (SELECT data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1) last_run),0)+COALESCE((SELECT SUM(octet_length(e.data::text)) FROM epochs e WHERE e.id=(SELECT data->>'epoch_id' FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1)),0)+COALESCE((SELECT SUM(octet_length(snapshot_record)) FROM messages WHERE session_id=$1),0) AS BIGINT)").bind(&id.0),proof).await.map_err(|error|proof.admission_failure(&error,StoreError::Conflict))?;
    let exists = sqlx::query("SELECT 1 FROM sessions WHERE id=$1")
        .bind(&id.0)
        .fetch_optional(&mut **tx)
        .await
        .map_err(db)?;
    if exists.is_none() {
        return Err(StoreError::NotFound);
    }
    let epoch_id = {
        let row =
        sqlx::query("SELECT CASE WHEN octet_length(data::text)<=33554432 THEN data::text END AS data FROM runs WHERE session_id=$1 ORDER BY seq DESC LIMIT 1")
            .bind(&id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?;
        row.map(|row| read::<Run>(&row).map(|run| run.epoch_id))
            .transpose()?
    };
    let selected = if let Some(epoch_id) = epoch_id {
        let row = sqlx::query("SELECT CASE WHEN octet_length(data::text)<=33554432 THEN data::text END AS data FROM epochs WHERE id=$1")
            .bind(&epoch_id.0)
            .fetch_optional(&mut **tx)
            .await
            .map_err(db)?
            .ok_or(StoreError::NotFound)?;
        let value: ContextEpoch = read(&row)?;
        if value.session_id != *id {
            return Err(StoreError::Validation(
                "epoch belongs to another session".into(),
            ));
        }
        Some(value)
    } else {
        None
    };
    let all = bounded_messages(tx, id).await?;
    crate::context_projection::project(all, selected)
}

async fn bounded_messages(
    tx: &mut Transaction<'_, Postgres>,
    id: &SessionId,
) -> Result<Vec<Message>, StoreError> {
    let rows=sqlx::query("SELECT CASE WHEN octet_length(snapshot_record)<=33554432 THEN snapshot_record END AS record FROM messages WHERE session_id=$1 ORDER BY seq")
        .bind(&id.0).fetch_all(&mut **tx).await.map_err(db)?;
    rows.into_iter()
        .map(|row| {
            decode_record(
                &row.try_get::<Option<String>, _>("record")
                    .map_err(db)?
                    .ok_or(StoreError::Conflict)?,
            )
        })
        .collect()
}
pub(super) async fn session(
    tx: &mut Transaction<'_, Postgres>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Option<(Session, i64)>, StoreError> {
    super::read_proof::charge(
        tx,
        sqlx::query_scalar(
            "SELECT CAST(octet_length(data::text)+8 AS BIGINT) FROM sessions WHERE id=$1",
        )
        .bind(&id.0),
        proof,
    )
    .await
    .map_err(|error| proof.admission_failure(&error, StoreError::Conflict))?;
    let row=sqlx::query("SELECT CASE WHEN octet_length(data::text)<=33554432 THEN data::text END AS data,public_revision FROM sessions WHERE id=$1").bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    row.map(|row| {
        let text = row
            .try_get::<Option<String>, _>("data")
            .map_err(db)?
            .ok_or(StoreError::Conflict)?;
        let session: Session = super::decode_record(&text)?;
        Ok((session, row.try_get("public_revision").map_err(db)?))
    })
    .transpose()
}

pub(super) async fn previous_epoch(
    tx: &mut Transaction<'_, Postgres>,
    id: &SessionId,
    proof: &mut Proof,
) -> Result<Option<ContextEpoch>, StoreError> {
    super::read_proof::charge(tx,sqlx::query_scalar("SELECT CAST(octet_length(data) AS BIGINT) FROM (SELECT e.data::text AS data FROM runs r JOIN epochs e ON e.id=r.data->>'epoch_id' WHERE r.session_id=$1 ORDER BY r.seq DESC LIMIT 1) selected").bind(&id.0),proof).await.map_err(|error|proof.admission_failure(&error,StoreError::Conflict))?;
    let value:Option<Option<String>>=sqlx::query_scalar("SELECT CASE WHEN octet_length(data)<=33554432 THEN data END FROM (SELECT e.data::text AS data FROM runs r JOIN epochs e ON e.id=r.data->>'epoch_id' WHERE r.session_id=$1 ORDER BY r.seq DESC LIMIT 1) selected").bind(&id.0).fetch_optional(&mut **tx).await.map_err(db)?;
    value
        .map(|v| {
            serde_json::from_str(&v.ok_or(StoreError::Conflict)?).map_err(|_| StoreError::Conflict)
        })
        .transpose()
}

fn read<T: serde::de::DeserializeOwned>(row: &sqlx::postgres::PgRow) -> Result<T, StoreError> {
    let text = row
        .try_get::<Option<String>, _>("data")
        .map_err(db)?
        .ok_or(StoreError::Conflict)?;
    serde_json::from_str(&text).map_err(|_| StoreError::Conflict)
}

pub(super) async fn pending(
    tx: &mut Transaction<'_, Postgres>,
    session: &SessionId,
    proof: &mut Proof,
) -> Result<bool, StoreError> {
    Box::pin(super::approval::verify_session_with_proof(
        tx, session, proof,
    ))
    .await
    .map_err(|error| {
        proof.admission_read(
            error,
            |_| StoreError::Validation("approval integrity failure".into()),
            StoreError::Validation("approval integrity failure".into()),
        )
    })?;
    let pending = Box::pin(super::tool_continuation::pending_in_session_with_proof(
        tx, session, proof,
    ))
    .await
    .map_err(|error| {
        proof.admission_read(
            error,
            |_| StoreError::Validation("invalid tool continuation evidence".into()),
            StoreError::Validation("invalid tool continuation evidence".into()),
        )
    })?;
    let approval: bool = sqlx::query_scalar(
        "SELECT EXISTS(SELECT 1 FROM continuation_barriers WHERE session_id=$1)",
    )
    .bind(&session.0)
    .fetch_one(&mut **tx)
    .await
    .map_err(db)?;
    Ok(approval || pending)
}
